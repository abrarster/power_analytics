import dagster
import dagster_dbt
import pandas as pd
import requests
import warnings
from datetime import date, datetime
from time import sleep
from entsoe.mappings import Area
from entsoe.exceptions import NoMatchingDataError, InvalidPSRTypeError
from eupower_core.scrapes import entsoe
from eupower_core.dagster_resources import (
    FilesystemResource,
    MySqlResource,
    PostgresResource,
)
from dagster_dbt import DbtCliResource, dbt_assets
from .constants import ASSET_GROUP
from .mapping_tables import entsoe_areas, entsoe_psr_types

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)


country_codes = {
    "generation_by_fuel": (
        "FR",
        "DE_LU",
        "DE_50HZ",
        "DE_TENNET",
        "DE_AMPRION",
        "DE_TRANSNET",
        "ES",
        "IT",
        "IT_CALA",
        "IT_SICI",
        "IT_CNOR",
        "IT_CSUD",
        "IT_NORD",
        "IT_SUD",
        "IT_SARD",
        "AT",
        "BE",
        "NL",
        "PT",
        "CH",
        "SI",
        "HU",
        "HR",
        "PL",
        "RO",
        "CZ",
    ),
    "generation_by_unit": (
        "FR",
        "ES",
        "DE_50HZ",
        "DE_TENNET",
        "DE_AMPRION",
        "DE_TRANSNET",
        "IT",
        "BE",
        "NL",
        "PT",
        "AT",
        "PL",
        "HU",
        "RO",
        "CZ",
    ),
    "demand": (
        "FR",
        "DE_LU",
        "ES",
        "PT",
        "DE_TENNET",
        "DE_TRANSNET",
        "DE_AMPRION",
        "DE_50HZ",
        "AT",
        "BE",
        "NL",
        "CH",
        "SI",
        "HU",
        "HR",
        "PL",
        "RO",
        "CZ",
        "IT",
        "IT_CALA",
        "IT_SICI",
        "IT_CNOR",
        "IT_CSUD",
        "IT_NORD",
        "IT_SUD",
        "IT_SARD",
    ),
    "crossborder_flows": (
        "FR",
        "DE_LU",
        "ES",
        "IT",
        "IT_CALA",
        "IT_SICI",
        "IT_CNOR",
        "IT_CSUD",
        "IT_NORD",
        "IT_SUD",
        "IT_SARD",
        "AT",
        "BE",
        "NL",
        "PT",
        "CH",
        "SI",
        "HU",
        "HR",
        "PL",
        "RO",
        "CZ",
    ),
    "da_prices": (
        "IT_CALA",
        "IT_SICI",
        "IT_CNOR",
        "IT_CSUD",
        "IT_NORD",
        "IT_SUD",
        "IT_SARD",
        "FR",
        "DE_LU",
        "CH",
        "AT",
        "SI",
        "GR",
        "ME",
    ),
}


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2023-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["generation_by_fuel"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
    kinds={"python", "file"},
)
def entsoe_generation_by_fuel_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    writer = fs.get_writer(
        f"entsoe/generation_by_fuel/{start_date.strftime('%Y%m%d')}/{region}"
    )
    writer.delete_data()
    output_path = writer.base_path
    scraper = entsoe.FileWritingEntsoeScraper(
        api_key=api_key, output_dir=output_path
    ).set_dates(start_date, end_date)
    for fuel_code in entsoe.PSRTYPE_MAPPINGS:
        if fuel_code[0] == "A":
            continue
        sleep(1)
        try:
            scraper.get_generation_by_fuel_type(region, fuel_code)
        except NoMatchingDataError:
            context.log.warning(
                f"No data for {region} and fuel code {fuel_code} on {start_date}"
            )
            continue
        except requests.exceptions.HTTPError:
            context.log.warning(
                f"HTTP error for {region} and fuel code {fuel_code} on {start_date}"
            )
            continue


@dagster.asset(
    deps=[entsoe_generation_by_fuel_raw],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2023-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["generation_by_fuel"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "postgres"},
    kinds={"python", "postgres"},
)
def entsoe_generation_by_fuel(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/generation_by_fuel/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    postgres_db = postgres.get_db_connection()
    stmt_create_table = """
        CREATE SCHEMA IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_generation_by_fuel (
            for_date VARCHAR(255),
            generation_mw FLOAT,
            bidding_zone VARCHAR(255),
            unit VARCHAR(255),
            psr_type VARCHAR(255),
            doc_type VARCHAR(255),
            flow_type VARCHAR(255),
            PRIMARY KEY (for_date, bidding_zone, psr_type, flow_type)
        )
        --END STATEMENT--
    """
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_generation_by_fuel")


@dagster.asset(
    deps=[entsoe_generation_by_fuel, entsoe_areas, entsoe_psr_types],
    group_name=ASSET_GROUP,
    tags={"storage": "postgres"},
    kinds={"python", "postgres"},
)
def fct_entsoe_generation_by_fuel(
    context: dagster.AssetExecutionContext, postgres: PostgresResource
):
    stmt = """
        CREATE SCHEMA IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.fct_entsoe_generation_by_fuel (
            bidding_zone_code VARCHAR(255),
            psr_type VARCHAR(10),
            bidding_zone VARCHAR(100),
            fuel VARCHAR(255),
            flow_type VARCHAR(255),
            for_date VARCHAR(255),
            generation_mw FLOAT,
            PRIMARY KEY (bidding_zone_code, psr_type, flow_type, for_date)
        )
        --END STATEMENT--

        TRUNCATE TABLE entsoe.fct_entsoe_generation_by_fuel
        --END STATEMENT--

        INSERT INTO entsoe.fct_entsoe_generation_by_fuel
        SELECT a.bidding_zone as bidding_zone_code,
               a.psr_type,
               b.name as bidding_zone,
               c.long_name as fuel,
               a.flow_type,
               a.for_date,
               CASE flow_type WHEN 'generation' THEN 1 ELSE -1 END * generation_mw as generation_mw
        FROM entsoe.entsoe_generation_by_fuel a
        INNER JOIN entsoe.entsoe_areas b on a.bidding_zone = b.code
        INNER JOIN entsoe.psr_types c on a.psr_type = c.fuel_code
        --END STATEMENT--
    """
    with postgres.get_db_connection() as db:
        db.execute_statements(stmt)


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2023-01-01"),
            "region": dagster.StaticPartitionsDefinition(country_codes["demand"]),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
    kinds={"python", "file"},
)
def entsoe_demand_raw(context: dagster.AssetExecutionContext, fs: FilesystemResource):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    writer = fs.get_writer(f"entsoe/demand/{start_date.strftime('%Y%m%d')}/{region}")
    writer.delete_data()
    output_path = writer.base_path
    scraper = entsoe.FileWritingEntsoeScraper(
        api_key=api_key, output_dir=output_path
    ).set_dates(start_date, end_date)
    try:
        scraper.get_load(region)
    except requests.exceptions.HTTPError:
        context.log.warning(f"HTTP error for {region} on {start_date}")
    except NoMatchingDataError:
        context.log.warning(f"No data for {region} on {start_date}")


@dagster.asset(
    deps=["entsoe_demand_raw"],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2023-01-01"),
            "region": dagster.StaticPartitionsDefinition(country_codes["demand"]),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "postgres"},
    kinds={"python", "postgres"},
)
def entsoe_demand(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/demand/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    postgres_db = postgres.get_db_connection()
    stmt_create_table = """
        CREATE SCHEMA IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_demand (
            for_date VARCHAR(255),
            load_mw FLOAT,
            mrid VARCHAR(255),
            business_type VARCHAR(255),
            object_aggregation VARCHAR(255),
            out_bidding_zone_mrid VARCHAR(255),
            quantity_measure_unit VARCHAR(255),
            curve_type VARCHAR(255),
            PRIMARY KEY (for_date, out_bidding_zone_mrid)
        )
        --END STATEMENT--
    """
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_demand")


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2022-01-01"),
            "region": dagster.StaticPartitionsDefinition(country_codes["da_prices"]),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
    kinds={"python", "file"},
)
def entsoe_da_prices_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    writer = fs.get_writer(f"entsoe/da_prices/{start_date.strftime('%Y%m%d')}/{region}")
    writer.delete_data()
    output_path = writer.base_path
    scraper = entsoe.FileWritingEntsoeScraper(
        api_key=api_key, output_dir=output_path
    ).set_dates(start_date, start_date)
    try:
        scraper.get_da_prices(region)
    except requests.exceptions.HTTPError:
        context.log.warning(f"HTTP error for {region} on {start_date}")
    except NoMatchingDataError:
        context.log.warning(f"No data for {region} on {start_date}")


@dagster.asset(
    deps=["entsoe_da_prices_raw"],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2022-01-01"),
            "region": dagster.StaticPartitionsDefinition(country_codes["da_prices"]),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "postgres"},
    kinds={"python", "postgres"},
)
def entsoe_da_prices(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/da_prices/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    postgres_db = postgres.get_db_connection()
    stmt_create_table = """
        CREATE SCHEMA IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_da_prices (
            for_date VARCHAR,
            price FLOAT,
            currency VARCHAR,
            unit VARCHAR,
            bidding_zone VARCHAR,
            resolution VARCHAR,
            PRIMARY KEY (for_date, bidding_zone, resolution)
        )
        --END STATEMENT--
    """
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_da_prices")


@dagster.asset(
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2022-01-01"),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
    kinds={"python", "file"},
)
def entsoe_production_units_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    as_of_date = pd.Timestamp(context.partition_time_window.start)
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    writer = fs.get_writer(f'entsoe/production_units/{as_of_date.strftime("%Y%m%d")}')
    writer.delete_data()
    scraper = entsoe.FileWritingEntsoeScraper(
        api_key=api_key, output_dir=writer.base_path
    ).set_dates(as_of_date, as_of_date)
    for area in Area:
        try:
            scraper.get_units(area.name, as_of_date)
        except NoMatchingDataError:
            context.log.warning(f"No data for {area.name}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                context.log.warning(f"HTTP 400 client error for {area.name}")
            else:
                raise


@dagster.asset(
    deps=["entsoe_production_units_raw"],
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2022-01-01"),
    group_name=ASSET_GROUP,
    tags={"storage": "postgres"},
    kinds={"python", "postgres"},
)
def entsoe_production_units(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    as_of_date = pd.Timestamp(context.partition_time_window.start)
    reader = fs.get_reader(f'entsoe/production_units/{as_of_date.strftime("%Y%m%d")}')
    parser = entsoe.EntsoeFileParser(reader.base_path)
    df = parser.parse_by_type("A95")
    df = df.drop_duplicates(subset=["resource_mRID"], keep="first").reset_index(
        drop=True
    )
    stmt_create_table = """
        CREATE SCHEMA IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_production_units (
            mrid VARCHAR(255) NOT NULL,
            business_type VARCHAR(255) NOT NULL,
            implementation_date TIMESTAMP NOT NULL,
            resource_name VARCHAR(255) NOT NULL,
            resource_mrid VARCHAR(255) NOT NULL,
            location VARCHAR(255) NOT NULL,
            bidding_zone VARCHAR(255) NOT NULL,
            provider_participant VARCHAR(255) NOT NULL,
            control_area_domain VARCHAR(255) NOT NULL,
            psr_type VARCHAR(255) NOT NULL,
            voltage_limit DOUBLE PRECISION NOT NULL,
            voltage_unit VARCHAR(255) NOT NULL,
            nominal_power DOUBLE PRECISION NOT NULL,
            power_unit VARCHAR(255) NOT NULL,
            unit_count DOUBLE PRECISION,
            total_unit_power DOUBLE PRECISION,
            unit_names JSONB,
            unit_details JSONB,
            PRIMARY KEY (resource_mrid, implementation_date)
        )
        --END STATEMENT--
    """
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe_incremental(df, "entsoe", "entsoe_production_units")


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["generation_by_unit"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
    kinds={"python", "file"},
)
def entsoe_generation_by_unit_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    writer = fs.get_writer(
        f"entsoe/generation_by_unit/{start_date.strftime('%Y%m%d')}/{region}"
    )
    writer.delete_data()
    output_path = writer.base_path
    scraper = entsoe.FileWritingEntsoeScraper(
        api_key=api_key, output_dir=output_path
    ).set_dates(start_date, end_date)
    for fuel_code in [
        "B01",
        "B02",
        "B03",
        "B04",
        "B05",
        "B06",
        "B10",
        "B11",
        "B12",
        "B14",
        "B18",
    ]:
        sleep(1)
        try:
            scraper.get_generation_by_unit(region, fuel_code)
        except (NoMatchingDataError, InvalidPSRTypeError):
            context.log.warning(
                f"No data for {region} and fuel code {fuel_code} on {start_date}"
            )
            continue
        except requests.exceptions.HTTPError:
            context.log.warning(
                f"HTTP error for {region} and fuel code {fuel_code} on {start_date}"
            )
            continue


@dagster.asset(
    deps=["entsoe_generation_by_unit_raw"],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["generation_by_unit"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "postgres"},
    kinds={"python", "postgres"},
)
def entsoe_generation_by_unit(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/generation_by_unit/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    postgres_db = postgres.get_db_connection()
    stmt_create_table = """
        CREATE SCHEMA IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_generation_by_unit (
            for_date VARCHAR(255),
            value FLOAT,
            unit VARCHAR(255),
            domain VARCHAR(255),
            psr_type VARCHAR(255),
            business_type VARCHAR(255),
            registered_resource VARCHAR(255),
            psr_name VARCHAR(255),
            psr_mrid VARCHAR(255),
            PRIMARY KEY (for_date, registered_resource, psr_mrid)
        )
        --END STATEMENT--
    """
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_generation_by_unit")


def create_entsoe_exchange_assets(
    exchange_type: str, partitions_def: dagster.MultiPartitionsDefinition
):
    """
    Factory function to create pairs of raw and processed exchange assets.

    Args:
        exchange_type: One of 'crossborder_flows', 'da_scheduled_exchange',
                      or 'da_total_scheduled_exchange'
        partitions_def: MultiPartitionsDefinition for date and region partitioning
    """
    path_mapping = {
        "crossborder_flows": "cross_border_flows",
        "da_scheduled_exchange": "da_scheduled_exchange",
        "da_total_scheduled_exchange": "da_total_scheduled_exchange",
        "dayahead_ntc": "dayahead_ntc",
    }

    scraper_method_mapping = {
        "crossborder_flows": "query_crossborder_flows",
        "da_scheduled_exchange": "query_da_scheduled_exchange",
        "da_total_scheduled_exchange": "query_da_total_scheduled_exchange",
        "dayahead_ntc": "query_dayahead_ntc",
    }

    raw_asset_name = f"entsoe_{exchange_type}_raw"
    processed_asset_name = f"entsoe_{exchange_type}"

    @dagster.asset(
        name=raw_asset_name,
        partitions_def=partitions_def,
        group_name=ASSET_GROUP,
        tags={"storage": "filesystem", "scrape_source": "entsoe"},
        kinds={"python", "file"},
    )
    def raw_asset(context: dagster.AssetExecutionContext, fs: FilesystemResource):
        api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
        for_date = context.partition_key.keys_by_dimension["date"]
        region = context.partition_key.keys_by_dimension["region"]

        start_date, end_date = _get_date_window(for_date)
        writer = fs.get_writer(
            f"entsoe/{path_mapping[exchange_type]}/{start_date.strftime('%Y%m%d')}/{region}"
        )
        writer.delete_data()
        output_path = writer.base_path
        scraper = entsoe.FileWritingEntsoeScraper(
            api_key=api_key, output_dir=output_path
        ).set_dates(start_date, end_date)
        try:
            getattr(scraper, scraper_method_mapping[exchange_type])(region, context.log)
        except requests.exceptions.HTTPError:
            context.log.warning(f"HTTP error for {region} on {start_date}")
        except NoMatchingDataError:
            context.log.warning(f"No data for {region} on {start_date}")

    @dagster.asset(
        name=processed_asset_name,
        deps=[raw_asset_name],
        partitions_def=partitions_def,
        group_name=ASSET_GROUP,
        tags={"storage": "postgres"},
        kinds={"python", "postgres"},
    )
    def processed_asset(
        context: dagster.AssetExecutionContext,
        fs: FilesystemResource,
        postgres: PostgresResource,
    ):
        for_date = context.partition_key.keys_by_dimension["date"]
        region = context.partition_key.keys_by_dimension["region"]
        folder_path = fs.get_writer(
            f"entsoe/{path_mapping[exchange_type]}/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
        ).base_path
        parser = entsoe.EntsoeFileParser(folder_path)
        df = parser.parse_files()
        postgres_db = postgres.get_db_connection()
        stmt_create_table = f"""
            CREATE SCHEMA IF NOT EXISTS entsoe
            --END STATEMENT--

            CREATE TABLE IF NOT EXISTS entsoe.entsoe_{exchange_type} (
                for_date VARCHAR(255),
                value FLOAT,
                unit VARCHAR(255),
                in_domain VARCHAR(255),
                out_domain VARCHAR(255),
                doc_type VARCHAR(255),
                PRIMARY KEY (for_date, in_domain, out_domain)
            )
            --END STATEMENT--
        """
        with postgres_db as db:
            db.execute_statements(stmt_create_table)
            db.write_dataframe(df, "entsoe", f"entsoe_{exchange_type}")

    return raw_asset, processed_asset


# Define the partitions once
exchange_partitions = dagster.MultiPartitionsDefinition(
    {
        "date": dagster.DailyPartitionsDefinition(start_date="2023-01-01"),
        "region": dagster.StaticPartitionsDefinition(
            country_codes["crossborder_flows"]
        ),
    }
)

# Create crossborder flows assets
entsoe_crossborder_flows_raw, entsoe_crossborder_flows = create_entsoe_exchange_assets(
    exchange_type="crossborder_flows", partitions_def=exchange_partitions
)

# Create day-ahead scheduled exchange assets
entsoe_da_scheduled_exchange_raw, entsoe_da_scheduled_exchange = (
    create_entsoe_exchange_assets(
        exchange_type="da_scheduled_exchange", partitions_def=exchange_partitions
    )
)

# Create day-ahead total scheduled exchange assets
entsoe_da_total_scheduled_exchange_raw, entsoe_da_total_scheduled_exchange = (
    create_entsoe_exchange_assets(
        exchange_type="da_total_scheduled_exchange", partitions_def=exchange_partitions
    )
)

# Create day-ahead NTC assets
entsoe_dayahead_ntc_raw, entsoe_dayahead_ntc = create_entsoe_exchange_assets(
    exchange_type="dayahead_ntc", partitions_def=exchange_partitions
)


def _get_date_window(
    dt: str,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_date = pd.Timestamp(dt, tz="UTC")
    end_date = pd.Timestamp(dt, tz="UTC") + pd.Timedelta(
        hours=23, minutes=59, seconds=59
    )
    return start_date, end_date


@dbt_assets(
    manifest="/Users/abrar/Python/power_analytics/dbt_pipelines/target/manifest.json",
    select="scrapes.entsoe",  # This selects all models that depend on entsoe sources
)
def entsoe_dbt_assets(context: dagster.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
