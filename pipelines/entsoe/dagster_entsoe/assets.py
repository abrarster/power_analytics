import dagster
import pandas as pd
import requests
from datetime import date, datetime
from time import sleep
from entsoe.exceptions import NoMatchingDataError, InvalidPSRTypeError
from eupower_core.scrapes import entsoe
from eupower_core.dagster_resources import FilesystemResource, MySqlResource

ASSET_GROUP = "entsoe"
country_codes = {
    "generation_by_fuel": ("FR", "DE_LU", "ES"),
    "generation_by_unit": (
        "FR",
        "ES",
        "DE_50HZ",
        "DE_TENNET",
        "DE_AMPRION",
        "DE_TRANSNET",
    ),
    "demand": ("FR", "DE_LU", "ES"),
    "crossborder_flows": ("FR", "DE_LU", "ES"),
}


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["generation_by_fuel"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
)
def entsoe_generation_by_fuel_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    output_path = fs.get_writer(
        f"entsoe/generation_by_fuel/{start_date.strftime('%Y%m%d')}/{region}"
    ).base_path
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
    deps=["entsoe_generation_by_fuel_raw"],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["generation_by_fuel"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "mysql"},
)
def entsoe_generation_by_fuel(
    context: dagster.AssetExecutionContext, fs: FilesystemResource, mysql: MySqlResource
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/generation_by_fuel/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    mysql_db = mysql.get_db_connection()
    stmt_create_table = """
        CREATE DATABASE IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_generation_by_fuel (
            for_date TIMESTAMP,
            generation_mw FLOAT,
            bidding_zone VARCHAR(255),
            unit VARCHAR(255),
            psr_type VARCHAR(255),
            doc_type VARCHAR(255),
            flow_type VARCHAR(255),
            CONSTRAINT pk_record PRIMARY KEY (for_date, bidding_zone, psr_type, flow_type)
        )
        --END STATEMENT--
    """
    with mysql_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_generation_by_fuel")


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(country_codes["demand"]),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
)
def entsoe_demand_raw(context: dagster.AssetExecutionContext, fs: FilesystemResource):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    output_path = fs.get_writer(
        f"entsoe/demand/{start_date.strftime('%Y%m%d')}/{region}"
    ).base_path
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
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(country_codes["demand"]),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "mysql"},
)
def entsoe_demand(
    context: dagster.AssetExecutionContext, fs: FilesystemResource, mysql: MySqlResource
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/demand/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    mysql_db = mysql.get_db_connection()
    stmt_create_table = """
        CREATE DATABASE IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_demand (
            for_date TIMESTAMP,
            load_mw FLOAT,
            mrid BIGINT,
            business_type VARCHAR(255),
            object_aggregation VARCHAR(255),
            out_bidding_zone_mrid VARCHAR(255),
            quantity_measure_unit VARCHAR(255),
            curve_type VARCHAR(255),
            CONSTRAINT pk_record PRIMARY KEY (for_date, out_bidding_zone_mrid)
        )
        --END STATEMENT--
    """
    with mysql_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_demand")


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
)
def entsoe_generation_by_unit_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    output_path = fs.get_writer(
        f"entsoe/generation_by_unit/{start_date.strftime('%Y%m%d')}/{region}"
    ).base_path
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
    tags={"storage": "mysql"},
)
def entsoe_generation_by_unit(
    context: dagster.AssetExecutionContext, fs: FilesystemResource, mysql: MySqlResource
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/generation_by_unit/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    mysql_db = mysql.get_db_connection()
    stmt_create_table = """
        CREATE DATABASE IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_generation_by_unit (
            for_date TIMESTAMP,
            value FLOAT,
            unit VARCHAR(255),
            domain VARCHAR(255),
            psr_type VARCHAR(255),
            business_type VARCHAR(255),
            registered_resource VARCHAR(255),
            psr_name VARCHAR(255),
            psr_mrid VARCHAR(255),
            CONSTRAINT pk_record PRIMARY KEY (for_date, registered_resource, psr_mrid)
        )
        --END STATEMENT--
    """
    with mysql_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_generation_by_unit")


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["crossborder_flows"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "filesystem", "scrape_source": "entsoe"},
)
def entsoe_crossborder_flows_raw(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    api_key = dagster.EnvVar("ENTSOE_API_KEY").get_value()
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]

    start_date, end_date = _get_date_window(for_date)
    output_path = fs.get_writer(
        f"entsoe/cross_border_flows/{start_date.strftime('%Y%m%d')}/{region}"
    ).base_path
    scraper = entsoe.FileWritingEntsoeScraper(
        api_key=api_key, output_dir=output_path
    ).set_dates(start_date, end_date)
    try:
        scraper.query_crossborder_flows(region)
    except requests.exceptions.HTTPError:
        context.log.warning(f"HTTP error for {region} on {start_date}")
    except NoMatchingDataError:
        context.log.warning(f"No data for {region} on {start_date}")


@dagster.asset(
    deps=["entsoe_crossborder_flows_raw"],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "region": dagster.StaticPartitionsDefinition(
                country_codes["crossborder_flows"]
            ),
        }
    ),
    group_name=ASSET_GROUP,
    tags={"storage": "mysql"},
)
def entsoe_crossborder_flows(
    context: dagster.AssetExecutionContext, fs: FilesystemResource, mysql: MySqlResource
):
    for_date = context.partition_key.keys_by_dimension["date"]
    region = context.partition_key.keys_by_dimension["region"]
    folder_path = fs.get_writer(
        f"entsoe/cross_border_flows/{pd.to_datetime(for_date).strftime('%Y%m%d')}/{region}"
    ).base_path
    parser = entsoe.EntsoeFileParser(folder_path)
    df = parser.parse_files()
    mysql_db = mysql.get_db_connection()
    stmt_create_table = """
        CREATE DATABASE IF NOT EXISTS entsoe
        --END STATEMENT--

        CREATE TABLE IF NOT EXISTS entsoe.entsoe_crossborder_flows (
            for_date TIMESTAMP,
            value FLOAT,
            unit VARCHAR(255),
            in_domain VARCHAR(255),
            out_domain VARCHAR(255),
            doc_type VARCHAR(255),
            CONSTRAINT pk_record PRIMARY KEY (for_date, in_domain, out_domain)
        )
        --END STATEMENT--
    """
    with mysql_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "entsoe", "entsoe_crossborder_flows")


def _get_date_window(
    dt: str,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_date = pd.Timestamp(dt, tz="UTC")
    end_date = pd.Timestamp(dt, tz="UTC") + pd.Timedelta(
        hours=23, minutes=59, seconds=59
    )
    return start_date, end_date
