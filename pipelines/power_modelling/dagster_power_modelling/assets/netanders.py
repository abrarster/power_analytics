import dagster
import requests
import os
import json
from datetime import datetime, date, timedelta
from eupower_core.scrapes.netanders import parameters as params
from eupower_core.scrapes.netanders.core import download_from_ned, parse_ned_response
from eupower_core.dagster_resources import FilesystemResource, PostgresResource
from .asset_groups import NETANDERS
from ..resources import RateLimiter

_generation_exclusions = [
    "NATURAL_GAS",
    "INDUSTRIAL_CONSUMERS_GAS_COMBINATION",
    "INDUSTRIAL_CONSUMERS_POWER_GAS_COMBINATION",
    "LOCAL_DISTRIBUTION_COMPANIES_COMBINATION",
    "ALL_CONSUMING_GAS",
    "GAS_DISTRIBUTION",
]


@dagster.asset(
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "data_type": dagster.StaticPartitionsDefinition(
                [
                    dt.name
                    for dt in params.DataType
                    if dt.name not in _generation_exclusions
                ]
            ),
        }
    ),
    group_name=NETANDERS,
    tags={"storage": "filesystem", "scrape_source": "netanders"},
)
def netanders_raw(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    rate_limiter: RateLimiter,
):
    api_key = dagster.EnvVar("NETANDERS_API_KEY").get_value()
    for_date = datetime.strptime(
        context.partition_key.keys_by_dimension["date"], "%Y-%m-%d"
    ).date()
    data_type = params.DataType[context.partition_key.keys_by_dimension["data_type"]]

    writer = fs.get_writer(f"netanders/{for_date.strftime('%Y%m%d')}/{data_type.name}")
    writer.delete_data()

    # Wait for rate limit capacity before making the API call
    rate_limiter.wait_for_capacity()

    with requests.Session() as session:
        response = download_from_ned(
            session=session,
            api_key=api_key,
            start_date=for_date,
            end_date=for_date + timedelta(days=1),
            point=params.Point.NETHERLANDS,  # You may want to make this configurable
            classification=params.Classification.CURRENT,
            granularity=params.Granularity.HOUR,  # You may want to make this configurable
            time_zone=params.TimeZone.UTC,
            activity=params.Activity.GENERATION,  # You may want to make this configurable
            data_type=data_type,
        )

        if response.status_code == 200:
            writer.write_file("data.json", response.text)
        else:
            context.log.error(f"Error downloading data: {response.status_code}")
            raise Exception(f"Failed to download data: {response.status_code}")


@dagster.asset(
    deps=[netanders_raw],
    partitions_def=dagster.MultiPartitionsDefinition(
        {
            "date": dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
            "data_type": dagster.StaticPartitionsDefinition(
                [dt.name for dt in params.DataType]
            ),
        }
    ),
    group_name=NETANDERS,
    tags={"storage": "postgres"},
)
def netanders_generation(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    for_date = datetime.strptime(
        context.partition_key.keys_by_dimension["date"], "%Y-%m-%d"
    ).date()
    data_type = params.DataType[context.partition_key.keys_by_dimension["data_type"]]
    file_path = os.path.join(
        fs.get_reader(
            f"netanders/{for_date.strftime('%Y%m%d')}/{data_type.name}"
        ).base_path,
        "data.json",
    )
    with open(file_path, "r") as f:
        file_data = json.load(f)

    cols = [
        "id",
        "point",
        "type",
        "granularity",
        "activity",
        "classification",
        "capacity",
        "volume",
        "percentage",
        "emission",
        "emissionfactor",
        "validfrom",
        "validto",
        "lastupdate",
        "timezone",
    ]
    df = parse_ned_response(file_data)[cols].rename(columns={"id": "row_id"})

    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS netanders.netanders_generation (
            row_id BIGINT,
            point VARCHAR,
            type VARCHAR,
            granularity VARCHAR,
            activity VARCHAR,
            classification VARCHAR,
            capacity DOUBLE PRECISION,
            volume DOUBLE PRECISION,
            percentage DOUBLE PRECISION,
            emission DOUBLE PRECISION,
            emissionfactor DOUBLE PRECISION,
            validfrom VARCHAR,
            validto VARCHAR,
            lastupdate VARCHAR,
            timezone VARCHAR,
            PRIMARY KEY (point, type, granularity, activity, classification, validfrom)
        )
        """
    with postgres.get_db_connection() as conn:
        conn.execute_statements(stmt_create_table)
        conn.write_dataframe(df, "netanders", "netanders_generation", upsert=True)
