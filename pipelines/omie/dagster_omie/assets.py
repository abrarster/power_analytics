import requests
import locale
import warnings
import os
import pandas as pd
from typing import Optional, List, Tuple
from datetime import date, datetime, timedelta
from dagster import EnvVar, asset, AssetExecutionContext, ExperimentalWarning, Config
from pydantic import Field
from eupower_core.scrapes.omie import download_from_omie_fs
from eupower_core.utils.sql import prepare_query_from_string
from eupower_core.dagster_resources import (
    FilesystemResource,
    DuckDBtoMySqlResource,
    MySqlResource,
    DuckDBtoPostgresResource,
    PostgresResource,
    mysql,
)
from eupower_core.dagster_resources.fs import FsReader
from eupower_core.scrapes.redelectrica.esios import (
    list_indicators,
    make_headers,
    get_indicators,
)
from eupower_core.scrapes.redelectrica import esios_indicators as ree_indicators
from .partitions import monthly_partition

warnings.simplefilter(action="ignore", category=ExperimentalWarning)
OMIE_LOAD_ASSETS = "omie_load_assets"
MYSQL_SCHEMA = "omie"
REDELECTRICA_ASSETS = "redelectrica"
locale.setlocale(locale.LC_NUMERIC, "eu_ES")


# Raw downloads
@asset(partitions_def=monthly_partition, group_name=OMIE_LOAD_ASSETS)
def omie_raw_cab(context: AssetExecutionContext, fs: FilesystemResource) -> None:
    _download_omie_monthly_ep("cab", context.partition_time_window.start, fs)


@asset(partitions_def=monthly_partition, group_name=OMIE_LOAD_ASSETS)
def omie_raw_det(context: AssetExecutionContext, fs: FilesystemResource) -> None:
    _download_omie_monthly_ep("det", context.partition_time_window.start, fs)


@asset(partitions_def=monthly_partition, group_name=OMIE_LOAD_ASSETS)
def omie_raw_pdbc(context: AssetExecutionContext, fs: FilesystemResource) -> None:
    _download_omie_monthly_ep("pdbc", context.partition_time_window.start, fs)


@asset(partitions_def=monthly_partition, group_name=OMIE_LOAD_ASSETS)
def omie_raw_curva_pbc_uof(
    context: AssetExecutionContext, fs: FilesystemResource
) -> None:
    _download_omie_monthly_ep("curva_pbc_uof", context.partition_time_window.start, fs)


@asset(
    deps=["omie_raw_curva_pbc_uof"],
    partitions_def=monthly_partition,
    group_name=OMIE_LOAD_ASSETS,
)
def omie_mysql_raw_curva_pbc_uof(
    context: AssetExecutionContext,
    fs: FilesystemResource,
    duckdb_mysql: DuckDBtoMySqlResource,
) -> None:
    # Make file pattern
    start_time = context.partition_time_window.start
    partition_id = start_time.strftime("%Y%m")
    reader = _get_raw_omie_reader("curva_pbc_uof", start_time, fs)
    folder_path = reader.base_path
    file_glob = f"{folder_path}/*.1"

    # DuckDb ETL statements
    template_duckdb_etl = """
        CREATE TABLE raw_curva_pbc_uof_{{ partition_id | sqlsafe }} AS
            SELECT * FROM read_csv(
                {{ file_glob }},
                skip=3,
                header=FALSE,
                delim=';',
                decimal_separator=',',
                columns={
                    'hour_ending': 'integer',
                    'for_day': 'date',
                    'country': 'varchar',
                    'unit_code': 'varchar',
                    'offer_type': 'varchar',
                    'volume': 'varchar',
                    'price': 'varchar',
                    'bid_offer': 'varchar',
                    'not_used': 'varchar'
                }
            )
        --END STATEMENT--

        CREATE TABLE curva_pbc_uof_{{ partition_id | sqlsafe }} AS
            SELECT CAST({{ partition_id }} AS VARCHAR) as partition_month,
                   for_day,
                   hour_ending,
                   country,
                   unit_code,
                   offer_type,
                   str_to_float(volume) as volume,
                   str_to_float(price)  as price,
                   bid_offer
            FROM raw_curva_pbc_uof_{{ partition_id | sqlsafe }}
            WHERE volume is not null
        --END STATEMENT--
        """
    stmt_duckdb_etl = prepare_query_from_string(
        template_duckdb_etl, {"partition_id": partition_id, "file_glob": file_glob}
    )

    # MySql ETL Statements
    template_mysql_etl = """
        CREATE TABLE IF NOT EXISTS mysql_db.{{ mysql_schema | sqlsafe }}.curva_pbc_uof (
            partition_month VARCHAR,
            for_day DATE,
            hour_ending INT,
            country VARCHAR,
            unit_code VARCHAR,
            offer_type VARCHAR,
            volume FLOAT,
            price FLOAT,
            bid_offer VARCHAR
        )
        --END STATEMENT--

        CALL mysql_execute('mysql_db', 'DROP TABLE IF EXISTS {{ mysql_schema | sqlsafe }}.curva_pbc_uof_{{ partition_id | sqlsafe }}')
        --END STATEMENT--

        CREATE TABLE mysql_db.{{ mysql_schema | sqlsafe }}.curva_pbc_uof_{{ partition_id | sqlsafe }} 
        AS FROM memory.curva_pbc_uof_{{ partition_id | sqlsafe }}
        --END STATEMENT--

        DELETE FROM mysql_db.{{ mysql_schema | sqlsafe }}.curva_pbc_uof
        WHERE partition_month = {{ partition_id }}
        --END STATEMENT--

        INSERT INTO mysql_db.{{ mysql_schema | sqlsafe }}.curva_pbc_uof
        SELECT * FROM mysql_db.{{ mysql_schema | sqlsafe }}.curva_pbc_uof_{{ partition_id | sqlsafe }}
        --END STATEMENT--

        CALL mysql_execute('mysql_db', 'DROP TABLE IF EXISTS {{ mysql_schema | sqlsafe }}.curva_pbc_uof_{{ partition_id | sqlsafe }}')
        --END STATEMENT--
        """

    stmt_mysql_etl = prepare_query_from_string(
        template_mysql_etl, {"mysql_schema": MYSQL_SCHEMA, "partition_id": partition_id}
    )

    # Execute statements
    with duckdb_mysql.get_db_connection(MYSQL_SCHEMA) as conn:
        conn.duckdb_conn.create_function("str_to_float", str_to_float_es)
        conn.execute(stmt_duckdb_etl)
        conn.validate_mysql_schema()
        conn.execute(stmt_mysql_etl)


@asset(partitions_def=monthly_partition, group_name=OMIE_LOAD_ASSETS)
def omie_raw_pdbf(context: AssetExecutionContext, fs: FilesystemResource) -> None:
    _download_omie_monthly_ep("pdbf", context.partition_time_window.start, fs)


def _download_omie_monthly_ep(
    endpoint, start: datetime, fs: FilesystemResource
) -> None:
    as_of_date = date(start.year, start.month, 1)
    session = requests.Session()
    zip_folder = download_from_omie_fs(session, endpoint, as_of_date)
    zipped_files = zip_folder.namelist()
    writer = fs.get_writer(f"omie/raw/{endpoint}/{as_of_date.strftime('%Y-%m')}")
    with writer as _:
        for zf in zipped_files:
            file_contents = zip_folder.open(zf).read()
            writer.write_file(zf, file_contents)


def _get_raw_omie_reader(endpoint, start: datetime, fs: FilesystemResource) -> FsReader:
    as_of_date = date(start.year, start.month, 1)
    reader = fs.get_reader(f"omie/raw/{endpoint}/{as_of_date.strftime('%Y-%m')}")
    return reader


def str_to_float_es(x: str) -> float:
    return locale.atof(x)


# ESIOS


class EsiosConfig(Config):
    indicators: str = Field(
        default_factory=lambda: "REALTIME_HISTORY",
        description="""
            Esios indicator group. Valid options available at eupower_core.scrapes.redelectrica.esios_indicators.
            Multiple indicators can be specified by separating them with commas.
        """,
    )
    start_date: Optional[str] = Field(
        default_factory=lambda: (date.today() - timedelta(days=5)).strftime("%Y-%m-%d"),
        description="Start date for indicator history",
    )
    end_date: Optional[str] = Field(
        default_factory=lambda: date.today().strftime("%Y-%m-%d"),
        description="End date for indicator history",
    )


@asset(group_name=REDELECTRICA_ASSETS, tags={"storage": "postgres"})
def esios_indicator_ids(
    context: AssetExecutionContext, postgres: PostgresResource
) -> None:
    api_key = EnvVar("ESIOS_API_KEY").get_value()
    session = requests.Session()
    headers = make_headers(api_key)
    indicators = list_indicators(session, headers)
    indicators = indicators.rename(columns={"name": "long_name", "id": "indicator_id"})[
        ["indicator_id", "short_name", "long_name", "description"]
    ]
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(
            indicators, "redelectrica", "esios_indicator_ids", upsert=False
        )


@asset(group_name=REDELECTRICA_ASSETS, tags={"storage": "filesystem"})
def esios_indicators_staging(
    context: AssetExecutionContext, fs: FilesystemResource, config: EsiosConfig
) -> None:
    indicators = config.indicators.split(",")
    indicators = {v.upper(): getattr(ree_indicators, v.upper()) for v in indicators}
    start_date = pd.to_datetime(config.start_date).date()
    end_date = pd.to_datetime(config.end_date).date()
    context.log.info(
        f"Downloading indicators {indicators} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
    )

    api_key = EnvVar("ESIOS_API_KEY").get_value()
    session = requests.Session()
    headers = make_headers(api_key)
    writer = fs.get_writer(f"redelectrica/esios/raw")
    if os.path.exists(writer.base_path):
        writer.delete_data()
    for indicator_group_name, indicator_ids in indicators.items():
        for chunk_start, chunk_end in _split_dates_into_chunk(start_date, end_date):
            try:
                response = get_indicators(
                    session, headers, indicator_ids, chunk_start, chunk_end
                )
                writer.write_file(
                    f"{indicator_group_name}_{chunk_start.strftime('%Y%m%d')}_{chunk_end.strftime('%Y%m%d')}.csv",
                    response,
                )
                context.log.info(
                    f"Downloaded {indicator_group_name} for {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                )
            except:
                context.log.warning(
                    f"Failed to download {indicator_group_name} for {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                )
                raise


@asset(
    deps=["esios_indicators_staging"],
    group_name=REDELECTRICA_ASSETS,
    tags={"storage": "postgres"},
)
def esios_indicators(
    context: AssetExecutionContext, fs: FilesystemResource, postgres: PostgresResource
) -> None:
    reader = fs.get_reader("redelectrica/esios/raw")
    files = reader.list_files()
    postgres_db = postgres.get_db_connection()
    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS redelectrica.esios_indicators (
            indicator_id INT,
            short_name VARCHAR(255),
            geo_id BIGINT,
            geo_name VARCHAR(255),
            for_date VARCHAR(255),
            for_date_cet VARCHAR(255),
            value FLOAT,
            composited BOOL,
            step_type VARCHAR(255),
            disaggregated BOOL,
            values_updated_at VARCHAR(255),
            PRIMARY KEY (indicator_id, geo_id, for_date)
        )
    """
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        for file in files:
            df = (
                pd.read_csv(f"{reader.base_path}/{file}", index_col=False)
                .astype(
                    {"for_date": str, "for_date_cet": str, "values_updated_at": str}
                )
                .drop_duplicates(
                    subset=["for_date", "geo_id", "indicator_id"], keep="last"
                )
            )
            db.write_dataframe(df, "redelectrica", "esios_indicators", upsert=True)


def _split_dates_into_chunk(start: date, end: date) -> List[Tuple[date, date]]:
    dr = pd.date_range(start, end, freq="D")
    if len(dr) < 8:
        return [
            (start, end),
        ]
    n = 7
    chunked_list = [dr[i : i + n] for i in range(0, len(dr), n)]
    date_range = []
    for c in chunked_list:
        sorted_c = sorted(c)
        date_range.append((sorted_c[0].date(), sorted_c[-1].date()))
    return date_range
