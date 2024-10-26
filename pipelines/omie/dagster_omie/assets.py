import requests
import locale
from datetime import date, datetime
from dagster import asset, AssetExecutionContext
from eupower_core.scrapes.omie import download_from_omie_fs
from eupower_core.utils.sql import prepare_query_from_string
from eupower_core.dagster_resources import FilesystemResource, DuckDBtoMySqlResource
from eupower_core.dagster_resources.fs import FsReader
from .partitions import monthly_partition

OMIE_LOAD_ASSETS = "omie_load_assets"
MYSQL_SCHEMA = "omie"
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
