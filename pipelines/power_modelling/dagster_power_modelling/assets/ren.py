import requests
import pandas as pd
import dagster
import duckdb
import os
from typing import Callable
from datetime import datetime, timedelta
from eupower_core.dagster_resources import FilesystemResource, PostgresResource
from eupower_core.scrapes.ren.download import (
    download_capacities,
    download_power_balance,
    download_water_balance,
    download_hydro_production,
)
from .asset_groups import REN


def get_end_of_last_month() -> str:
    # Note: This is evaluated at module load time. 
    # The Dagster service needs to be restarted at the start of each month
    # to pick up the new end date for capacities partitions
    today = datetime.now()
    first_of_month = today.replace(day=1) - timedelta(days=1)
    return first_of_month.date().strftime("%Y-%m-%d")


START_DATE = "2023-01-01"
partitions_def = dagster.DailyPartitionsDefinition(
    start_date=START_DATE,
)
capacities_partitions_def = dagster.DailyPartitionsDefinition(
    start_date=START_DATE,
    end_date=get_end_of_last_month(),
)


def create_ren_asset(
    name: str,
    description: str,
    download_fn: Callable,
    partitions_def: dagster.DailyPartitionsDefinition = partitions_def,
):
    """Factory function to create REN download assets with common logic"""

    @dagster.asset(
        name=f"{name}_raw",
        partitions_def=partitions_def,
        group_name=REN,
        description=description,
        tags={"storage": "filesystem", "scrape_source": "ren"},
    )
    def ren_asset(
        context: dagster.AssetExecutionContext, fs: FilesystemResource
    ) -> None:
        date = datetime.strptime(context.partition_key, "%Y-%m-%d").date()
        writer = fs.get_writer(f"ren/{name}/{date.strftime('%Y%m%d')}")
        writer.delete_data()

        with requests.Session() as session:
            result = (
                download_fn(session, date)
                if "session" in download_fn.__code__.co_varnames
                else download_fn(date)
            )
            writer.write_file(f"{name}.csv", result)

    return ren_asset


# Create the assets using the factory function
ren_power_balance_raw = create_ren_asset(
    name="ren_power_balance",
    description="Raw power balance data from REN (Portuguese TSO)",
    download_fn=download_power_balance,
)

ren_water_balance_raw = create_ren_asset(
    name="ren_water_balance",
    description="Raw water balance data from REN (Portuguese TSO)",
    download_fn=download_water_balance,
)

ren_capacities_raw = create_ren_asset(
    name="ren_capacities",
    description="Raw capacities data from REN (Portuguese TSO)",
    download_fn=download_capacities,
    partitions_def=capacities_partitions_def,
)

ren_hydro_production_raw = create_ren_asset(
    name="ren_hydro_production",
    description="Raw hydro production data from REN (Portuguese TSO)",
    download_fn=download_hydro_production,
)


@dagster.asset(
    deps=[ren_water_balance_raw],
    name="ren_water_balance",
    partitions_def=partitions_def,
    group_name=REN,
    tags={"storage": "postgres"},
)
def ren_water_balance(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
) -> None:
    as_of_date = context.partition_time_window.start
    reader = fs.get_reader(f"ren/ren_water_balance/{as_of_date.strftime('%Y%m%d')}")
    file_names = reader.list_files()
    if len(file_names) == 0:
        raise dagster.SkipReason("No data found")

    # Parse file into dataframe
    sql_create_tbl = """
        CREATE TABLE pt_water_balance 
        (
            field VARCHAR, 
            reservoir FLOAT,
            run_of_river FLOAT,
            small_hydro FLOAT,
            total_hydro FLOAT
        )
        """
    sql_insert_statement = f"""
        INSERT INTO pt_water_balance 
        SELECT * 
        FROM read_csv(
            '{reader.base_path}/*.csv', 
            delim=';', 
            header=false, 
            new_line='\\n',
            skip=1
        )
        """
    duckdb.sql(sql_create_tbl)
    duckdb.sql(sql_insert_statement)

    raw_data = duckdb.sql("SELECT * FROM pt_water_balance").to_df()
    data = raw_data.copy()
    data["for_day"] = pd.to_datetime(as_of_date)
    tidy_data = (
        data[~(data.field == "[%]")]
        .melt(id_vars=["field", "for_day"])
        .rename(columns={"variable": "hydro_type"})
        .astype({"for_day": str})[["field", "for_day", "hydro_type", "value"]]
    )
    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS ren.pt_water_balance (
            field VARCHAR NOT NULL,
            for_day VARCHAR NOT NULL,
            hydro_type VARCHAR NOT NULL,
            value FLOAT,
            PRIMARY KEY (field, for_day, hydro_type)
        )
        --END STATEMENT--
    """
    with postgres.get_db_connection() as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(tidy_data, "ren", "pt_water_balance")


@dagster.asset(
    deps=[ren_capacities_raw],
    name="ren_capacities",
    partitions_def=capacities_partitions_def,
    group_name=REN,
    tags={"storage": "postgres"},
)
def ren_capacities(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
) -> None:
    as_of_date = context.partition_time_window.start
    reader = fs.get_reader(f"ren/ren_capacities/{as_of_date.strftime('%Y%m%d')}")
    file_names = reader.list_files()
    if len(file_names) == 0:
        raise dagster.SkipReason("No data found")

    sql_create_tbl = """
        CREATE TABLE pt_capacity 
        (
            field VARCHAR, 
            capacity FLOAT,
            peak_gen FLOAT,
            peak_date VARCHAR
        )
        """
    sql_insert_statement = f"""
        INSERT INTO pt_capacity 
        SELECT * 
        FROM read_csv(
            '{reader.base_path}/*.csv', 
            delim=';', 
            header=false, 
            new_line='\\n',
            skip=1
        )
        """
    duckdb.sql(sql_create_tbl)
    duckdb.sql(sql_insert_statement)
    raw_data = duckdb.sql("SELECT * FROM pt_capacity").to_df()
    data = raw_data.copy()
    data["for_day"] = pd.to_datetime(as_of_date)
    data = data[["for_day", "field", "capacity"]].astype({"for_day": str})

    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS ren.pt_capacity (
            for_day VARCHAR NOT NULL,
            field VARCHAR NOT NULL,
            capacity FLOAT,
            PRIMARY KEY (for_day, field)
        )
        --END STATEMENT--
    """
    with postgres.get_db_connection() as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(data, "ren", "pt_capacity")


@dagster.asset(
    deps=[ren_power_balance_raw],
    name="ren_power_balance",
    partitions_def=partitions_def,
    group_name=REN,
    tags={"storage": "postgres"},
)
def ren_power_balance(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
) -> None:
    as_of_date = context.partition_time_window.start
    reader = fs.get_reader(f"ren/ren_power_balance/{as_of_date.strftime('%Y%m%d')}")
    file_names = reader.list_files()
    if len(file_names) == 0:
        raise dagster.SkipReason("No data found")
    file_name = file_names[0]
    df = pd.read_csv(os.path.join(reader.base_path, file_name), sep=";", skiprows=2)
    df.columns = [x.replace(" - ", " ").replace(" ", "_").lower() for x in df.columns]
    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS ren.power_balance (
            date_and_time VARCHAR NOT NULL,
            hydro DOUBLE PRECISION,
            wind DOUBLE PRECISION,
            solar DOUBLE PRECISION,
            biomass DOUBLE PRECISION,
            wave DOUBLE PRECISION,
            natural_gas_combined_cycle DOUBLE PRECISION,
            natural_gas_cogeneration DOUBLE PRECISION,
            coal DOUBLE PRECISION,
            other_thermal DOUBLE PRECISION,
            imports DOUBLE PRECISION,
            exports DOUBLE PRECISION,
            pumping DOUBLE PRECISION,
            consumption DOUBLE PRECISION,
            PRIMARY KEY (date_and_time)
        )
        --END STATEMENT--
    """
    with postgres.get_db_connection() as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "ren", "power_balance")


@dagster.asset(
    deps=[ren_hydro_production_raw],
    name="ren_hydro_production",
    partitions_def=partitions_def,
    group_name=REN,
    tags={"storage": "postgres"},
)
def ren_hydro_production(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
) -> None:
    as_of_date = context.partition_time_window.start
    reader = fs.get_reader(f"ren/ren_hydro_production/{as_of_date.strftime('%Y%m%d')}")
    file_names = reader.list_files()
    if len(file_names) == 0:
        raise dagster.SkipReason("No data found")
    file_name = file_names[0]
    df = pd.read_csv(os.path.join(reader.base_path, file_name))
    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS ren.hydro_production (
            for_date VARCHAR NOT NULL,
            pumped_storage DOUBLE PRECISION,
            small_hydro DOUBLE PRECISION,
            run_of_river DOUBLE PRECISION,
            reservoir DOUBLE PRECISION,
            total_hydro DOUBLE PRECISION,
            PRIMARY KEY (for_date)
        )
    """
    with postgres.get_db_connection() as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(df, "ren", "hydro_production")
