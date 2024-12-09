import os
import dagster
import pandas as pd
import json
from datetime import date
from typing import Optional
from eupower_core.dagster_resources import FilesystemResource, PostgresResource
from eupower_core.scrapes.elia import EliaTs, EliaRemits
from .asset_groups import ELIA


table_metadata = {
    "total_load": {
        "schema": "elia",
        "tablename": "total_load",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "totalload": "FLOAT",
            "mostrecentforecast": "FLOAT",
            "mostrecentconfidence10": "FLOAT",
            "mostrecentconfidence90": "FLOAT",
            "dayaheadforecast": "FLOAT",
            "dayaheadconfidence10": "FLOAT",
            "dayaheadconfidence90": "FLOAT",
            "weekaheadforecast": "FLOAT",
        },
        "primary_key": "datetime",
    },
    "grid_load": {
        "schema": "elia",
        "tablename": "grid_load",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "eliagridload": "FLOAT",
        },
        "primary_key": "datetime",
    },
    "da_gen_byfuel": {
        "schema": "elia",
        "tablename": "da_gen_byfuel",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "fuelcode": "VARCHAR(100)",
            "dayaheadgenerationschedule": "FLOAT",
        },
        "primary_key": ["datetime", "fuelcode"],
    },
    "rt_gen_byfuel": {
        "schema": "elia",
        "tablename": "rt_gen_byfuel",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "fuelcode": "VARCHAR(100)",
            "generatedpower": "FLOAT",
            "totalgeneratedpower": "FLOAT",
        },
        "primary_key": ["datetime", "fuelcode"],
    },
    "wind_generation_hist": {
        "schema": "elia",
        "tablename": "wind_generation_hist",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "offshoreonshore": "VARCHAR(200)",
            "region": "VARCHAR(200)",
            "gridconnectiontype": "VARCHAR(200)",
            "measured": "FLOAT",
            "mostrecentforecast": "FLOAT",
            "mostrecentconfidence10": "FLOAT",
            "mostrecentconfidence90": "FLOAT",
            "dayahead11hforecast": "FLOAT",
            "dayahead11hconfidence10": "FLOAT",
            "dayahead11hconfidence90": "FLOAT",
            "dayaheadforecast": "FLOAT",
            "dayaheadconfidence10": "FLOAT",
            "dayaheadconfidence90": "FLOAT",
            "weekaheadforecast": "FLOAT",
            "weekaheadconfidence10": "FLOAT",
            "weekaheadconfidence90": "FLOAT",
            "monitoredcapacity": "FLOAT",
            "loadfactor": "FLOAT",
            "decrementalbidid": "VARCHAR(100)",
        },
        "primary_key": ["datetime", "offshoreonshore", "region", "gridconnectiontype"],
    },
    "solar_generation_hist": {
        "schema": "elia",
        "tablename": "solar_generation_hist",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "region": "VARCHAR(200)",
            "measured": "FLOAT",
            "mostrecentforecast": "FLOAT",
            "mostrecentconfidence10": "FLOAT",
            "mostrecentconfidence90": "FLOAT",
            "dayahead11hforecast": "FLOAT",
            "dayahead11hconfidence10": "FLOAT",
            "dayahead11hconfidence90": "FLOAT",
            "dayaheadforecast": "FLOAT",
            "dayaheadconfidence10": "FLOAT",
            "dayaheadconfidence90": "FLOAT",
            "weekaheadforecast": "FLOAT",
            "weekaheadconfidence10": "FLOAT",
            "weekaheadconfidence90": "FLOAT",
            "monitoredcapacity": "FLOAT",
            "loadfactor": "FLOAT",
        },
        "primary_key": ["datetime", "region"],
    },
    "itc_phys_flow": {
        "schema": "elia",
        "tablename": "itc_phys_flow",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "controlarea": "VARCHAR(100)",
            "physicalflowatborder": "FLOAT",
        },
        "primary_key": ["datetime", "controlarea"],
    },
    "itc_da_comex": {
        "schema": "elia",
        "tablename": "itc_da_comex",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "country": "VARCHAR(100)",
            "direction": "VARCHAR(100)",
            "commercialschedule": "FLOAT",
        },
        "primary_key": ["datetime", "country", "direction"],
    },
    "itc_total_comex": {
        "schema": "elia",
        "tablename": "itc_total_comex",
        "table_schema": {
            "datetime": "VARCHAR(255)",
            "resolutioncode": "VARCHAR(100)",
            "import_value": "FLOAT",
            "export_value": "FLOAT",
            "country": "VARCHAR(100)",
        },
        "primary_key": ["datetime", "country"],
    },
    "forced_outages": {
        "schema": "elia",
        "tablename": "forced_outages",
        "table_schema": {
            "startdatetime": "VARCHAR",
            "enddatetime": "VARCHAR",
            "startoutagetstime": "VARCHAR",
            "endoutagetstime": "VARCHAR",
            "productionunitname": "VARCHAR",
            "uniteic": "VARCHAR",
            "technicalpmax": "FLOAT",
            "availablepowerduringoutage": "FLOAT",
            "outagereason": "VARCHAR",
            "outagestatus": "VARCHAR",
            "mrid": "VARCHAR",
            "version": "bigint",
            "lastupdated": "VARCHAR",
        },
        "primary_key": ["mrid", "version"],
    },
    "planned_outages": {
        "schema": "elia",
        "tablename": "planned_outages",
        "table_schema": {
            "startdatetime": "VARCHAR",
            "enddatetime": "VARCHAR",
            "startoutagetstime": "VARCHAR",
            "endoutagetstime": "VARCHAR",
            "productionunitname": "VARCHAR",
            "uniteic": "VARCHAR",
            "technicalpmax": "FLOAT",
            "availablepowerduringoutage": "FLOAT",
            "outagereason": "VARCHAR",
            "outagestatus": "VARCHAR",
            "mrid": "VARCHAR",
            "version": "bigint",
            "lastupdated": "VARCHAR",
        },
        "primary_key": ["mrid", "version"],
    },
    "generation_units": {
        "schema": "elia",
        "tablename": "generation_units",
        "table_schema": {
            "date": "VARCHAR",
            "technicalunitname": "VARCHAR",
            "unittype": "VARCHAR",
            "technicalpmax": "FLOAT",
            "fueltypepublication": "VARCHAR",
        },
        "primary_key": ["date", "technicalunitname"],
    },
}


def create_table_statement(
    schema: str,
    tablename: str,
    table_schema: dict[str, str],
    primary_key: str | list[str],
) -> str:
    # Format column definitions
    columns = [
        f"    {col_name} {data_type}" for col_name, data_type in table_schema.items()
    ]

    # Handle primary key - convert to list if string
    if isinstance(primary_key, str):
        primary_key = [primary_key]

    # Add primary key constraint
    pk_constraint = f"    PRIMARY KEY ({', '.join(primary_key)})"

    # Combine all parts of the statement
    create_statement = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{tablename} (
        {',\n'.join(columns)},
        {pk_constraint}
        )
    """
    return create_statement


def create_elia_raw_asset(
    data_type: EliaTs,
    path_prefix: str,
    old_data_type: Optional[EliaTs] = None,
    is_remits: bool = False,
):
    """Factory function to create Elia data assets with common configuration."""

    @dagster.asset(
        group_name=ELIA,
        name=f"elia_{path_prefix}_raw",  # Add unique name based on path_prefix
        tags={"storage": "filesystem"},
        partitions_def=dagster.DailyPartitionsDefinition(start_date="2022-01-01"),
        kinds={"python", "file"},
    )
    def _elia_asset_raw(
        context: dagster.AssetExecutionContext,
        fs: FilesystemResource,
    ):
        as_of_date = context.partition_time_window.start
        writer = fs.get_writer(f"elia/{path_prefix}/{as_of_date.strftime('%Y%m%d')}")
        writer.delete_data()
        if old_data_type is None:
            data_type_to_query = data_type
        else:
            if as_of_date.date() < date(2024, 5, 22):
                data_type_to_query = old_data_type
            else:
                data_type_to_query = data_type
        response = data_type_to_query.exec_query(as_of_date, as_of_date)
        writer.write_file(f"{path_prefix}.json", response.content)

    @dagster.asset(
        deps=[f"elia_{path_prefix}_raw"],
        group_name=ELIA,
        name=f"elia_{path_prefix}",
        tags={"storage": "postgres"},
        partitions_def=dagster.DailyPartitionsDefinition(start_date="2022-01-01"),
        kinds={"python", "postgres"},
    )
    def _elia_asset_db(
        context: dagster.AssetExecutionContext,
        fs: FilesystemResource,
        postgres: PostgresResource,
    ):
        as_of_date = context.partition_time_window.start
        reader = fs.get_reader(f"elia/{path_prefix}/{as_of_date.strftime('%Y%m%d')}")
        file_names = reader.list_files()
        if len(file_names) == 0:
            raise dagster.SkipReason("No data found")

        if not is_remits:
            df = pd.read_json(
                f"{reader.base_path}/{file_names[0]}", dtype={"datetime": str}
            )
        else:
            with open(f"{reader.base_path}/{file_names[0]}", 'r') as f:
                df = pd.DataFrame.from_records(json.load(f)["results"])

        records_exist = True
        if len(df) == 0:
            if not is_remits:
                raise dagster.SkipReason("No records found")
            else:
                context.log.info("No records found for remits")
                records_exist = False
        if records_exist:
            table_params = table_metadata[path_prefix]
            stmt_create_table = create_table_statement(**table_params)
            postgres_db = postgres.get_db_connection()
            with postgres_db as db:
                db.execute_statements(stmt_create_table)
                db.write_dataframe(df, table_params["schema"], table_params["tablename"])

    return _elia_asset_raw, _elia_asset_db


# Create the assets using the factory function
elia_total_load_raw, elia_total_load = create_elia_raw_asset(
    EliaTs.TOTAL_LOAD, "total_load"
)
elia_grid_load_raw, elia_grid_load = create_elia_raw_asset(
    EliaTs.GRID_LOAD, "grid_load"
)
# elia_realtime_load_raw, elia_realtime_load = create_elia_raw_asset(EliaTs.RT_LOAD, "realtime_load")
elia_da_gen_byfuel_raw, elia_da_gen_byfuel = create_elia_raw_asset(
    EliaTs.DA_GEN_BYFUEL, "da_gen_byfuel", EliaTs.DA_GEN_BYFUEL_OLD
)
elia_rt_gen_byfuel_raw, elia_rt_gen_byfuel = create_elia_raw_asset(
    EliaTs.RT_GEN_BYFUEL, "rt_gen_byfuel", EliaTs.RT_GEN_BYFUEL_OLD
)
elia_wind_generation_hist_raw, elia_wind_generation_hist = create_elia_raw_asset(
    EliaTs.WIND_GENERATION_HIST, "wind_generation_hist"
)
elia_solar_generation_hist_raw, elia_solar_generation_hist = create_elia_raw_asset(
    EliaTs.SOLAR_GENERATION_HIST, "solar_generation_hist"
)
# elia_wind_gen_rt_raw = create_elia_raw_asset(EliaTs.WIND_GENERATION_RT, "wind_gen_rt")
# elia_solar_gen_rt_raw = create_elia_raw_asset(
#     EliaTs.SOLAR_GENERATION_RT, "solar_gen_rt"
# )
elia_itc_da_comex_raw, elia_itc_da_comex = create_elia_raw_asset(
    EliaTs.ITC_DA_COMEX, "itc_da_comex"
)
elia_itc_total_comex_raw, elia_itc_total_comex = create_elia_raw_asset(
    EliaTs.ITC_TOTAL_COMEX, "itc_total_comex"
)
elia_itc_phys_flow_raw, elia_itc_phys_flow = create_elia_raw_asset(
    EliaTs.ITC_PHYS_FLOW, "itc_phys_flow"
)
elia_planned_outages_raw, elia_planned_outages = create_elia_raw_asset(
    EliaRemits.PLANNED_OUTAGES, "planned_outages", is_remits=True
)
elia_forced_outages_raw, elia_forced_outages = create_elia_raw_asset(
    EliaRemits.FORCED_OUTAGES, "forced_outages", is_remits=True
)
elia_generation_units_raw, elia_generation_units = create_elia_raw_asset(
    EliaTs.GENERATION_UNITS, "generation_units", is_remits=True
)
