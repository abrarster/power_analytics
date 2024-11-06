import os
import pandas as pd
import logging
import warnings
import urllib3
from datetime import date, timedelta
from dagster import (
    asset,
    AssetExecutionContext,
    EnvVar,
    Config,
    ExperimentalWarning,
)
from pydantic import Field
from eupower_core.dagster_resources import (
    FilesystemResource,
    MySqlResource,
    DuckDBtoMySqlResource,
    PostgresResource,
    DuckDBtoPostgresResource
)
from eupower_core.scrapes import rte

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=ExperimentalWarning)
logger = logging.getLogger(__name__)
MYSQL_SCHEMA = "rte"
ASSETS_GROUP = "rte"


class RteObservationConfig(Config):
    days_back: int = Field(
        default_value=5, description="Number of days to look back from today"
    )
    days_forward: int = Field(
        default_value=0, description="Number of days to look forward from today"
    )


@asset(
    group_name=ASSETS_GROUP,
    tags={"storage": "filesystem"},
)
def eco2mix_generation_raw(
    context: AssetExecutionContext, fs: FilesystemResource, config: RteObservationConfig
):
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)
    dates = pd.date_range(
        start_date, min(end_date, date.today() - timedelta(days=0)), freq="D"
    )
    dates = [x.date() for x in list(dates)]
    fs.get_writer(f"rte/eco2mix/raw").delete_data()
    for dt in dates:
        writer = fs.get_writer(f"rte/eco2mix/raw/{dt.strftime('%Y-%m-%d')}")
        for region in [*rte.RTE_REGIONS, None]:
            df = rte.download_rte_generation_mix(dt, region)
            writer.write_file(f"{region or 'FR'}.csv", df)


@asset(
    deps=["eco2mix_generation_raw"], tags={"storage": "postgres"}, group_name=ASSETS_GROUP
)
def eco2mix_balances(
    context: AssetExecutionContext,
    fs: FilesystemResource,
    duckdb_postgres: DuckDBtoPostgresResource,
):
    reader = fs.get_reader()
    path_glob = os.path.join(reader.base_path, "rte/eco2mix/raw/*/FR.csv")

    template_duckdb_etl = """
        CREATE TABLE eco2mix AS 
        SELECT * FROM read_csv('{path_glob}', all_varchar=True)
        --END STATEMENT--

        CREATE TABLE eco2mix_cleaned (
            perimeter VARCHAR,
            nature VARCHAR,
            for_date TIMESTAMP,
            consumption FLOAT,
            forecast_j_minus_1 FLOAT,
            forecast_j FLOAT,
            fuel_oil FLOAT,
            coal FLOAT,
            gas FLOAT,
            nuclear FLOAT,
            wind FLOAT,
            solar FLOAT,
            hydro FLOAT,
            pumped_storage FLOAT,
            bioenergy FLOAT,
            border_flows FLOAT,
            co2_rate FLOAT,
            uk_flows FLOAT,
            spain_flows FLOAT,
            italy_flows FLOAT,
            switzerland_flows FLOAT,
            germany_belgium_flows FLOAT,
            fuel_oil_tac FLOAT,
            fuel_oil_cogen FLOAT,
            fuel_oil_other FLOAT,
            gas_tac FLOAT,
            gas_cogen FLOAT,
            gas_ccgt FLOAT,
            gas_other FLOAT,
            hydro_run_of_river FLOAT,
            hydro_reservoir FLOAT,
            hydro_wwtp_turbines FLOAT,
            bioenergy_waste FLOAT,
            bioenergy_biomass FLOAT,
            bioenergy_biogas FLOAT,
            battery_storage FLOAT,
            battery_clearance FLOAT,
            onshore_wind FLOAT,
            offshore_wind FLOAT,
            corrected_consumption FLOAT
        )
        --END STATEMENT--

        INSERT INTO eco2mix_cleaned
        SELECT 
            NULLIF(NULLIF("Périmètre", 'ND'), '-')::VARCHAR as perimeter,
            NULLIF(NULLIF("Nature", 'ND'), '-')::VARCHAR as nature,
            ((strptime(concat("Date", ' ', "Heures"), '%Y-%m-%d %H:%M') AT TIME ZONE 'Europe/Paris') AT TIME ZONE 'UTC')::TIMESTAMP as for_date,
            CAST(NULLIF(NULLIF("Consommation", 'ND'), '-') AS FLOAT) as consumption,
            CAST(NULLIF(NULLIF("Prévision J-1", 'ND'), '-') AS FLOAT) as forecast_j_minus_1,
            CAST(NULLIF(NULLIF("Prévision J", 'ND'), '-') AS FLOAT) as forecast_j,
            CAST(NULLIF(NULLIF("Fioul", 'ND'), '-') AS FLOAT) as fuel_oil,
            CAST(NULLIF(NULLIF("Charbon", 'ND'), '-') AS FLOAT) as coal,
            CAST(NULLIF(NULLIF("Gaz", 'ND'), '-') AS FLOAT) as gas,
            CAST(NULLIF(NULLIF("Nucléaire", 'ND'), '-') AS FLOAT) as nuclear,
            CAST(NULLIF(NULLIF("Eolien", 'ND'), '-') AS FLOAT) as wind,
            CAST(NULLIF(NULLIF("Solaire", 'ND'), '-') AS FLOAT) as solar,
            CAST(NULLIF(NULLIF("Hydraulique", 'ND'), '-') AS FLOAT) as hydro,
            CAST(NULLIF(NULLIF("Pompage", 'ND'), '-') AS FLOAT) as pumped_storage,
            CAST(NULLIF(NULLIF("Bioénergies", 'ND'), '-') AS FLOAT) as bioenergy,
            CAST(NULLIF(NULLIF("Ech. physiques", 'ND'), '-') AS FLOAT) as border_flows,
            CAST(NULLIF(NULLIF("Taux de Co2", 'ND'), '-') AS FLOAT) as co2_rate,
            CAST(NULLIF(NULLIF("Ech. comm. Angleterre", 'ND'), '-') AS FLOAT) as uk_flows,
            CAST(NULLIF(NULLIF("Ech. comm. Espagne", 'ND'), '-') AS FLOAT) as spain_flows,
            CAST(NULLIF(NULLIF("Ech. comm. Italie", 'ND'), '-') AS FLOAT) as italy_flows,
            CAST(NULLIF(NULLIF("Ech. comm. Suisse", 'ND'), '-') AS FLOAT) as switzerland_flows,
            CAST(NULLIF(NULLIF("Ech. comm. Allemagne-Belgique", 'ND'), '-') AS FLOAT) as germany_belgium_flows,
            CAST(NULLIF(NULLIF("Fioul - TAC", 'ND'), '-') AS FLOAT) as fuel_oil_tac,
            CAST(NULLIF(NULLIF("Fioul - Cogén.", 'ND'), '-') AS FLOAT) as fuel_oil_cogen,
            CAST(NULLIF(NULLIF("Fioul - Autres", 'ND'), '-') AS FLOAT) as fuel_oil_other,
            CAST(NULLIF(NULLIF("Gaz - TAC", 'ND'), '-') AS FLOAT) as gas_tac,
            CAST(NULLIF(NULLIF("Gaz - Cogén.", 'ND'), '-') AS FLOAT) as gas_cogen,
            CAST(NULLIF(NULLIF("Gaz - CCG", 'ND'), '-') AS FLOAT) as gas_ccgt,
            CAST(NULLIF(NULLIF("Gaz - Autres", 'ND'), '-') AS FLOAT) as gas_other,
            CAST(NULLIF(NULLIF("Hydraulique - Fil de l?eau + éclusée", 'ND'), '-') AS FLOAT) as hydro_run_of_river,
            CAST(NULLIF(NULLIF("Hydraulique - Lacs", 'ND'), '-') AS FLOAT) as hydro_reservoir,
            CAST(NULLIF(NULLIF("Hydraulique - STEP turbinage", 'ND'), '-') AS FLOAT) as hydro_wwtp_turbines,
            CAST(NULLIF(NULLIF("Bioénergies - Déchets", 'ND'), '-') AS FLOAT) as bioenergy_waste,
            CAST(NULLIF(NULLIF("Bioénergies - Biomasse", 'ND'), '-') AS FLOAT) as bioenergy_biomass,
            CAST(NULLIF(NULLIF("Bioénergies - Biogaz", 'ND'), '-') AS FLOAT) as bioenergy_biogas,
            CAST(NULLIF(NULLIF("Stockage batterie", 'ND'), '-') AS FLOAT) as battery_storage,
            CAST(NULLIF(NULLIF("Déstockage batterie", 'ND'), '-') AS FLOAT) as battery_clearance,
            CAST(NULLIF(NULLIF("Eolien terrestre", 'ND'), '-') AS FLOAT) as onshore_wind,
            CAST(NULLIF(NULLIF("Eolien offshore", 'ND'), '-') AS FLOAT) as offshore_wind,
            CAST(NULLIF(NULLIF("Consommation corrigée", 'ND'), '-') AS FLOAT) as corrected_consumption
        FROM eco2mix
        --END STATEMENT--
        """

    template_postgres_etl = """
        CALL postgres_execute('postgres_db', 'DROP TABLE IF EXISTS {schema}.eco2mix_balances_temp')
        --END STATEMENT--

        CALL postgres_execute(
            'postgres_db',
            'CREATE TABLE IF NOT EXISTS {schema}.eco2mix_balances (
                perimeter VARCHAR(255),
                nature VARCHAR(255),
                for_date TIMESTAMP WITHOUT TIME ZONE,
                consumption DOUBLE PRECISION,
                forecast_j_minus_1 DOUBLE PRECISION,
                forecast_j DOUBLE PRECISION,
                fuel_oil DOUBLE PRECISION,
                coal DOUBLE PRECISION,
                gas DOUBLE PRECISION,
                nuclear DOUBLE PRECISION,
                wind DOUBLE PRECISION,
                solar DOUBLE PRECISION,
                hydro DOUBLE PRECISION,
                pumped_storage DOUBLE PRECISION,
                bioenergy DOUBLE PRECISION,
                border_flows DOUBLE PRECISION,
                co2_rate DOUBLE PRECISION,
                uk_flows DOUBLE PRECISION,
                spain_flows DOUBLE PRECISION,
                italy_flows DOUBLE PRECISION,
                switzerland_flows DOUBLE PRECISION,
                germany_belgium_flows DOUBLE PRECISION,
                fuel_oil_tac DOUBLE PRECISION,
                fuel_oil_cogen DOUBLE PRECISION,
                fuel_oil_other DOUBLE PRECISION,
                gas_tac DOUBLE PRECISION,
                gas_cogen DOUBLE PRECISION,
                gas_ccgt DOUBLE PRECISION,
                gas_other DOUBLE PRECISION,
                hydro_run_of_river DOUBLE PRECISION,
                hydro_reservoir DOUBLE PRECISION,
                hydro_wwtp_turbines DOUBLE PRECISION,
                bioenergy_waste DOUBLE PRECISION,
                bioenergy_biomass DOUBLE PRECISION,
                bioenergy_biogas DOUBLE PRECISION,
                battery_storage DOUBLE PRECISION,
                battery_clearance DOUBLE PRECISION,
                onshore_wind DOUBLE PRECISION,
                offshore_wind DOUBLE PRECISION,
                corrected_consumption DOUBLE PRECISION,
                PRIMARY KEY (perimeter, nature, for_date)
            )'
        )
        --END STATEMENT--

        CREATE TABLE postgres_db.{schema}.eco2mix_balances_temp 
        AS FROM memory.eco2mix_cleaned
        --END STATEMENT--

        CALL postgres_execute(
            'postgres_db',
            'INSERT INTO {schema}.eco2mix_balances 
             SELECT * FROM {schema}.eco2mix_balances_temp 
             ON CONFLICT (perimeter, nature, for_date) DO UPDATE 
             SET consumption = EXCLUDED.consumption,
                 forecast_j_minus_1 = EXCLUDED.forecast_j_minus_1,
                 forecast_j = EXCLUDED.forecast_j,
                 fuel_oil = EXCLUDED.fuel_oil,
                 coal = EXCLUDED.coal,
                 gas = EXCLUDED.gas,
                 nuclear = EXCLUDED.nuclear,
                 wind = EXCLUDED.wind,
                 solar = EXCLUDED.solar,
                 hydro = EXCLUDED.hydro,
                 pumped_storage = EXCLUDED.pumped_storage,
                 bioenergy = EXCLUDED.bioenergy,
                 border_flows = EXCLUDED.border_flows,
                 co2_rate = EXCLUDED.co2_rate,
                 uk_flows = EXCLUDED.uk_flows,
                 spain_flows = EXCLUDED.spain_flows,
                 italy_flows = EXCLUDED.italy_flows,
                 switzerland_flows = EXCLUDED.switzerland_flows,
                 germany_belgium_flows = EXCLUDED.germany_belgium_flows,
                 fuel_oil_tac = EXCLUDED.fuel_oil_tac,
                 fuel_oil_cogen = EXCLUDED.fuel_oil_cogen,
                 fuel_oil_other = EXCLUDED.fuel_oil_other,
                 gas_tac = EXCLUDED.gas_tac,
                 gas_cogen = EXCLUDED.gas_cogen,
                 gas_ccgt = EXCLUDED.gas_ccgt,
                 gas_other = EXCLUDED.gas_other,
                 hydro_run_of_river = EXCLUDED.hydro_run_of_river,
                 hydro_reservoir = EXCLUDED.hydro_reservoir,
                 hydro_wwtp_turbines = EXCLUDED.hydro_wwtp_turbines,
                 bioenergy_waste = EXCLUDED.bioenergy_waste,
                 bioenergy_biomass = EXCLUDED.bioenergy_biomass,
                 bioenergy_biogas = EXCLUDED.bioenergy_biogas,
                 battery_storage = EXCLUDED.battery_storage,
                 battery_clearance = EXCLUDED.battery_clearance,
                 onshore_wind = EXCLUDED.onshore_wind,
                 offshore_wind = EXCLUDED.offshore_wind,
                 corrected_consumption = EXCLUDED.corrected_consumption'
        )
        --END STATEMENT--

        CALL postgres_execute('postgres_db', 'DROP TABLE {schema}.eco2mix_balances_temp')
        --END STATEMENT--
        """
    stmt_duckdb_etl = template_duckdb_etl.format(path_glob=path_glob)
    stmt_postgres_etl = template_postgres_etl.format(schema=MYSQL_SCHEMA)
    with duckdb_postgres.get_db_connection(MYSQL_SCHEMA) as conn:
        conn.validate_mysql_schema()
        conn.execute(stmt_duckdb_etl)
        conn.execute(stmt_postgres_etl)


@asset(tags={"storage": "postgres"}, group_name=ASSETS_GROUP)
def rte_generation_byunit(
    context: AssetExecutionContext, postgres: PostgresResource, config: RteObservationConfig
):

    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)
    dates = pd.date_range(
        start_date, min(end_date, date.today() - timedelta(days=0)), freq="D"
    )
    dates = [x.date() for x in list(dates)]

    token_type, access_token = rte.get_token(rte_id, rte_secret)
    postgres_db = postgres.get_db_connection()
    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS rte.rte_query_generation_byunit (
            production_type VARCHAR(255),
            unit_name VARCHAR(255),
            unit_code VARCHAR(255),
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            updated_date TIMESTAMP,
            value BIGINT,
            PRIMARY KEY (unit_code, start_date)
        )
    """
    with postgres_db as db:
        db.execute_statements(stmt_create_table)
        for dt in dates:
            df = rte.query_generation_byunit(token_type, access_token, dt, dt)
            db.write_dataframe(df, MYSQL_SCHEMA, "rte_query_generation_byunit")


@asset(tags={"storage": "postgres"}, group_name=ASSETS_GROUP)
def rte_generation_byfuel_15min(
    context: AssetExecutionContext, postgres: PostgresResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.generation_by_fuel_15min (
                start_date VARCHAR(255),
                end_date VARCHAR(255),
                updated_date VARCHAR(255),
                value DOUBLE PRECISION,
                production_type VARCHAR(255),
                production_subtype VARCHAR(255),
                PRIMARY KEY (production_type, production_subtype, start_date)
            )
        """
        db.execute_statements(stmt_create_table)

        token_type, access_token = rte.get_token(rte_id, rte_secret)
        for dt in date_range:
            try:
                df = rte.query_generation_mix_15min(token_type, access_token, dt)
                db.write_dataframe(df, MYSQL_SCHEMA, "generation_by_fuel_15min")
            except TypeError:
                logger.warning(f"No data for {dt}")
                continue


@asset(tags={"storage": "postgres"}, group_name=ASSETS_GROUP)
def rte_generation_byfuel(
    context: AssetExecutionContext, postgres: PostgresResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)

    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.generation_by_fuel (
                production_type VARCHAR(255),
                start_date VARCHAR(255),
                end_date VARCHAR(255),
                updated_date VARCHAR(255),
                value FLOAT,
                PRIMARY KEY (production_type, start_date)
            )
        """
        db.execute_statements(stmt_create_table)
        token_type, access_token = rte.get_token(rte_id, rte_secret)
        df = rte.query_generation_bytype(token_type, access_token, start_date, end_date)
        db.write_dataframe(df, MYSQL_SCHEMA, "generation_by_fuel")


@asset(tags={"storage": "postgres"}, group_name=ASSETS_GROUP)
def rte_realtime_consumption_raw(
    context: AssetExecutionContext, postgres: PostgresResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)

    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.realtime_consumption_raw (
                data_type VARCHAR(255),
                updated_date VARCHAR(255),
                start_date VARCHAR(255),
                end_date VARCHAR(255),
                value DOUBLE PRECISION,
                PRIMARY KEY (start_date)
            )
        """
        db.execute_statements(stmt_create_table)
        token_type, access_token = rte.get_token(rte_id, rte_secret)
        df = rte.query_realtime_consumption(
            token_type, access_token, start_date, end_date, "observed"
        )
        db.write_dataframe(df, MYSQL_SCHEMA, "realtime_consumption_raw")


@asset(tags={"storage": "postgres"}, group_name=ASSETS_GROUP)
def rte_exchange_phys_flows(
    context: AssetExecutionContext, postgres: PostgresResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)

    countries = list(rte.EXCHANGE_COUNTERPARTIES.keys())
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.exchange_phys_flows (
                counterparty VARCHAR(10),
                for_date TIMESTAMP,
                imports FLOAT,
                exports FLOAT,
                net_imports FLOAT,
                PRIMARY KEY (counterparty, for_date)
            )
        """
        db.execute_statements(stmt_create_table)
        token_type, access_token = rte.get_token(rte_id, rte_secret)
        for country in countries:
            if country == "GB":
                continue
            df = rte.query_physical_flows(
                token_type, access_token, start_date, end_date, country
            )
            db.write_dataframe(df, MYSQL_SCHEMA, "exchange_phys_flows")
