import pandas as pd
from datetime import date, timedelta
from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext, EnvVar
from eupower_core.dagster_resources import FilesystemResource, MySqlResource
from eupower_core.scrapes import rte

MYSQL_SCHEMA = "rte"
ASSETS_GROUP = "rte"


@asset(partitions_def=DailyPartitionsDefinition("2018-01-01"), group_name=ASSETS_GROUP)
def eco2mix_generation_raw(context: AssetExecutionContext, fs: FilesystemResource):
    as_of_date = context.partition_time_window.start.date()
    writer = fs.get_writer(f"rte/eco2mix/raw/{as_of_date.strftime('%Y-%m-%d')}")
    for region in rte.RTE_REGIONS:
        df = rte.download_rte_generation_mix(as_of_date, region)
        writer.write_file(f"{region}.csv", df)


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_generation_byunit(context: AssetExecutionContext, mysql: MySqlResource):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=5)
    end_date = date.today()
    token_type, access_token = rte.get_token(rte_id, rte_secret)
    result = rte.query_generation_byunit(token_type, access_token, start_date, end_date)

    mysql_db = mysql.get_db_connection()
    stmt_create_table = """
        CREATE TABLE IF NOT EXISTS rte.rte_query_generation_byunit (
            production_type VARCHAR(255),
            unit_name VARCHAR(255),
            unit_code VARCHAR(255),
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            updated_date TIMESTAMP,
            value BIGINT,
            CONSTRAINT pk_unit_date PRIMARY KEY (unit_code, start_date)
        )
    """
    with mysql_db as db:
        db.execute_statements(stmt_create_table)
        db.write_dataframe(result, MYSQL_SCHEMA, "rte_query_generation_byunit")


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_generation_byfuel_15min(context: AssetExecutionContext, mysql: MySqlResource):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=5)
    end_date = date.today()
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    mysql_db = mysql.get_db_connection()
    with mysql_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.generation_by_fuel_15min (
                production_type VARCHAR(255),
                production_subtype VARCHAR(255),
                start_date TIMESTAMP,
                mw FLOAT,
                CONSTRAINT pk_entry PRIMARY KEY (production_type, production_subtype, start_date)
            )
        """
        db.execute_statements(stmt_create_table)

        token_type, access_token = rte.get_token(rte_id, rte_secret)
        for dt in date_range:
            df = rte.query_generation_mix_15min(token_type, access_token, dt)
            db.write_dataframe(df, MYSQL_SCHEMA, "generation_by_fuel_15min")


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_generation_byfuel(context: AssetExecutionContext, mysql: MySqlResource):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=5)
    end_date = date.today()

    mysql_db = mysql.get_db_connection()
    with mysql_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.generation_by_fuel (
                production_type VARCHAR(255),
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                updated_date TIMESTAMP,
                value FLOAT,
                CONSTRAINT pk_fuel_date PRIMARY KEY (production_type, start_date)
            )
        """
        db.execute_statements(stmt_create_table)
        token_type, access_token = rte.get_token(rte_id, rte_secret)
        df = rte.query_generation_bytype(token_type, access_token, start_date, end_date)
        db.write_dataframe(df, MYSQL_SCHEMA, "generation_by_fuel")


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_realtime_consumption(context: AssetExecutionContext, mysql: MySqlResource):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=5)
    end_date = date.today()

    mysql_db = mysql.get_db_connection()
    with mysql_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.realtime_consumption (
                data_type VARCHAR(255),
                updated_date TIMESTAMP,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                value FLOAT,
                PRIMARY KEY start_date
            )
        """
        db.execute_statements(stmt_create_table)
        token_type, access_token = rte.get_token(rte_id, rte_secret)
        df = rte.query_realtime_consumption(
            token_type, access_token, start_date, end_date, "observed"
        )
        db.write_dataframe(df, MYSQL_SCHEMA, "realtime_consumption")


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_realtime_consumption(context: AssetExecutionContext, mysql: MySqlResource):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=5)
    end_date = date.today()

    countries = list(rte.EXCHANGE_COUNTERPARTIES.keys())
    mysql_db = mysql.get_db_connection()
    with mysql_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.exchange_phys_flows (
                counterparty VARCHAR(10),
                for_date TIMESTAMP,
                imports FLOAT,
                exports FLOAT,
                net_imports FLOAT
                CONSTRAINT pk_entry PRIMARY KEY (counterparty, for_date)
            )
        """
        db.execute_statements(stmt_create_table)
        token_type, access_token = rte.get_token(rte_id, rte_secret)
        for country in countries:
            df = rte.query_physical_flows(token_type, access_token, start_date, end_date, country)
            db.write_dataframe(df, MYSQL_SCHEMA, "exchange_phys_flows")
