import pandas as pd
import logging
import warnings
from datetime import date, timedelta
from dagster import (
    asset,
    DailyPartitionsDefinition,
    AssetExecutionContext,
    EnvVar,
    Config,
)
from pydantic import Field
from eupower_core.dagster_resources import FilesystemResource, MySqlResource
from eupower_core.scrapes import rte

warnings.simplefilter(action="ignore", category=FutureWarning)
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
    partitions_def=DailyPartitionsDefinition("2018-01-01"),
    group_name="partitioned_rte",
    tags={"storage": "filesystem"},
)
def eco2mix_generation_raw(context: AssetExecutionContext, fs: FilesystemResource):
    as_of_date = context.partition_time_window.start.date()
    writer = fs.get_writer(f"rte/eco2mix/raw/{as_of_date.strftime('%Y-%m-%d')}")
    for region in rte.RTE_REGIONS:
        df = rte.download_rte_generation_mix(as_of_date, region)
        writer.write_file(f"{region}.csv", df)


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_generation_byunit(
    context: AssetExecutionContext, mysql: MySqlResource, config: RteObservationConfig
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
        for dt in dates:
            df = rte.query_generation_byunit(token_type, access_token, dt, dt)
            db.write_dataframe(df, MYSQL_SCHEMA, "rte_query_generation_byunit")


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_generation_byfuel_15min(
    context: AssetExecutionContext, mysql: MySqlResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)
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
            try:
                df = rte.query_generation_mix_15min(token_type, access_token, dt)
                db.write_dataframe(df, MYSQL_SCHEMA, "generation_by_fuel_15min")
            except TypeError:
                logger.warning(f"No data for {dt}")
                continue


@asset(tags={"storage": "mysql"}, group_name=ASSETS_GROUP)
def rte_generation_byfuel(
    context: AssetExecutionContext, mysql: MySqlResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)

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
def rte_realtime_consumption(
    context: AssetExecutionContext, mysql: MySqlResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)

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
def rte_realtime_consumption(
    context: AssetExecutionContext, mysql: MySqlResource, config: RteObservationConfig
):
    rte_id = EnvVar("RTE_ID").get_value()
    rte_secret = EnvVar("RTE_SECRET").get_value()
    start_date = date.today() - timedelta(days=config.days_back)
    end_date = date.today() + timedelta(days=config.days_forward)

    countries = list(rte.EXCHANGE_COUNTERPARTIES.keys())
    mysql_db = mysql.get_db_connection()
    with mysql_db as db:
        stmt_create_table = f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_SCHEMA}.exchange_phys_flows (
                counterparty VARCHAR(10),
                for_date TIMESTAMP,
                imports FLOAT,
                exports FLOAT,
                net_imports FLOAT,
                CONSTRAINT pk_entry PRIMARY KEY (counterparty, for_date)
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
