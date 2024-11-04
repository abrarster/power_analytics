import dagster
import pandas as pd
import entsoe.mappings as mappings
from eupower_core.dagster_resources import PostgresResource
from .constants import ASSET_GROUP

SCHEMA = "entsoe"


@dagster.asset(group_name=ASSET_GROUP, tags={"storage": "postgres"})
def entsoe_areas(postgres: PostgresResource):
    df = pd.DataFrame(
        [(x.name, x.value, x.meaning, x.tz) for x in mappings.Area],
        columns=["name", "code", "meaning", "tz"],
    )
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "entsoe", "entsoe_areas", upsert=False)


@dagster.asset(group_name=ASSET_GROUP, tags={"storage": "postgres"})
def entsoe_psr_types(postgres: PostgresResource):
    df = _dict_to_frame(mappings.PSRTYPE_MAPPINGS, ["fuel_code", "long_name"])
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "entsoe", "psr_types", upsert=False)


@dagster.asset(group_name=ASSET_GROUP, tags={"storage": "postgres"})
def entsoe_doc_status(postgres: PostgresResource):
    df = _dict_to_frame(mappings.DOCSTATUS, ["doc_status_code", "meaning"])
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "entsoe", "doc_status", upsert=False)


@dagster.asset(group_name=ASSET_GROUP, tags={"storage": "postgres"})
def entsoe_business_types(postgres: PostgresResource):
    df = _dict_to_frame(mappings.BSNTYPE, ["code", "meaning"])
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "entsoe", "business_types", upsert=False)


@dagster.asset(group_name=ASSET_GROUP, tags={"storage": "postgres"})
def entsoe_document_types(postgres: PostgresResource):
    df = _dict_to_frame(mappings.DOCUMENTTYPE, ["code", "meaning"])
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "entsoe", "document_types", upsert=False)


@dagster.asset(group_name=ASSET_GROUP, tags={"storage": "postgres"})
def entsoe_process_types(postgres: PostgresResource):
    df = _dict_to_frame(mappings.PROCESSTYPE, ["code", "meaning"])
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "entsoe", "process_types", upsert=False)


def _dict_to_frame(d: dict, columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(list(d.items()), columns=columns)
