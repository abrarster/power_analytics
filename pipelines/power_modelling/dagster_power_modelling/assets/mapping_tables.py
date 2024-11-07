import os
import dagster
import pandas as pd
from eupower_core.dagster_resources import FilesystemResource, PostgresResource
from .asset_groups import MAPPING_TABLES


@dagster.asset(
    group_name=MAPPING_TABLES,
    tags={"storage": "postgres"},
)
def unit_mapping(
    context: dagster.AssetExecutionContext,
    fs: FilesystemResource,
    postgres: PostgresResource,
):
    reader = fs.get_reader("mapping_tables")
    file_path = os.path.join(reader.base_path, "unit_mapping.xlsx")
    df = read_sheet(file_path, "master")
    postgres_db = postgres.get_db_connection()
    with postgres_db as db:
        db.write_dataframe(df, "mapping_tables", "unit_mapping", upsert=False)


def read_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    return (
        pd.read_excel(file_path, sheet_name=sheet_name)
        .pipe(lambda df: df.rename(columns=lambda x: x.lower().replace(' ', '_')))
    )