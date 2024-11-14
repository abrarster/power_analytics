import os
import dagster
import pandas as pd
from eupower_core.dagster_resources import FilesystemResource, PostgresResource
from .asset_groups import MAPPING_TABLES


def create_mapping_asset(excel_file: str, sheet_name: str, table_name: str):
    """Factory function to create mapping table assets with common logic"""

    @dagster.asset(
        group_name=MAPPING_TABLES,
        tags={"storage": "postgres"},
        name=table_name,
    )
    def mapping_asset(
        context: dagster.AssetExecutionContext,
        fs: FilesystemResource,
        postgres: PostgresResource,
    ):
        reader = fs.get_reader("mapping_tables")
        file_path = os.path.join(reader.base_path, excel_file)
        df = read_sheet(file_path, sheet_name)
        with postgres.get_db_connection() as db:
            db.write_dataframe(df, "mapping_tables", table_name, upsert=False)

    return mapping_asset


def read_sheet(file_path: str, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(file_path, sheet_name=sheet_name).pipe(
        lambda df: df.rename(columns=lambda x: x.lower().replace(" ", "_"))
    )


# Create the assets using the factory function
unit_mapping = create_mapping_asset(
    excel_file="unit_mapping.xlsx", sheet_name="master", table_name="unit_mapping"
)

elia_fuel_codes = create_mapping_asset(
    excel_file="fuel_mappings.xlsx", sheet_name="elia", table_name="elia_fuel_codes"
)
