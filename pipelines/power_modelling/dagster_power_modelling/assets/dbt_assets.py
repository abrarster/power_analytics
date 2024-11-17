import dagster
import dagster_dbt
from dagster_dbt import DbtCliResource, dbt_assets

from . import elia  # Import the elia assets module

@dbt_assets(
    manifest="/Users/abrar/Python/power_analytics/dbt_pipelines/target/manifest.json",
    select="source:elia+"  # This selects all models that depend on elia sources
)
def elia_dbt_assets(context: dagster.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context)

# Add this to __all__ in the assets/__init__.py
__all__ = ["elia_dbt_assets"]
