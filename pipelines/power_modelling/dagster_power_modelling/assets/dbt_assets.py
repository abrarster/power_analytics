import dagster
from dagster_dbt import DbtCliResource, dbt_assets


@dbt_assets(
    manifest="/Users/abrar/Python/power_analytics/dbt_pipelines/target/manifest.json",
    select="scrapes.elia",  # This selects all models that depend on elia sources
)
def elia_dbt_assets(context: dagster.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dbt_assets(
    manifest="/Users/abrar/Python/power_analytics/dbt_pipelines/target/manifest.json",
    select="historic_balances",  # This selects all models under historic_balances
)
def historic_balances_dbt_assets(
    context: dagster.AssetExecutionContext, dbt: DbtCliResource
):
    yield from dbt.cli(["build"], context=context).stream()


# Update __all__ to include both asset functions
__all__ = ["elia_dbt_assets", "historic_balances_dbt_assets"]
