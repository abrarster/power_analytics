import dagster
from dagster_dbt import DbtCliResource, dbt_assets


def create_dbt_asset(name: str, select_key: str):
    """Factory function to create dbt assets with consistent configuration."""

    @dbt_assets(
        manifest="/Users/abrar/Python/power_analytics/dbt_pipelines/target/manifest.json",
        select=select_key,
        name=name,  # This is sufficient for Dagster's asset identification
    )
    def dbt_asset(context: dagster.AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    return dbt_asset


# Create assets using the factory
elia_dbt_assets = create_dbt_asset("elia_dbt_assets", "scrapes.elia")
historic_balances_dbt_assets = create_dbt_asset(
    "historic_balances_dbt_assets", "historic_balances"
)
transmission_assets = create_dbt_asset("transmission_assets", "transmission")
