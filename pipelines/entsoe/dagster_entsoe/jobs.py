import dagster
from dagster_entsoe import assets


job_gen_by_fuel = dagster.define_asset_job(
    "Scrape_gen_by_fuel",
    selection=dagster.AssetSelection.assets(
        assets.entsoe_generation_by_fuel_raw, assets.entsoe_generation_by_fuel
    ),
)
