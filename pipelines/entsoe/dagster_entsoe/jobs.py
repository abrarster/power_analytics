import dagster
from dagster_entsoe import assets


job_gen_by_fuel = dagster.define_asset_job(
    "Scrape_gen_by_fuel",
    selection=dagster.AssetSelection.assets(
        assets.entsoe_generation_by_fuel_raw, assets.entsoe_generation_by_fuel
    ),
)

job_gen_by_unit = dagster.define_asset_job(
    "Scrape_gen_by_unit",
    selection=dagster.AssetSelection.assets(
        assets.entsoe_generation_by_unit_raw, assets.entsoe_generation_by_unit
    ),
)

job_demand = dagster.define_asset_job(
    "Scrape_demand",
    selection=dagster.AssetSelection.assets(
        assets.entsoe_demand_raw, assets.entsoe_demand
    ),
)

job_crossborder_flows = dagster.define_asset_job(
    "Scrape_crossborder_flows",
    selection=dagster.AssetSelection.assets(
        assets.entsoe_crossborder_flows_raw, assets.entsoe_crossborder_flows
    ),
)
