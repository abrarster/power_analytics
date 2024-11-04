import dagster
from dagster_entsoe import assets, mapping_tables


job_gen_by_fuel = dagster.define_asset_job(
    "Scrape_gen_by_fuel",
    selection=dagster.AssetSelection.assets(
        assets.entsoe_generation_by_fuel_raw,
        assets.entsoe_generation_by_fuel,
        assets.fct_entsoe_generation_by_fuel,
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

job_mapping_tables = dagster.define_asset_job(
    "Build_mapping_tables",
    selection=dagster.AssetSelection.assets(
        mapping_tables.entsoe_areas,
        mapping_tables.entsoe_psr_types,
        mapping_tables.entsoe_doc_status,
        mapping_tables.entsoe_business_types,
        mapping_tables.entsoe_document_types,
        mapping_tables.entsoe_process_types,
    ),
)
