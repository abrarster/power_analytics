import dagster
from . import assets


def has_dbt_kind(asset):
    # Get the kind tag and convert to set for membership testing
    kinds_str = asset.tags.get("kind", "")
    if isinstance(kinds_str, str):
        kinds = {k.strip() for k in kinds_str.split(",")}
    else:
        kinds = set(kinds_str)
    return "dbt" in kinds


elia_rt_history = dagster.define_asset_job(
    "elia_rt_history_scrape",
    selection=dagster.AssetSelection.assets(
        assets.elia.elia_total_load_raw,
        assets.elia.elia_total_load,
        assets.elia.elia_grid_load_raw,
        assets.elia.elia_grid_load,
        assets.elia.elia_rt_gen_byfuel_raw,
        assets.elia.elia_rt_gen_byfuel,
        assets.elia.elia_wind_generation_hist_raw,
        assets.elia.elia_wind_generation_hist,
        assets.elia.elia_solar_generation_hist_raw,
        assets.elia.elia_solar_generation_hist,
        assets.elia.elia_itc_phys_flow_raw,
        assets.elia.elia_itc_phys_flow,
        assets.elia.elia_itc_total_comex_raw,
        assets.elia.elia_itc_total_comex,
    ),
)
elia_da_history = dagster.define_asset_job(
    "elia_da_history_scrape",
    selection=dagster.AssetSelection.assets(
        assets.elia.elia_da_gen_byfuel_raw,
        assets.elia.elia_da_gen_byfuel,
        assets.elia.elia_itc_da_comex_raw,
        assets.elia.elia_itc_da_comex,
    ),
)
ren_history = dagster.define_asset_job(
    "ren_history_scrape",
    selection=dagster.AssetSelection.assets(
        assets.ren.ren_power_balance_raw,
        assets.ren.ren_water_balance_raw,
        assets.ren.ren_hydro_production_raw,
        assets.ren.ren_power_balance,
        assets.ren.ren_water_balance,
        assets.ren.ren_hydro_production,
    ),
)
ren_capacity = dagster.define_asset_job(
    "ren_capacity_scrape",
    selection=dagster.AssetSelection.assets(
        assets.ren.ren_capacities_raw, assets.ren.ren_capacities
    ),
)
process_elia = dagster.define_asset_job(
    "process_elia",
    selection=dagster.AssetSelection.assets(
        # Use the full asset keys including the group
        ("elia", "stg_da_gen_byfuel"),
        ("elia", "stg_elia_solar_generation_hist"),
        ("elia", "stg_elia_wind_generation_hist"),
        ("elia", "stg_rt_gen_byfuel"),
        ("elia", "da_gen_byfuel_elia_to_entsoe"),
        ("elia", "elia_solar_generation_hist_clean"),
        ("elia", "elia_wind_generation_hist_clean"),
        ("elia", "rt_gen_byfuel_elia_to_entsoe"),
        ("elia", "elia_gen_byfuel_multisource"),
        ("elia", "stg_elia_grid_load"),
        ("elia", "stg_elia_total_load"),
        ("elia", "elia_demand"),
    )
)