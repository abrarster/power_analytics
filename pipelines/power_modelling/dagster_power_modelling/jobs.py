import dagster
from . import assets


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
