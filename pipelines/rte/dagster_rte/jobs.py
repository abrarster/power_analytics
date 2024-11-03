from dagster import (
    AssetSelection,
    define_asset_job,
    ConfigMapping,
    Field,
    Shape,
)
from dagster_rte import assets


def create_op_config(config: assets.RteObservationConfig) -> dict:
    return {
        "ops": {
            "rte_exchange_phys_flows": {
                "config": {
                    "days_back": config["days_back"],
                    "days_forward": config["days_forward"],
                }
            },
            "rte_generation_byfuel": {
                "config": {
                    "days_back": config["days_back"],
                    "days_forward": config["days_forward"],
                }
            },
            "rte_realtime_consumption_raw": {
                "config": {
                    "days_back": config["days_back"],
                    "days_forward": config["days_forward"],
                }
            },
            "rte_generation_byunit": {
                "config": {
                    "days_back": config["days_back"],
                    "days_forward": config["days_forward"],
                }
            },
            "rte_generation_byfuel_15min": {
                "config": {
                    "days_back": config["days_back"],
                    "days_forward": config["days_forward"],
                }
            },
            "eco2mix_generation_raw": {
                "config": {
                    "days_back": config["days_back"],
                    "days_forward": config["days_forward"],
                }
            },
        }
    }


rte_config_schema = {
    "days_back": Field(int, default_value=10, is_required=False),
    "days_forward": Field(int, default_value=0, is_required=False),
}

rte_core_assets = AssetSelection.assets(
    assets.rte_generation_byunit,
    assets.rte_realtime_consumption_raw,
    assets.rte_generation_byfuel_15min,
    assets.rte_generation_byfuel,
    assets.rte_exchange_phys_flows,
    assets.eco2mix_generation_raw,
    assets.eco2mix_balances,
)

download_job = define_asset_job(
    "Scrape_RTE",
    selection=rte_core_assets,
    config=ConfigMapping(
        config_schema=Shape(rte_config_schema), config_fn=create_op_config
    ),
)
