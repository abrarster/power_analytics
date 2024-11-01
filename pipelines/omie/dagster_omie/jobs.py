from dagster import AssetSelection, define_asset_job, ConfigMapping, Field, Shape
from . import assets
from .assets import OMIE_LOAD_ASSETS
from .partitions import monthly_partition

omie = AssetSelection.groups(OMIE_LOAD_ASSETS)
redelectrica = AssetSelection.assets(
    assets.esios_indicators_staging, assets.esios_indicators
)

# Omie
job_omie = define_asset_job(
    "Scrape_Omie", selection=omie, partitions_def=monthly_partition
)


# Redelectrica
redelectrica_config_schema = {
    "indicators": Field(str, default_value="REALTIME_HISTORY", is_required=False),
    "start_date": Field(str, is_required=True),
    "end_date": Field(str, is_required=True),
}


def create_redelectrica_config(config: dict) -> dict:
    return {
        "ops": {
            "esios_indicators_staging": {"config": config},
        }
    }


job_redelectrica = define_asset_job(
    "Scrape_Redelectrica",
    selection=redelectrica,
    config=ConfigMapping(
        config_schema=Shape(redelectrica_config_schema),
        config_fn=create_redelectrica_config,
    ),
)
