from dagster import AssetSelection, define_asset_job
from .assets import ASSETS_GROUP

rte_assets = AssetSelection.groups(ASSETS_GROUP)
download_job = define_asset_job(
    "Scrape_RTE", selection=rte_assets
)
