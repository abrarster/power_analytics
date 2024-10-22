from dagster import AssetSelection, define_asset_job
from ..assets.omie import OMIE_LOAD_ASSETS
from ..partitions import monthly_partition

omie_load_assets = AssetSelection.groups(OMIE_LOAD_ASSETS)
download_job = define_asset_job(
    "download_job", selection=omie_load_assets, partitions_def=monthly_partition
)
