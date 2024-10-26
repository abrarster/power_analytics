from dagster import asset, DailyPartitionsDefinition, AssetExecutionContext
from eupower_core.dagster_resources import FilesystemResource
from eupower_core.scrapes.rte import download_rte_generation_mix, RTE_REGIONS

MYSQL_SCHEMA = "rte"


@asset(partitions_def=DailyPartitionsDefinition("2018-01-01"))
def eco2mix_generation_raw(context: AssetExecutionContext, fs: FilesystemResource):
    as_of_date = context.partition_time_window.start.date()
    writer = fs.get_writer(f"rte/eco2mix/raw/{as_of_date.strftime('%Y-%m-%d')}")
    for region in RTE_REGIONS:
        df = download_rte_generation_mix(as_of_date, region)
        writer.write_file(f"{region}.csv", df)
