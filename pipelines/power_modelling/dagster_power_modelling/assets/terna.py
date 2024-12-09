import dagster
import pandas as pd
from eupower_core.scrapes import terna as ts
from eupower_core.dagster_resources import FilesystemResource, PostgresResource
from .asset_groups import TERNA


@dagster.asset(
    group_name=TERNA,
    partitions_def=dagster.DailyPartitionsDefinition(start_date="2023-01-01"),
    tags={"storage": "filesystem"},
    kinds={"python", "file"},
)
def terna_detail_available_cap_fs(
    context: dagster.AssetExecutionContext, fs: FilesystemResource
):
    as_of_date = context.partition_time_window.start
    writer = fs.get_writer(f"terna/detail_available_cap/{as_of_date.strftime('%Y%m%d')}")
    writer.delete_data()
    client_id = dagster.EnvVar("TERNA_CLIENT_ID").get_value()
    client_secret = dagster.EnvVar("TERNA_CLIENT_SECRET").get_value()

    response = ts.get_detailed_available_capacity(client_id, client_secret)
    writer.write_file("detail_available_cap.json", response.content)

