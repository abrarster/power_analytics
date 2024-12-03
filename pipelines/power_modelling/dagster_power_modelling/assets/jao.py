import dagster
import dlt
import dagster_embedded_elt.dlt as ddlt
from collections.abc import Iterable
from datetime import date
from eupower_core.dlt.jao import jao_sources
from eupower_core.dagster_resources import FilesystemResource
from .asset_groups import JAO

jao_upstream_asset = dagster.SourceAsset("JAO_api", group_name=JAO)


class CustomDagsterDltTranslator(ddlt.DagsterDltTranslator):
    def get_asset_key(self, resource: ddlt.DagsterDltResource) -> dagster.AssetKey:
        return dagster.AssetKey(f"{resource.source_name}")

    def get_deps_asset_keys(
        self, resource: ddlt.DagsterDltResource
    ) -> Iterable[dagster.AssetKey]:
        return [dagster.AssetKey("JAO_api")]


# Create a dictionary to store pipeline instances
_pipeline_cache = {}


def create_jao_asset(resource_name: str):
    """Creates a DLT asset for a specific JAO resource."""

    # Get or create pipeline
    # print('HERE')
    # print(resource_name)
    if resource_name not in _pipeline_cache:
        _pipeline_cache[resource_name] = dlt.pipeline(
            pipeline_name=f"jao_{resource_name}",
            destination="postgres",
            dataset_name="jao",
            progress="log",
        )

    @ddlt.dlt_assets(
        name=resource_name,
        dlt_source=jao_sources[resource_name](),
        dlt_pipeline=_pipeline_cache[resource_name],
        group_name=JAO,
        partitions_def=dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
        dagster_dlt_translator=CustomDagsterDltTranslator(),
    )
    def _jao_asset(
        context: dagster.AssetExecutionContext,
        dlt_resource: ddlt.DagsterDltResource,
        fs: FilesystemResource,
    ):
        as_of_date = date.fromisoformat(context.partition_key).strftime("%Y-%m-%d")
        fs_root_path = fs.get_writer("jao").base_path
        yield from dlt_resource.run(
            context=context,
            dlt_source=jao_sources[resource_name](
                as_of_date=as_of_date,
                root_dir=fs_root_path,
                resource_logger=context.log,
            ),
        )

    return _jao_asset


# Create assets for each JAO source
jao_assets = [create_jao_asset(name) for name in jao_sources.keys()]

# # Create source assets for each JAO asset
# jao_source_assets = [
#     dagster.SourceAsset(
#         key,
#         group_name=JAO,
#         partitions_def=dagster.DailyPartitionsDefinition(start_date="2024-01-01"),
#     )
#     for asset in jao_assets
#     for key in asset.dependency_keys
# ]
