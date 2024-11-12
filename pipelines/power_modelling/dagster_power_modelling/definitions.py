import warnings
import dagster
from eupower_core.dagster_resources import (
    FilesystemResource,
    PostgresResource,
)
from . import jobs
from . import assets

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

resources = {
    "fs": FilesystemResource(root_folder=dagster.EnvVar("FSRESOURCE_ROOT")),
    "postgres": PostgresResource(
        user=dagster.EnvVar("POSTGRES_USER"), password=dagster.EnvVar("POSTGRES_PWD")
    ),
}
assets = dagster.load_assets_from_package_module(assets)

defs = dagster.Definitions(
    assets=assets,
    resources=resources,
    jobs=[jobs.elia_rt_history, jobs.elia_da_history],
)
