import warnings
import dagster
import dagster_dbt
from eupower_core.dagster_resources import (
    FilesystemResource,
    PostgresResource,
)
from . import jobs
from . import assets
from .resources import RateLimiter, postgres_io_manager

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

DBT_PROJECT_DIR = "/Users/abrar/Python/power_analytics/dbt_pipelines"
DBT_PROFILES_DIR = "/Users/abrar/Python/power_analytics/dbt_pipelines/config"

resources = {
    "fs": FilesystemResource(root_folder=dagster.EnvVar("FSRESOURCE_ROOT")),
    "postgres": PostgresResource(
        user=dagster.EnvVar("POSTGRES_USER"), password=dagster.EnvVar("POSTGRES_PWD")
    ),
    "rate_limiter": RateLimiter(calls_per_minute=200),
    "dbt": dagster_dbt.DbtCliResource(
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
    ),
    "postgres_io": postgres_io_manager.configured(
        {"connection_url": "postgresql://..."}
    ),
}

all_assets = dagster.load_assets_from_package_module(assets)

defs = dagster.Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[
        jobs.elia_rt_history,
        jobs.elia_da_history,
        jobs.ren_history,
        jobs.ren_capacity,
    ],
)
