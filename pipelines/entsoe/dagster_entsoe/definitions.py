import warnings
import dagster
from eupower_core.dagster_resources import FilesystemResource, MySqlResource
from . import assets, mapping_tables
from . import jobs

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

FS_CONFIG = {"root_folder": dagster.EnvVar("FSRESOURCE_ROOT")}
RESOURCES = {
    "fs": FilesystemResource(**FS_CONFIG),
    "mysql": MySqlResource(
        mysql_user=dagster.EnvVar("MYSQL_USER"),
        mysql_password=dagster.EnvVar("MYSQL_PWD"),
    ),
}

entsoe_assets = dagster.load_assets_from_modules([assets, mapping_tables])
defs = dagster.Definitions(
    assets=entsoe_assets,
    resources=RESOURCES,
    jobs=[
        jobs.job_gen_by_fuel,
        jobs.job_gen_by_unit,
        jobs.job_demand,
        jobs.job_crossborder_flows,
        jobs.job_mapping_tables,
    ],
)