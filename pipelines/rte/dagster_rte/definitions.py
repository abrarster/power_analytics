import warnings
from dagster import Definitions, load_assets_from_modules, EnvVar, ExperimentalWarning
from eupower_core.dagster_resources import (
    FilesystemResource,
    DuckDBtoMySqlResource,
    MySqlResource,
    PostgresResource,
    DuckDBtoPostgresResource,
)
from . import assets
from .jobs import download_job

warnings.filterwarnings("ignore", category=ExperimentalWarning)

FS_CONFIG = {"root_folder": EnvVar("FSRESOURCE_ROOT")}
RESOURCES = {
    "fs": FilesystemResource(**FS_CONFIG),
    "duckdb_mysql": DuckDBtoMySqlResource(
        mysql_user=EnvVar("MYSQL_USER"), mysql_password=EnvVar("MYSQL_PWD")
    ),
    "mysql": MySqlResource(
        mysql_user=EnvVar("MYSQL_USER"), mysql_password=EnvVar("MYSQL_PWD")
    ),
    "postgres": PostgresResource(
        user=EnvVar("POSTGRES_USER"), password=EnvVar("POSTGRES_PWD")
    ),
    "duckdb_postgres": DuckDBtoPostgresResource(
        user=EnvVar("POSTGRES_USER"), password=EnvVar("POSTGRES_PWD")
    ),
}

rte_assets = load_assets_from_modules([assets])
defs = Definitions(assets=rte_assets, resources=RESOURCES, jobs=[download_job])
