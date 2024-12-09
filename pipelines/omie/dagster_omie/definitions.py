import warnings
from dagster import Definitions, load_assets_from_modules, EnvVar, ExperimentalWarning
warnings.filterwarnings("ignore", category=ExperimentalWarning)

from eupower_core.dagster_resources import (
    FilesystemResource,
    DuckDBtoMySqlResource,
    MySqlResource,
    PostgresResource,
    DuckDBtoPostgresResource,
)
from . import assets
from .jobs import job_omie, job_redelectrica



# Resource setup
RESOURCES = {
    "fs": FilesystemResource(root_folder=EnvVar("FSRESOURCE_ROOT")),
    "duckdb_mysql": DuckDBtoMySqlResource(
        mysql_user=EnvVar("MYSQL_USER"), mysql_password=EnvVar("MYSQL_PWD")
    ),
    "mysql": MySqlResource(
        mysql_user=EnvVar("MYSQL_USER"),
        mysql_password=EnvVar("MYSQL_PWD"),
    ),
    "postgres": PostgresResource(
        user=EnvVar("POSTGRES_USER"), password=EnvVar("POSTGRES_PWD")
    ),
    "duckdb_postgres": DuckDBtoPostgresResource(
        user=EnvVar("POSTGRES_USER"), password=EnvVar("POSTGRES_PWD")
    ),
}

omie_assets = load_assets_from_modules([assets])
all_jobs = [job_omie, job_redelectrica]

defs = Definitions(assets=omie_assets, resources=RESOURCES, jobs=all_jobs)
