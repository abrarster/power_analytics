import os
from dagster import Definitions, load_assets_from_modules, EnvVar
from .assets import omie
from .resources import FilesystemResource, DuckDBtoMySqlResource
from .jobs import download_job

# Resource setup

FS_CONFIG = {"root_folder": EnvVar("FSRESOURCE_ROOT")}
RESOURCES = {
    "fs": FilesystemResource(**FS_CONFIG),
    "duckdb_mysql": DuckDBtoMySqlResource(
        mysql_user=EnvVar("MYSQL_USER"), mysql_password=EnvVar("MYSQL_PWD")
    ),
}

omie_assets = load_assets_from_modules([omie])
all_jobs = [download_job]

defs = Definitions(assets=omie_assets, resources=RESOURCES, jobs=all_jobs)
