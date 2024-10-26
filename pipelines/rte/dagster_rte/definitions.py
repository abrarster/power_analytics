from dagster import Definitions, load_assets_from_modules, EnvVar
from eupower_core.dagster_resources import FilesystemResource, DuckDBtoMySqlResource
from . import assets

FS_CONFIG = {"root_folder": EnvVar("FSRESOURCE_ROOT")}
RESOURCES = {
    "fs": FilesystemResource(**FS_CONFIG),
    "duckdb_mysql": DuckDBtoMySqlResource(
        mysql_user=EnvVar("MYSQL_USER"), mysql_password=EnvVar("MYSQL_PWD")
    ),
}

rte_assets = load_assets_from_modules([assets])
defs = Definitions(assets=rte_assets, resources=RESOURCES)
