import time
import threading
import pandas as pd
from datetime import datetime
from dagster import (
    ConfigurableResource,
    InitResourceContext,
    resource,
    IOManager,
    InputContext,
    OutputContext,
    ConfigurableIOManager,
)
from dagster_embedded_elt.dlt import DagsterDltResource
from typing import Optional, Any
from pydantic import Field
from eupower_core.utils.databases import PostgresDb


class RateLimiterClient:
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self._lock = threading.Lock()
        self._calls = []

    def wait_for_capacity(self):
        with self._lock:
            now = datetime.now()
            # Remove calls older than 1 minute
            self._calls = [t for t in self._calls if (now - t).total_seconds() < 60]

            if len(self._calls) >= self.calls_per_minute:
                # Wait until the oldest call is more than 1 minute old
                sleep_time = 60 - (now - self._calls[0]).total_seconds()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # Clear old calls after waiting
                self._calls = [t for t in self._calls if (now - t).total_seconds() < 60]

            self._calls.append(now)


class RateLimiter(ConfigurableResource):
    calls_per_minute: int = 200
    _client: Optional[RateLimiterClient] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """This method is called by Dagster during resource initialization"""
        self._client = RateLimiterClient(calls_per_minute=self.calls_per_minute)

    def wait_for_capacity(self):
        """Delegate to the client's wait_for_capacity method"""
        if self._client is None:
            raise RuntimeError("RateLimiter client not initialized")
        return self._client.wait_for_capacity()


class PostgresIOManager(ConfigurableIOManager):
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    user: str = Field(default="postgres")
    password: str = Field(default="")

    def _get_db_connection(self) -> PostgresDb:
        return PostgresDb(self.user, self.password, self.host, self.port)

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handles saving the output to Postgres"""
        table_name = context.asset_key.path[-1]
        schema = (
            context.asset_key.path[-2] if len(context.asset_key.path) > 1 else "public"
        )

        with self._get_db_connection() as db:
            if isinstance(obj, pd.DataFrame):
                # Handle DataFrame
                obj.to_sql(
                    name=table_name,
                    con=db.engine,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                )
            else:
                # For other types, you might want to implement different storage logic
                raise NotImplementedError(
                    f"Storage of type {type(obj)} not supported by PostgresIOManager"
                )

    def load_input(self, context: InputContext) -> Any:
        """Loads input from Postgres"""
        table_name = context.asset_key.path[-1]
        schema = (
            context.asset_key.path[-2] if len(context.asset_key.path) > 1 else "public"
        )

        with self._get_db_connection() as db:
            # By default, load as DataFrame
            df = pd.read_sql_table(table_name=table_name, schema=schema, con=db.engine)
            return df


@resource(config_schema={"connection_url": str})
def postgres_io_manager(init_context):
    return PostgresIOManager(
        connection_url=init_context.resource_config["connection_url"],
    )

dlt_resource = DagsterDltResource()