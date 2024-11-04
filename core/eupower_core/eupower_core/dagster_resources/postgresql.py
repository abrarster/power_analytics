from __future__ import annotations

import duckdb
import logging
from dagster import ConfigurableResource
from typing import Any, Dict, Optional, Generator
from contextlib import contextmanager
from dagster._utils.backoff import backoff
from pydantic import Field
from eupower_core.utils.databases import PostgresDb

logger = logging.getLogger(__name__)


class PostgresResource(ConfigurableResource):
    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    user: str = Field(default="postgres")
    password: str = Field(default="")

    def get_db_connection(self) -> PostgresDb:
        return PostgresDb(
            self.user, self.password, self.host, self.port
        )
    

class DuckDBtoPostgresResource(ConfigurableResource):

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    user: str = Field(default="postgres")
    password: str = Field(default="")

    duckdb_database: str = Field(
        description=(
            "Path to the DuckDB database. Setting database=':memory:' will use an in-memory"
            " database "
        ),
        default=":memory:",
    )
    duckdb_connection_config: Dict[str, Any] = Field(
        description=(
            "DuckDB connection configuration options. See"
            " https://duckdb.org/docs/sql/configuration.html"
        ),
        default={},
    )

    @contextmanager
    def get_db_connection(self, schema: str) -> Generator[DuckDbPostgresConnection, None, None]:
        conn = backoff(
            fn=DuckDbPostgresConnection,
            retry_on=(RuntimeError, duckdb.IOException),
            kwargs={
                "schema": schema,
                "database": self.duckdb_database,
                "read_only": False,
                "config": {
                    "custom_user_agent": "dagster",
                    **self.duckdb_connection_config,
                },
            },
            max_retries=10,
        )

        stmt_install = "INSTALL postgres"
        stmt_attach = f"host={self.host} user={self.user} port={self.port} password={self.password}".replace(
            "'", ""
        )
        stmt_attach = f"ATTACH '{stmt_attach}' as postgres_db (TYPE postgres_scanner)"
        conn.sql(stmt_install)
        conn.sql(stmt_attach)

        yield conn

        conn.close()


class DuckDbPostgresConnection:

    def __init__(
        self,
        schema: str,
        database: Optional[str] = None,
        read_only: bool = False,
        config: Optional[Dict] = None,
    ):
        self.schema = schema
        self.duckdb_conn = duckdb.connect(
            database or ":memory:", 
            read_only, 
            config or {}
        )

    def validate_mysql_schema(self):
        stmt_information_schema = "SELECT * FROM postgres_query('postgres_db', 'SELECT schema_name FROM information_schema.schemata')"
        existing_schemas = [
            x[0] for x in self.duckdb_conn.sql(stmt_information_schema).fetchall()
        ]
        if self.schema not in existing_schemas:
            stmt_create_schema = f"CREATE SCHEMA postgres_db.{self.schema}"
            self.duckdb_conn.execute(stmt_create_schema)

    def sql(self, query, *args, **kwargs):
        return self.duckdb_conn.sql(query, *args, **kwargs)

    def execute(self, query: str) -> None:
        cursor = self.duckdb_conn.cursor()
        statements = query.split("--END STATEMENT--")
        for stmt in statements:
            formatted_stmt = stmt.replace("--END STATEMENT--", "").strip()
            if len(formatted_stmt) < 3:
                continue
            logger.debug(formatted_stmt)
            cursor.execute(formatted_stmt)

    def close(self):
        self.duckdb_conn.close()