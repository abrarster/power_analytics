from __future__ import annotations

import duckdb
import logging
from dagster import ConfigurableResource
from typing import Any, Dict, Optional, Generator
from contextlib import contextmanager
from dagster._utils.backoff import backoff
from pydantic import Field
from eupower_core.utils.databases import MySqlDb


logger = logging.getLogger(__name__)


class MySqlResource(ConfigurableResource):
    mysql_host: str = Field(default="localhost")
    mysql_port: int = Field(default=3306)
    mysql_user: str = Field(default="root")
    mysql_password: str = Field(default="")

    def get_db_connection(self) -> MySqlDb:
        return MySqlDb(
            self.mysql_user, self.mysql_password, self.mysql_host, self.mysql_port
        )


class DuckDBtoMySqlResource(ConfigurableResource):

    mysql_host: str = Field(default="localhost")
    mysql_port: int = Field(default=3306)
    mysql_user: str = Field(default="root")
    mysql_password: str = Field(default="")

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
    def get_db_connection(self, mysql_schema: str) -> Generator[DuckDbMySqlConnection, None, None]:
        conn = backoff(
            fn=DuckDbMySqlConnection,
            retry_on=(RuntimeError, duckdb.IOException),
            kwargs={
                "mysql_schema": mysql_schema,
                "database": self.duckdb_database,
                "read_only": False,
                "config": {
                    "custom_user_agent": "dagster",
                    **self.duckdb_connection_config,
                },
            },
            max_retries=10,
        )

        stmt_install = "INSTALL mysql"
        stmt_attach = f"host={self.mysql_host} user={self.mysql_user} port={self.mysql_port} password={self.mysql_password}".replace(
            "'", ""
        )
        stmt_attach = f"ATTACH '{stmt_attach}' as mysql_db (TYPE mysql_scanner)"
        conn.sql(stmt_install)
        conn.sql(stmt_attach)

        yield conn

        conn.close()


class DuckDbMySqlConnection:

    def __init__(
        self,
        mysql_schema: str,
        database: Optional[str] = None,
        read_only: bool = False,
        config: Optional[Dict] = None,
    ):
        self.mysql_schema = mysql_schema
        self.duckdb_conn = duckdb.connect(database, read_only, config)

    def validate_mysql_schema(self):
        stmt_information_schema = "SELECT * FROM mysql_query('mysql_db', 'SELECT SCHEMA_NAME FROM information_schema.SCHEMATA')"
        existing_schemas = [
            x[0] for x in self.duckdb_conn.sql(stmt_information_schema).fetchall()
        ]
        if self.mysql_schema not in existing_schemas:
            stmt_create_schema = f"CREATE SCHEMA mysql_db.{self.mysql_schema}"
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
