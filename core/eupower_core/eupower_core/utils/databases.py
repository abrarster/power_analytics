from __future__ import annotations

import random
import string
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class MySqlDb:

    def __init__(
        self, username: str, password: str, host: str = "localhost", port="3306"
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.engine: Engine | None = None

    def __enter__(self):
        connection_string = f"mysql+mysqlconnector://{self.username}:{self.password}@{self.host}:{self.port}"
        self.engine = create_engine(connection_string)
        self.conn = self.engine.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()
        self.engine.dispose()
        self.conn = None
        self.engine = None

    @property
    def databases(self) -> list[str]:
        if self.conn is None:
            raise Exception("Must access property in context handler")
        result = self.conn.execute(text("SHOW DATABASES"))
        rows = result.fetchall()
        return [x[0] for x in rows]

    def execute_statements(self, query: str) -> None:
        statements = query.split("--END STATEMENT--")
        trans = self.conn.begin()  # Start a transaction
        try:
            for statement in statements:
                formatted_stmt = statement.replace("--END STATEMENT--", "").strip()
                if len(formatted_stmt) < 3:
                    continue
                self.conn.execute(text(formatted_stmt))
            trans.commit()  # Commit the entire transaction

        except Exception as e:
            trans.rollback()  # Rollback on error
            raise e  # Re-raise the exception after rollback

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        if self.conn is None:
            raise Exception("Must access property in context handler")
        return pd.read_sql(query, self.conn)

    def write_dataframe(
        self, df: pd.DataFrame, database_name: str, table_name: str, upsert: bool = True
    ) -> None:
        if not upsert:
            df.to_sql(
                table_name,
                self.conn,
                schema=database_name,
                if_exists="replace",
                index=False,
            )
        else:
            temp_table_name = f"{table_name}_{generate_random_string()}"
            stmt_create_temp_table = f"CREATE TABLE {database_name}.{temp_table_name} LIKE {database_name}.{table_name}"
            self.execute_statements(stmt_create_temp_table)
            df.to_sql(
                temp_table_name,
                self.conn,
                schema=database_name,
                if_exists="replace",
                index=False,
            )
            stmt_replace = f"""
                REPLACE INTO {database_name}.{table_name}
                SELECT * FROM {database_name}.{temp_table_name}
                --END STATEMENT--
                """
            stmt_drop_temp_table = f"""
                DROP TABLE {database_name}.{temp_table_name}
                --END STATEMENT--
            """
            self.execute_statements(stmt_replace)
            self.execute_statements(stmt_drop_temp_table)


def generate_random_string(length: int = 6) -> str:
    characters = string.ascii_letters + string.digits
    return "".join(random.choice(characters) for _ in range(length))
