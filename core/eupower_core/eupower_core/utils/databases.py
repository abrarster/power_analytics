from __future__ import annotations

import random
import string
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from time import sleep


class BaseDb:

    stmt_create_table_like: str

    def __init__(
        self, username: str, password: str, host: str = "localhost", port="3306"
    ):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.conn = None
        self.engine: Engine | None = None

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
            stmt_create_temp_table = self.stmt_create_table_like.format(
                database_name=database_name,
                temp_table_name=temp_table_name,
                table_name=table_name,
            )
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
    return "".join(random.choice(characters) for _ in range(length)).lower()


class MySqlDb(BaseDb):

    stmt_create_table_like = "CREATE TABLE {database_name}.{temp_table_name} LIKE {database_name}.{table_name}"

    def __enter__(self):
        connection_string = f"mysql+mysqlconnector://{self.username}:{self.password}@{self.host}:{self.port}"
        self.engine = create_engine(connection_string)
        self.conn = self.engine.connect()
        return self

    

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
            stmt_create_temp_table = self.stmt_create_table_like.format(
                database_name=database_name,
                temp_table_name=temp_table_name,
                table_name=table_name,
            )
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


class PostgresDb(BaseDb):

    stmt_create_table_like = "CREATE TABLE {database_name}.{temp_table_name} (LIKE {database_name}.{table_name} INCLUDING ALL)"

    def __enter__(self):
        connection_string = f"postgresql+psycopg://{self.username}:{self.password}@{self.host}:{self.port}"
        self.engine = create_engine(connection_string)
        self.conn = self.engine.connect()
        return self

    def query_to_dataframe(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)
        
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
            stmt_create_temp_table = self.stmt_create_table_like.format(
                database_name=database_name,
                temp_table_name=temp_table_name,
                table_name=table_name,
            )
            self.execute_statements(stmt_create_temp_table)
            df.to_sql(
                temp_table_name,
                self.conn,
                schema=database_name,
                if_exists="replace",
                index=False,
            )

            # Get primary key info and non-pk columns
            pk_columns = self._get_primary_key_columns(database_name, table_name)
            non_pk_columns = self._get_non_pk_columns(database_name, table_name, pk_columns)
            update_clause = ", ".join(
                f"{col} = EXCLUDED.{col}" for col in non_pk_columns
            )

            stmt_replace = f"""
                INSERT INTO {database_name}.{table_name}
                SELECT * FROM {database_name}."{temp_table_name}"
                ON CONFLICT ({', '.join(pk_columns)})
                DO UPDATE SET {update_clause}
                --END STATEMENT--
            """
            stmt_drop_temp_table = f"""
                DROP TABLE {database_name}.{temp_table_name}
                --END STATEMENT--
            """
            print(stmt_replace)
            self.execute_statements(stmt_replace)
            self.execute_statements(stmt_drop_temp_table)

    def _get_primary_key_columns(self, database_name: str, table_name: str) -> list[str]:
        query = """
            SELECT array_agg(kcu.column_name::text) as key_columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = :schema
                AND tc.table_name = :table
            GROUP BY tc.constraint_name
        """
        with self.engine.connect() as connection:
            result = connection.execute(
                text(query),
                {"schema": database_name, "table": table_name}
            ).fetchone()
            if result is None:
                raise ValueError(f"No primary key found for table {database_name}.{table_name}")
            return result[0]
    
    def _get_non_pk_columns(self, database_name: str, table_name: str, pk_columns: list[str]) -> list[str]:
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = :schema 
            AND table_name = :table 
            AND column_name != ALL(:pk_columns)
        """
        with self.engine.connect() as connection:
            result = connection.execute(
                text(query), 
                {
                    "schema": database_name, 
                    "table": table_name,
                    "pk_columns": pk_columns
                }
            )
            return [row[0] for row in result]