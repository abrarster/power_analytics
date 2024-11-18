from dagster import IOManager, OutputContext
from .postgresql import PostgresResource


class PostgresQueryIOManager(IOManager):
    def __init__(self, postgres_resource: PostgresResource):
        self.postgres = postgres_resource

    def handle_output(self, context: OutputContext, obj: str):
        # Get partition key information if available
        partition_key = context.partition_key if context.has_partition_key else None

        # If the output is a SQL query string, execute it with parameters
        if isinstance(obj, str):
            with self.postgres.get_db_connection() as db:
                # Pass partition key as a parameter to the query
                params = {"partition_key": partition_key} if partition_key else {}
                db.execute_statements(obj, params=params)
