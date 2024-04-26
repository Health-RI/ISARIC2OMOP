from core.db_connector import PostgresController


def increment_last_id(table: str, column: str, postgres: PostgresController):
    query = f"SELECT MAX({column}) FROM {postgres.schema}.{table};"
    result = postgres.postgres_fetch(query)["max"].values.tolist()[0]
    if result is None:
        return 1
    return result + 1
