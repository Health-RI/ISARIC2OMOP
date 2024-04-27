import pandas as pd

from core.db_connector import PostgresController


def increment_last_id(table: str, column: str, postgres: PostgresController):
    query = f"SELECT MAX({column}) FROM {postgres.schema}.{table};"
    result = postgres.postgres_fetch(query)["max"].values.tolist()[0]
    if result is None:
        return 1
    return result + 1


def prepare_autoincrement_index(df: pd.DataFrame,
                                table: str,
                                index_name: str,
                                postgres: PostgresController) -> pd.DataFrame:
    increment_by_index = increment_last_id(table, index_name, postgres)
    df.reset_index(drop=True, inplace=True)
    df.index += increment_by_index
    df.index.name = index_name
    df.reset_index(drop=False, inplace=True)
    return df
