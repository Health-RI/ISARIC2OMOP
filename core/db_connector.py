import logging
import pandas as pd
import psycopg2
from contextlib import closing
from sqlalchemy import create_engine, MetaData
from typing import List

logger = logging.getLogger(__name__)


class PostgresController:
    """Class to manage DB connection"""
    def __init__(self, db_name, postgres_connection, staging_schema, chunksize=None):
        self.db_name = db_name
        self.engine = create_engine("postgresql+psycopg2://" + postgres_connection)
        self.schema = staging_schema
        self.chunksize = chunksize or None
        self.connect()
        self.metadata = MetaData(self.engine)

    def connect(self):
        """Connect to postgres DB"""
        try:
            with closing(self.engine.connect()) as conn:
                conn.execute("Select 1")
        except psycopg2.DatabaseError as e:
            logger.error(f"Could not connect to {self.db_name} database. Please check connection settings.")
            raise e

    def set_working_schema(self, schema: str):
        """Sets schema to work, by default it is the one passed in class constructor"""
        self.schema = schema

    def postgres_fetch(self, query: str, column_names: List = None) -> pd.DataFrame:
        """fetches query result as pandas dataframe. If column are s"""
        result = pd.DataFrame(data=self.engine.execute(query).fetchall())
        if column_names and not result.empty:
            result.columns = column_names
        return result

    def df_to_postgres(self, table: str, df: pd.DataFrame):
        """
        Saves data from a pandas dataframe to database
        :param table: str, target table name
        :param df: pd.DataFrame, data to save
        """
        df.to_sql(table, schema=self.schema, con=self.engine, index=False, if_exists="append", chunksize=self.chunksize)
