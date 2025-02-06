import logging

import pandas as pd
from pandas import DataFrame

from database.database_engine import DatabaseEngine

o_logger = logging.getLogger(__name__)


class SqlRequests(DatabaseEngine):

    def __post_init__(self) -> None:
        super().__post_init__()

    def insert_dataframe_into_database(self, df: DataFrame) -> None:
        """
        Insert a DataFrame into the database
        :param df: DataFrame
        :return: None
        """
        with self.o_database_engine.connect() as o_connection:
            try:
                df.to_sql(self.s_table_name, o_connection, if_exists='append', index=False)
            except Exception as o_exception:
                o_logger.error(f"Failed to insert data into the database: {o_exception}")

    def get_all_distinct_primary_keys(self) -> list[list[str]]:
        """
        Get all distinct primary keys from the database
        :return: list[list[str]]
        """
        s_query = f"SELECT DISTINCT business_id FROM {self.s_table_name};"
        return pd.read_sql_query(s_query, self.o_database_engine).values.tolist()

    def get_all_distinct_urls(self) -> list[str]:
        """
        Get all distinct urls from the database
        :return: list[str]
        """
        s_query = f"SELECT DISTINCT url FROM {self.s_table_name};"
        return pd.read_sql_query(s_query, self.o_database_engine).values.tolist()
