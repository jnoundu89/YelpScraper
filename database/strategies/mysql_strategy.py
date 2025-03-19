from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from database.strategies.base_strategy import DatabaseStrategy


class MySQLStrategy(DatabaseStrategy):
    def create_schema(self, engine: Engine, schema: str, username: str) -> None:
        with engine.connect() as connection:
            connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {schema};"))

    def create_engine(self, connection_string: str, schema: str) -> Engine:
        return create_engine(f"{connection_string}/{schema}")
