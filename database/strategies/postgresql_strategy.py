from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from database.strategies.base_strategy import DatabaseStrategy


class PostgreSQLStrategy(DatabaseStrategy):
    def create_schema(self, engine: Engine, schema: str, username: str) -> None:
        with engine.connect() as connection:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {username}; COMMIT;"))
            connection.execute(text(f'GRANT ALL ON SCHEMA "{schema}" TO "{username}";'))

    def create_engine(self, connection_string: str, schema: str) -> Engine:
        return create_engine(connection_string, connect_args={"options": f"-csearch_path={schema}"})
