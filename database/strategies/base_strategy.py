from abc import ABC, abstractmethod

from sqlalchemy.engine import Engine


class DatabaseStrategy(ABC):
    @abstractmethod
    def create_schema(self, engine: Engine, schema: str, username: str) -> None:
        pass

    @abstractmethod
    def create_engine(self, connection_string: str, schema: str) -> Engine:
        pass
