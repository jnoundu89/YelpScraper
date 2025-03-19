import urllib.parse
from dataclasses import dataclass, field

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from database.generate_orm_tables import Base
from database.strategies.base_strategy import DatabaseStrategy
from database.strategies.mysql_strategy import MySQLStrategy
from database.strategies.postgresql_strategy import PostgreSQLStrategy
from utilities.helper import get_database_credentials, bind_database_engine_type
from utilities.logging_utils import LoggerManager

o_logger = LoggerManager.get_logger(__name__)


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


@dataclass
class DatabaseEngine(metaclass=SingletonMeta):
    s_table_name: str = field(init=False)
    o_database_engine: Engine = field(init=False)
    strategy: DatabaseStrategy = field(init=False)

    def __post_init__(self) -> None:
        try:
            dc_setup_database = get_database_credentials('inputs/setup_database.json')
            dc_yelp_config = get_database_credentials('inputs/yelp_config.json')["Yelp"]["params"]
            s_table_name = f"{dc_yelp_config['find_desc']}_{dc_yelp_config['find_loc']}".replace(" ", "_").lower()

            s_encoded_password = urllib.parse.quote_plus(dc_setup_database['password'])
            s_engine_type = bind_database_engine_type(dc_setup_database)
            connection_string = f'{s_engine_type}://{dc_setup_database["username"]}:{s_encoded_password}@{dc_setup_database["hostname"]}:{dc_setup_database["port"]}'

            if dc_setup_database['engine'] == 'postgresql':
                self.strategy = PostgreSQLStrategy()
                o_logger.info("Connecting to PostgreSQL database...")
            elif dc_setup_database['engine'] == 'mysql':
                self.strategy = MySQLStrategy()
                o_logger.info("Connecting to MySQL database...")
            else:
                raise ValueError("Unsupported database engine")

            pre_engine = create_engine(connection_string)
            with pre_engine.connect() as connection:
                if not connection.dialect.has_schema(connection, dc_setup_database["schema"]):
                    o_logger.warning(f"Schema {dc_setup_database['schema']} does not exist. Creating it...")
            self.strategy.create_schema(pre_engine, dc_setup_database["schema"], dc_setup_database["username"])
            pre_engine.dispose()

            self.o_database_engine = self.strategy.create_engine(connection_string, dc_setup_database["schema"])
            self.s_table_name = s_table_name

            with self.o_database_engine.connect() as connection:
                Base.metadata.create_all(self.o_database_engine)
                o_logger.info(f"Connection to {dc_setup_database['schema']}.{self.s_table_name} database established!")

        except Exception as o_exception:
            raise CantConnectToDataBaseException from o_exception

    def __del__(self) -> None:
        self.o_database_engine.dispose()


class CantConnectToDataBaseException(Exception):
    def __str__(self) -> str:
        return "Can't connect to database. Check your connection inputs in the setup_database.json file."