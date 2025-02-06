import logging
import urllib.parse
from dataclasses import dataclass, field

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from database.generate_orm_tables import Base
from utilities.helper import get_database_credentials, bind_database_engine_type

o_logger = logging.getLogger(__name__)


@dataclass
class DatabaseEngine:
    """
    Class to handle the database engine and connection to the database using SQLAlchemy ORM and MySQL database engine
    """
    s_table_name: str = field(init=False)
    o_database_engine: Engine = field(init=False)

    def __post_init__(self) -> None:
        """
        Initialize the database engine
        :return: None
        """
        try:
            dc_setup_database = get_database_credentials('inputs/setup_database.json')
            dc_yelp_config = get_database_credentials('inputs/yelp_config.json')["Yelp"]["params"]
            s_table_name = f"{dc_yelp_config['find_desc']}_{dc_yelp_config['find_loc']}".lower()

            s_encoded_password = urllib.parse.quote_plus(dc_setup_database['password'])
            s_engine_type = bind_database_engine_type(dc_setup_database)
            # Create engine without specifying schema for initial connection
            pre_engine = create_engine(
                f'{s_engine_type}://{dc_setup_database["username"]}:{s_encoded_password}@{dc_setup_database["hostname"]}:{dc_setup_database["port"]}')

            with pre_engine.connect() as o_connection:
                # Create schema if it doesn't exist
                if not o_connection.dialect.has_schema(o_connection, dc_setup_database["schema"]):
                    o_connection.execute(text(f"CREATE SCHEMA `{dc_setup_database['schema']}`;"))
                    o_logger.warning(
                        f"No schema found. Creation of schema: {dc_setup_database['schema']} successful!")
            # Dispose of the pre-engine
            pre_engine.dispose()

            # Now create engine with the schema
            self.o_database_engine = create_engine(
                f'{s_engine_type}://{dc_setup_database["username"]}:{s_encoded_password}@{dc_setup_database["hostname"]}:{dc_setup_database["port"]}/{dc_setup_database["schema"]}')
            self.s_table_name = s_table_name

            # Create tables in the specified schema
            Base.metadata.create_all(self.o_database_engine)
            o_logger.info(f"Connection to {dc_setup_database['schema']}.{self.s_table_name} database established!")

        except Exception as o_exception:
            raise CantConnectToDataBaseException from o_exception

    def __del__(self) -> None:
        """
        Destructor for the database engine class to dispose of the engine when the object is deleted from memory to prevent memory leaks and other issues
        :return: None
        """
        self.o_database_engine.dispose()


class CantConnectToDataBaseException(Exception):
    """
    Exception to raise when the database engine can't connect to the database
    """

    def __str__(self) -> str:
        return "Can't connect to database. Check your connection inputs in the setup_database.json file."
