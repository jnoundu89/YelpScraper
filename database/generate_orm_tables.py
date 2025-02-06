import datetime
from typing import Any

from sqlalchemy import String, Date, Text, Integer, Float
from sqlalchemy.orm import mapped_column, DeclarativeBase, Mapped

from utilities.helper import get_database_credentials


class Base(DeclarativeBase):
    """
    Base class for all database models
    """
    __table_args__: dict[str, Any] = {'mysql_engine': 'InnoDB', 'mysql_charset': 'utf8mb4',
                                      'postgresql_engine': 'InnoDB', 'postgresql_charset': 'utf8mb4'}


class YelpTable(Base):
    """
    Model for the Yelp table
    """
    dc_database = get_database_credentials("inputs/setup_database.json")
    dc_yelp_config = get_database_credentials('inputs/yelp_config.json')["Yelp"]["params"]
    s_table_name = f"{dc_yelp_config['find_desc']}_{dc_yelp_config['find_loc']}"
    __tablename__: str = s_table_name
    __table_args__: dict[str, Any] = {'schema': dc_database['schema']}

    business_id: Mapped[str] = mapped_column(String(100), nullable=False)
    url: Mapped[str] = mapped_column(String(255), nullable=False, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    rating: Mapped[float] = mapped_column(Float, nullable=False)
    review_count: Mapped[int] = mapped_column(Integer, nullable=False)
    price_range: Mapped[str] = mapped_column(String(10), nullable=False)
    categories: Mapped[list[str]] = mapped_column(Text, nullable=False)
    phone: Mapped[str] = mapped_column(String(255), nullable=False)
    website: Mapped[str] = mapped_column(String(255), nullable=False)
    latitude: Mapped[float] = mapped_column(Float, nullable=False)
    longitude: Mapped[float] = mapped_column(Float, nullable=False)
    street_address: Mapped[str] = mapped_column(String(255), nullable=False)
    postal_code: Mapped[str] = mapped_column(String(10), nullable=False)
    address_locality: Mapped[str] = mapped_column(String(255), nullable=False)
    address_country: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    amneties: Mapped[str] = mapped_column(Text, nullable=False)
    hours: Mapped[list[str]] = mapped_column(Text, nullable=False)
    images: Mapped[list[str]] = mapped_column(Text, nullable=False)
    date_insertion: Mapped[datetime.date] = mapped_column(Date, nullable=False)
