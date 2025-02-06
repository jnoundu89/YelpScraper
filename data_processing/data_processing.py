import logging
from dataclasses import dataclass

from pandas import DataFrame

o_logger = logging.getLogger(__name__)


@dataclass
class DataProcessing:
    """
    DataProcessing class to process data from a source to a DataFrame
    """
    dc_configuration: dict[str, str]

    def _get_data(self) -> DataFrame:
        """
        Get data from the source to a DataFrame
        :return: DataFrame
        """
        raise NotImplementedError

    def _parse_data(self, df: DataFrame) -> DataFrame:
        """
        Parse data from the source to a DataFrame
        :param df: DataFrame
        :return: DataFrame
        """
        raise NotImplementedError

    def process_data(self) -> [DataFrame | None]:
        """
        Process data from the source to a DataFrame
        :return: [DataFrame | None]
        """
        return self._get_data()
