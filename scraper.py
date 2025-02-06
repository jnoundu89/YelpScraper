import logging
import os
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Any

from pandas import DataFrame

from database.sql_requests import SqlRequests
from pages.yelp import Yelp
from utilities.helper import get_today_date

o_logger = logging.getLogger(__name__)


@dataclass
class MainScraper:
    """
    MainScraper class to execute the main process of the script to get data from the website, parse it and save it
    """
    dc_configuration: dict[str, Any]
    obj_argparse: ArgumentParser

    async def execute(self) -> None:
        """
        Execute the main process
        :return: None
        """
        try:
            await self._main_scraper()
        except Exception as o_exception:
            o_logger.error(f"Error in main process: {o_exception}")
            raise o_exception

    async def _main_scraper(self) -> None:
        """
        Main process of the script: get data from the website, parse it and save it
        :return: None
        """
        o_logger.info('Main process started.')
        o_sql_requests = SqlRequests() if not self.obj_argparse.no_database else None
        df_yelp = await Yelp(self.dc_configuration, o_sql_requests, self.obj_argparse).process_data()
        o_logger.info(f"length of dataframe yelp: {len(df_yelp)} row(s)")
        self.save_data_to_csv(df_yelp) if not self.obj_argparse.no_csv else None
        o_logger.info('Main process ended.')

    def save_data_to_csv(self, df_yelp: DataFrame) -> None:
        dc_path = self.dc_configuration["Yelp"]
        dc_params = dc_path["params"]
        dc_params['find_loc'] = dc_params['find_loc'].lower()
        dc_params['find_desc'] = dc_params['find_desc'].lower()
        s_output_filename = ""
        for s_key, s_value in dc_params.items():
            s_output_filename += f"{s_value}_".replace(" ", "_")
        s_today_date = get_today_date()
        s_output_filename += f"{s_today_date}.csv"
        s_inner_dir = f"{dc_params['find_loc']}".strip().replace(" ", "_")
        s_sub_dir = f"{dc_params['find_desc']}".strip().replace(" ", "_")
        os.makedirs(f"outputs/{s_inner_dir}/{s_sub_dir}", exist_ok=True)
        s_output_filename = f"outputs/{s_inner_dir}/{s_sub_dir}/{s_output_filename}"
        df_yelp.to_csv(s_output_filename, index=False)
        o_logger.info(f"Data saved to {s_output_filename}")
