import argparse
import json
import logging
import os
import traceback
from datetime import date

from scrapling.engines.toolbelt import Response

from utilities.config_loader import ConfigLoader

o_logger = logging.getLogger(__name__)


def parse_arguments():
    obj_argparse = argparse.ArgumentParser(description='Yelp scraper')
    obj_argparse.add_argument('--no-database', action='store_true', help='Do not use the database')
    obj_argparse.add_argument('--no-csv', action='store_true', help='Do not save data to csv')
    obj_parser = obj_argparse.parse_args()
    if obj_parser.no_database:
        o_logger.info('The <no-database> flag is set.')
    if obj_parser.no_csv:
        o_logger.info('The <no-csv> flag is set.')
    return obj_parser


def extract_json_data_from_html(response: Response, s_css_class: str) -> dict | None:
    """
    Extract JSON data from the HTML
    :param response: Response
    :param s_css_class: str
    :return: dict | None
    """
    script_list = response.find_all("script", {"type": "application/json"})
    json_data = {}
    for script in script_list:
        if script.html_content.find(f'{s_css_class}') != -1:
            json_data = json.loads(script.text.replace("<!--", "").replace("-->", "").replace("&quot;", '"').strip())
            break
    return json_data


def bind_database_engine_type(dc_setup_database: dict) -> str | None:
    """
    Bind the database engine type to the SQLAlchemy engine
    :param dc_setup_database: dict - database setup data
    :return: str | None - database engine type
    """
    if dc_setup_database['engine'] == 'mysql':
        return 'mysql+pymysql'
    elif dc_setup_database['engine'] == 'postgresql':
        return 'postgresql+psycopg2'


def get_database_credentials(s_path) -> dict:
    """
    Get database credentials from the setup_database.json file in the inputs folder of the project
    :return: dict
    """
    return ConfigLoader(os.path.basename(os.path.dirname(__file__)), s_path).dc_config_data


def get_today_date() -> str:
    """
    Get today's date in the format of dd_mm_yyyy (e.g. 01_01_2021)
    :return: str
    """
    return date.today().strftime('%d_%m_%Y')


def get_traceback(o_exception: Exception) -> str:
    """
    Get traceback of an exception as a string format to log or print with the exception message
    :param o_exception: Exception
    :return: str
    """
    s_lines = traceback.format_exception(type(o_exception), o_exception, o_exception.__traceback__)
    return ''.join(s_lines)
