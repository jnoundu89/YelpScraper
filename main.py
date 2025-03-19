import asyncio
import os

from scraper import MainScraper
from utilities.config_loader import ConfigLoader
from utilities.helper import parse_arguments
from utilities.logging_utils import LoggerManager

# Initialize the logger
s_script_name = os.path.basename(os.path.dirname(__file__))
LoggerManager(log_level='INFO', process_name=s_script_name)

o_logger = LoggerManager.get_logger(__name__)  # Factory Method for getting the logger


async def main() -> None:
    """
    Main function to execute the script and log the start and end of the script execution
    :return: None
    """
    o_logger.info(f'Script `{s_script_name}` started.')
    str_path = os.path.abspath(__file__)
    obj_config_loader = ConfigLoader(str_path, 'inputs/yelp_config.json')
    obj_argparse = parse_arguments()
    obj_scraper = MainScraper(obj_config_loader.dc_config_data, obj_argparse)
    await obj_scraper.execute()
    o_logger.info('Script ended.')


if __name__ == '__main__':
    asyncio.run(main())
