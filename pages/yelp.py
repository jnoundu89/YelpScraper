import logging
import sys
from argparse import ArgumentParser
from dataclasses import dataclass

import pandas as pd
import scrapling
from pandas import DataFrame
from sqlalchemy.exc import ProgrammingError
from tqdm.asyncio import tqdm

from data_processing.data_processing import DataProcessing
from data_processing.models.business_model import BusinessExtractor, BusinessSearchExtractor
from database.sql_requests import SqlRequests
from utilities.helper import get_today_date, extract_json_data_from_html
from utilities.request_utils import make_request_with_retries

o_logger = logging.getLogger(__name__)


@dataclass
class Yelp(DataProcessing):
    """
    Yelp class to get data from the website Yelp and parse it to a DataFrame
    """
    o_sql_requests: SqlRequests | None
    obj_argparse: ArgumentParser

    def __post_init__(self):
        super(Yelp, self)

    async def _get_data(self) -> DataFrame:
        """
        Get the data from the website and return it as a DataFrame
        :return: DataFrame
        """
        df = pd.DataFrame()
        dc_path = self.dc_configuration["Yelp"]
        s_base_url = dc_path["urls"]["base"]
        s_search_url = dc_path["urls"]["search"]
        dc_params = dc_path["params"]
        l_links_failed_to_process = []

        s_url = f"{s_base_url}{s_search_url}"
        df_search_page = await self._retrieve_elements_from_search_page(s_url, dc_params)
        df_search_page['url'] = df_search_page['url'].apply(lambda _url: f"{s_base_url}{_url}")

        if not self.obj_argparse.no_database:
            l_distinct_urls_in_database = self.o_sql_requests.get_all_distinct_urls()
            o_logger.info(f"length of distinct urls in database: {len(l_distinct_urls_in_database)} row(s)")
            l_links = [url for url in df_search_page['url'].to_list() if [url] not in l_distinct_urls_in_database]
            o_logger.info(f"length of research after removing urls already in the database: {len(l_links)} row(s)")
        else:
            l_links = df_search_page['url'].to_list()

        for s_url in tqdm(l_links, file=sys.stdout):
            o_logger.info(f"link number {l_links.index(s_url)}/{len(l_links)}")
            try:
                o_response = await make_request_with_retries(s_url)
                if o_response and o_response.status == 200:
                    df_link = await self._parse_data(o_response)
                    if not df_link.empty:
                        df_link = pd.merge(df_search_page, df_link, on='business_id', how='inner')
                    else:
                        o_logger.error(f"Parsed data is empty for {s_url}")
                    df_link = post_processing_data(df_link, dc_params)
                    df = pd.concat([df, df_link], ignore_index=True)
                    if not self.obj_argparse.no_database:
                        try:
                            self.o_sql_requests.insert_dataframe_into_database(df_link)
                            o_logger.info(f"Data inserted into the database")
                        except ProgrammingError as e:
                            o_logger.error(f"Failed to insert data into the database: {e}")
                else:
                    o_logger.error(f"Request failed for {s_url} with status {o_response.status}")
                    l_links_failed_to_process.append(s_url)
            except Exception as e:
                o_logger.error(f"Failed to process {s_url}: {e}, {type(e)}")
                l_links_failed_to_process.append(s_url)
        o_logger.warning(f"Links failed to process: {l_links_failed_to_process}")
        return df

    async def _parse_data(self, o_response: scrapling.Adaptor) -> DataFrame:
        """
        Parse the data from the website and return it as a DataFrame with a builder pattern approach to extract data
        :param o_response: scrapling.Adaptor
        :return: DataFrame
        """
        o_logger.info('Parsing data')
        business_page = BusinessExtractor(o_response)
        data_business_page = await business_page.extract()
        df = pd.DataFrame([data_business_page.model_dump()])
        df.fillna("", inplace=True)
        return df

    @staticmethod
    async def _retrieve_elements_from_search_page(s_url: str, dc_params: dict[str]) -> DataFrame:
        """
        Retrieve the elements from the search page of the website Yelp
        :param s_url: str
        :param dc_params: dict[str]
        :return: DataFrame
        """
        df = pd.DataFrame()
        int_nb_business = 0
        bool_is_last_page = False
        business_search = BusinessSearchExtractor()
        while not bool_is_last_page:
            url = parse_url_with_query_params(s_url, dc_params, int_nb_business)
            o_logger.info(f"Retrieving links from {url}, page {int_nb_business // 10 + 1}")
            o_page_response = await make_request_with_retries(url)
            str_button_next_page = o_page_response.find_by_text("Next Page").parent.html_content
            if o_page_response.status == 200:
                json_data = extract_json_data_from_html(o_page_response, "data-hypernova-key=")
                json_data_path = json_data['legacyProps']['searchAppProps']['searchPageProps']
                json_main_content_path = json_data_path['mainContentComponentsListProps']
                df_main_content = business_search.extract_data_from_main_content(json_main_content_path)
                df = pd.concat([df, df_main_content], ignore_index=True)
            else:
                o_logger.error(f"Error while retrieving the page: {o_page_response.url} {o_page_response.status}")
            if 'disabled' in str_button_next_page:
                bool_is_last_page = True
            else:
                int_nb_business += 10
        return df


def post_processing_data(df: DataFrame, dc_params: dict[str]) -> DataFrame:
    """
    Post-processing of the data from the DataFrame before inserting it into the database
    :param df: DataFrame
    :param dc_params: dict[str]
    """
    try:
        df['date_insertion'] = get_today_date()
        df['date_insertion'] = pd.to_datetime(df['date_insertion'], format='%d_%m_%Y').dt.date
        df['website'] = df['website'].apply(lambda x: x.replace("http://", "") \
                                            .replace("https://", "").strip() if isinstance(x, str) else x)
        df['description'] = df['description'].apply(
            lambda x: x.replace("Specialties: ", "").strip() if isinstance(x, str) else x)
        df['categories'] = df['categories'].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
        df['street_address'] = df['street_address'].apply(
            lambda x: x.replace("None", "").strip() if isinstance(x, str) else x)
        df['images'] = df['images'].apply(
            lambda x: [i.replace(i.split("/")[-1], "o.jpg") for i in x] if isinstance(x, list) else x)
        df['images'] = df['images'].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
        df['hours'] = df['hours'].apply(lambda x: ", ".join([" : ".join(i) for i in x]) if isinstance(x, list) else x)
        for s_key, s_value in df.items():
            if s_value.dtype == 'object':
                df[s_key] = df[s_key].apply(
                    lambda x: x \
                        .replace("amp;", "") \
                        .replace("&#x27;", "'") \
                        .replace(" ", "") \
                        .strip() if isinstance(x, str) else x)
    except Exception as e:
        o_logger.error(f"Failed to post-process data: {e}")
    return df


def parse_url_with_query_params(s_url: str, dc_params: dict[str], int_nb_business: int) -> str:
    """
    Parse the URL with the query parameters
    :param s_url: str
    :param dc_params: dict[str]
    :param int_nb_business: int
    :return: str
    """
    s_url += "?"
    for s_key, s_value in dc_params.items():
        s_url += f"{s_key}={s_value}&"
    s_url = s_url[:-1].replace(" ", "%20")
    s_url += f"&start={int_nb_business}"
    return s_url
