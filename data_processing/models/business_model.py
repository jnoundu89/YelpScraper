from dataclasses import field

import pandas as pd
from pandas import DataFrame
from pydantic import BaseModel, Field

from utilities.helper import o_logger, extract_json_data_from_html
from utilities.request_utils import make_request_with_retries


class SearchDataMainContent(BaseModel):
    """
    BusinessSearchData class to store the data of a business from Yelp website in a structured way using Pydantic
    """
    business_id: str = Field(default="")
    url: str = Field(default="")
    name: str = Field(default="")
    rating: float = Field(default=0.0)
    review_count: int = Field(default=0)
    price_range: str = Field(default="")
    categories: list[str] = Field(default=list)
    website: str = Field(default="")


class SearchDataMapState(BaseModel):
    """
    BusinessSearchData class to store the data of a business from Yelp website in a structured way using Pydantic
    """
    business_id: str = Field(default="")
    latitude: float = Field(default=0.0)
    longitude: float = Field(default=0.0)


class BusinessPageData(BaseModel):
    """
    Restaurant class to store the data of a restaurant from Yelp website in a structured way using Pydantic
    """
    business_id: str = Field(default="")
    street_address: str = Field(default="")
    postal_code: str = Field(default="")
    address_locality: str = Field(default="")
    address_country: str = Field(default="")
    phone: str = Field(default="")
    description: str = Field(default="")
    amneties: str = Field(default="")
    hours: list[list[str]] = Field(default=list)
    images: list[str] = Field(default=list)


class BusinessSearchExtractor:
    """
    BusinessSearchExtractor class to extract data from the Yelp json data of the search page
    """

    @staticmethod
    def extract_data_from_main_content(json_main_content: dict) -> DataFrame:
        """
        Extract data from the main content of the JSON data
        :param json_main_content: dict
        :return: DataFrame
        """
        json_main_content = [item for item in json_main_content if 'bizId' in item]
        df_main_content = pd.DataFrame()
        for item in json_main_content:
            website = item['searchResultBusiness'].get('website')
            website = website["href"] if isinstance(website, dict) and "href" in website else (
                website if isinstance(website, str) else "")

            business_data = SearchDataMainContent(
                business_id=item['bizId'],
                url=item['searchResultBusiness'].get('businessUrl', ""),
                name=item['searchResultBusiness'].get('name', ""),
                rating=item['searchResultBusiness'].get('rating', 0.0),
                review_count=item['searchResultBusiness'].get('reviewCount', 0),
                price_range=item['searchResultBusiness'].get('priceRange', "") or "",
                categories=[cat["title"] for cat in item['searchResultBusiness'].get('categories', []) if
                            isinstance(cat, dict)],
                website=website
            )
            df_main_content = pd.concat([df_main_content, pd.DataFrame([business_data.model_dump()])])
        return df_main_content

    @staticmethod
    def extract_data_from_map_state(json_map_state: dict) -> DataFrame:
        """
        Extract data from the map state of the JSON data
        :param json_map_state: dict
        :return: DataFrame
        """
        df_map_state = pd.DataFrame()
        json_map_state = [item for item in json_map_state if 'resourceId' in item]
        for item in json_map_state:
            business_data = SearchDataMapState(
                business_id=item['resourceId'],
                latitude=item['location']['latitude'],
                longitude=item['location']['longitude']
            )
            df_map_state = pd.concat([df_map_state, pd.DataFrame([business_data.model_dump()])])
        return df_map_state


class BusinessExtractor:
    """
    BusinessExtractor class to extract data from the Yelp website
    """
    json_data: dict = field(init=False)

    def __init__(self, o_response):
        """
        Initialize the BusinessExtractor class
        :param o_response: scrapling.Adaptor - Response of the request
        :param o_logger: Logger - Logger to log the data extraction
        :param dc_data: dict - Dictionary to store the extracted data
        """
        self.o_response = o_response
        self.o_logger = o_logger
        self.dc_data = {}

    async def extract(self) -> BusinessPageData:
        """
        Extract the data from the website and return it as a Business object
        :return: Business
        """
        self.json_data = await self._retry_extract_json_data(self.o_response)
        self._extract_business_id()
        self._extract_address()
        self._extract_phone_number()
        self._extract_description()
        self._extract_amneties()
        self._extract_hours()
        await self._extract_images()
        try:
            self.o_logger.info(f"url: {self.o_response.url}")
            for s_key, s_value in self.dc_data.items():
                if s_value == "" or s_value == []:
                    self.o_logger.warning(f"Missing value for {s_key}")
        except Exception as obj_exception:
            self.o_logger.error(f"Error while extracting data: {obj_exception}")
        return BusinessPageData(**self.dc_data)

    async def _retry_extract_json_data(self, o_response):
        """
        Retry extracting JSON data from the response until a valid key is found or a different key name is used.
        :param o_response: Response object
        :return: Extracted JSON data
        """
        json_data = extract_json_data_from_html(o_response, "")
        try:
            while not any(key.startswith('Business:') for key in json_data.keys()):
                self.o_logger.warning("No data found, retrying...")
                o_response = await make_request_with_retries(o_response.url)
                json_data = extract_json_data_from_html(o_response, "")
        except AttributeError:
            self.o_logger.warning(f"No business found, retrying with another key name for the data")
            json_data = extract_json_data_from_html(o_response, "data-apollo-state")
        except Exception as obj_exception:
            self.o_logger.error(f"Error while extracting data: {obj_exception}")
        return json_data

    def _extract_business_id(self):
        """
        Extract the business id of the restaurant
        :return: self
        """
        try:
            self.dc_data['business_id'] = self.o_response.find("meta", {"name": "yelp-biz-id"}).attrib["content"]
        except AttributeError as obj_exception:
            self.dc_data['business_id'] = ""
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting business id: {obj_exception}")
        return self

    def _extract_address(self):
        """
        Extract the address of the restaurant
        :return: self
        """
        try:
            json_data = self.json_data[f'BusinessLocation:{self.dc_data["business_id"]}']
            self.dc_data['street_address'] = (f"{json_data['address']['addressLine1']} "
                                              f"{json_data['address']['addressLine2']} "
                                              f"{json_data['address']['addressLine3']}")
            self.dc_data['postal_code'] = json_data['address']['postalCode']
            self.dc_data['address_locality'] = json_data['address']['city']
            self.dc_data['address_country'] = json_data['country']['code']
        except AttributeError as obj_exception:
            self.dc_data['street_address'] = ""
            self.dc_data['postal_code'] = ""
            self.dc_data['address_locality'] = ""
            self.dc_data['address_country'] = ""
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting address: {obj_exception}")
        return self

    def _extract_phone_number(self):
        """
        Extract the phone number of the restaurant
        :return: self
        """
        try:
            self.dc_data['phone'] = \
                self.json_data[f'Business:{self.dc_data["business_id"]}']['phoneNumber']['formatted']
        except AttributeError as obj_exception:
            self.dc_data['phone'] = ""
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting phone number: {obj_exception}")
        return self

    def _extract_description(self):
        """
        Extract the description of the restaurant
        :return: self
        """
        try:
            self.dc_data['description'] = self.o_response.find("meta", {"property": "og:description"}).attrib["content"]
        except AttributeError as obj_exception:
            self.dc_data['description'] = ""
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting description: {obj_exception}")
        return self

    def _extract_amneties(self):
        """
        Extract the amneties of the restaurant
        :return: self
        """
        try:
            business_id = self.dc_data['business_id']
            amneties_data = self.json_data[f'Business:{business_id}'] \
                ['organizedProperties({"clientPlatform":"WWW"})'][0]['properties']
            amenities_list = []
            for amenity in amneties_data:
                display_text = amenity['displayText']
                is_active = amenity['isActive']
                amenities_list.append(f"[✓] : {display_text}") if is_active \
                    else amenities_list.append(f"[X] : {display_text}")
            self.dc_data['amneties'] = '; '.join(amenities_list)
        except IndexError as obj_exception:
            self.dc_data['amneties'] = ""
        except AttributeError as obj_exception:
            self.dc_data['amneties'] = ""
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting amneties: {obj_exception}")
        return self

    def _extract_hours(self):
        """
        Extract the hours of the restaurant
        :return: self
        """
        try:
            list_hours = []
            hours_data = self.json_data[f'Business:{self.dc_data["business_id"]}'] \
                ['operationHours']['regularHoursMergedWithSpecialHoursForCurrentWeek']
            for element in hours_data:
                day_of_week = element['dayOfWeekShort']
                hours = element['hours'][0]
                hours = hours.split("(")[0].strip() if "(" in hours else hours
                if "AM" in hours or "PM" in hours:
                    start_time, end_time = hours.split(' - ')
                    start_time_24h = pd.to_datetime(start_time, format='%I:%M %p').strftime('%Hh%M')
                    if end_time == '12:00 PM':
                        end_time_24h = '00h00'
                    else:
                        end_time_24h = pd.to_datetime(end_time, format='%I:%M %p').strftime('%Hh%M')
                    hours = f"{start_time_24h} - {end_time_24h}"
                else:
                    hours = hours.replace("Closed", "Fermé")
                day_of_week = (day_of_week
                               .replace("Mon", "Lundi")
                               .replace("Tue", "Mardi")
                               .replace("Wed", "Mercredi")
                               .replace("Thu", "Jeudi")
                               .replace("Fri", "Vendredi")
                               .replace("Sat", "Samedi")
                               .replace("Sun", "Dimanche")
                               )
                list_hours.append([day_of_week, hours])
            self.dc_data['hours'] = list_hours
        except IndexError as obj_exception:
            self.dc_data['hours'] = []
        except AttributeError as obj_exception:
            self.dc_data['hours'] = []
        except TypeError as obj_exception:
            self.dc_data['hours'] = []
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting hours: {obj_exception}")
        return self

    async def _extract_images(self):
        """
        Extract the images of the restaurant
        :return: self
        """
        try:
            if not any(key.startswith('BusinessPhoto:') for key in self.json_data.keys()):
                o_logger.info("No images found")
                self.dc_data['images'] = []
                return self
            images_list = []
            int_nb_images = 0
            bool_is_last_page = False
            base_url_images = f"https://www.yelp.fr/biz_photos/{self.dc_data['business_id']}"
            while not bool_is_last_page:
                o_response_images = await make_request_with_retries(base_url_images)
                base_url_images = o_response_images.url
                if o_response_images.status == 200:
                    o_logger.info(f"Extracting images from {base_url_images}, page {int_nb_images // 30 + 1}")
                    ul_path = o_response_images.find("div", {"class": "media-landing_gallery photos"}).children.first
                    for li in ul_path.find_all("li"):
                        str_images_url = li.find("img").attrib["srcset"].split(" ")[0]
                        images_list.append(str_images_url)
                else:
                    self.dc_data['images'] = []
                int_nb_images += 30
                if "start=" in base_url_images:
                    base_url_images = base_url_images.split("?")[0]
                base_url_images = f"{base_url_images}?start={int_nb_images}"
                adaptator_last_page = o_response_images.find_by_text("Suivant")
                try:
                    s_last_page = adaptator_last_page.text
                    bool_is_last_page = False if s_last_page == "Suivant" else True
                except AttributeError:
                    bool_is_last_page = True
            images_list = list(set(images_list))
            self.dc_data['images'] = images_list
        except AttributeError as obj_exception:
            self.dc_data['images'] = []
        except Exception as obj_exception:
            o_logger.error(f"Error while extracting images: {obj_exception}")
        return self
