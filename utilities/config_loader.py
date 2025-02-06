import json
import os
from dataclasses import dataclass, InitVar, field
from typing import Any


@dataclass
class ConfigLoader:
    """
    ConfigLoader class to load configuration data from a JSON file to a dictionary object
    :param s_file_path: str - file path
    :param s_file_name: str - file name
    :param dc_config_data: dict[str, Any] - configuration data
    """
    s_file_path: InitVar[str]
    s_file_name: InitVar[str]
    dc_config_data: dict[str, Any] = field(init=False)

    def __post_init__(self, s_file_path: str, s_file_name: str) -> None:
        """
        Initialize ConfigLoader class
        :param s_file_path: str - file path
        :param s_file_name: str - file name
        :return: None
        """
        s_cur_dir = os.path.dirname(os.path.abspath(s_file_path))
        s_conf_path = os.path.join(s_cur_dir, s_file_name)
        with open(s_conf_path, 'r') as o_file:
            self.dc_config_data = json.load(o_file)
