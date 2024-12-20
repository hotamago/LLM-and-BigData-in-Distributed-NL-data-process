import requests
import json
from modules.managerKey import ManagerKey
from typing import Dict, List, Union
from enum import Enum

from modules.searchEngine.base import BaseSearchEngine, BaseConfigSearchEngine


class TypeSearch(Enum):
    SEARCH = "search"
    IMAGES = "images"
    VIDEOS = "videos"
    PLACES = "places"
    MAPS = "maps"
    NEWS = "news"
    SHOPPING = "shopping"
    SCHOLAR = "scholar"
    AUTOCOMPLETE = "autocomplete"


class Country(Enum):
    VN = "vn"
    US = "us"
    NONE = ""


class Location(Enum):
    VN = "vn"
    US = "us"
    NONE = ""


class Locale(Enum):
    VI = "vi"
    EN = "en"
    NONE = ""


class Config(BaseConfigSearchEngine):
    def __init__(
        self,
        batch: bool,
        typeSearch: str,
        autocomplete: bool,
        query: Union[str, List[str]],
        country: str,
        location: str,
        locale: str,
        page: int,
        num: int,
    ):
        self.batch = batch
        self.typeSearch = typeSearch
        self.autocomplete = autocomplete
        self.query = query
        self.country = country
        self.location = location
        self.locale = locale
        self.page = page
        self.num = num

    def _getConfig(self, *arg, **kwarg) -> Dict:
        return {
            "batch": self.batch,
            "typeSearch": self.typeSearch,
            "body": {
                "autocomplete": self.autocomplete,
                "q": self.query,
                "gl": self.country,
                "location": self.location,
                "hl": self.locale,
                "page": self.page,
                "num": self.num,
            }
        }


class Engine(BaseSearchEngine):
    def __init__(self, keyManager: ManagerKey):
        super().__init__(keyManager)

    def _query(self, _config: Config):
        config = _config.getConfig()
        url = "https://google.serper.dev/" + config["typeSearch"]

        if config["batch"]:
            tempPayload = []
            for i in range(len(config["body"]["q"])):
                tempPayload.append(
                    {
                        "autocomplete": config["body"]["autocomplete"],
                        "q": config["body"]["q"][i],
                        "gl": config["body"]["gl"],
                        "location": config["body"]["location"],
                        "hl": config["body"]["hl"],
                        "page": config["body"]["page"],
                        "num": config["body"]["num"],
                    }
                )

            payload = json.dumps(tempPayload)
        else:
            payload = json.dumps(config["body"])

        headers = {
            "X-API-KEY": self.keyManager.getKey(),
            "Content-Type": "application/json",
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        res = json.loads(response.text)

        # Convert to list of dict with key: url, title, snippet
        if config["batch"]:
            tempRes = []
            for i in range(len(res)):
                # add element in res[i]["organic"] to tempRes
                tempRes.extend(res[i]["organic"])
            res = tempRes
        else:
            res = res["organic"]

        res = [
            {
                "url": item["link"],
                "title": item["title"],
                "snippet": item["snippet"],
            }
            for item in res
        ]

        return res
