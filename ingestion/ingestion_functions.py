# Contains functions that are used in the ingestion scripts

# Packages imported
import os
import requests
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
from dotenv import load_dotenv
from copy import deepcopy
from datetime import datetime

# Loading the API key from the .env file
load_dotenv()
api_key = os.getenv("SNCF_API_KEY")

BASE_URL = "https://api.sncf.com/v1/coverage/sncf"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(3),
       wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type((requests.exceptions.ConnectionError, requests.exceptions.Timeout)),
       before_sleep=before_sleep_log(logger, logging.WARNING))
def verified_request(url, params=None):
    """
    Function to wrap the request and its verification
    :param url: url to request
    :param params: dict of params to use
    :return: data, a json dict obtained from request
    """
    try:
        response = requests.get(url, auth=(api_key, ""), params=params, timeout=10)
        response.raise_for_status()
        logger.info(f"Request successful : {url}")
        return response.json()
    except requests.exceptions.HTTPError:
        if response.status_code == 429:
            logger.error(f"Rate limit exceeded, retry after : {response.headers.get('Retry-After')} seconds")
        else:
            logger.error(f"HTTPError {response.status_code} : {response.text}")
        raise

def dict_merge(data_list, req_keyword):
    if req_keyword.endswith("departures"):
        req_keyword = "departures"
    elif req_keyword.endswith("arrivals"):
        req_keyword = "arrivals"
    else:
        if req_keyword != "disruptions":
            raise ValueError ("The req_keyword used is not handled")
    if len(data_list) == 0:
        print(f"WARNING : No data fetched for {req_keyword}")
        return {}
    else:
        # First item will serve as base. "feed_publishers" and "context" are not changing and set as that of the first item
        temp_data = deepcopy(data_list[0])
        # Some fields of the dict contain lists, these are stacked
        keys_to_merge = ["disruptions", "origins", "terminus", "departures", "arrivals", "notes", "exceptions"]
        for key in keys_to_merge:
            if key in temp_data or any(key in req for req in data_list[1:]):
                temp_data[key] = temp_data.get(key, [])
                for req in data_list[1:]:
                    temp_data[key] += req.get(key, [])
        # "pagination" stays the same and a new entry is added containing the number of data row fetched
        temp_data["pagination"]["result_fetched"] = len(temp_data[req_keyword])
        # "links" is the same without data about the request pages
        for link in temp_data["links"][::]:
            if link.get("type") in ["next", "last", "first"]:
                temp_data["links"].remove(link)
        return temp_data

def fetch_all_pages(base_url, page_keyword, params=None):
    """Fetch all pages of data from the API """
    if params is None:
        params = {"count": 500}
    is_flux_data = page_keyword.endswith("departures") or page_keyword.endswith("arrivals")
    if page_keyword not in ["stop_areas", "stop_points", "lines", "networks", "disruptions"] and not is_flux_data:
        raise ValueError(f"Only stop_points, stop_areas, lines and networks page_keyword are supported, you used : {page_keyword}")
    url = f"{base_url}/{page_keyword}"
    all_data = []
    while url:
        data = verified_request(url, params=params)
        if page_keyword == "stop_areas":
            values = [[dat.get("id"), dat.get("name"), dat.get("coord")] for dat in data[page_keyword] if dat.get("name") != ""]
            all_data += [(line[0], line[1], line[2]["lon"], line[2]["lat"]) for line in values]
        elif page_keyword == "stop_points":
            values = [[dat.get("id"), dat.get("name"), dat.get("stop_area", {}).get("id"), dat.get("coord", {})] for dat in data[page_keyword] if dat.get("name") != ""]
            all_data += [(line[0], line[1], line[2], line[3].get("lon"), line[3].get("lat")) for line in values]
        elif page_keyword == "lines" or page_keyword == "networks":
            all_data += [(dat.get("id"), dat.get("name")) for dat in data[page_keyword] if dat.get("name") != ""]
        else:
            all_data.append(data)
        params = None
        next_link = next((l["href"] for l in data.get("links", []) if l.get("type") == "next"), None)
        url = next_link
    if is_flux_data or page_keyword ==  "disruptions":
        all_data = dict_merge(all_data, page_keyword)
    return all_data

def parse_time(value):
    return datetime.strptime(value, "%Y%m%dT%H%M%S") if value is not None else None
