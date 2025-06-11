import os
import logging
import time
import requests
import yaml
from random import randint
from datetime import datetime
from lxml import etree
from typing import Optional, Dict, Any
import copy

# Constants
MAX_REQUESTS = 30
REQUEST_COUNT = 0  # Track number of requests
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_FOLDER = os.path.join(BASE_DIR, "FILES", "infopost")
LOG_FOLDER = os.path.join(BASE_DIR, "LOGS","infopost" )
CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")

# Ensure necessary directories exist
os.makedirs(FILE_FOLDER, exist_ok=True)
os.makedirs(LOG_FOLDER, exist_ok=True)

def setup_logging():
    log_filename = os.path.join(LOG_FOLDER, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        filename=log_filename,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logging.getLogger().addHandler(console_handler)
    logging.info(f"Logging initialized: {log_filename}")

def load_config_from_yaml(yaml_file: str) -> Dict[str, Any]:
    try:
        with open(yaml_file, 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logging.error(f"Config file not found: {yaml_file}")
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML file: {e}")
    return {}

def get_url_resp(url: str,
                headers: Optional[Dict[str, str]] = None,
                session: Optional[requests.Session] = None,
                request_type: str = "GET",
                payload: Optional[Dict[str, Any]] = None
            ) -> Optional[requests.Response]:
    
    global REQUEST_COUNT
    if REQUEST_COUNT >= MAX_REQUESTS:
        logging.warning(f"Max requests ({MAX_REQUESTS}) reached.")
        return None

    try:
        if request_type.upper() == "GET":
            resp = session.get(url, headers=headers, timeout=10)
        elif request_type.upper() == "POST":
            resp = session.post(url, data=payload, headers=headers, timeout=10)
        else:
            logging.error(f"Invalid request type: {request_type}")
            return None

        REQUEST_COUNT += 1
        logging.info(f"[{request_type.upper()}] {url} - Status Code: {resp.status_code} (Request {REQUEST_COUNT}/{MAX_REQUESTS})")

        if REQUEST_COUNT >= MAX_REQUESTS:
            logging.warning(f"Reached MAX_REQUESTS ({MAX_REQUESTS}). Halting further requests.")

        sleep_time = randint(5,8)
        logging.info(f"Sleeping for {sleep_time} seconds...")
        time.sleep(sleep_time)

        return resp
    except requests.RequestException as e:
        logging.error(f"Request failed for {url}: {e}")
        return None

def get_parsed_data(resp_content: bytes, ele_parser: str) -> list:
    tree = etree.HTML(resp_content)
    try:
        return tree.xpath(ele_parser)
    except Exception as e:
        logging.error(f"Error parsing data using XPath '{ele_parser}': {e}")
        return []

def format_datetime(input_str: str) -> str:
    try:
        parts = input_str.split(" ", 1)  
        date_part, rest_part = parts[0], parts[1]

        date_obj = datetime.strptime(date_part, "%m/%d/%Y")
        formatted_date = date_obj.strftime("%Y%m%d")

        formatted_rest = rest_part.replace(" ", "_").replace(":", "_").replace("-", "_")

        return f"{formatted_date}_{formatted_rest}"
    except Exception as e:
        logging.error(f"Error formatting datetime '{input_str}': {e}")
        return "invalid_date"


def get_second_page_data(base_url, source_conf, session):
    logging.info(f"Fetching data from {base_url}")
    base_resp = get_url_resp(base_url, source_conf['headers'], session, "GET")
    if base_resp is None:
        return

    api_payload = copy.deepcopy(source_conf['api_payload'])
    base_page_index = get_parsed_data(base_resp.content, source_conf['parser']['viewstate_page_index'])[0]
    base_page_validation = get_parsed_data(base_resp.content, source_conf['parser']['event_validation'])[0]

    api_payload['__VIEWSTATE_PAGE_INDEX'] = base_page_index
    api_payload['__EVENTVALIDATION'] = base_page_validation

    page_resp = get_url_resp(source_conf['listing_api'], source_conf['api_headers'], session, "POST", api_payload)
    page_index = get_parsed_data(page_resp.content, source_conf['parser']['viewstate_page_index'])[0]
    page_validation = get_parsed_data(page_resp.content, source_conf['parser']['event_validation'])[0]
    api_payload['__VIEWSTATE_PAGE_INDEX'] = page_index
    api_payload['__EVENTVALIDATION'] = page_validation
    

    return api_payload  ,page_resp

    

def download_data(source_conf: Dict[str, Any], session: requests.Session):
    base_url = source_conf.get("base_url")
    if not base_url:
        logging.error("Missing base_url in source configuration.")
        return

    payload, page_resp = get_second_page_data(base_url, source_conf, session)


    file_ids = get_parsed_data(page_resp.content, source_conf['parser']['file_ids'])
    menu_title = get_parsed_data(page_resp.content, source_conf['parser']['menu_title'])[0].split("-")[0].strip()
    doc_titles = get_parsed_data(page_resp.content, source_conf['parser']['document_title'])
    file_dict = dict(zip(file_ids, doc_titles))
    
    d_count = 0
    for file_id, doc_title in file_dict.items():
        if REQUEST_COUNT >= MAX_REQUESTS:
            break
        download_id = file_id.replace("_", ":")
        payload['__EVENTTARGET'] = download_id
        
        if d_count == 3:
            session.cookies.clear()
            logging.info(f"Invalid navigation detected .. Resetting the session")
            payload, page_resp = get_second_page_data(base_url, source_conf, session)
            payload['__EVENTTARGET'] = download_id

            d_count =0

        download_resp = get_url_resp(source_conf['listing_api'], source_conf['api_headers'], session, "POST", payload)
        if download_resp is None or download_resp.status_code != 200:
            logging.error(f"Failed to download file {doc_title}. Status: {download_resp.status_code if download_resp else 'No Response'}")
            continue
        

        file_name = os.path.join(FILE_FOLDER, f"{menu_title.lower().replace(' ', '_')}_{format_datetime(doc_title)}.xls")

        with open(file_name, "wb") as file:
            file.write(download_resp.content)
            d_count += 1
        logging.info(f"File downloaded successfully: {file_name}")

def main():
    setup_logging()
    logging.info("Starting InfoPost Scraper")

    configs = load_config_from_yaml(CONFIG_FILE)
    source_name = "infopost"
    source_conf = configs.get("source", {}).get(source_name, {})

    if not source_conf:
        logging.error(f"No configuration found for source: {source_name}")
        return

    session = requests.Session()
    download_data(source_conf, session)

    logging.info(f"Scraper execution completed. Total requests made: {REQUEST_COUNT}/{MAX_REQUESTS}")

if __name__ == "__main__":
    main()
