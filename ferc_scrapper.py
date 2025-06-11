import os, copy
import logging
import time
import requests
import yaml
from random import randint
from datetime import datetime
from lxml import etree
from typing import Optional, Dict, Any

# Constants
MAX_REQUESTS = 5
REQUEST_COUNT = 0  # Track number of requests
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, "DATA", "ferc.gov")
LOG_FOLDER = os.path.join(BASE_DIR, "LOGS","ferc.gov" )
CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")

# Ensure necessary directories exist
os.makedirs(DATA_FOLDER, exist_ok=True)
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
            resp = session.get(url, headers=headers, timeout=30)
        elif request_type.upper() == "POST":
            resp = session.post(url, data=payload, headers=headers, timeout=30)
        else:
            logging.error(f"Invalid request type: {request_type}")
            return None

        REQUEST_COUNT += 1
        logging.info(f"[{request_type.upper()}] {url} - Status Code: {resp.status_code} (Request {REQUEST_COUNT}/{MAX_REQUESTS})")

        if REQUEST_COUNT >= MAX_REQUESTS:
            logging.warning(f"Reached MAX_REQUESTS ({MAX_REQUESTS}). Halting further requests.")

        sleep_time = randint(5,7)
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






    

def fetch_data(source_conf: Dict[str, Any], session: requests.Session):
    out_file = open(os.path.join(DATA_FOLDER, 'ferc_gov.txt'),'w')
    base_url = source_conf.get("base_url")
    if not base_url:
        logging.error("Missing base_url in source configuration.")
        return
    base_resp = get_url_resp(base_url, source_conf['headers'], session, "GET" )
    if base_resp is None:
        return
    validation_id = get_parsed_data(base_resp.content, source_conf['parser']['event_validation'])[0]
    viewstate_id  = get_parsed_data(base_resp.content, source_conf['parser']['viewstate_page_index'])[0]

    api_payload = copy.deepcopy(source_conf['api_payload'])
    api_payload['__VIEWSTATE'] = viewstate_id
    api_payload['__EVENTVALIDATION'] = validation_id

    page_resp = get_url_resp(source_conf['api_url'],source_conf['api_headers'],session,"POST",api_payload)
    logging.info("parsing checksum from page response")
    if page_resp is None:
        return
    sha_checksum = get_parsed_data(page_resp.content, source_conf['parser']['sha_checksum'])[0].strip()
    out_file.write(sha_checksum)

def main():
    setup_logging()
    logging.info("Starting Ferc.gov Scraper")

    configs = load_config_from_yaml(CONFIG_FILE)
    source_name = "ferc.gov"
    source_conf = configs.get("source", {}).get(source_name, {})

    if not source_conf:
        logging.error(f"No configuration found for source: {source_name}")
        return

    session = requests.Session()
    fetch_data(source_conf, session)

    logging.info(f"Scraper execution completed. Total requests made: {REQUEST_COUNT}/{MAX_REQUESTS}")

if __name__ == "__main__":
    main()
