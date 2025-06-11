import os, re, json,csv
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
PAGE_SIZE = 200
REQUEST_COUNT = 0  # Track number of requests
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, "DATA", "pjm-eis.com")
LOG_FOLDER = os.path.join(BASE_DIR, "LOGS","pjm-eis.com" )
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

def set_page_size(page_size, source_conf ,session, callable_state):
    size_resp = None
    try:
        payload = source_conf['api_payload']
        payload['__DXCallbackArgument'] = source_conf['size_argument'].replace("p_size" ,str(page_size))
        
        grid_view_str = payload['GridView']
        grid_view_dict = json.loads(grid_view_str)  
        grid_view_dict['callbackState'] = callable_state
        payload['GridView'] = json.dumps(grid_view_dict)
        size_resp = get_url_resp(source_conf['listing_api'], source_conf['api_headers'] ,session,"POST" ,payload)

    except Exception as e:
        logging.error(f"Error in set_page_size: {e}")
    
    return size_resp



def get_callback_state(response):
    callback_state = None
    try:
        pattern = r"'callbackState':'(.*?)'"
        match = re.search(pattern, response.text , re.DOTALL)
        if match:
            callback_state = match.group(1)
            
    except Exception as e:
        logging.error("Error while fetching callback_state")
    return callback_state
    

def fetch_data(source_conf: Dict[str, Any], session: requests.Session):
    out_file = os.path.join(DATA_FOLDER, 'renewable_generators_registered_in_GATS.csv')
    base_url = source_conf.get("base_url")
    if not base_url:
        logging.error("Missing base_url in source configuration.")
        return
    base_resp = get_url_resp(base_url, source_conf['headers'], session, "GET" )
    if base_resp is None:
        return
    
    callable_state = get_callback_state(base_resp)
    
    if callable_state is not None:
        logging.info(f"Setting the page size to {PAGE_SIZE}")
        set_size_resp = set_page_size(PAGE_SIZE,source_conf ,session, callable_state)

        if base_resp is None:
            return
        callable_state = get_callback_state(set_size_resp)

         #page Index start from 0 for page 2 index will be 1

        for page_no in range(1,2):
            logging.info(f"Fetching page {page_no+1}")
            payload = source_conf['api_payload']
            payload['__DXCallbackArgument'] = source_conf['page_argument'].replace("page_number" ,str(page_no))

            grid_view_str = payload['GridView']
            grid_view_dict = json.loads(grid_view_str)  
            grid_view_dict['callbackState'] = callable_state
            payload['GridView'] = json.dumps(grid_view_dict)

            page_resp = get_url_resp(source_conf['listing_api'], source_conf['api_headers'] ,session,"POST" ,payload)

            page_content = page_resp.content
            callable_state = get_callback_state(page_resp)
            logging.info(f"parsing the table data")
            headertexts = get_parsed_data(page_content, source_conf['parser']['table_headers'])
            t_headers = [" ".join(td.xpath(".//text()")).strip() for td in headertexts]
            t_rows = get_parsed_data(page_content, source_conf['parser']['table_rows'])
            table_data = []
            for row in t_rows:
                values = [val.replace("¬†", " ").strip() for val in row.xpath('./td/text()')] 
                row_dict = dict(zip(t_headers, values))  
                table_data.append(row_dict)
            
            with open(out_file, mode="w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=t_headers)  
                writer.writeheader()  
                writer.writerows(table_data)  

                logging.info(f"Data saved to {out_file}")

def main():
    setup_logging()
    logging.info("Starting Pjmeis Scraper")

    configs = load_config_from_yaml(CONFIG_FILE)
    source_name = "pjm-eis.com"
    source_conf = configs.get("source", {}).get(source_name, {})

    if not source_conf:
        logging.error(f"No configuration found for source: {source_name}")
        return

    session = requests.Session()
    fetch_data(source_conf, session)

    logging.info(f"Scraper execution completed. Total requests made: {REQUEST_COUNT}/{MAX_REQUESTS}")

if __name__ == "__main__":
    main()
