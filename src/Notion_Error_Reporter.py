#!/usr/bin/env python3
"""
Aria Corona - December 12th, 2024

Notion Error Reporter
This script reports errors to a Notion page. It can either add a comment to the page or update the page properties with the error message.
It has built in redundency to ensure that the error message is reported so long as the page_id is correct.
Modules:
    NotionApiHelper: A helper module to interact with the Notion API.
    logging: Provides logging capabilities.
    sys: Provides access to system-specific parameters and functions.
    requests: Allows sending HTTP requests.
    datetime: Supplies classes for manipulating dates and times.
    json: Provides methods for parsing JSON.
    time: Provides various time-related functions.
Constants:
    ERROR_STATUS: A dictionary representing the error status property for a Notion page.
    WEBHOOK_URL: The URL for the webhook to send error reports.
    NOW: The current date and time formatted as a string.
Functions:
    catch_variable(): Parses command line arguments to get the page ID and error message.
    report_to_comments(page_id, error_message): Adds a comment with the error message to the specified Notion page.
    report_to_properties(page, error_message): Updates the properties of the specified Notion page with the error message.
    main(): The main function that orchestrates the error reporting process.
Usage:
    python3 Notion_Error_Reporter.py <page_id> <error_message>
"""

from NotionApiHelper import NotionApiHelper
import logging, sys, requests, time
from datetime import datetime
import json

notion_helper = NotionApiHelper(header_path="src/headers.json")

ERROR_STATUS = notion_helper.selstat_prop_gen("System status", "select", "Error")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/Notion_Error_Reporter.log'),
                        logging.StreamHandler()
                    ])

# Create a logger for this module
logger = logging.getLogger(__name__)

# Load system configuration
with open('conf/MOD_System_Conf.json', 'r') as config_file:
    SYS_CONF = json.load(config_file)

RETRY_CODES = [409, 429, 500, 502, 503, 504]

WEBHOOK_URL = "https://hook.us1.make.com/2waybxgtfc8utztl6dqi432go83iacj4"
NOW = datetime.now().strftime("%m-%d-%Y:%H-%M")


def catch_variable():
    if len(sys.argv) == 3:
        page_id = sys.argv[1] # Command line argument
        error_message = sys.argv[2]
        logger.info(f"Page ID Recieved: {page_id}")
        return page_id, error_message
    sys.exit("Invalid arguments --\nUsage: python3 Notion_Error_Reporter.py <page_id> <error_message>")

def report_to_comments(page_id, error_message):
    package = {}
    
    package = {'parent': {'page_id': page_id}, 'rich_text': [{'text': {'content': error_message}}]}
    
    with open('src/headers.json', 'r') as header_file:
        headers = json.load(header_file)
    
    response = requests.post(SYS_CONF['NOTION_COMMENT_ENDPOINT'], headers=headers, data=json.dumps(package))

    if response.status_code in RETRY_CODES:
        logger.info("Retrying in 30...")
        time.sleep(30)
        report_to_comments(page_id, error_message)
    elif response.status_code == 200:
        logger.info("Comment added successfully")
    else:
        logger.error(f"Error adding comment: {response.status_code}")
        
    pass

def report_to_properties(page, error_message):
    existing_log = notion_helper.return_property_value(page['properties']['Log'], page_id)
    new_log = f"{existing_log}\n{NOW} - {error_message}"
    new_log_package = notion_helper.rich_text_prop_gen("Log", "rich_text", new_log)
    
    package = {**ERROR_STATUS, **new_log_package}
    notion_helper.update_page(page_id, package)
    pass

def main():
    page_id, error_message = catch_variable()

    page = notion_helper.get_page(page_id)
    
    if not page:
        report_to_comments(page_id, error_message)
    
    else:
        report_to_properties(page, error_message)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        page_id, error_message = catch_variable()
        
        try:
            report_to_comments(page_id, error_message)
        except:
            logger.error(f"Error in error reporting: {e}", exc_info=True)
            sys.exit(1)
