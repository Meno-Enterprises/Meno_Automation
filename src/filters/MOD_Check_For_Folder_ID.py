#!/usr/bin/env python3
"""
Aria Corona - December 3rd, 2024

This is a glorified filter. It checks if the asset folder exists in the shipment page. If it doesn't, it sends a webhook to the Make API to make one.
Functions:
    catch_variable():
        Retrieves the page ID from command line arguments. Exits if no page ID is provided.
    get_page_info(page_id):
        Fetches shipment information from Notion using the provided page ID.
    main():
        Main function that orchestrates the process of fetching shipment info, checking conditions, and sending jobs to review.
Constants:
    JOB_DB_ID (str): The ID of the job database in Notion.
    WEBHOOK_URL (str): The URL of the webhook to send job review requests.
Usage:
    Run the script with a page ID as a command line argument to process the corresponding job shipment.
"""

from NotionApiHelper import NotionApiHelper
import logging, sys, requests, datetime

notion_helper = NotionApiHelper(header_path="src/headers.json")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
file_handler = logging.FileHandler('logs/MOD_Check_For_Folder_ID.log')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.propagate = False

WEBHOOK_URL = "https://hook.us1.make.com/2waybxgtfc8utztl6dqi432go83iacj4"

def catch_variable():
    if len(sys.argv) == 2:
        page_id = sys.argv[1] # Command line argument
        logger.info(f"Shipment ID Recieved: {page_id}")
        return page_id
    sys.exit("No Page ID Provided")

def get_page_info(page_id):
    try:
        shipment_info = notion_helper.get_page(page_id)
        return shipment_info
    except Exception as e:
        logger.error(f"Error in getting shipment info: {e}")
        return None

def main():
    page_id = catch_variable()
    page_data = get_page_info(page_id)

    if page_data:
        page_id = page_data['id']
        asset_folder = notion_helper.return_property_value(page_data['properties']['Asset folder'], page_id)

        if not asset_folder:
            logger.info(f"Asset folder does not exist: {page_id}")
            payload = {
                page_id.replace("-",""): {"application": "MOD_Check_For_Folder_ID.py"}
            }
            response = requests.post(WEBHOOK_URL, json=payload)
            logger.info(f"Response: {response.status_code}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        page_id = catch_variable()
        
        page = notion_helper.get_page(page_id)
        existing_log = notion_helper.return_property_value(page['properties']['Log'], page_id)
        current_time = datetime.now().strftime("%m-%d-%Y:%H-%M")
        logger.error(f"Error occurred at {current_time}")

        error_status = notion_helper.selstat_prop_gen("System status", "select", "Error")
        error_message = notion_helper.rich_text_prop_gen("Log", "rich_text", f"{existing_log}\n{current_time} - Error in MOD_Check_For_Folder_ID.py:\n{str(e)}")
        
        package = {**error_status, **error_message}
        notion_helper.update_page(page_id, package)
