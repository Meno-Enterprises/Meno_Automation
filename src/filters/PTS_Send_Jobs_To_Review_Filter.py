#!/usr/bin/env python3
"""
Aria Corona - November 12th, 2024

This script processes job shipments by interacting with the Notion API and sending jobs to review based on specific conditions.
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
import logging, sys, requests

notion_helper = NotionApiHelper(header_path="src/headers_pts.json")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

JOB_DB_ID = "b41325e2ff494544a787717cfb1f77b4"
WEBHOOK_URL = "https://hook.us1.make.com/2cgamyfl651tjl57j4obtv0eofv67yoo"

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
        preflight_status = notion_helper.return_property_value(page_data['properties']['Preflight status'], page_id)
        ppd_status = notion_helper.return_property_value(page_data['properties']['PPD Status'],  page_id)
        
        if ppd_status == "To server" and (preflight_status == "Passed" or preflight_status == "Passed with warnings"):
            logger.info(f"Sending Job to Review: {page_id}")
            payload = {
                page_id.replace("-",""): {"preflight_status": preflight_status, "ppd_status": ppd_status}
            }
            response = requests.post(WEBHOOK_URL, json=payload)
            logger.info(f"Response: {response.status_code}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Error in main: {e}")
        page_id = catch_variable()

        error_status = notion_helper.selstat_prop_gen("Preflight status", "select", "Error")
        error_message = notion_helper.rich_text_prop_gen("Error Messages", "rich_text", f"PTS_Send_Jobs_To_Review_Filter.py:\n{str(e)}")
        
        package = {**error_status, **error_message}
        notion_helper.update_page(page_id, package)
