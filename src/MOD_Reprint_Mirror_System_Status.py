#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
import logging, sys, json
from datetime import datetime

notion_helper = NotionApiHelper(header_path="src/headers.json")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/MOD_Reprint_Mirror_System_Status.log'),
                        logging.StreamHandler()
                    ])

# Create a logger for this module
logger = logging.getLogger(__name__)

WEBHOOK_URL = "https://hook.us1.make.com/2waybxgtfc8utztl6dqi432go83iacj4"

def catch_variable():
    if len(sys.argv) == 2:
        page_id = sys.argv[1] # Command line argument
        logger.info(f"Page ID Recieved: {page_id}")
        return page_id
    sys.exit("No Page ID Provided")

def get_page_info(page_id):
    try:
        logger.info(f"Getting page info for {page_id}")
        page_info = notion_helper.get_page(page_id)
        return page_info
    except Exception as e:
        logger.error(f"Error in getting page info: {e}")
        return None

def main():
    page_id = catch_variable()
    page_data = get_page_info(page_id)

    if page_data:
        logger.info(f"Page data found for {page_id}")
        page_id = page_data['id']
        job_sys_status = notion_helper.return_property_value(page_data['properties']['Job system status'], page_id)
        log_message = notion_helper.return_property_value(page_data['properties']['Log'], page_id)
        
        if not log_message:
            log_message = ""
        else:
            log_message = "\n" + log_message
        
        current_time = datetime.now().strftime("%m-%d-%Y:%H-%M")
        
        new_message = f"{current_time} - MOD_Reprint_Mirror_System_Status.py - System status updated to {job_sys_status} (Current job status){log_message}"

        system_status = notion_helper.selstat_prop_gen("System status", "select", job_sys_status)
        log_message = notion_helper.rich_text_prop_gen("Log", "rich_text", [new_message])
        
        package = {**system_status, **log_message}
        
        print(f"Package: {json.dumps(package)}")
        
        notion_helper.update_page(page_id, package)
        
    else:
        logger.info(f"Page data not found for {page_id}")
        package = notion_helper.selstat_prop_gen("System status", "select", "Error")

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
