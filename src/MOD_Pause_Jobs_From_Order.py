#!/usr/bin/env python3

"""
Aria Corona - Dec 19th, 2024

This script pauses jobs associated with a Notion order when the order is paused.
Modules:
    NotionApiHelper: A helper module to interact with the Notion API.
    logging: Provides logging capabilities.
    subprocess: Allows you to spawn new processes.
    datetime: Supplies classes for manipulating dates and times.
    argparse: Parses command-line arguments.
Functions:
    main(): The main function that orchestrates the pausing of jobs.
    get_page_info(page_id): Retrieves information about a Notion page given its ID.
    catch_args(): Parses and returns command-line arguments.
Usage:
    Run this script with the required --page_id argument to pause jobs for a specific order.
    Example: python3 MOD_Pause_Jobs_From_Order.py --page_id <NotionPageID>
"""

from NotionApiHelper import NotionApiHelper
import logging, subprocess, datetime, sys

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/MOD_Pause_Jobs_From_Order.log'),
                        logging.StreamHandler()
                    ])

# Create a logger for this module
logger = logging.getLogger(__name__)

notion_helper = NotionApiHelper(header_path="src/headers.json")

JOB_PACKAGE = {'System status': {'select': {'name': 'Paused'}}}
NOW = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

def main():
    logger.info("[Start]")
    
    page_id = catch_variable()
    page_data = get_page_info(page_id)
    
    system_status = notion_helper.return_property_value(page_data['properties']['System status'], page_id)
    job_list = notion_helper.return_property_value(page_data['properties']['Jobs'], page_id)
    
    # Update order log
    old_log = notion_helper.return_property_value(page_data['properties']['Log'], page_id)
    log_message = f"{NOW} - Order {page_id} was paused, setting all job status to Paused."
    new_log = old_log + "\n" + log_message if old_log else log_message
    log_package = notion_helper.rich_text_prop_gen("Log", "rich_text", [new_log])
    notion_helper.update_page(page_id, log_package)

    if system_status == "Paused" and job_list:
        for job_id in job_list:
            logger.info(f"Pausing job {job_id}")
            
            # Update job log
            job_data = notion_helper.get_page(job_id)
            old_log = notion_helper.return_property_value(job_data['properties']['Log'], job_id)
            log_message = f"{NOW} - Order {page_id} paused, setting all job status to Paused."
            new_log = old_log + "\n" + log_message if old_log else log_message
            log_package = notion_helper.rich_text_prop_gen("Log", "rich_text", [new_log])
            
            package = {**JOB_PACKAGE, **log_package}
            notion_helper.update_page(job_id, package)

def get_page_info(page_id):
    try:
        shipment_info = notion_helper.get_page(page_id)
        return shipment_info
    except Exception as e:
        logger.error(f"Error in getting shipment info: {e}")
        return None

def catch_variable():
    if len(sys.argv) == 2:
        page_id = sys.argv[1] # Command line argument
        logger.info(f"Page ID Recieved: {page_id}")
        return page_id
    sys.exit("No Page ID Provided")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        page_id = catch_variable()
        
        error_message = f"MOD_Pause_Jobs_From_Order.py - Error in main()"
        logger.error(error_message, exc_info=True)
        
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])