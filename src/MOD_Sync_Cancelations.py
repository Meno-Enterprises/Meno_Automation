#!/usr/bin/env python3

"""
Aria Corona - December 12th, 2024

This script synchronizes cancellations between jobs and orders in a Notion database.
Modules:
    NotionApiHelper: A helper module to interact with the Notion API.
    logging: Provides logging capabilities.
    sys: Provides access to some variables used or maintained by the interpreter.
    subprocess: Allows you to spawn new processes, connect to their input/output/error pipes, and obtain their return codes.
    datetime: Supplies classes for manipulating dates and times.
    json: Provides methods for parsing JSON.
Functions:
    catch_variable(): Retrieves the page ID from the command line arguments.
    get_page_info(page_id): Fetches page information from Notion using the provided page ID.
    cancel_jobs(job_list, order_number, original_job_id): Cancels jobs associated with a given order.
    cancel_from_order(page_data, page_id): Cancels jobs if the order status is "Canceled".
    cancel_from_job(page_data, page_id): Cancels the order and associated jobs if the job status is "Canceled".
    process_page(page_data): Processes the page data to determine if it belongs to an order or job database and initiates cancellation accordingly.
    main(): Main function that orchestrates the cancellation process.
Usage:
    Run the script with a page ID as a command line argument to synchronize cancellations.
"""


from NotionApiHelper import NotionApiHelper
import logging, sys, subprocess
from datetime import datetime
import json

notion_helper = NotionApiHelper(header_path="src/headers.json")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/MOD_Sync_Cancelations.log'),
                        logging.StreamHandler()
                    ])

# Create a logger for this module
logger = logging.getLogger(__name__)

# Load system configuration
with open('conf/MOD_System_Conf.json', 'r') as config_file:
    SYS_CONF = json.load(config_file)

WEBHOOK_URL = "https://hook.us1.make.com/2waybxgtfc8utztl6dqi432go83iacj4"
NOW = datetime.now().strftime("%m-%d-%Y:%H-%M")


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


def cancel_jobs(job_list, order_number, original_job_id):
    for job_id in job_list:
        
        if job_id == original_job_id:
            continue
        
        job_page = notion_helper.get_page(job_id)
        job_properties = job_page['properties']
        job_status = notion_helper.return_property_value(job_properties['Job status'], job_id)
        
        if job_status == "Canceled":
            continue
        
        old_job_log = notion_helper.return_property_value(job_properties['Log'], job_id) + "\n"
        new_job_log = old_job_log + f"{NOW} - Job/Order {original_job_id} canceled, setting all order {order_number} and job status to Canceled."
        
        log_package = notion_helper.rich_text_prop_gen("Log", "rich_text", [new_job_log])
        status_package = notion_helper.selstat_prop_gen("Job status", "select", "Canceled")
        package = {**log_package, **status_package}
        
        logger.info(f"Canceling job {job_id}")
        
        try:                
            notion_helper.update_page(job_id, package)
        except:
            error_message = f"MOD_Sync_Cancelations.py - Error canceling job {job_id}"
            logger.error(error_message)
            subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        return


def cancel_from_order(page_data, page_id):
    order_properties = page_data['properties']
    order_status = notion_helper.return_property_value(order_properties['Status'], page_id)
    
    if order_status != "Canceled":
        logger.info(f"Order {page_id} is not canceled, ending process")
        return
    
    job_list = notion_helper.return_property_value(order_properties['Jobs'], page_id)
    
    if not job_list:
        error_message = f"MOD_Sync_Cancelations.py - Job list not found for order {page_id}"
        logger.error(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        return
    
    order_number = notion_helper.return_property_value(order_properties['Order number'], page_id)
    cancel_jobs(job_list, order_number, page_id)
    pass


def cancel_from_job(page_data, page_id):
    
    def cancel_order(order_id, order_page):
        status_package = notion_helper.selstat_prop_gen("Status", "select", "Canceled")
        order_log = notion_helper.return_property_value(order_page['properties']['Log'], order_id) + "\n"
        new_log = order_log + f"{NOW} - Job {page_id} canceled, setting order status to Canceled."
        log_package = notion_helper.rich_text_prop_gen("Log", "rich_text", [new_log])
        
        logger.info(f"Canceling order {order_id}")
        
        try:
            response = notion_helper.update_page(order_id, {**status_package, **log_package})
        except:
            error_message = f"MOD_Sync_Cancelations.py - Error canceling order {order_id}"
            logger.error(error_message)
            subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        return
                
                
    page_properties = page_data['properties']
    job_status = notion_helper.return_property_value(page_properties['Job status'], page_id)
    
    if job_status != "Canceled":
        logger.info(f"Job {page_id} is not canceled, ending process")
        return
    
    order_id = notion_helper.return_property_value(page_properties['Order'], page_id)
    
    if not order_id:
        error_message = f"MOD_Sync_Cancelations.py - Order ID not found for job {page_id}"
        logger.error(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        return
    
    order_id = order_id[0].strip()
    order_page = notion_helper.get_page(order_id)

    if not order_page:
        error_message = f"MOD_Sync_Cancelations.py - Order page not found for {order_id}"
        logger.error(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        return
    
    order_status = notion_helper.return_property_value(order_page['properties']['Status'], order_id)    

    if order_status != "Canceled":
        cancel_order(order_id, order_page)
    
    job_list = notion_helper.return_property_value(order_page['properties']['Jobs'], order_id)
    order_number = notion_helper.return_property_value(order_page['properties']['Order number'], order_id)
    
    if not job_list:
        error_message = f"MOD_Sync_Cancelations.py - Job list not found for order {order_id}"
        logger.error(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        return
    
    cancel_jobs(job_list, order_number, page_id)
    
    pass


def process_page(page_data):
    if page_data:
        page_id = page_data['id'].replace("-", "")
        logger.info(f"Page data found for {page_id}")
        
        page_db_id = page_data['parent']['database_id'].replace("-", "")
        
        if page_db_id == SYS_CONF['ORDERS_DB_ID']:
            cancel_from_order(page_data, page_id)
        
        elif page_db_id == SYS_CONF['JOBS_DB_ID']:
            cancel_from_job(page_data, page_id)
        
        else:
            error_message = f"MOD_Sync_Cancelations.py - Valid database ID missing from system configuration for {page_id}."
            logger.error(f"Page {page_id} is not in a valid database")
            subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
            return
        
    else:
        error_message = f"MOD_Sync_Cancelations.py - Page data not found for {page_id}."
        logger.info(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        
    pass


def main():
    page_id = catch_variable()
    page_data = get_page_info(page_id)
    process_page(page_data)


if __name__ == "__main__":
    try:
        logger.info("[Start]")
        main()
        logger.info("[End]")
    except KeyboardInterrupt:
        logger.error("Keyboard interrupt")
        sys.exit(0)    
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        page_id = catch_variable()
        error_message = f"MOD_Sync_Cancelations.py - Error in main: {e}"

        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])

        logger.error("[End]")
