#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# RMS-MarkOrdersComplete.py
# Created on 2024-11-01 by Aria Corona

"""
This script updates the status of print orders in a Notion database.
Functions:
    catch_variable(): Retrieves the print page ID from command line arguments.
    get_page_info(page_id): Fetches page information from Notion using the provided page ID.
    get_property(page_data, property_name): Retrieves the value of a specified property from the page data.
Global Variables:
    print_complete_list (list): A list to track the completion status of print requests.
    status_list (list): A list to track the status of orders.
Execution Flow:
    1. Retrieves the print page ID from command line arguments.
    2. Fetches the print page information from Notion.
    3. Checks if the print request is marked as complete.
    4. Retrieves the associated purchase order page ID.
    5. Fetches the purchase order page information from Notion.
    6. Retrieves the list of print orders associated with the purchase order.
    7. Checks the completion status of each print order.
    8. Updates the status of the purchase order to include "transfer" if all print orders are complete.
    9. Logs the completion of the process and exits.
"""

from NotionApiHelper import NotionApiHelper
import logging, sys

notion_helper = NotionApiHelper()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

print_complete_list = []
status_list = []

def catch_variable():
    if len(sys.argv) > 1:
        print_page_id = sys.argv[1] # Command line argument
        logger.info(f"Print ID Recieved: {print_page_id}")
        return print_page_id
    sys.exit(1)

def get_page_info(page_id):
    try:
        page_info = notion_helper.get_page(page_id)
        return page_info
    except Exception as e:
        logger.error(f"Error in getting page request info: {e}")
        return None

def get_property(page_data, property_name):
    return notion_helper.return_property_value(page_data['properties'][property_name], page_data['id'])

if __name__ == "__main__":
    printpage_id = catch_variable()
    printpage_data = get_page_info(printpage_id)
    
    if get_property(printpage_data, 'Print Complete') == False:
        logger.info("Print request is not marked as complete. Exiting.")
        sys.exit(1)
        
    order_page_id = get_property(printpage_data, "Purchase Orders")
    if order_page_id:
        order_page_id = order_page_id[0]
    
    order_page_data = get_page_info(order_page_id)    
    print_request_id_list = get_property(order_page_data, "Print Orders")

    
    for id in print_request_id_list:
        if id == printpage_id:
            continue
        
        print_request_data = get_page_info(id)
        
        if get_property(print_request_data, "Print Complete"): 
            print_complete_list.append(True) 
        else: 
            print_complete_list.append(False)
        
    order_status = get_property(order_page_data, "Status")
    
    if order_status == [] or order_status == None:
        order_status = ['transfer']
        
    if "transfer" not in order_status:
        order_status.append("transfer")
    
    print(f"### Print Complete List: {print_complete_list}")
    if all(print_complete_list):
        
        if "printing" in order_status:
            order_status.remove("printing")

    status_package = notion_helper.mulsel_prop_gen("Status", "multi_select", order_status)
    response = notion_helper.update_page(order_page_id, status_package)
    
    logger.info("Process Complete.")
    sys.exit(0)