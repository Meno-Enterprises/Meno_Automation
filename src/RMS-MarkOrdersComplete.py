#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# RMS-MarkOrdersComplete.py
# Created on 2024-10-30 by Aria Corona

"""
This script processes shipment orders and updates their status in a Notion database.
Modules:
    NotionApiHelper: A helper module to interact with the Notion API.
    logging: Provides logging capabilities.
    sys: Provides access to command-line arguments.
Constants:
    SHIPPING_DB_ID (str): The ID of the Notion database for shipping.
Functions:
    catch_variable():
        Retrieves the shipment ID from the command-line arguments.
        Returns:
            str: The shipment ID if provided, otherwise None.
    get_page_info(shipment_id):
        Fetches the shipment information from the Notion database.
        Args:
            shipment_id (str): The ID of the shipment.
        Returns:
            dict: The shipment information if successful, otherwise None.
    get_property(shipment_data, property_name):
        Retrieves a specific property value from the shipment data.
        Args:
            shipment_data (dict): The shipment data.
            property_name (str): The name of the property to retrieve.
        Returns:
            Any: The value of the specified property.
    process_shipment_item(item_data):
        Processes a single shipment item, updating its invoiced quantity and marking it as invoiced complete if applicable.
        Args:
            item_data (dict): The data of the shipment item.
Main Execution:
    - Retrieves the shipment ID from the command-line arguments.
    - Fetches the shipment data from the Notion database.
    - Checks if the shipment is marked as invoiced; exits if not.
    - Processes each shipment item, updating their status in the Notion database.
    - Fetches the purchase order data and marks it as invoiced complete if applicable.
    - Logs the completion of the process and exits.
"""

from NotionApiHelper import NotionApiHelper
import logging, sys

notion_helper = NotionApiHelper()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SHIPPING_DB_ID = "4e38e274e96f494aafb0a5e507ab5656"

def catch_variable():
    if len(sys.argv) > 1:
        shipment_id = sys.argv[1] # Command line argument
        logger.info(f"Shipment ID Recieved: {shipment_id}")
        return shipment_id
    return None

def get_page_info(shipment_id):
    try:
        shipment_info = notion_helper.get_page(shipment_id)
        return shipment_info
    except Exception as e:
        logger.error(f"Error in getting shipment info: {e}")
        return None

def get_property(page_data, property_name):
    return notion_helper.return_property_value(page_data['properties'][property_name], page_data['id'])

def process_shipment_item(item_data):
    order_quantity = get_property(item_data, "Order Qty")
    if order_quantity == None:
        order_quantity = 0
        
    shipment_quantity = get_property(item_data, "Shipment Qty")
    if shipment_quantity == None:
        shipment_quantity = 0
    
    total_shipped = get_property(item_data, "Total Shipped")
    if total_shipped == None:
        total_shipped = 0
        
    invoiced_quantity = get_property(item_data, "Invoiced Qty")
    if invoiced_quantity == None:
        invoiced_quantity = 0
    
    sub_item_id = get_property(item_data, "PO+Item# (Select Me)")
    
    # Update Invoiced Quantity
    if invoiced_quantity != shipment_quantity:
        invoiced_quantity = shipment_quantity
        logger.info(f"Updated Invoiced Quantity to {invoiced_quantity}")
        ship_item_package = notion_helper.simple_prop_gen("Invoiced Qty", "number", invoiced_quantity)
        response = notion_helper.update_page(item_data['id'], ship_item_package)
        print(f"Shipment Item: {item_data['id']} Update status: {response}")
        
    # Update Sub Item as Invoiced Complete
    if total_shipped + shipment_quantity >= order_quantity and sub_item_id != None and order_quantity + shipment_quantity + total_shipped != 0:
        logger.info(f"Marking Sub Item [{sub_item_id}] as Invoiced Complete")
        sub_item_package = notion_helper.simple_prop_gen("Invoiced Complete", "checkbox", True)
        response = notion_helper.update_page(sub_item_id[0], sub_item_package)
        print(f"Sub Item: {sub_item_id} Update status: {response}")
        
        
if __name__ == "__main__":
    shipment_id = catch_variable()
    shipment_data = get_page_info(shipment_id)
    
    if notion_helper.return_property_value(shipment_data['properties']['Invoiced'], shipment_data['id']) == False:
        logger.info("Shipment is not marked invoiced. Exiting.")
        sys.exit(1)
        
    item_ids = get_property(shipment_data, "Shipment Items")
    
    for id in item_ids:
        item_data = notion_helper.get_page(id)
        process_shipment_item(item_data)
        
    order_id = get_property(shipment_data, "Purchase Order")
    if order_id != None:
        order_data = notion_helper.get_page(order_id[0])
    else: 
        logger.info("No Order Page ID found. Exiting.")
        sys.exit(1)
        
    if get_property(order_data, "PO Invoiced Complete"):
        logger.info(f"Marking Order [{order_data['id']}] as Invoiced Complete.")
        order_package = notion_helper.simple_prop_gen("Invoiced Complete", "checkbox", True)
        response = notion_helper.update_page(order_id[0], order_package)
    
    logger.info("Process Complete.")
    sys.exit(0)