#!/usr/bin/env python3

"""
Aria Corona - November 18th, 2024

AAM_Weekly_Inventory_Export.py
This script performs the following tasks:
1. Queries the Notion API to retrieve inventory data based on specific filters.
2. Processes the response to create a dictionary of inventory items.
3. Generates a CSV file from the processed inventory data.
4. Saves the generated CSV file to a specified output directory.
5. Sends an email with the CSV file attached.
Modules:
    NotionApiHelper: A helper module to interact with the Notion API.
    AutomatedEmails: A module to handle automated email sending.
    logging: Standard Python logging module.
    datetime: Standard Python datetime module.
Functions:
    create_dict_from_response(response):
        Creates a dictionary from the Notion API response.
    generate_csv(output_dict):
        Generates a CSV string from the inventory dictionary.
    save_csv(csv):
        Saves the generated CSV string to a file.
    send_email(csv_directory):
        Sends an email with the CSV file attached.
    main():
        Main function to orchestrate the workflow.
Usage:
    Run this script directly to execute the workflow.
"""

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
import logging
from datetime import datetime

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler('logs/AAM_Weekly_Inventory_Export.log')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

notion = NotionApiHelper()
automailer = AutomatedEmails()

OUTPUT_DIRECTORY = 'output'
CONF_DIRECTORY = 'conf/AAM_Inventory_Alert_Email_Conf.json'

DB_ID = "4a4197611dc6400b9484512b3827fcee"
QUERY_FILTER = {
    "and": [
        {
            "property": "Customer",
            "rollup": {
                "any": {
                    "relation": {
                        "contains": "9471ea8c0d964b78a9583d86086896bb"
                    }
                }
            }
        },
        {
            "property": "Track Inventory",
            "checkbox": {
                "equals": True
            } 
        } 
    ]
}
    
def create_dict_from_response(response):
    logger.info("Creating dictionary from response")
    output_dict = {}
    
    for item in response:
        id = item['id']
        item_dict = {}
        item_dict['Product Code'] = notion.return_property_value(item['properties']['Product Code'], id).strip()
        item_dict['Item Description'] = notion.return_property_value(item['properties']['Item Description'], id).strip()
        item_dict['Unallocated Inventory'] = notion.return_property_value(item['properties']['Unallocated Inventory'], id)
        
        output_dict[id] = item_dict
    
    return output_dict

def generate_csv(output_dict):
    logger.info("Generating CSV")
    csv = ",Negative quantities mean that we have open orders that we do not have inventory for.\n\n"
    csv += "Product Code,Item Description,Unallocated Inventory\n"
    
    for id in output_dict:
        item = output_dict[id]
        csv += f"{item['Product Code']},{item['Item Description']},{item['Unallocated Inventory']}\n"
    
    return csv

def save_csv(csv):
    logging.info("Saving CSV")
    today_date = datetime.today().strftime('%Y-%m-%d')
    csv_directory = f"{OUTPUT_DIRECTORY}/AAM_Weekly_Inventory_Export_{today_date}.csv"
    with open(csv_directory, 'w') as output_file:
        output_file.write(csv)
        
    
    return csv_directory

def send_email(csv_directory):
    logger.info("Sending email")
    today_date = datetime.today().strftime('%Y-%m-%d')
    subject = f"AAM Weekly Inventory Export {today_date}"
    body = 'Please find attached the weekly inventory export for AA Mills. Please note that these quantites take into account for current orders and allocations.'
    
    automailer.send_email(CONF_DIRECTORY, subject, body, [csv_directory])
    
    logger.info("Email sent")

def main():
    response = notion.query(DB_ID, content_filter=QUERY_FILTER)
    print(response)
    if response is None:
        logger.error("No response from Notion API")
        return
    
    output_dict = create_dict_from_response(response)
    csv = generate_csv(output_dict) 
    csv_directory = save_csv(csv)   
    send_email(csv_directory)
    
    pass
    
if __name__ == '__main__':
    logger.info("Starting AAM Weekly Inventory Export")
    
    try:
        main()
    except Exception as e:
        logger.error(f"Global Error in AAM Weekly Inventory Export: {e}")