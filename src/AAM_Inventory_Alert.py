#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
import sys, logging, json


notion_helper = NotionApiHelper()
automated_emails = AutomatedEmails()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
    
RECORD_HISTORY_PATH = "storage/AAM_Inventory_Alert_Record_History.json"
EMAIL_CONFIG_PATH = "conf/AAM_Inventory_Alert_Email_Conf.json"
email_subject = "AAM Inventory Alert - #PRODUCT_NUMBER#"
email_body = '''
This is a notification that the unallocated inventory for #PRODUCT_NUMBER# has fallen below 0.
The last order received that included #PRODUCT_NUMBER# was #PURCHASE_ORDER#.

This is an automated email. Please do not reply.
'''


def catch_argument():
    try:
        product_page_id = sys.argv[1]
    except IndexError:
        print("Please provide the product page number as an argument.")
        sys.exit(1)
    return product_page_id

def load_storage():
    try:
        with open(RECORD_HISTORY_PATH, 'r') as file:
            record_history = json.load(file)
        logger.info("Record history loaded.")
    except FileNotFoundError:
        logger.error("Record history file not found. Creating new record history.")
        record_history = {'storage':{}}
    return record_history
    
def save_storage(record_history):
    with open(RECORD_HISTORY_PATH, 'w') as file:
        json.dump(record_history, file, indent=4)
    logger.info("Record history saved.")

def find_recent_order(orders_list):
    recent_order = None
    
    # Find the most recent order
    for order_id in orders_list:
        order_page = notion_helper.get_page(order_id)
        if order_page is None:
            continue
        
        if recent_order is None:
            recent_order = order_page
            
        elif order_page['created_time'] > recent_order['created_time']:
            recent_order = order_page
            
    return recent_order

def Inventory_Alert(product_page_id, email_config_path, email_subject, email_body):
    record_history = load_storage()
    history = record_history['storage']
    
    if product_page_id in history:
        last_alert = history[product_page_id]
        last_alert_date = datetime.strptime(last_alert, '%Y-%m-%d').date()
        today_date = datetime.today().date()

        if last_alert_date == today_date:
            logger.info(f"Alert already sent today for {product_page_id}.")
            sys.exit(0)
    
    product_page = notion_helper.get_page(product_page_id)
    if product_page is None:
        print("Product page not found.")
        sys.exit(1)
        
    product_properties = product_page['properties']
    product_number = notion_helper.return_property_value(product_properties['Product Code'], product_page_id)
    orders_list = notion_helper.return_property_value(product_properties['Purchase Orders'], product_page_id)
    current_inventory = notion_helper.return_property_value(product_properties['Unallocated Inventory'], product_page_id)
    
    if orders_list:
        recent_order = find_recent_order(orders_list)
        
    if recent_order is None:
        print("No recent orders found.")
        sys.exit(1)
        
    purchase_order = notion_helper.return_property_value(recent_order['properties']['PO #'], recent_order['id'])
    
    email_subject = email_subject.replace("#PRODUCT_NUMBER#", product_number)
    email_body = email_body.replace("#PRODUCT_NUMBER#", product_number).replace("#PURCHASE_ORDER#", purchase_order)
    
    print(email_subject)
    print(email_body)
    
    try:
        automated_emails.send_email(email_config_path, email_subject, email_body)
        logger.info(f"{email_subject} sent successfully.")
        record_history['storage'][product_page_id] = str(datetime.today().date())
        save_storage(record_history)
    except Exception as e:
        logger.error(f"Error sending email: {e}")
    
    pass

if __name__ == '__main__':
    product_page = catch_argument()
    if product_page:
        logger.info(f"Product page: {product_page}")
        Inventory_Alert(product_page, EMAIL_CONFIG_PATH, email_subject, email_body)