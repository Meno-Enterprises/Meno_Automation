#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
import logging, os, json

notion = NotionApiHelper()
automailer = AutomatedEmails()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
   logging.FileHandler('MOD_Data_Tracker.log'),
   logging.StreamHandler()
    ]
)

logger = logging.getLogger('MOD_Data_Tracker')
logger.info('Logging setup complete')

STORAGE_DIRECTORY = 'storage/MOD_Data_Tracker.json'

ORDERS_DB_ID = "d2747a287e974348870a636fbfa91e3e"
JOBS_DB_ID = "f11c954da24143acb6e2bf0254b64079"
PRODUCTS_DB_ID = ""
CUSTOMERS_DB_ID = ""

TODAY = datetime.now().strftime('%Y-%m-%d')

JORDER_CONTENT_FILTER = [{"and":[{"property":"Status","select":{"does_not_equal":"Canceled"}},
    {"timestamp":"created_time","created_time":{"equals":TODAY}}]},{"and":[{"property":"Job status",
    "select":{"does_not_equal":"Canceled"}},{"timestamp":"created_time","created_time":{"equals":TODAY}}]}]

def query_db(db_id, query_filter = None):
    logger.info(f'Querying database {db_id} with filter {query_filter}')
    response = notion.query(db_id, content_filter=query_filter)
    return response

def get_product_quantities(data):
    product_dict = {}
    
    for page in data:
        product_number = notion.return_property_value(page['properties']['Product ID'], page['id'])
        quantity = notion.return_property_value(page['properties']['Quantity'], page['id'])
        
        if product_number == None or quantity == None:
            continue
        
        if product_number in product_dict:
            product_dict[product_number] += quantity
        else:
            product_dict[product_number] = quantity
    
    return product_dict

def write_to_storage(data):
    logger.info('Writing data to storage')
    
    if os.path.exists(STORAGE_DIRECTORY):
        with open(STORAGE_DIRECTORY, 'r') as file:
            stored_data = json.load(file)
    else:
        stored_data = []

    stored_data.append(data)

    with open(STORAGE_DIRECTORY, 'w') as file:
        json.dump(stored_data, file, indent=4)

def main():
    order_db_data = query_db(ORDERS_DB_ID, JORDER_CONTENT_FILTER[0])
    job_db_data = query_db(JOBS_DB_ID, JORDER_CONTENT_FILTER[1])
    
    products_ordered = get_product_quantities(job_db_data)
    
    orders_received = len(order_db_data)
    jobs_received = len(job_db_data)
    
    logging.info(f'Orders received: {orders_received}')
    logging.info(f'Jobs received: {jobs_received}')
    logging.info(f"Products ordered: {products_ordered}")
    
    data = {}
    data[TODAY] = {
        'Orders Received': orders_received,
        'Jobs Received': jobs_received,
        'Products Ordered': products_ordered
    }
    
    write_to_storage(data)
    

if __name__ == '__main__':


    main()