#!/usr/bin/env python3

# Aria Corona - December 20th, 2024



from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime, timezone, timedelta
import csv, os, logging

print("Starting Daily Report...")
notion_helper = NotionApiHelper()
CSV_DIRECTORY = "output"
CSV_FILE_NAME = os.path.join(CSV_DIRECTORY, f"MOD_Daily_Report_{datetime.now().strftime('%Y-%m-%d')}.csv")
os.makedirs(CSV_DIRECTORY, exist_ok=True)

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/NotionEventListener.log'),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

TODAY = datetime.now()
TODAY_STR = TODAY.strftime("%Y-%m-%d")

automated_emails = AutomatedEmails()
EMAIL_CONFIG_PATH = "conf/MOD_DailyReport_Email_Conf.json"
SUBJECT = f"MOD Daily Report {TODAY_STR}"
BODY = "Please find the attached daily report for Meno On-Demand.\n\n\nThis is an automated email, please do not reply."

JOB_DB_ID = "f11c954da24143acb6e2bf0254b64079"
ORDER_DB_ID = "d2747a287e974348870a636fbfa91e3e"

ACTIVE_ORDERS_FILTER = {
    "and": [
        {
            "property": "Status",
            "select": {
                "does_not_equal": "Canceled"
            }
        },
        {
            "property": "Status",
            "select": {
                "does_not_equal": "Invoiced"
            }
        },
        {
            "property": "Status",
            "select": {
                "does_not_equal": "Shipped"
            }
        },
        {
            "property": "System status",
            "select": {
                "equals": "Active"
            }
        }
    ]
}

ACTIVE_JOBS_FILTER = {
    "and": [
        {
            "property": "Job status",
            "select": 
            {
                "does_not_equal": "Canceled"
            }
        },
        {
            "property": "Job status",
            "select": {
                "does_not_equal": "Complete"
            }
        },
        {
            "property": "System status",
            "select": {
                "equals": "Active"
            }
        }
    ]
}

ORDERS_SHIPPED_TODAY_FILTER = {
    "and": [
        {
            "property": "Shipped date",
            "date": {
                "equals": TODAY_STR
            }
        }
    ]
}

ORDERS_SHIPPED_THIS_WEEK_FILTER = {
    "and": [
        {
            "property": "Shipped date",
            "date": {
                "this_week": {}
            }
        }
    ]
}

JOBS_IN_ERROR_FILTER = {
    "and": [
        {
            "property": "System status",
            "select": {
                "equals": "Error"
            }
        },
        {
            "property": "Order status",
            "rollup": {
                "any": {
                    "select": {
                        "does_not_equal": "Shipped"
                    }
                }
            }
        },
        {
            "property": "Order status",
            "rollup": {
                "any": {
                    "select": {
                        "does_not_equal": "Canceled"
                    }
                }
            }
        },
        {
            "property": "Order status",
            "rollup": {
                "any": {
                    "select": {
                        "does_not_equal": "Invoiced"
                    }
                }
            }
        }
    ]
}

status_tracker = {
    'Queued': 0,
    'Nest': 0,
    'Nesting': 0,
    'Print': 0,
    'Production': 0,
    'Packout': 0
}

def query_db(db_id, filter):
    response = notion_helper.query(db_id, content_filter=filter)
    return response

def get_customer_data(data):
    customer_data = {}
    for page in data:
        props = page['properties']
        pid = page['id']
        customer_name = notion_helper.return_property_value(props['Customer name'], pid)
        
        created_str = page['created_time'] #"2024-10-31T13:31:00.000Z"
        current_date = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        
        # Order Age
        delta_days = 0
        while current_date < TODAY:
            current_date += timedelta(days=1)
            if current_date.weekday() < 5:  # Monday to Friday are 0-4
                delta_days += 1
        
        if customer_name not in customer_data:
            customer_data[customer_name] = {
                'total_active_orders': 0,
                0: 0,
                1: 0,
                2: 0,
                3: 0,
                4: 0,
                5: 0,
                6: 0,
            }
            
        customer_data[customer_name]['total_active_orders'] += 1
        
        if delta_days < 7:
            customer_data[customer_name][delta_days] += 1
        else:
            customer_data[customer_name][6] += 1
        
    return customer_data

def get_product_data(data):
    product_data = {}
    for page in data:
        props = page['properties']
        pid = page['id']
        
        product_id = notion_helper.return_property_value(props['Product ID'], pid)
        if not product_id:
            continue
        
        job_status = notion_helper.return_property_value(props['Job status'], pid)
        if not job_status:
            job_status = "Queued"
        
        quantity = notion_helper.return_property_value(props['Quantity'], pid)
        if not quantity:
            quantity = 1
        
        created_str = page['created_time'] #"2024-10-31T13:31:00.000Z"
        created_date = datetime.strptime(created_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        
        delta_days = 0
        while created_date < TODAY:
            created_date += timedelta(days=1)
            if created_date.weekday() < 5:  # Monday to Friday are 0-4
                delta_days += 1

        if product_id not in product_data:
            product_data[product_id] = {
                'total_active_items': 0,
                0: 0,
                1: 0,
                2: 0,
                3: 0,
                4: 0,
                5: 0,
                6: 0,
            }
            
        product_data[product_id]['total_active_items'] += quantity
        
        if delta_days < 7:
            product_data[product_id][delta_days] += quantity
        else:
            product_data[product_id][6] += quantity
        
        status_tracker[job_status] += 1
        
    return product_data

def write_csv_jobs_by_cust(csv_writer, customer_dict, total_orders):
    logger.info("write_csv_jobs_by_cust() called.")
    header = ["Customer", "Total Active Orders", "Day 0", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6+"]
    csv_writer.writerow(header)
    
    for customer, data in customer_dict.items():
        row = [customer] + [data["total_active_orders"], data[0], data[1], data[2], data[3], data[4], data[5], data[6]]
        csv_writer.writerow(row)
    csv_writer.writerow(["Total Orders", total_orders])
    
    logger.info("Finished write_csv_jobs_by_cust().")
    
def write_csv_jobs_by_product(csv_writer, product_dict, total_jobs):
    logger.info("write_csv_jobs_by_product() called.")
    
    item_count = 0
    
    for key, value in product_dict.items():
        item_count += value['total_active_items']
    
    csv_writer.writerow(["Product ID", "Total Active Items",  "Day 0", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6+"])
    for product_id, data in product_dict.items():
        row = [product_id] + [data["total_active_items"], data[0], data[1], data[2], data[3], data[4], data[5], data[6]]
        csv_writer.writerow(row)
    csv_writer.writerow(["Total Items", item_count])
    
    logger.info("Finished write_csv_jobs_by_product().")
    
def write_csv_status_count(csv_writer, status_count_dict):
    logger.info("write_csv_status_count() called.")
    csv_writer.writerow(["Job Status", "Jobs Count"])
    for status, count in status_count_dict.items():
        csv_writer.writerow([status, count])
    logger.info("Finished write_csv_status_count().")

def build_csv(ord_ship_today, ord_ship_week, error_jobs, customer_data, product_data, total_ord, total_job):
    logger.info("write_csv() called.")
    logger.info(f"Writing to CSV {CSV_FILE_NAME}...")
    with open(CSV_FILE_NAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        
        csv_writer.writerow(["Orders Shipped Today:", ord_ship_today, "Orders Shipped This Week:", ord_ship_week, "Jobs in Error:", error_jobs])

        csv_writer.writerow([])

        write_csv_jobs_by_cust(csv_writer, customer_data, total_ord)
        
        csv_writer.writerow([])
        
        write_csv_jobs_by_product(csv_writer, product_data, total_job)

        csv_writer.writerow([])
        
        write_csv_status_count(csv_writer, status_tracker)
        
        
    logger.info("Finished write_csv().")

def main():
    logger.info("[START]")
    
    active_orders = query_db(ORDER_DB_ID, ACTIVE_ORDERS_FILTER)
    active_jobs = query_db(JOB_DB_ID, ACTIVE_JOBS_FILTER)
    orders_shipped_today = len(query_db(ORDER_DB_ID, ORDERS_SHIPPED_TODAY_FILTER))
    orders_shipped_this_week = len(query_db(ORDER_DB_ID, ORDERS_SHIPPED_THIS_WEEK_FILTER))
    jobs_in_error = len(query_db(JOB_DB_ID, JOBS_IN_ERROR_FILTER))

    customer_data = get_customer_data(active_orders)
    product_data = get_product_data(active_jobs)
    total_orders = len(active_orders)
    total_jobs = len(active_jobs)
    
    build_csv(orders_shipped_today, orders_shipped_this_week, jobs_in_error, customer_data, product_data, total_orders, total_jobs)
    
    automated_emails.send_email(EMAIL_CONFIG_PATH, SUBJECT, BODY, [CSV_FILE_NAME])
    
    logger.info("[END]")
    

if __name__ == "__main__":
    main()