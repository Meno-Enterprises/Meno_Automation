#!/usr/bin/env python3
# Aria Corona Dec 5th, 2024
# Daily Report script for Meno On-Demand.
# This script will query the Meno On-Demand Notion database for all jobs created in the past week, and generate a CSV report with the following information:
# - Total active jobs by customer
# - Total active items by product
# - Job status counts
# The script will then send an email with the CSV report attached.

'''
Dependencies:
- NotionApiHelper.py
- AutomatedEmails.py

Notion Database Property Dependencies:
Job Database:
- ID
- Customer
- Job status
- Product Description
- Reprint count
- Quantity
- Product ID
'''

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime, timezone, timedelta
import csv, os, logging, re

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

TODAY = datetime.now(timezone.utc)

automated_emails = AutomatedEmails()
EMAIL_CONFIG_PATH = "conf/MOD_DailyReport_Email_Conf.json"
SUBJECT = f"MOD Daily Report {datetime.now().strftime('%m-%d-%Y')}"
BODY = "Please find the attached daily report for Meno On-Demand.\n\n\nThis is an automated email, please do not reply."

JOB_DB_ID = "f11c954da24143acb6e2bf0254b64079"

CONTENT_FILTER = {
    "and": [
        {
            "property": "Job status",
            "select": 
            {
                "does_not_equal": "Canceled"
            }
        },
        {
            "timestamp": "created_time",
            "created_time": 
            {
                "past_week": {}   
            }
        },
        {
            "property": "Product Description",
            "formula": 
            {
                "string": 
                {
                    "is_not_empty": True
                }
            }
        },
        {"property": "Quantity", "number": {"greater_than": 0}}
    ]
}



job_id = []
status_count_dict = {"Queued": 0, "Nest": 0, "Print": 0, "Production": 0, "Packout": 0, "Complete": 0, "Canceled": 0}
LATE_JOBS = []
DAYS_CONSIDERED_LATE = 3
LATE_DATE_REGEX = r".*(\d{4}-\d{2}-\d{2}).*"

def process_response(notion_response):
    logger.info("process_response() called.")
    
    customer_dict = {}
    product_dict = {}
    total_jobs = 0
    total_items = 0
    
    print("Processing Notion API response...")
    for page in notion_response:
        jid = page["properties"]["ID"]["unique_id"]["number"]
        try:   
            customer = page["properties"]["Customer"]["formula"]["string"]
            job_status = page["properties"]["Job status"]["select"]["name"]

            if customer and jid not in job_id:
                job_id.append(jid)
                page_id = page["id"]
                status_count_dict[job_status] += 1
                if job_status != "Canceled":
                    product_description = notion_helper.return_property_value(page["properties"]["Product Description"], page_id)
                    created = datetime.fromisoformat(page["created_time"].replace('Z', '+00:00'))
                    last_edited = datetime.fromisoformat(page["last_edited_time"].replace('Z', '+00:00'))
                    reprint_count = notion_helper.return_property_value(page["properties"]["Reprint count"], page_id)
                    product_quantity = notion_helper.return_property_value(page["properties"]["Quantity"], page_id)
                    product_id = notion_helper.return_property_value(page["properties"]["Product ID"], page_id)
                    system_status = notion_helper.return_property_value(page['properties']['System status'], page_id)
                    order_id = notion_helper.return_property_value(page['properties']['Order'], page_id)

                    # Calculate job_age excluding weekends
                    job_age = 0
                    created_date = created
                    while (TODAY-created_date).days > 0:
                        if created_date.weekday() < 5:  # Monday to Friday are counted
                            job_age += 1
                        created_date += timedelta(days=1)

                    age_label = f"Day {job_age}" if job_age < 6 else "Day 6+"
                    shipped_today = True if (job_status == "Complete") and ((TODAY - last_edited).days == 0) else False
                    
                    if job_age > DAYS_CONSIDERED_LATE and order_id:
                        if order_id[0] not in LATE_JOBS:
                            logger.info(f"Order {order_id[0]} added to late jobs.")
                            LATE_JOBS.append(order_id[0])

                    if customer not in customer_dict:
                        logger.info(f"New customer found: {customer}")
                        customer_dict[customer] = {
                            "Total Jobs": 0,
                            "Day 0": 0,
                            "Day 1": 0,
                            "Day 2": 0,
                            "Day 3": 0,
                            "Day 4": 0,
                            "Day 5": 0,
                            "Day 6+": 0,
                            "Shipped Today": 0
                        }

                    if product_id not in product_dict:
                        logger.info(f"New product found: {product_id}")
                        product_dict[product_id] = {
                            "Total Items": 0,
                            "Day 0": 0,
                            "Day 1": 0,
                            "Day 2": 0,
                            "Day 3": 0,
                            "Day 4": 0,
                            "Day 5": 0,
                            "Day 6+": 0,
                            "Total Reprints": 0
                        }   
                    if shipped_today:
                        customer_dict[customer]["Shipped Today"] += 1

                    if job_status != "Complete":
                        total_jobs += 1
                        total_items += product_quantity
                        customer_dict[customer]["Total Jobs"] += 1
                        customer_dict[customer][age_label] += 1
                        product_dict[product_id]["Total Items"] += product_quantity
                        product_dict[product_id][age_label] += 1
                        product_dict[product_id]["Total Reprints"] += reprint_count
        except Exception as e:
            logger.error(f"Error processing job {jid}: {e}")
    
    logger.info("Finished process_response().")
    return product_dict, customer_dict, status_count_dict, total_jobs, total_items


def get_order_list(jobs_list):
    logger.info("get_order_list() called.")
    order_id_list = []
    ship_date_pattern = re.compile(LATE_DATE_REGEX)
    
    for id in jobs_list:
        page_data = notion_helper.get_page(id)
        
        if not page_data:
            print(f"Error retrieving page data for job {id}")
            continue
        
        order_id = notion_helper.return_property_value(page_data['properties']['Order number'], id)
        ship_date = notion_helper.return_property_value(page_data['properties']['Ship date'], id)
        
        match = ship_date_pattern.search(ship_date)
        if match:
            ship_date = match.group(1)
        
        customer = notion_helper.return_property_value(page_data['properties']['Customer name'], id)
        products = notion_helper.return_property_value(page_data['properties']['Products'], id)
        if products:
            products = products.replace(',',' ')
        else:
            products = "Missing product"
        
        order_id_list.append((order_id, ship_date, customer, products))
        
    logger.info("Finished get_order_list().")
    return order_id_list


def write_csv_jobs_by_cust(csv_writer, customer_dict, total_jobs):
    logger.info("write_csv_jobs_by_cust() called.")
    header = ["Customer", "Total Active Jobs", "Day 0", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6+", "Shipped Today"]
    csv_writer.writerow(header)
    for customer, data in customer_dict.items():
        row = [customer] + [data["Total Jobs"], data["Day 0"], data["Day 1"], data["Day 2"], data["Day 3"], data["Day 4"], data["Day 5"], data["Day 6+"], data["Shipped Today"]]
        csv_writer.writerow(row)
    csv_writer.writerow(["Total Jobs", total_jobs])
    
    logger.info("Finished write_csv_jobs_by_cust().")


def write_csv_jobs_by_product(csv_writer, product_dict, total_items):
    logger.info("write_csv_jobs_by_product() called.")
    csv_writer.writerow(["Product ID", "Total Active Items",  "Day 0", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6+", "Total Reprints"])
    for product_id, data in product_dict.items():
        row = [product_id] + [data["Total Items"], data["Day 0"], data["Day 1"], data["Day 2"], data["Day 3"], data["Day 4"], data["Day 5"], data["Day 6+"], data["Total Reprints"]]
        csv_writer.writerow(row)
    csv_writer.writerow(["Total Items", total_items])
    
    logger.info("Finished write_csv_jobs_by_product().")


def write_csv_status_count(csv_writer, status_count_dict):
    logger.info("write_csv_status_count() called.")
    csv_writer.writerow(["Job Status", "Jobs Count"])
    for status, count in status_count_dict.items():
        csv_writer.writerow([status, count])
    logger.info("Finished write_csv_status_count().")


def write_csv_late_orders(csv_writer, order_list):
    logger.info("write_csv_late_orders() called.")
    csv_writer.writerow(['Orders older than 3 days.'])
    csv_writer.writerow(["Order ID", "Ship Date", "Customer", "Products"])
    for order_id, ship_date, customer, products in order_list:
        csv_writer.writerow([order_id, ship_date, customer, products])
    logger.info("Finished write_csv_late_orders().")


def write_csv(customer_dict, product_dict, status_count_dict, order_list, total_jobs, total_items):
    logger.info("write_csv() called.")
    logger.info(f"Writing to CSV {CSV_FILE_NAME}...")
    with open(CSV_FILE_NAME, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        write_csv_jobs_by_cust(csv_writer, customer_dict, total_jobs)
        
        csv_writer.writerow([])
        
        write_csv_jobs_by_product(csv_writer, product_dict, total_items)

        csv_writer.writerow([])
        
        write_csv_status_count(csv_writer, status_count_dict)  
        
        csv_writer.writerow([])
        
        write_csv_late_orders(csv_writer, order_list)
        
    logger.info("Finished write_csv().")
    

def main():
    logger.info("main() called.")
    
    notion_response = notion_helper.query(JOB_DB_ID, content_filter=CONTENT_FILTER)

    product_dict, customer_dict, status_count_dict, total_jobs, total_items = process_response(notion_response)

    order_list = get_order_list(LATE_JOBS)

    write_csv(customer_dict, product_dict, status_count_dict, order_list, total_jobs, total_items)
    
    automated_emails.send_email(EMAIL_CONFIG_PATH, SUBJECT, BODY, [CSV_FILE_NAME])
    
    logger.info("Finished main().")
    
    
    
if __name__ == "__main__":
    logger.info("Starting Daily Report...")
    main()
    logger.info("End of script.") 