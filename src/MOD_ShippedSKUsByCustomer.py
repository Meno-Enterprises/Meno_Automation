#!/usr/bin/env python3
# Aria Corona Sept 19th, 2024
# Script to pull data from Notion API and generate a weekly report of shipped SKUs by customer.

'''
Dependencies:
- NotionApiHelper.py
- AutomatedEmails.py
- conf/MOD_ShippedSKUsByCustomer_Email_Conf.json
'''

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
import csv, os, time

print("Starting Weekly Shipped SKUs Report...")
notion_helper = NotionApiHelper()
csv_directory = "MOD_WeeklyReportOutput"
os.makedirs(csv_directory, exist_ok=True)
order_content_filter = {"and": [{"property": "Status", "select": {"equals": "Shipped"}}]}
job_content_filter = {
    "and": [
        {"property": "Created", "date": {"past_month": {}}},
        {"or": [
        {"property": "Job status", "select": {"equals": "Complete"}},
        {"property": "Job status", "select": {"equals": "Packout"}}
        ]}
    ]
}

order_filter_properties = [r"oKZj", r"E%5Bqx", r"iLNe", r"%7B%7BfG", r"qW%7D%5E", r"gS%5Cd"] # Shipped Date, Order Number, Jobs, Customer Name, ID, Status
job_filter_properties = [r"Oe~K", r"nNsG", r"vruu", r"zUY%3F", r"%7CVjk", r"KQKT", r"LIf%7B"] # Order ID, ID, Customer Name, Product ID, Product Description, Quantity, Job revenue
order_db_id = "d2747a287e974348870a636fbfa91e3e"
job_db_id = "f11c954da24143acb6e2bf0254b64079"
output_dict = {}
job_dict = {}
customer_list = []
output_key_list = []
file_list = []
errored_jobs = []
errored_orders = []

print("Querying Notion API for orders...")
order_notion_response = notion_helper.query(order_db_id, order_filter_properties, order_content_filter)
time.sleep(.5)
print("Querying Notion API for jobs...")
job_notion_response = notion_helper.query(job_db_id, job_filter_properties, job_content_filter)
for page in job_notion_response: # Parsing Jobs into a dictionary by Job Page ID, to be used later when parsing Orders.
    jid = page["properties"]["ID"]["unique_id"]["number"]
    job_page_id = page["id"]
    try:
        customer_name = page["properties"]["Customer Name"]["formula"]["string"]
        product_id = page["properties"]["Product ID"]["formula"]["string"]
        product_description = page["properties"]["Product Description"]["formula"]["string"]
        quantity = page["properties"]["Quantity"]["number"]
        job_revenue = page["properties"]["Job revenue"]["formula"]["number"]
        order_id = page["properties"]["Order ID"]["formula"]["string"]

        if job_page_id not in job_dict:
            print(f"New job found: {jid}")
            job_dict[job_page_id] = {
                "Customer Name": customer_name,
                "Product ID": product_id,
                "Product Description": product_description,
                "Quantity": quantity,
                "Job Revenue": job_revenue,
                "Order ID": order_id
            }
    except Exception as e:
        print(f"Error processing job {jid}: {e}")
        errored_jobs.append(jid)
        
for page in order_notion_response: # Iterating through Orders to find Shipped Orders and matching Jobs. Adding to output_dict.
    oid = page["properties"]["ID"]["unique_id"]["number"]
    try:
        shipped_date = datetime.fromisoformat(page["properties"]["Shipped date"]["date"]["start"].replace('Z', '+00:00')).strftime('%m-%d-%Y')
        order_number = page["properties"]["Order number"]["rich_text"][0]["plain_text"]
        jobs_relation_property = page["properties"]["Jobs"]["relation"]
        customer_name = page["properties"]["Customer name"]["formula"]["string"]
        status = page["properties"]["Status"]["select"]["name"]
        if customer_name not in customer_list:
            customer_list.append(customer_name)
            print(f"New customer found: {customer_name}")
        for id in jobs_relation_property:   # Iterating through Jobs in the Order
            job_page_id = id["id"]
            print(f"Processing job {job_page_id} for order {order_number}")
            if job_page_id in job_dict and status == "Shipped":
                if order_number not in output_dict:
                    output_dict[order_number] = {    # New Order Number, adding to output_dict
                        job_dict[job_page_id]["Product ID"]: {
                            "Customer Name": customer_name,
                            "Shipped Date": shipped_date,
                            "Product Description": job_dict[job_page_id]["Product Description"],
                            "Quantity": job_dict[job_page_id]["Quantity"],
                            "Job Revenue": job_dict[job_page_id]["Job Revenue"]
                        }
                    }
                    output_key_list.append(order_number)
                    print(f"New order found: {order_number}")
                    print(f"New item found for order {order_number}: {job_dict[job_page_id]['Product ID']}")
                else:   # Existing Order Number, checking for existing Product ID
                    if job_dict[job_page_id]["Product ID"] in output_dict[order_number]:   # Existing Product ID for existing Order Number, updating values
                        output_dict[order_number][job_dict[job_page_id]["Product ID"]]["Quantity"] += job_dict[job_page_id]["Quantity"]
                        output_dict[order_number][job_dict[job_page_id]["Product ID"]]["Job Revenue"] += job_dict[job_page_id]["Job Revenue"]
                        print(f"Existing item found for order {order_number}: {job_dict[job_page_id]['Product ID']}")
                    else: 
                        output_dict[order_number] = {    # New Product for existing Order Number, adding to output_dict
                            job_dict[job_page_id]["Product ID"]: {
                                "Customer Name": customer_name,
                                "Shipped Date": shipped_date,
                                "Product Description": job_dict[job_page_id]["Product Description"],
                                "Quantity": job_dict[job_page_id]["Quantity"],
                                "Job Revenue": job_dict[job_page_id]["Job Revenue"]
                            }
                        }
                        print(f"New item found for order {order_number}: {job_dict[job_page_id]['Product ID']}")
                print(f"New item found for order {order_number}: {job_dict[job_page_id]['Product ID']}")
    except Exception as e:
        print(f"Error processing order ORD-{oid}: {e}")
        errored_orders.append(oid)
    
for customer in customer_list: # Creating a list of jobs for each customer.
    csv_file_name = os.path.join(csv_directory, f"MOD_ShippedSKUs_{customer}_{datetime.now().strftime('%Y-%m-%d')}.csv")
    file_list.append(csv_file_name)
    with open(csv_file_name, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Ship Date", "Order number", "Product", "Description", "Quantity", "Cost"])
        for key in output_dict:
            for product in output_dict[key]:
                if output_dict[key][product]["Customer Name"] == customer:
                    csv_writer.writerow([
                        output_dict[key][product]["Shipped Date"],
                        key,
                        product,
                        output_dict[key][product]["Product Description"],
                        output_dict[key][product]["Quantity"],
                        "${:,.2f}".format(output_dict[key][product]["Job Revenue"])
                    ])
    print(f"CSV written successfully as {csv_file_name}")

print("Errored Jobs:\n", errored_jobs)
print("Errored Orders:\n", errored_orders)
print("Preparing to send email...")
automated_emails = AutomatedEmails()
email_config_path = "conf/MOD_ShippedSKUsByCustomer_Email_Conf.json"
subject = f"MOD Shipped SKUs by Customer {datetime.now().strftime('%m-%d-%Y')}"
body = "Attached are the shipped SKUs for each customer for Meno On-Demand.\n\n\n\nThis is an automated email being sent by the MOD Shipped SKUs by Customer script. Please do not reply to this email. If you have any questions or concerns, please contact Aria Corona directly at acorona@menoenterprises.com."
automated_emails.send_email(email_config_path, subject, body, file_list)
print("End of script.")