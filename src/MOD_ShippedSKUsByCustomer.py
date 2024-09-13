from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime, timezone, timedelta
import csv, os, time

print("Starting Daily Report...")
notion_helper = NotionApiHelper()
csv_directory = "MOD_WeeklyReportOutput"
os.makedirs(csv_directory, exist_ok=True)
order_content_filter = {"and": [{"property": "Status", "select": {"equals": "Shipped"}}]}
job_content_filter = {"or": [{"property": "Job status", "select": {"equals": "Complete"}}, {"property": "Job status", "select": {"equals": "Packout"}}]}
order_filter_properties = [r"oKZj", r"E%5Bqx", r"iLNe", r"%7B%7BfG", r"qW%7D%5E", r"gS%5Cd"] # Shipped Date, Order Number, Jobs, Customer Name, ID, Status
job_filter_properties = [r"Oe~K", r"nNsG", r"vruu", r"zUY%3F", r"%7CVjk", r"KQKT", r"LIf%7B"] # Order ID, ID, Customer Name, Product ID, Product Description, Quantity, Job revenue
order_db_id = "d2747a287e974348870a636fbfa91e3e"
job_db_id = "f11c954da24143acb6e2bf0254b64079"
output_dict = {}
job_dict = {}
customer_list = []
output_key_list = []
job_page_list = []
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
        shipped_date = datetime.fromisoformat(page["properties"]["Shipped date"]["date"]["start"].replace('Z', '+00:00'))
        order_number = page["properties"]["Order number"]["rich_text"][0]["plain_text"]
        jobs = page["properties"]["Jobs"]["relation"]
        customer_name = page["properties"]["Customer name"]["formula"]["string"]
        status = page["properties"]["Status"]["select"]["name"]
        if customer_name not in customer_list:
            customer_list.append(customer_name)
            print(f"New customer found: {customer_name}")
        for item in jobs:
            job_page_id = item["id"]
            if job_page_id in job_dict and job_page_id not in output_key_list and status == "Shipped":
                output_dict[job_page_id] = {
                    "Shipped Date": shipped_date,
                    "Order Number": order_number,
                    "Customer Name": customer_name,
                    "Product ID": job_dict[job_page_id]["Product ID"],
                    "Product Description": job_dict[job_page_id]["Product Description"],
                    "Quantity": job_dict[job_page_id]["Quantity"],
                    "Job Revenue": job_dict[job_page_id]["Job Revenue"]
                }
                output_key_list.append(job_page_id)
                print(f"New item found for order {oid}: {job_dict[job_page_id]['Product ID']}")

    except Exception as e:
        print(f"Error processing order ORD-{oid}: {e}")
        errored_orders.append(oid)
    
for customer in customer_list: # Creating a list of jobs for each customer.
    csv_file_name = os.path.join(csv_directory, f"MOD_ShippedSKUs_{customer}_{datetime.now().strftime('%Y-%m-%d')}.csv")
    file_list.append(csv_file_name)
    with open(csv_file_name, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Shipped Date", "Order Number", "Customer Name", "Product ID", "Product Description", "Quantity", "Job Revenue"])
        for key in output_key_list:
            if output_dict[key]["Customer Name"] == customer:
                csv_writer.writerow([
                    output_dict[key]["Shipped Date"],
                    output_dict[key]["Order Number"],
                    output_dict[key]["Customer Name"],
                    output_dict[key]["Product ID"],
                    output_dict[key]["Product Description"],
                    output_dict[key]["Quantity"],
                    "${:,.2f}".format(output_dict[key]["Job Revenue"])
                ])
    print(f"CSV written successfully as {csv_file_name}")