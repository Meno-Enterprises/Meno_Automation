from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
import csv, os, time

print("Starting Weekly Shipped Orders Report...")
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
order_filter_properties = [r"oKZj", r"E%5Bqx", r"iLNe", r"%7B%7BfG", r"qW%7D%5E", r"gS%5Cd", r"%60%7BTl", r"ppZT", r"~LUW"] # Shipped Date, Order Number, Jobs, Customer Name, ID, Status, Ship method, Shipment cost, Tracking
job_filter_properties = [r"Oe~K", r"nNsG", r"vruu", r"zUY%3F", r"%7CVjk", r"KQKT", r"LIf%7B", r"title"] # Order ID, ID, Customer Name, Product ID, Product Description, Quantity, Job revenue, Title
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
        line_item = page["properties"]["Job"]["title"][0]["plain_text"]

        if job_page_id not in job_dict: # Only adding jobs that haven't been added to the job_dict yet, in case of duplicate jobs for some reason.
            print(f"New job found: {jid}")
            job_dict[job_page_id] = {
                "Customer Name": customer_name,
                "Product ID": product_id,
                "Product Description": product_description,
                "Quantity": quantity,
                "Job Revenue": job_revenue,
                "Order ID": order_id,
                "Line Item": line_item
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
        shipping_cost = page["properties"]["Shipment cost"]["number"]
        tracking = page["properties"]["Tracking"]["rich_text"][0]["plain_text"] if page["properties"]["Tracking"]["rich_text"] else ""
        shipping_method = page["properties"]["Ship method"]["select"]["name"]
        if customer_name not in customer_list:
            customer_list.append(customer_name)
            print(f"New customer found: {customer_name}")
        for id in jobs_relation_property:   # Iterating through Jobs in the Order
            job_page_id = id["id"]
            print(f"Processing job {job_page_id} for order {order_number}")
            if job_page_id in job_dict and status == "Shipped": # Only processing Shipped Orders, also checks to make sure the job exists.
                if order_number not in output_dict: # New Order Number, adding to output_dict
                    output_dict[order_number] = {    
                        job_dict[job_page_id]["Line Item"]: {
                            "Product ID": job_dict[job_page_id]["Product ID"],
                            "Customer Name": customer_name,
                            "Shipped Date": shipped_date,
                            "Product Description": job_dict[job_page_id]["Product Description"],
                            "Quantity": job_dict[job_page_id]["Quantity"],
                            "Job Revenue": job_dict[job_page_id]["Job Revenue"],
                            "Shipping Method": shipping_method,
                            "Tracking": tracking
                        }
                    }
                    if shipping_cost > 0:   # Adding Shipping Charge to output_dict
                        output_dict[order_number][f"SHIP_CHG_{order_number}"] = {    # Adding Shipping Charge to output_dict
                            "Product ID": "Shipping Charge",
                            "Customer Name": customer_name,
                            "Shipped Date": shipped_date,
                            "Product Description": "Shipping Charge",
                            "Quantity": 1,
                            "Job Revenue": shipping_cost,
                            "Shipping Method": shipping_method,
                            "Tracking": tracking
                        }
                        print(f"Shipping charge found for order {order_number}: {shipping_cost}")
                    output_key_list.append(order_number)
                    print(f"New order found: {order_number}")
                    print(f"New line item found for order {order_number}: {job_dict[job_page_id]['Line Item']}")
                else:   # Existing Order Number
                    output_dict[order_number][job_dict[job_page_id]["Line Item"]] = {    # New line item for existing Order Number, adding to output_dict
                        "Product ID": job_dict[job_page_id]["Product ID"],
                        "Customer Name": customer_name,
                        "Shipped Date": shipped_date,
                        "Product Description": job_dict[job_page_id]["Product Description"],
                        "Quantity": job_dict[job_page_id]["Quantity"],
                        "Job Revenue": job_dict[job_page_id]["Job Revenue"],
                        "Shipping Method": shipping_method,
                        "Tracking": tracking
                    }
                    print(f"New line item found for order {order_number}: {job_dict[job_page_id]['Line Item']}")
    except Exception as e:
        print(f"Error processing order ORD-{oid}: {e}")
        errored_orders.append(oid)
    
for customer in customer_list: # Creating a list of jobs for each customer.
    csv_file_name = os.path.join(csv_directory, f"MOD_ShippedOrders_{customer}_{datetime.now().strftime('%Y-%m-%d')}.csv")
    file_list.append(csv_file_name)
    with open(csv_file_name, mode='w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["Ship Date", "Order number", "Line Item", "Product", "Description", "Quantity", "Cost", "Shipping Method", "Tracking"])
        for key in output_dict: # Iterates through each order number in output_dict
            for line_item in output_dict[key]: # Iterates through each line item in the order number
                if output_dict[key][line_item]["Customer Name"] == customer:
                    csv_writer.writerow([
                        output_dict[key][line_item]["Shipped Date"],
                        key,
                        line_item,
                        output_dict[key][line_item]["Product ID"],
                        output_dict[key][line_item]["Product Description"],
                        output_dict[key][line_item]["Quantity"],
                        "${:,.2f}".format(output_dict[key][line_item]["Job Revenue"]),
                        output_dict[key][line_item]["Shipping Method"],
                        output_dict[key][line_item]["Tracking"]
                    ])
    print(f"CSV written successfully as {csv_file_name}")

print("Errored Jobs:\n", errored_jobs)
print("Errored Orders:\n", errored_orders)
print("Preparing to send email...")
automated_emails = AutomatedEmails()
email_config_path = "conf/MOD_ShippedSKUsByCustomer_Email_Conf.json"
subject = f"MOD Shipped Orders by Customer {datetime.now().strftime('%m-%d-%Y')}"
body = "Attached are the shipped orders for each customer for Meno On-Demand.\n\n\n\nThis is an automated email being sent by the MOD Shipped SKUs by Customer script. Please do not reply to this email. If you have any questions or concerns, please contact Aria Corona directly at acorona@menoenterprises.com."
# automated_emails.send_email(email_config_path, subject, body, file_list)
print("End of script.")