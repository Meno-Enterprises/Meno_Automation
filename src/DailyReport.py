#!/usr/bin/env python3
# Aria Corona Sept 19th, 2024
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
'''

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime, timezone, timedelta
import csv, os

print("Starting Daily Report...")
notion_helper = NotionApiHelper()
csv_directory = "DailyReportOutput"
csv_file_name = os.path.join(csv_directory, f"MOD_Daily_Report_{datetime.now().strftime('%Y-%m-%d')}.csv")
os.makedirs(csv_directory, exist_ok=True)
content_filter = {
    "and": [
        {"property": "Job status", "select": {"does_not_equal": "Canceled"}},
        {"property": "Created", "date": {"past_week": {}}},
        {"property": "Product Description", "formula": {"string": {"is_not_empty": True}}},
        {"property": "Quantity", "number": {"is_greater_than": 0}}
    ]
}
today = datetime.now(timezone.utc)
customer_dict = {}
product_dict = {}
job_id = []
status_count_dict = {"Queued": 0, "Nest": 0, "Print": 0, "Production": 0, "Packout": 0, "Complete": 0, "Canceled": 0}
total_jobs = 0  # I could do a len(notion_response) but I want to be explicit to check for any errors.
total_items = 0 # I could do a sum of the product_quantity but I want to be explicit to check for any errors.

print("Querying Notion API...")
notion_response = notion_helper.query("f11c954da24143acb6e2bf0254b64079", [r"%7CVjk", r"Ye%40l", r"Mgz%3F", r"KQKT", r"zUY%3F", r"%3AL%5EW", r"a%3Ceu", r"nNsG"], content_filter)

print("Processing Notion API response...")
for page in notion_response:
    jid = page["properties"]["ID"]["unique_id"]["number"]
    try:    # Jobs are supposed to have each of these properties, so this wlil filter out any jobs that are missing properties. This typically means the job was created, but not accepted in the system for various reasons.
        customer = page["properties"]["Customer"]["formula"]["string"]
        job_status = page["properties"]["Job status"]["select"]["name"]

        if customer and jid not in job_id:
            job_id.append(jid)
            status_count_dict[job_status] += 1
            if job_status != "Canceled":
                product_description = page["properties"]["Product Description"]["formula"]["string"]
                created = datetime.fromisoformat(page["created_time"].replace('Z', '+00:00'))
                last_edited = datetime.fromisoformat(page["last_edited_time"].replace('Z', '+00:00'))
                reprint_count = page["properties"]["Reprint count"]["formula"]["number"]
                product_quantity = page["properties"]["Quantity"]["number"]
                product_id = page["properties"]["Product ID"]["formula"]["string"]

                # Calculate job_age excluding weekends
                job_age = 0
                created_date = created
                while (today-created_date).days > 0:
                    if created_date.weekday() < 5:  # Monday to Friday are counted
                        job_age += 1
                    created_date += timedelta(days=1)

                age_label = f"Day {job_age}" if job_age < 6 else "Day 6+"
                shipped_today = True if (job_status == "Complete") and ((today - last_edited).days == 0) else False

                if customer not in customer_dict:
                    print(f"New customer found: {customer}")
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
                    print(f"New product found: {product_id}")
                    product_dict[product_id] = {
                        # "Total Jobs": 0,
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
                    # product_dict[product_id]["Total Jobs"] += 1
                    product_dict[product_id]["Total Items"] += product_quantity
                    product_dict[product_id][age_label] += 1
                    product_dict[product_id]["Total Reprints"] += reprint_count
    except Exception as e:
        print(f"Error processing job {jid}: {e}")

print(f"Processing finished.\nWriting to CSV {csv_file_name}...")
with open(csv_file_name, mode='w', newline='') as csv_file:
    csv_writer = csv.writer(csv_file)
    header = ["Customer", "Total Active Jobs", "Day 0", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6+", "Shipped Today"]
    csv_writer.writerow(header)
    for customer, data in customer_dict.items():
        row = [customer] + [data["Total Jobs"], data["Day 0"], data["Day 1"], data["Day 2"], data["Day 3"], data["Day 4"], data["Day 5"], data["Day 6+"], data["Shipped Today"]]
        csv_writer.writerow(row)
    csv_writer.writerow(["Total Jobs", total_jobs])
    csv_writer.writerow([])
    csv_writer.writerow(["Product ID", "Total Active Items",  "Day 0", "Day 1", "Day 2", "Day 3", "Day 4", "Day 5", "Day 6+", "Total Reprints"])
    for product_id, data in product_dict.items():
        row = [product_id] + [data["Total Items"], data["Day 0"], data["Day 1"], data["Day 2"], data["Day 3"], data["Day 4"], data["Day 5"], data["Day 6+"], data["Total Reprints"]]
        csv_writer.writerow(row)
    csv_writer.writerow(["Total Items", total_items])
    csv_writer.writerow([])
    csv_writer.writerow(["Job Status", "Jobs Count"])
    for status, count in status_count_dict.items():
        csv_writer.writerow([status, count])
print("CSV written successfully as ", csv_file_name)

print("Preparing to send email...")
automated_emails = AutomatedEmails()
email_config_path = "conf/MOD_DailyReport_Email_Conf.json"
attachment_path = [csv_file_name]
subject = f"MOD Daily Report {datetime.now().strftime('%m-%d-%Y')}"
body = "Please find the attached daily report for Meno On-Demand.\n\n\nThis is an automated email being sent on behalf of Aria Corona, please do not reply. If you have any questions or concerns, please contact Aria directly at acorona@menoenterprises.com."
automated_emails.send_email(email_config_path, subject, body, attachment_path)
print("End of script.") 