#!/usr/bin/env python3

'''
Dependencies:
- NotionApiHelper.py
- AutomatedEmails.py
- conf/RMS_Weekly_Kodi_Report_Email_Conf.json
- conf/Aria_Email_Conf.json

Notion Database Property Dependencies:
- PO Database:
    - PO #
    - Job Description
    - Customer
    - CIC + Quantity
    - PO Due Date
    - Status
    - Priority
    - Total Item Quantity
    - Address
    - Invoiced Complete
    - Ship-By Date
'''


from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime, timedelta
from collections import OrderedDict
import csv, os

print("Starting Weekly Kodi Report...")
notion_helper = NotionApiHelper()
automated_emails = AutomatedEmails()
CSV_DIRECTORY = "output/RMS_WeeklyReportOutput"
EMAIL_CONF_PATH = "conf/RMS_Weekly_Kodi_Report_Email_Conf.json"
EMAIL_ME_PATH = "conf/Aria_Email_Conf.json"
EMAIL_SUBJECT = f"RMS Kodi Weekly Report {datetime.now().strftime('%Y%m%d')}"
EMAIL_BODY = f"RMS Kodi Weekly Report {datetime.now().strftime('%Y%m%d')}.\n\n\n\nThis is an automated email. Please do not reply."
os.makedirs(CSV_DIRECTORY, exist_ok=True)
start_date = datetime(datetime.now().year, 1, 1)
months_expanded = []
current_date = start_date
while current_date <= datetime.now():
    months_expanded.append(current_date.strftime("%B"))
    next_month = current_date.replace(day=28) + timedelta(days=4)
    current_date = next_month.replace(day=1)
start_date = start_date.strftime("%Y-%m-%d")
po_content_filter = {
    "and": [
        {"property": "Base PO", "relation": {"is_empty": True}},
        {"property": "Customer", "relation": {"contains": "0d691000-3dfb-4b0d-a76a-94d29c12e1b4"}},
        {"property": "Status", "multi_select":{"does_not_contain": "canceled"}},
        {"property": "Created", "date": {"on_or_after": start_date}}  
    ]
}
# Job Description, PO Number, Customer, Sub Item, Created, Product Code
po_filter_properties = [r"%3Bbpx", r"title", r"R~~r", r"Qy%7DZ", r"%5DoIf", r"wxy_"]
sub_item_filter_properties = r"iM%5Dz" #Item Qty
po_db_id = "e51caaf845a34283a46cdf4cadaaeea3"
output_dict = {}
output_key_list = []

for each in months_expanded:
    output_dict[each] = {}

print("Querying Notion API for POs...")
po_notion_response = notion_helper.query(po_db_id, po_filter_properties, po_content_filter)

for page in po_notion_response:
    try:
        po_number = ""
        if page["properties"][r"PO #"]["title"]:
            po_number = page["properties"][r"PO #"]["title"][0]["plain_text"]
        print(f"Processing PO: {po_number}")
        customer_id = ""
        if page["properties"]["Customer"]["relation"]:
            customer_id = page["properties"]["Customer"]["relation"][0]["id"]
        print(f"Customer ID: {customer_id}")
        created_date = "01-01-2000"
        if page["properties"]["Created"]["created_time"]:
            created_date = datetime.strptime(page["properties"]["Created"]["created_time"], "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%m-%d-%Y")
        print(f"Created Date: {created_date}")
        if len(page['properties']['Sub-Item']['relation']) > 0:
            print("Sub-Items found.")
            for sub_item in page['properties']['Sub-Item']['relation']:
                sub_item_page = notion_helper.get_page(sub_item['id'])
                product = "null"
                if sub_item_page['properties']['Product Code']['formula']['string']:
                    product = sub_item_page['properties']['Product Code']['formula']['string']
                elif page['properties']['Product Code']['formula']['string']:
                    product = page['properties']['Product Code']['formula']['string']
                else:
                    print(f"No product code found: {po_number}")
                    continue
                print(f"Product Code: {product}")
                if customer_id == "0d691000-3dfb-4b0d-a76a-94d29c12e1b4":
                    created_month = datetime.strptime(created_date, "%m-%d-%Y").strftime("%B")
                    if sub_item_page['properties']['Item Qty']['number']:
                        total_item_quantity = sub_item_page['properties']['Item Qty']['number']
                    if product in output_dict[created_month]:
                        output_dict[created_month][product] += total_item_quantity
                    else:
                        output_dict[created_month][product] = total_item_quantity
                    print(f"Total Item Quantity: {total_item_quantity}")
        else:
            print("No Sub-Items found.")
            if page['properties']['Product Code']['formula']['string']:
                product = page['properties']['Product Code']['formula']['string']
            else:
                continue
            print(f"Product Code: {product}")
            total_item_quantity = 0
            if page['properties']['Item Qty']['number']:
                if page['properties']['Item Qty']['number'] > 0:
                    total_item_quantity = page['properties']['Item Qty']['number']
            print(f"Total Item Quantity: {total_item_quantity}")
            if total_item_quantity == 0:
                print(f"No item quantity found: {po_number}")
                continue
            if customer_id == "0d691000-3dfb-4b0d-a76a-94d29c12e1b4":
                created_month = datetime.strptime(created_date, "%m-%d-%Y").strftime("%B")
                if product in output_dict[created_month]:
                    output_dict[created_month][product] += total_item_quantity
                else:
                    output_dict[created_month][product] = total_item_quantity
                print(f"Total Item Quantity: {total_item_quantity}")
    except Exception as e:
        print(f"Error processing: {e}")

print("Writing to CSV...")
csv_file = [f"{CSV_DIRECTORY}/RMS_Kodi_Past_Quarterly_Prod_Qty_{datetime.now().strftime('%Y%m%d')}.csv"]
with open(csv_file[0], mode='w', newline='') as file:
    csv_writer = csv.writer(file)
    for month in output_dict:
        csv_writer.writerow([month])
        csv_writer.writerow(["Product Code", "Total Item Quantity"])
        for product in output_dict[month]:
            csv_writer.writerow([product, output_dict[month][product]])
        csv_writer.writerow(["",""])
#print("Preparing email...")
#automated_emails.send_email(EMAIL_CONF_PATH, EMAIL_SUBJECT, EMAIL_BODY, csv_file)
print("Weekly Kodi Report Complete.")