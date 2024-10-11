#!/usr/bin/env python3

'''
Dependencies:
- NotionApiHelper.py
- AutomatedEmails.py
- conf/RMS_Weekly_Order_Report_Email_Conf.json
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
    - Production Notes
    - Ship Method
'''


from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
from collections import OrderedDict
import csv, os, json

print("Starting RMS Open Order Report...")
notion_helper = NotionApiHelper()
automated_emails = AutomatedEmails()
csv_directory = "output/RMS_Open_Order_ReportOutput"
stored_output_file = f"{csv_directory}/RMS_Open_Order_Report.json"
email_conf_path = "conf/RMS_Weekly_Order_Report_Email_Conf.json"
email_me_path = "conf/Aria_Email_Conf.json"
email_subject = f"RMS Open Order Report {datetime.now().strftime('%Y%m%d')}"
email_body = f"RMS Open Order Report {datetime.now().strftime('%Y%m%d')}. Changes in orders from the prior spreadsheet export will be marked with an asterisk(*).\n\n\n\nThis is an automated email. Please do not reply."
os.makedirs(csv_directory, exist_ok=True)
po_content_filter = {
    "and": [
        {"property": "Base PO", "relation": {"is_empty": True}},
        {"property": "Customer", "relation": {"does_not_contain": "9471ea8c0d964b78a9583d86086896bb"}}, # Exclused AAM
        {"property": "Invoiced Complete", "checkbox": {"equals": False}},
        {"property": "Status", "multi_select":{"does_not_contain": "canceled"}},
        {"property": "Created", "date": {"past_year": {}}}  
    ]
}
# Job Description, PO Number, Customer, CIC+Quant, PO Due Date, Status, Priority, Total Item Quantity, Address, Invoiced Complete, Ship-By Date, Production Notes, Shipping Method
po_filter_properties = [r"%3Bbpx", r"title", r"wxy_", r"DLvO", r"%3CvR%3B", r"hp%5C%3E", r"%5B%7DLB", r"H%5BTO", r"%5E%3Dgd", r"ERGt", r"ewny", r"%5C%60pr", r"KaEX"] 
po_db_id = "e51caaf845a34283a46cdf4cadaaeea3"
output_dict = {}
output_key_list = []
prior_export = {}

if os.path.exists(stored_output_file):
    with open(stored_output_file, 'r', encoding='utf-8') as json_file:
        prior_export = json.load(json_file)
        for po_number in prior_export:
            for key in prior_export[po_number]:
                if isinstance(prior_export[po_number][key], str):
                    prior_export[po_number][key] = prior_export[po_number][key].replace('*', '')

print("Querying Notion API for POs...")
po_notion_response = notion_helper.query(po_db_id, po_filter_properties, po_content_filter)

for page in po_notion_response:
    try:
        po_number = ""
        if page["properties"][r"PO #"]["title"]:
            po_number = page["properties"][r"PO #"]["title"][0]["plain_text"]
        # print(f"Processing PO: {po_number}")
        job_description = ""
        if page["properties"]["Job Description"]["rich_text"]:
            job_description = page["properties"]["Job Description"]["rich_text"][0]["plain_text"]
        # print(f"Job Description: {job_description}")
        customer_id = ""
        if page["properties"]["Customer"]["relation"]:
            customer_id = page["properties"]["Customer"]["relation"][0]["id"]
        # print(f"Customer ID: {customer_id}")
        cic_quant = ""
        if page["properties"][r"CIC + Quantity"]["rich_text"]:
            cic_quant = page["properties"][r"CIC + Quantity"]["rich_text"][0]["plain_text"]
        # print(f"CIC+Quant: {cic_quant}")
        po_due_date = "01-01-2000"
        if page["properties"]["PO Due Date"]["date"]:
            po_due_date = datetime.strptime(page["properties"]["PO Due Date"]["date"]["start"], "%Y-%m-%d").strftime("%m-%d-%Y")
        ship_by_date = "01-01-2000"
        if page["properties"]["Ship-By Date"]["date"]:
            ship_by_date = datetime.strptime(page["properties"]["Ship-By Date"]["date"]["start"], "%Y-%m-%d").strftime("%m-%d-%Y")
        # print(f"PO Due Date: {po_due_date}")
        # status_list = page["properties"]["Status"]["multi_select"]
        # status = []
        # if status_list:
        #    for each in status_list:
        #        status.append(each["name"])
        # print(f"Status: {status}")
        # priority = page["properties"]["Priority"]["select"]["name"]
        # print(f"Priority: {priority}")
        total_item_quantity = page["properties"]["Total Item Qty"]["rollup"]["number"]
        # print(f"Total Item Quantity: {total_item_quantity}")
        address = []
        address_list = page["properties"]["Address"]["rollup"]["array"]
        if address_list:
            for each in address_list:
                if each["rich_text"]:
                    address.append(each["rich_text"][0]["plain_text"])
        address = " ".join(address)
        # print(f"Address: {address}")
        po_invoiced_complete = page["properties"]["Invoiced Complete"]["checkbox"]
        # print(f"Invoiced Complete: {po_invoiced_complete}")
        production_notes = ""
        if page["properties"]["Production Notes"]["rich_text"]:
            production_notes = page["properties"]["Production Notes"]["rich_text"][0]["plain_text"]

        ship_method_list = page["properties"]["Ship Method"]["multi_select"]
        ship_method = []
        if ship_method_list:
            for each in ship_method_list:
                ship_method.append(each["name"])
        ship_method = ", ".join(ship_method)

        if po_invoiced_complete is False:
            output_dict[po_number] = {
                "Job Description": job_description,
                "CIC+Quant": cic_quant,
                "Total Item Quantity": total_item_quantity,
                "Address": address,
                "Ship_Method": ship_method,
                "Production Notes": production_notes,
                "Ship-By Date": ship_by_date,                
                "PO Due Date": po_due_date
            }
            print(f"Added PO: {po_number} to output_dict.")
            output_key_list.append(po_number)
            if po_number in prior_export:
                for key in prior_export[po_number]:
                    if output_dict[po_number][key] != prior_export[po_number][key]:
                        if key is not "Ship-By Date" or key is not "PO Due Date":
                            output_dict[po_number][key] = f"*{output_dict[po_number][key]}"
                        else:
                            output_dict[po_number]["Production Notes"] = f"*Dates have changed.\n{output_dict[po_number]['Production Notes']}"
                        print(f"Change detected in {po_number} for {key}.")
            else:
                print(f"New PO: {po_number} added to output_dict.")
                for key in output_dict[po_number]:
                    output_dict[po_number][key] = f"*{output_dict[po_number][key]}"
                        
    except Exception as e:
        print(f"Error processing POs:{page}\n{e}")

with open(stored_output_file, 'w', encoding='utf-8') as json_file:
    json.dump(output_dict, json_file, ensure_ascii=False, indent=4)

print(f"JSON output written to {stored_output_file}")

output_dict = OrderedDict(sorted(output_dict.items(), key=lambda item: datetime.strptime(item[1]["PO Due Date"], "%m-%d-%Y")))

print("Writing to CSV...")
csv_file = [f"{csv_directory}/RMS_Open_Order_Report_{datetime.now().strftime('%Y%m%d')}.csv"]
with open(csv_file[0], mode='w', newline='', encoding='utf-8') as file:
    csv_writer = csv.writer(file)
    csv_writer.writerow(["PO Number", "Description", "CIC+Quantity", "Total Item Quantity", "PO Due Date", "Estimated Ship-By Date", "Notes", "Preferred Shipping Method", "Ship-To Address"])
    for order in output_dict:
        csv_writer.writerow([order, output_dict[order]["Job Description"], output_dict[order]["CIC+Quant"], output_dict[order]["Total Item Quantity"], output_dict[order]["PO Due Date"], output_dict[order]["Ship-By Date"], output_dict[order]["Production Notes"], output_dict[order]["Ship_Method"], output_dict[order]["Address"]])
    
print("Preparing email...")
automated_emails.send_email(email_conf_path, email_subject, email_body, csv_file)
print("RMS Open Order Report Complete.")