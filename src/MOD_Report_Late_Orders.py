#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
import csv

notion_helper = NotionApiHelper()
automated_emails = AutomatedEmails()

ORDER_DB_ID = 'd2747a287e974348870a636fbfa91e3e'

csv_headers = ['Order Number', 'Creation Date', 'Ship-by Date', 'Order Status', 'System Status', 'Product', 'Error Log']

NOW = datetime.now()
NOW_STR = NOW.strftime('%Y-%m-%d')

csv_file_output = f'output/{NOW_STR}_Late_Orders.csv'
EMAIL_CONF = 'conf/MOD_DailyReport_Email_Conf.json'
SUBJECT = f'MOD Past Due Report {NOW_STR}'
BODY = f'Please see the attached spreadsheet for orders that are past due as of {NOW.strftime("%Y-%m-%d, %H:%M:%S")}'
FILE_ATTACHMENT_PATH = [csv_file_output]

job_counter = 0
order_counter = 0

CONTENT_FILTER = {
    'and': [
        {
            'property': 'Ship date',
            'date': {
                'before': NOW_STR
            }
        },
        {
            'property': 'Status',
            'select': {
                'does_not_equal': 'Shipped'
            }
        },
        {
            'property': 'Status',
            'select': {
                'does_not_equal': 'Canceled'
            }
        },
        {
            'property': 'Status',
            'select': {
                'does_not_equal': 'Invoiced'
            }
        },
        {
            'property': 'System status',
            'select': {
                'does_not_equal': 'Pause'
            }
        }
    ]
}

results = notion_helper.query(ORDER_DB_ID, content_filter=CONTENT_FILTER)

output_library = []

def fix_date(date_str):
    if date_str:
        return date_str.replace('.000Z', '').replace('.000+00:00', '')
    else:
        return ""

def fix_value(value):
    if value:
        return value
    else:
        return ""

if not results:
    print('No results found')
    exit()

for page in results:
    oid = page['id']
    order_number = notion_helper.return_property_value(page['properties']['Order number'], oid)

    order_dict = {
        'order_number': None,
        'creation_date': None,
        'ship-by_date': None,
        'date_shipped': None,
        'status': None,
        'log': None,
        'system_status': None,
        'job_data': []
    }
    
    page_dict = order_dict.copy()
    job_list = []
    page_props = page['properties']
    
    page_dict['order_number'] = order_number
    
    created_time_str = page['created_time'] #"YYYY-MM-DDTHH:MM:SS.000Z" -> "YYYY-MM-DDTHH:MM:SS"
    page_dict['creation_date'] = fix_date(created_time_str)
    
    ship_date_str = notion_helper.return_property_value(page_props['Ship date'], oid)
    page_dict['ship-by_date'] = fix_date(ship_date_str)
    
    page_dict['status'] = fix_value(notion_helper.return_property_value(page_props['Status'], oid))
    page_dict['system_status'] = fix_value(notion_helper.return_property_value(page_props['System status'], oid))
    
    job_list = notion_helper.return_property_value(page_props['Jobs'], oid)
    
    for job_id in job_list:
        job_page = notion_helper.get_page(job_id)
        
        if job_page:
            job_props = job_page['properties']
            
            temp_dict = {}
            temp_dict['job_id'] = job_id
            internal_id = job_props['ID']['unique_id']
            if internal_id:
                temp_dict['internal_uid'] = job_props['ID']['unique_id']['prefix'] + "-" + str(job_props['ID']['unique_id']['number'])
            temp_dict['product'] = fix_value(notion_helper.return_property_value(job_props['Product ID'], job_id))
            temp_dict['system_status'] = fix_value(notion_helper.return_property_value(job_props['System status'], job_id))
            temp_dict['status'] = fix_value(notion_helper.return_property_value(job_props['Job status'], job_id))
            
            log = notion_helper.return_property_value(job_props['Log'], job_id)
            temp_dict['log'] = log.replace('\n', ' ') if log else ""
            
            job_counter += 1
            page_dict['job_data'].append(temp_dict)

    output_library.append(page_dict)
                    
with open(csv_file_output, mode='w', newline='') as file:
    writer = csv.writer(file)
    
    writer.writerow(['Total Orders', len(output_library), 'Total Jobs', job_counter])
    writer.writerow([])
    
    writer.writerow(csv_headers)
    
    for order in reversed(output_library):
        writer.writerow([str(order['order_number']), order['creation_date'], order['ship-by_date'], order['status'], order['system_status']])
        
        if order['job_data']:
            for job in order['job_data']:
                writer.writerow([job['internal_uid'], "", "", job['status'], job['system_status'], job['product'], job['log']])
                
        writer.writerow([])
                    
automated_emails.send_email(EMAIL_CONF, SUBJECT, BODY, file_attachment_paths=FILE_ATTACHMENT_PATH)