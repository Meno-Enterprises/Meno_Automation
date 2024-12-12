#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
from datetime import datetime
import csv

notion_helper = NotionApiHelper()

ORDER_DB_ID = 'd2747a287e974348870a636fbfa91e3e'
csv_file_output = 'output/2024_12_11_Late_Orders_2.csv'
csv_headers = ['Order Number', 'Creation Date', 'Ship-by Date', 'Order Status', 'System Status', 'Product', 'Date Shipped', 'Tracking Number']



CONTENT_FILTER = {
    'and': [
            {
            'timestamp': 'created_time',
            'created_time': {
                'on_or_after': '2024-11-29'
            }
        }
    ]
}

LIST_PATH = 'src/Resources/2024_12_11_Late_Orders.txt'

with open(LIST_PATH, 'r') as file:
    late_order_list = file.read().splitlines()
    


results = notion_helper.query(ORDER_DB_ID, content_filter=CONTENT_FILTER)

output_library = []

def fix_date(date_str):
    if date_str:
        return date_str.replace('.000Z', '')
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
    
    if order_number in late_order_list:
        order_dict = {
            'order_number': None,
            'creation_date': None,
            'ship-by_date': None,
            'date_shipped': None,
            'status': None,
            'system_status': None,
            'tracking': None,
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
        
        date_shipped_str = notion_helper.return_property_value(page_props['Shipped date'], oid) #'YYYYY-MM-DD'
        page_dict['date_shipped'] = date_shipped_str if date_shipped_str else ""
        
        page_dict['status'] = fix_value(notion_helper.return_property_value(page_props['Status'], oid))
        page_dict['system_status'] = fix_value(notion_helper.return_property_value(page_props['System status'], oid))
        page_dict['tracking'] = fix_value(notion_helper.return_property_value(page_props['Tracking'], oid))
        
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
                
                page_dict['job_data'].append(temp_dict)
                
        output_library.append(page_dict)
                    
with open(csv_file_output, mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # ['Order Number', 'Creation Date', 'Ship-by Date', 'Order Status', 'System Status', 'Product', 'Date Shipped', 'Tracking Number']
    writer.writerow(csv_headers)
    
    for order in output_library:
        writer.writerow([str(order['order_number']), order['creation_date'], order['ship-by_date'], order['status'], order['system_status'], "", order['date_shipped'], str(order['tracking'])])
        
        if order['job_data']:
            for job in order['job_data']:
                writer.writerow([job['internal_uid'], "", "", job['status'], job['system_status'], job['product'], "", "", ""])
                
        writer.writerow([])
                    