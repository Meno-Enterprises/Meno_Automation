from NotionApiHelper import NotionApiHelper
import json

notion_helper = NotionApiHelper()
    
def Prop_ID_Capture():
    page_id = "cc44b728898c4d3d952dce6fb5b15dba"
    properties_dict = {}
    response = notion_helper.get_page(page_id)
    for prop in response['properties']:
        properties_dict[prop] = response['properties'][prop]['id']
    with open(f'{page_id}_properties.json', 'w') as json_file:
        json.dump(properties_dict, json_file, indent=4)
        
def Internal_Storage_ID_Capture():
    db_id = "f631a4f0-9c27-427d-be70-f4d7a2e61e9c"
    prop_list = []
    response = notion_helper.query(db_id)
    for prop in response:
        prop_list.append(prop['properties']['Internal storage ID']['formula']['string'])
    with open(f'{db_id}_internal_storage_ids.json', 'w') as json_file:
        json.dump(prop_list, json_file, indent=4)
        
#Prop_ID_Capture()
Internal_Storage_ID_Capture()