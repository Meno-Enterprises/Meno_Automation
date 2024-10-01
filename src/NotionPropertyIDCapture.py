from NotionApiHelper import NotionApiHelper
import json

page_id = "cc44b728898c4d3d952dce6fb5b15dba"
properties_dict = {}
notion_helper = NotionApiHelper()
response = notion_helper.get_page(page_id)
for prop in response['properties']:
    properties_dict[prop] = response['properties'][prop]['id']

with open(f'{page_id}_properties.json', 'w') as json_file:
    json.dump(properties_dict, json_file, indent=4)