from NotionApiHelper import NotionApiHelper
import json

page_id = "a4ed0c2ca9884af093e9ba08c8e49f40"
properties_dict = {}
notion_helper = NotionApiHelper()
response = notion_helper.get_page(page_id)
for prop in response['properties']:
    properties_dict[prop] = response['properties'][prop]['id']

with open(f'{page_id}_properties.json', 'w') as json_file:
    json.dump(properties_dict, json_file, indent=4)