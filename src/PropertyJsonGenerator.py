from NotionApiHelper import NotionApiHelper
from datetime import datetime
import pyperclip, json

notion_helper = NotionApiHelper()
properties = notion_helper.properties_to_json()
pyperclip.copy(properties)  

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
filename = f'properties_{timestamp}.json'

# Write properties to a new JSON file
with open(filename, 'w') as json_file:
    json.dump(properties, json_file)