#!/usr/bin/env python3

'''
dependancies:
- NotionApiHelper.py
- AutomatedEmails.py
- Notion database must contain the "Modified" property.
'''


from NotionApiHelper import NotionApiHelper
import time, os, requests, uuid
import logging
import json

class NotionEventListener:
    def __init__(self):
        self.notion_helper = NotionApiHelper()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.storage_directory = "storage"
        self.config_path = "conf/NotionEventListener_Conf.json"
        self.config = {}

        os.makedirs(self.storage_directory, exist_ok=True)

    def listen(self):
        for db_id in self.config:
            
            # Pull Relevant Config Data
            trigger = []
            action = []
            uid = []
            filter_properties = []
            active_properties = []
            if_created = False
            was_created = False
            
            for each in db_id: # Set up query property filters. Trying to do as few queries as possible to maximize efficiency.
                for property in each['filter_properties']:
                    filter_properties.append(property)
            filter_properties = list(set(filter_properties)) # Remove duplicates
            if filter_properties == []:
                filter_properties = None
            
            print(f"Listening to database {db_id}, with filter properties {filter_properties}")
            
            ''' # Might not be necessary. I can probably work with property ids.
            # Generate list of properties to check for changes.
            for andor in trigger:
                for trigprop in andor:
                    if trigprop == "created":
                        if_created = True
                        continue
                    if trigprop is "and" or trigprop is "or":
                        for subtrig in trigprop:
                            active_properties.append(subtrig['property'])
                    active_properties.append(trigprop['property'])
            '''
            
            # Load previous query
            if not self.storage_exists(db_id): # Storage does not exist, create it.
                self.save_storage(db_id, self.query_database(db_id))
                continue
            last_query = self.load_storage(db_id)
            
            # Query DB
            response = self.query_database(db_id)
            if response is None or response == last_query:
                time.sleep(.5)
                continue

            for page in response:
                if any(item['id'] == page['id'] for item in last_query):
                    index = next((i for i, item in enumerate(last_query) if item['id'] == page['id']), None)
                    if index is not None:
                        # Compare properties to detect changes
                        for prop in active_properties:
                            if page['properties'][prop] != last_query[index]['properties'][prop]:
                                self.check_trigger(trigger, page['properties'])
                                break
                
            time.sleep(5)
            print("Database updated.")
            
    def check_triggers(self, data, trigger_config):
        def parse_trigger(config):
            if 'and' in config:
                return lambda item: all(parse_trigger(cond)(item) for cond in config['and'])
            elif 'or' in config:
                return lambda item: any(parse_trigger(cond)(item) for cond in config['or'])
            else:
                key, value = next(iter(config.items()))
                if key == 'created':
                    return lambda item: 'created' in item
                elif key == 'property':
                    return lambda item: item['properties'].get(value['property']) == value['select']['equals']


        filter_func = parse_trigger(trigger_config)
        return [item for item in data if filter_func(item)]

    def check_change(self, db_id, active_properties):
        last_query = self.load_storage(db_id)
        
        # Query DB
        response = self.query_database(db_id)
        if response is None or response == last_query:
            return

        filter_config = self.config.get('filter', {})
        filtered_response = self.check_triggers(response, filter_config)

        for page in filtered_response:
            if any(item['id'] == page['id'] for item in last_query):
                index = next((i for i, item in enumerate(last_query) if item['id'] == page['id']), None)
                if index is not None:
                    # Compare properties to detect changes
                    for prop in active_properties:
                        if page['properties'][prop] != last_query[index]['properties'][prop]:
                            self.check_triggers(trigger, page['properties'])
                            break
                        
            
    def take_action(self, action, data = None):
        pass

    def load_config(self):
        with open(self.config_path, 'r') as config_file:
            self.config = json.load(config_file)

    def save_config(self):
        with open(self.config_path, 'w') as config_file:
            json.dump(self.config, config_file, indent=4)

    def load_storage(self, database_id):
        with open(f"{self.storage_directory}/{database_id}.json", 'r') as storage_file:
            return json.load(storage_file)

    def save_storage(self, database_id, data):
        with open(f"{self.storage_directory}/{database_id}.json", 'w') as storage_file:
            json.dump(data, storage_file, indent=4)

    def storage_exists(self, database_id):
        return os.path.exists(f"{self.storage_directory}/{database_id}.json")

    def find_config_index(self, uid):
        for index in self.config['config']:
            if uid in self.config['config'][index]['uid']:
                return index

    def update_config(self, uid, key, value):
        self.config['config'][self.find_config_index(uid)][key] = value
        self.save_config()

    def update_config(self, dictionary):
        if 'uid' not in dictionary:
            dictionary['uid'] = str(uuid.uuid4())
        if dictionary['uid'] is None:
            dictionary['uid'] = str(uuid.uuid4())
        self.config['config'].append(dictionary)
        self.save_config()

   
    # Trigger construction mimicks the Notion filter structure. https://developers.notion.com/reference/post-database-query-filter#the-filter-object
    def build_config_part(self, database_id, trigger, action = None,content_filter = None, properties_filter = None): 
        config_part = {
            "uid": str(uuid.uuid4()),
            "database_id": database_id, # String
            "action": action, # Dictionary
            "content_filter": content_filter, # Dictionary
            "properties_filter": properties_filter, # List of Strings
            "trigger": trigger # Dictionary
        }
        print(config_part)
        pass

    def query_database(self, db_id, filter_properties = None, content_filter = None):
        print(f"Querying database {db_id}")
        return self.notion_helper(db_id, filter_properties, content_filter)
    
    def store_previous_query(self, data, db_id):
        with open(os.path.join(self.storage_directory, f"{db_id}.json"), 'w') as file:
            json.dump(data, file, indent=4)
        pass

    def notify_email(self, email, config_directory = None, data = None):
        pass

    def notify_webhook(self, webhook_url, data = None):
        pass

    def notify_slack(self, data):
        pass

'''
{
    "e51caaf845a34283a46cdf4cadaaeea3": [
            {
            "uid": "7b1b3b1b-0b7b-4b1b-8b1b-0b7b1b1b7b1b",
            "filter_properties": ["Modified"],
            "action": {
                "webhook": ["https://webhook.site/7b1b3b1b-0b7b-4b1b-8b1b-0b7b1b1b7b1b"],
                "slack": ["https://hooks.slack.com/services/T01JGK1J5J4/B01JGK1J5J4/6ZQ2GK1J5J4"],
                "email": {
                    ["email@email.com"],
            }
            "trigger":{
                "and": [
                    "or": [
                        {
                            "property": "Status",
                            "select": {
                                "equals": "In Progress"
                            }
                        },
                        {
                            "property": "Priority",
                            "select": {
                                "equals": "High"
                            }
                        },
                        {
                            "created": {}
                        }
                        ],                    
                    {
                        "property": "Status",
                        "select": {
                            "equals": "In Progress"
                        }
                    },
                    {
                        "property": "Priority",
                        "select": {
                            "equals": "High"
                        }
                    }
                ]
                }
            }
    ]
}
'''

if __name__ == "__main__":
    listener = NotionEventListener()
    counter = 0 # Tracking number of loops to trigger different events. ie. refreshing config, pinging monitoring service, clean memory, etc.
    listener.load_config()

    while True:
        counter += 1    
        listener.listen()