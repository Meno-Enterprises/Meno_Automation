#!/usr/bin/env python3

'''
dependancies:
- NotionApiHelper.py
- AutomatedEmails.py
- Notion database must contain the "Last edited time" property.
- pip install deepdiff
'''


from NotionApiHelper import NotionApiHelper
from deepdiff import DeepDiff
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
            db_id = db_id.replace('-','') # Normalizing db ID now so it's not a problem again later.
            print(f"Listening to database {db_id}")
            # Pull Relevant Config Data
            trigger = []
            action = []
            uid = []
            filter_properties = []
            if_created = False
            was_created = False
            
            for db_config_page in self.config[db_id]: # Set up query property filters. Trying to do as few queries as possible to maximize efficiency.
                print(f"Checking config page {db_config_page['uid']}")
                if 'filter_properties' in db_config_page:
                    print(f"Filter properties found in config page {db_config_page['uid']}")
                    for property_id in db_config_page['filter_properties']:
                        print(f"Adding property {property_id} to filter list.")
                        if property_id in filter_properties:
                            print(f"Property {property_id} already in filter list.")
                            continue
                        filter_properties.append(property_id)
            if filter_properties == []:
                filter_properties = None
            
            active_properties = self.get_active_properties(db_id)
            print(f"Listening to database {db_id}, with filter properties {filter_properties}")
            

            content_filter = {
                "and": [
                    {"property": "Last edited time", "date": {"past_week":{}}}
                ]
            }
            
            # Load previous query
            if not self.storage_exists(db_id): # Storage does not exist, create it.
                self.save_storage(db_id, self.query_database(db_id, filter_properties, content_filter))
                continue
            last_query = self.load_storage(db_id)
            print("Previous query loaded.")
            # Query DB
            filter_properties = None
            response = self.query_database(db_id, filter_properties, content_filter)
            print("Database queried.")
            
            # Skips if returned no response because of a network issue or something.
            if response is None:
                print(f"No response from queried database: {db_id}.")
                time.sleep(1)
                continue
                
            print("Checking for changes.")
            difference = self.check_change(last_query, response, active_properties)
            # If no bulk changes detected, continue to next db.
            if difference == {}:
                print(f"No changes detected in database {db_id}.")
                time.sleep(.5)
                continue

            print(f"Difference: {json.dumps(difference, indent=4)}")
            
            for db_config_page in self.config[db_id]:
                for page in difference:
                    activate_trigger = self.check_triggers(difference[page], db_config_page['trigger'])
            
            '''for page in self.config[db_id]:
                print(f"Checking trigger for page: {page['uid']}")
                if self.check_triggers(response, page['trigger']):
                    print(f"Trigger met for page: {page['uid']}")
                    #self.take_action(page['action'], response)'''
            self.save_storage(db_id, response)
                
            time.sleep(5)
            print("Database updated.")
            
    def get_active_properties(self, db_id): # Returns a unique list of DB properties based off the config files for a given DB.
        active_properties = []
        last_edited = ""
        def parse_trigger(trigger):
            if 'and' in trigger:
                for each in trigger['and']:
                    parse_trigger(each)
            elif 'or' in trigger:
                for each in trigger['or']:
                    parse_trigger(each)
            else:
                if 'property' in trigger:
                    if trigger['property'] not in active_properties:
                        active_properties.append(trigger['property'])

        if self.config[db_id] is None:
            print(f"Database {db_id} not found in config.")
            return []

        for conf in self.config[db_id]:
            parse_trigger(conf['trigger'])
        return active_properties
            
            
    def check_triggers(self, data, trigger_config): # Returns true if the trigger condition is met.
        '''def parse_trigger(config):
            if 'and' in config: # If the key is 'and', all conditions nested in it must be met. Checks them recursively
                return lambda item: all(parse_trigger(cond)(item) for cond in config['and'])
            elif 'or' in config: # If the key is 'or', any condition nested in it can be met. Checks them recursively
                return lambda item: any(parse_trigger(cond)(item) for cond in config['or'])
            else:
                key, value = next(iter(config.items()))
                if key == 'created':
                    return lambda item: 'created' in item
                elif key == 'property':
                    return lambda item: item['properties'].get(value['property']) == value['select']['equals']
        filter_func = parse_trigger(trigger_config)
        filtered_data = [item for item in data if filter_func(item)]'''

        def parse_trigger(data, config):
            if 'and' in config:
                return all(parse_trigger(data, each) for each in config['and'])
            elif 'or' in config:
                return any(parse_trigger(data, each) for each in config['or'])
            elif 'created' in config:
                if 'new page' in data:
                    return 'created' in data
                else:
                    return False
            elif 'property' in config:
                router = {
                    'checkbox': self.trigger_compare(data['property changed']['new property'][property]['checkbox'], config['checkbox']),
                    'email': self.trigger_compare(data['property changed']['new property'][property]['email'], config['email']),
                    'number': self.trigger_compare(data['property changed']['new property'][property]['number'], config['number']),
                    'phone_number': self.trigger_compare(data['property changed']['new property'][property]['phone_number'], config['phone_number']),
                    'url': self.trigger_compare(data['property changed']['new property'][property]['url'], config['url']),
                    'select': self.trigger_compare(data['property changed']['new property'][property]['select'], config['select']),
                    'status': self.trigger_compare(data['property changed']['new property'][property]['status'], config['status']),
                    'date': self.trigger_compare(data['property changed']['new property'][property]['date'], config['date']),
                    'files': self.trigger_compare(data['property changed']['new property'][property]['files'], config['files']),
                    'multi_select': self.trigger_compare(data['property changed']['new property'][property]['multi_select'], config['multi_select']),
                    'relation': self.trigger_compare(data['property changed']['new property'][property]['relation'], config['relation']),
                    'people': self.trigger_compare(data['property changed']['new property'][property]['people'], config['people']),
                    'rich_text': self.trigger_compare(data['property changed']['new property'][property]['rich_text'], config['rich_text']),
                    'title': self.trigger_compare(data['property changed']['new property'][property]['title'], config['title'])
                }
                for property in data['property changed']['new property']:
                    if property == config['property']:
                        return router[data['property changed']['new property'][property]['type']]
                        
            else:
                return False
            pass  
        return parse_trigger(data, trigger_config)
    
    def trigger_compare(self, new_property, config): # Aria add the rest of the things like dates and whatnot
        if 'contains' in config:
            return config['contains'] in new_property
        elif 'does_not_contain' in config:
            return config['does_not_contain'] not in new_property
        elif 'equals' in config:
            return new_property == config['equals']
        elif 'does_not_equal' in config:
            return new_property != config['does_not_equal']
        elif 'ends_with' in config:
            return new_property.endswith(config['ends_with'])
        elif 'starts_with' in config:
            return new_property.startswith(config['starts_with'])
        elif 'is_empty' in config:
            return new_property == ''
        elif 'is_not_empty' in config:
            return new_property != ''
        elif 'less_than' in config:
            return new_property < config['less_than']
        elif 'greater_than' in config:
            return new_property > config['greater_than']
        elif 'less_than_or_equal_to' in config:
            return new_property <= config['less_than_or_equal_to']
        elif 'greater_than_or_equal_to' in config:
            return new_property >= config['greater_than_or_equal_to']
        else:
            self.logger.error(f"Trigger config error: {config}")
            return False

    # Specifically checks if the New Data has added or changed a property from the old data. Does NOT check if old data has things that the new data does not. I'll add it if it becomes relevant.
        pass

        # Checks for changes in the data, returns a dictionary of changes and additions.
    def check_change(self, old_data, new_data, active_properties):
        old_dictionary = {}
        new_dictionary = {}
        changes = {}
        print("Reformatting old data.")
        for page in old_data:
            old_dictionary[page['id'].replace('-','')] = page['properties']
        print("Reformatting new data.")
        for page in new_data:
            new_dictionary[page['id'].replace('-','')] = page['properties']
        print("Comparing...")
        for page_id in new_dictionary:
            print(f"Checking page {page_id}")
            if page_id not in old_dictionary:
                print(f"New page detected: {page_id}")
                changes[page_id] = {'new page': new_dictionary[page_id]}
            else:
                for property in new_dictionary[page_id]:
                    if property in active_properties:
                        if property in old_dictionary[page_id]:
                            #print(f"Old: {old_dictionary[page_id][property]}, New: {new_dictionary[page_id][property]}")
                            if DeepDiff(old_dictionary[page_id][property], new_dictionary[page_id][property]) != {}:
                                print(f"Property {property} changed in page {page_id}")
                                changes[page_id]['property changed']['old property'][property] = old_dictionary[page_id][property]
                                changes[page_id]['property changed']['new property'][property] = new_dictionary[page_id][property]
                        else:
                            print(f"New property {property} detected in page {page_id}")
                            changes[page_id]['new property'][property] = new_dictionary[page_id][property]
                            changes[page_id]['old property'][property] = {}
        return changes                 
            
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
        return self.notion_helper.query(db_id, filter_properties, content_filter)
    
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
                "email": ["email@email.com"]
            },
            "trigger":{
                "and": [
                    {
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
                    ]
                    },                    
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

    listener.listen()
    while False:
        counter += 1    
        listener.listen()
        time.sleep(5)