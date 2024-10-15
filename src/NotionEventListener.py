#!/usr/bin/env python3

'''
dependancies:
- NotionApiHelper.py
- AutomatedEmails.py
- Notion database must contain the "Last edited time" property.
- pip install deepdiff
'''


from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from deepdiff import DeepDiff
import time, os, requests, uuid
import logging
import json
from datetime import datetime, timedelta

class NotionEventListener:
    def __init__(self):
        self.notion_helper = NotionApiHelper()
        self.automated_emails = AutomatedEmails()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.STORAGE_DIRECTORY = "storage"
        self.CONFIG_PATH = "conf/NotionEventListener_Conf.json"
        self.query_lookback_time = 45 # Minutes, How far back to look for changes on initial start.
        self.QUERY_LOOKBACK_PADDING = 10 # Minutes, Padding on how far back to look for changes on subsequent queries.
        self.QUERY_LOOKBACK = (datetime.now() - timedelta(minutes=self.query_lookback_time)).strftime('%Y-%m-%dT%H:%M:%S')
        self.query_filter = {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": self.QUERY_LOOKBACK}} # Default query filter. Overrided by config.
        self.config = {}
    
        os.makedirs(self.STORAGE_DIRECTORY, exist_ok=True)
        
    def update_filter_time(self):
        self.query_filter['last_edited_time'] = {"on_or_after": (datetime.now() - timedelta(minutes=self.query_lookback_time)).strftime('%Y-%m-%dT%H:%M:%S')}

    def listen(self):
        start_time = datetime.now()
        for db_id in self.config:
            db_id = db_id.replace('-','') # Normalizing db ID now so it's not a problem again later.
            print(f"Listening to database {db_id}")
            filter_properties = []
            
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
            
            # Pull Content Filter from Config, defaults to self.query_filter if not found.
            if 'content_filter' in self.config[db_id]:
                content_filter = self.config[db_id]['content_filter']
            else:
                content_filter = self.query_filter
            
            # Load previous query
            if not self.storage_exists(db_id): # Storage does not exist, create it.
                self.save_storage(db_id, self.query_database(db_id, filter_properties, content_filter))
                continue
            
            last_query = self.load_storage(db_id)
            print("Previous query loaded.")
            
            # Query DB
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
                    print(f" Active Trigger: {activate_trigger}")
                    if activate_trigger:
                        print(f"Trigger met for page {page} in database {db_id}.")
                        
                        if 'action' in db_config_page:
                            package = {page: difference[page]}
                            self.take_action(db_config_page['action'], package)
                            
                        else:
                            print(f"No action found for page {page} in database {db_id}.")
            
            self.save_storage(db_id, response)
            time.sleep(5)
            print("Database updated.")
            
        self.query_lookback_time = (datetime.now() - start_time).total_seconds() / 60 + 10
            
    def get_active_properties(self, db_id): # Returns a unique list of DB properties based off the config files for a given DB.
        active_properties = []
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
        def parse_trigger(data, config):
            if 'and' in config:
                return all(parse_trigger(data, each) for each in config['and'])
            
            elif 'or' in config:
                return any(parse_trigger(data, each) for each in config['or'])
            
            elif 'created' in config:
                if 'new page' in data:
                    return True
                
                else:
                    return False         
            
            elif 'property' in config:
                if "property changed" in data:
                    for property in data['property changed']['new property']:
                        if property == config['property']:
                            prop_type = data['property changed']['new property'][property]['type']
                            return self.trigger_compare(data['property changed']['new property'][property][prop_type], config[prop_type])
                        
                else:
                    return False
                    
            else:
                return False
            
            pass  
        
        return parse_trigger(data, trigger_config)
    
    def trigger_compare(self, new_property, config):
        def date_convert(new_prop, config_prop):
            try:
                new_prop = datetime.fromisoformat(new_prop)
            except ValueError:
                self.logger.error(f"Invalid date format for property: {new_prop}")
                return False
            
            try:
                config_prop = datetime.fromisoformat(config_prop)
            except ValueError:
                self.logger.error(f"Invalid date format for config: {config_prop}")
                return False
            
            return new_prop, config_prop
        
        def date_convert(new_prop):
            try:
                new_prop = datetime.fromisoformat(new_prop)
            except ValueError:
                self.logger.error(f"Invalid date format for property: {new_prop}")
                return False
            
            return new_prop
        
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
        
        elif 'any' in config:
            return any(self.trigger_compare(new_property, each) for each in config['any'])
        
        elif 'every' in config:
            return all(self.trigger_compare(new_property, each) for each in config['every'])
        
        elif 'none' in config:
            return not any(self.trigger_compare(new_property, each) for each in config['none'])
        
        elif 'after' in config:
            new_property, config['after'] = date_convert(new_property, config['after'])
            return new_property > config['after']
        
        elif 'before' in config:
            new_property, config['before'] = date_convert(new_property, config['before'])
            return new_property < config
        
        elif 'next_month' in config:
            new_property = date_convert(new_property)
            
            if new_property:
                today = datetime.now()
                one_month_from_today = today + timedelta(days=30)
                return today <= new_property <= one_month_from_today
            
            return False
        
        elif 'next_year' in config:
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_year_from_today = today + timedelta(days=365)
                return today <= new_property <= one_year_from_today
            return False
        
        elif 'next_week' in config:
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_week_from_today = today + timedelta(days=7)
                return today <= new_property <= one_week_from_today
            return False
        
        elif 'on_or_after' in config:
            new_property, config['on_or_after'] = date_convert(new_property, config['on_or_after'])
            if new_property and config['on_or_after']:
                return new_property >= config['on_or_after']
            return False
        
        elif 'on_or_before' in config:
            new_property, config['on_or_before'] = date_convert(new_property, config['on_or_before'])
            if new_property and config['on_or_before']:
                return new_property <= config['on_or_before']
            return False
        
        elif 'past_month' in config:
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_month_ago = today - timedelta(days=30)
                return one_month_ago <= new_property <= today
            return False
        
        elif 'past_year' in config:
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_year_ago = today - timedelta(days=365)
                return one_year_ago <= new_property <= today
            return False
        
        elif 'past_week' in config:
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_week_ago = today - timedelta(days=7)
                return one_week_ago <= new_property <= today
            return False
        
        elif 'this_week' in config:
            new_property = date_convert(new_property)
            if new_property:
                current_week = datetime.now().isocalendar()[1]
                next_prop_week = new_property.isocalendar()[1]
                return current_week == next_prop_week
            return False
        
        else:
            self.logger.error(f"Trigger config error: {config}")
            return False


    # Specifically checks if the New Data has added or changed a property from the old data. Does NOT check if old data has things that the new data does not. I'll add it if it becomes relevant.
        pass

        # Checks for changes in the data, returns a dictionary of changes and additions.
    def check_change(self, old_data, new_data, active_properties):
        """_summary_

        Args:
            old_data (dict): Previously stored data.
            new_data (dict): Recently queried data.
            active_properties (list): List of active properties from the conf. to check for changes.

        Returns:
            dict: Data formatted as:
            {
                page_id: {
                    "property changed": {
                        "old property": {{property: value}, {property: {}}},
                        "new property": {{property: value}, {property: value}}
                    },
                    "new page": page_id,
                }
            }
        """
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
                changes[page_id] = {"new page": page_id}
    
            else:
                for property in new_dictionary[page_id]:
                    if property in active_properties:
                        if property in old_dictionary[page_id]:
                            if DeepDiff(old_dictionary[page_id][property], new_dictionary[page_id][property]) != {}:
                                print(f"Property {property} changed in page {page_id}")
                                if page_id not in changes:
                                    changes[page_id] = {'property changed': {'new property': {}, 'old property': {}}}
                                changes[page_id]['property changed']['old property'][property] = old_dictionary[page_id][property]
                                changes[page_id]['property changed']['new property'][property] = new_dictionary[page_id][property]
                                
                        else:
                            print(f"New property {property} detected in page {page_id}")
                            if page_id not in changes:
                                changes[page_id] = {'property changed': {'new property': {}, 'old property': {}}}
                            changes[page_id]['property changed']['new property'][property] = new_dictionary[page_id][property]
                            changes[page_id]['property changed']['old property'][property] = {}
                            
        return changes                 
            
    def take_action(self, action, data = None):
        
        if "webhook" in action:
            for url in action['webhook']:
                self.notify_webhook(url, data)
                
        if "email" in action:
            for conf in action['email']:
                self.notify_email(conf, data)
        '''
        "action": {
            "email": [{"subject": "Subject", "body": "Body", "path": "path/to/config.json"}]
        }
        '''
                
        if "slack" in action:
            for slack in action['slack']:
                self.notify_slack(slack, data)
                
        pass

    def load_config(self):
        with open(self.CONFIG_PATH, 'r') as config_file:
            self.config = json.load(config_file)

    def save_config(self):
        with open(self.CONFIG_PATH, 'w') as config_file:
            json.dump(self.config, config_file, indent=4)

    def load_storage(self, database_id):
        with open(f"{self.STORAGE_DIRECTORY}/{database_id}.json", 'r') as storage_file:
            return json.load(storage_file)

    def save_storage(self, database_id, data):
        with open(f"{self.STORAGE_DIRECTORY}/{database_id}.json", 'w') as storage_file:
            json.dump(data, storage_file, indent=4)

    def storage_exists(self, database_id):
        return os.path.exists(f"{self.STORAGE_DIRECTORY}/{database_id}.json")

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
        print(f"Filter properties: {filter_properties}")
        print(f"Content filter: {content_filter}")
        self.update_filter_time()
        return self.notion_helper.query(db_id, filter_properties, content_filter)
    
    def store_previous_query(self, data, db_id):
        with open(os.path.join(self.STORAGE_DIRECTORY, f"{db_id}.json"), 'w') as file:
            json.dump(data, file, indent=4)
        pass

    def notify_email(self, email, config_directory = None, data = None):
        pass

    def notify_webhook(self, webhook_url, data = None):
        print(f"Sending data to webhook: {webhook_url}")
        headers = {'Content-Type': 'application/json'}
        print(f"Data: {json.dumps(data)}")
        try:
            response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
            response.raise_for_status()
            self.logger.info(f"Successfully sent data to webhook: {webhook_url}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to send data to webhook: {webhook_url}, error: {e}")
        pass

    def notify_slack(self, data):
        pass

if __name__ == "__main__":
    listener = NotionEventListener()
    counter = 0 # Tracking number of loops to trigger different events. ie. refreshing config, pinging monitoring service, clean memory, etc.
    listener.load_config()

    try:
        while True:
            counter += 1    
            listener.listen()
            time.sleep(5)
    except KeyboardInterrupt:
        pass