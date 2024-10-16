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
import time, os, requests, uuid, cronitor, gc
import logging
import json
from datetime import datetime, timedelta

CRONITOR_KEY_PATH = "conf/Cronitor_API_Key.txt"
with open(CRONITOR_KEY_PATH, "r") as file:
    cronitor_api_key = file.read()
        
cronitor.api_key = cronitor_api_key
MONITOR = cronitor.Monitor("Notion Event Listener")
gc.enable()
SLEEP_TIMER = 30 # Seconds
PING_CYCLE = 100 # Number of loops before pinging cronitor.
GC_CYCLE = 500 # Number of loops before running garbage collection.
CONFIG_RELOAD_CYCLE = 100 # Number of loops before reloading the config file.
STOP_CYCLE = 10000 # Number of loops before stopping the script.
class NotionEventListener:
    def __init__(self):
        self.notion_helper = NotionApiHelper()
        self.automated_emails = AutomatedEmails()
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.STORAGE_DIRECTORY = "storage"
        self.CONFIG_PATH = "conf/NotionEventListener_Conf.json"
        self.EMAIL_ME_PATH = "conf/Aria_Email_Conf.json"
        self.query_lookback_time = 45 # Minutes, How far back to look for changes on initial start.
        self.QUERY_LOOKBACK_PADDING = 5 # Minutes, Padding on how far back to look for changes on subsequent queries.
        self.QUERY_LOOKBACK = (datetime.now() - timedelta(minutes=self.query_lookback_time)).strftime('%Y-%m-%dT%H:%M:%S')
        self.query_filter = {"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": self.QUERY_LOOKBACK}} # Default query filter. Overrided by config.
        self.config = {}
        self.first_run = True
    
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
            if not self.storage_exists(db_id) or self.first_run: # Storage does not exist, create it.
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
        self.first_run = False
            
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
            
    def check_triggers(self, data, config):
        def is_selstat(data, property, prop_type):
            return data['property changed']['new property'][property][prop_type]["name"]
        
        def is_formula(data, property, prop_type):
            form_type = data['property changed']['new property'][property][prop_type]["type"]
            try:
                if form_type == "date": # This might break, I'm not sure if the date format is the same as the date format in the config.
                    return data['property changed']['new property'][property][prop_type]["date"]["start"]
                else:
                    return data['property changed']['new property'][property][prop_type][form_type]
            except KeyError: # jk, this'll probably work if it did break.
                return data['property changed']['new property'][property][prop_type][form_type]        
        
        def is_rich_text(data, property, prop_type):
            text_list = []
            for text in data['property changed']['new property'][property][prop_type]:
                text_list.append(text['plain_text'])
            return ", ".join(text_list)
        
        def is_relation(data, property, prop_type):
            package = []
                                
            if "has_more" in data['property changed']['new property'][property]: # If there are more than 25 relations, replace data with full data (up to 100).
                response = self.notion_helper.get_page_property(data, data['property changed']['new property'][property]['id'])
                data['property changed']['new property'][property][prop_type] = response[prop_type]
            
            for id in data['property changed']['new property'][property][prop_type]:
                package.append(id['id'])
        
        def is_date(data, property, prop_type):
            return data['property changed']['new property'][property][prop_type]['start']
        
        def is_files(data, property, prop_type):
            package = []
            for file in data['property changed']['new property'][property][prop_type]:
                package.append(file['external']['url'])
            return package
        
        def is_last_edited_by(data, property, prop_type):
            return data['property changed']['new property'][property][prop_type]['id']
        
        def is_multi_select(data, property, prop_type):
            package = []
            for select in data['property changed']['new property'][property][prop_type]:
                package.append(select['name'])
            return package
        
        def is_rollup(data, property, prop_type): # rollups are a special snowflake.
            if data['property changed']['new property'][property][prop_type]['type'] == "number":
                return self.trigger_compare(data['property changed']['new property'][property][prop_type]['number'], config[prop_type])
            elif data['property changed']['new property'][property][prop_type]['type'] == "date":
                return self.trigger_compare(data['property changed']['new property'][property][prop_type]['date']['start'], config[prop_type])
            else: # This is an array of possibilities. I'm not supporting the other formats.
                return_list = []
                # Itterate through each rollup, repackage the data as a single property and rerun the parse on each.
                for each in data['property changed']['new property'][property][prop_type]['array']:
                    new_data = {"property changed": {"new property": {property: each}}}
                    return_list.append(self.check_triggers(new_data, config[prop_type]['any']))                                       
                if "any" in config[prop_type]:
                    return any(return_list)
                elif "every" in config[prop_type]: 
                    return all(return_list)
                elif "none" in config[prop_type]:
                    return not any(return_list)
                else:
                    return False            
        
        if 'and' in config: # Recursively check for 'and' and 'or' triggers.
            return all(self.check_triggers(data, each) for each in config['and'])
        
        elif 'or' in config: # Recursively check for 'and' and 'or' triggers.
            return any(self.check_triggers(data, each) for each in config['or'])
        
        elif 'created' in config:
            if 'new page' in data:
                return True
            else:
                return False         
        
        elif 'property' in config: 
            if "property changed" in data:
                for property in data['property changed']['new property']:
                    if property == config['property']:
                        try:
                            prop_type = data['property changed']['new property'][property]['type']
                            package = data['property changed']['new property'][property][prop_type]
                            router = { # This is a dictionary of functions that return the data in the correct format.
                                'select': is_selstat(data, property, prop_type),
                                'status': is_selstat(data, property, prop_type),
                                'formula': is_formula(data, property, prop_type),
                                'rich_text': is_rich_text(data, property, prop_type),
                                'relation': is_relation(data, property, prop_type),
                                'date': is_date(data, property, prop_type),
                                'files': is_files(data, property, prop_type),
                                'last_edited_by': is_last_edited_by(data, property, prop_type),
                                'multi_select': is_multi_select(data, property, prop_type),
                                'rollup': is_rollup(data, property, prop_type)
                            }
                            package = router[prop_type]
                        except Exception as e:
                            self.logger.error(f"Something in check_triggers failed: {property}\n{e}")
                            self.automated_emails.send_email(self.EMAIL_ME_PATH, "NotionEventListener:Error in check_triggers", f"Error in check_triggers: {property}\n{e}")
                            return False
                        
                        return self.trigger_compare(package, config[prop_type])  
            else:
                return False
                
        else:
            return False
    
        return False 
    
    def trigger_compare(self, new_property, config): # Refactor this as a dictionary of functions.
        def date_convert(new_prop, config_prop=None): # Converts date strings to datetime objects.
            try:
                new_prop = datetime.fromisoformat(new_prop)
            except ValueError:
                self.logger.error(f"Invalid date format for property: {new_prop}")
                return False
            
            if config_prop:
                try:
                    config_prop = datetime.fromisoformat(config_prop)
                except ValueError:
                    self.logger.error(f"Invalid date format for config: {config_prop}")
                    return False
            
            return new_prop, config_prop if config_prop else new_prop

        def check_after(new_property, config_value):
            new_property, config_value = date_convert(new_property, config_value)
            if new_property and config_value:
                return new_property > config_value
            return False
        
        def check_before(new_property, config_value):
            new_property, config_value = date_convert(new_property, config_value)
            if new_property and config_value:
                return new_property < config_value
            return False
        
        def check_next_month(new_property):
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_month_from_today = today + timedelta(days=30)
                return today <= new_property <= one_month_from_today
            return False
        
        def check_next_year(new_property):
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_year_from_today = today + timedelta(days=365)
                return today <= new_property <= one_year_from_today
            return False
        
        def check_next_week(new_property):
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_week_from_today = today + timedelta(days=7)
                return today <= new_property <= one_week_from_today
            return False
        

        def check_on_or_after(self, new_property, config_value):
            new_property, config_value = date_convert(new_property, config_value)
            if new_property and config_value:
                return new_property >= config_value
            return False

        def check_on_or_before(self, new_property, config_value):
            new_property, config_value = date_convert(new_property, config_value)
            if new_property and config_value:
                return new_property <= config_value
            return False

        def check_past_month(self, new_property):
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_month_ago = today - timedelta(days=30)
                return one_month_ago <= new_property <= today
            return False

        def check_past_year(self, new_property):
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_year_ago = today - timedelta(days=365)
                return one_year_ago <= new_property <= today
            return False

        def check_past_week(self, new_property):
            new_property = date_convert(new_property)
            if new_property:
                today = datetime.now()
                one_week_ago = today - timedelta(days=7)
                return one_week_ago <= new_property <= today
            return False

        def check_this_week(self, new_property):
            new_property = date_convert(new_property)
            if new_property:
                current_week = datetime.now().isocalendar()[1]
                next_prop_week = new_property.isocalendar()[1]
                return current_week == next_prop_week
            return False        
        
        config_checks = { # Dictionary of functions that compare the new property to the config value.
            'contains': lambda np, cv: cv in np,
            'does_not_contain': lambda np, cv: cv not in np,
            'equals': lambda np, cv: np == cv,
            'does_not_equal': lambda np, cv: np != cv,
            'ends_with': lambda np, cv: np.endswith(cv),
            'starts_with': lambda np, cv: np.startswith(cv),
            'is_empty': lambda np, _: np == '',
            'is_not_empty': lambda np, _: np != '',
            'less_than': lambda np, cv: np < cv,
            'greater_than': lambda np, cv: np > cv,
            'less_than_or_equal_to': lambda np, cv: np <= cv,
            'greater_than_or_equal_to': lambda np, cv: np >= cv,
            'after': check_after,
            'before': check_before,
            'next_month': check_next_month,
            'next_year': check_next_year,
            'next_week': check_next_week,
            'on_or_after': check_on_or_after,
            'on_or_before': check_on_or_before,
            'past_month': check_past_month,
            'past_year': check_past_year,
            'past_week': check_past_week,
            'this_week': check_this_week
        }
        
        for key, check_function in config_checks.items():
            if key in config:
                if key in ['on_or_after', 'on_or_before', 'less_than', 'greater_than']:
                    return check_function(new_property, config[key])
                else:
                    return check_function(new_property, config[key] if key in config else None)        
        
        self.logger.error(f"Trigger config error: {config}")
        return False

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
                "11f07f31a45e80c1bd54d5560360f4ca": {
                    "new page": "11f07f31a45e80c1bd54d5560360f4ca",
                    "property changed": {
                        "old property": {
                            "property1": "value1",
                            "property2": null
                            },
                        "new property": {
                            "property1": "newvalue1",
                            "property2": "newvalue2"
                            }
                    }
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
    MONITOR.ping(state='run')

    onoff = True
    try:
        while onoff:
            counter += 1    
            listener.listen()
            time.sleep(SLEEP_TIMER)
            if counter % PING_CYCLE == 0:
                MONITOR.ping()
            if counter % GC_CYCLE == 0:
                gc.collect()
            if counter % CONFIG_RELOAD_CYCLE == 0:
                listener.load_config()
    except KeyboardInterrupt:
        MONITOR.ping(state='complete')
        pass