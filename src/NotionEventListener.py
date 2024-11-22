#!/usr/bin/env python3
# Aria Corona Sept 19th, 2024
# Some comments and docstrings are AI generated and may not be accurate, but seem to be mostly correct probably.


'''This script listens for changes in Notion databases and triggers actions based on the configuration.
Dependencies:
- NotionApiHelper.py
- AutomatedEmails.py
- pip install deepdiff
Modules:
- NotionApiHelper: Helper functions for interacting with the Notion API.
- AutomatedEmails: Functions for sending automated emails.
- deepdiff: Library for deep comparison of Python objects.
- time, os, requests, uuid, cronitor, gc, logging, json, datetime, timedelta: Standard Python libraries.
Classes:
- NotionEventListener: Main class that listens for changes in Notion databases and triggers actions.
Configuration:
- CRONITOR_KEY_PATH: Path to the Cronitor API key file.
- SLEEP_TIMER: Time in seconds to sleep between each loop.
- PING_CYCLE: Number of loops before pinging Cronitor.
- GC_CYCLE: Number of loops before running garbage collection.
- CONFIG_RELOAD_CYCLE: Number of loops before reloading the config file.
- STOP_CYCLE: Number of loops before stopping the script.
Methods:
- __init__: Initializes the NotionEventListener class.
- update_filter_time: Updates the query filter time.
- listen: Listens to changes in the configured Notion databases and triggers actions based on the configuration.
- get_active_properties: Returns a unique list of database properties based on the configuration files for a given database.
- check_triggers: Recursively checks for triggers in the data.
- trigger_compare: Compares a new property value against a configuration using various comparison functions.
- check_change: Checks for changes in the data and returns a dictionary of changes and additions.
- query_database: Queries the specified database and returns the response.
- load_storage: Loads the previous query results from storage.
- save_storage: Saves the current query results to storage.
- storage_exists: Checks if storage exists for a given database.
- take_action: Takes an action based on the specified action configuration.
- check_config: Checks the configuration file for changes and reloads it if necessary.

'''



from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from deepdiff import DeepDiff
import time, os, requests, uuid, cronitor, gc
import logging, subprocess, sys
import json
from datetime import datetime, timedelta

CRONITOR_KEY_PATH = "conf/Cronitor_API_Key.txt"
with open(CRONITOR_KEY_PATH, "r") as file:
    cronitor_api_key = file.read()
        
cronitor.api_key = cronitor_api_key
MONITOR = cronitor.Monitor("Notion Event Listener")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/NotionEventListener.log'),
                        logging.StreamHandler()
                    ])

gc.enable() # Garbage Collection

SLEEP_TIMER = 3 # Seconds
PING_CYCLE = 15 # Number of loops before pinging cronitor.
GC_CYCLE = 60 # Number of loops before running garbage collection.
CONFIG_RELOAD_CYCLE = 5 # Number of loops before reloading the config file.
END_WINDOW = ("22:52", "22:54")

class NotionEventListener:
    def __init__(self):
        
        self.notion_helper = NotionApiHelper()
        self.planet_helper = NotionApiHelper(header_path = "src/headers_pts.json")
        self.automated_emails = AutomatedEmails()
        
        self.logger = logging.getLogger(__name__)
        
        self.STORAGE_DIRECTORY = "storage"
        self.CONFIG_PATH = "conf/NotionEventListener_Conf.json"
        self.EMAIL_ME_PATH = "conf/Aria_Email_Conf.json"
        self.query_lookback_time = 45 # Changing this doesn't actually do anything anymore.
        self.QUERY_LOOKBACK_PADDING = 5 # Minutes, Padding on how far back to look for changes on subsequent queries.
        self.QUERY_LOOKBACK = (datetime.now() - timedelta(minutes=self.query_lookback_time)).strftime('%Y-%m-%dT%H:%M:%S')
        self.query_filter = {
            "timestamp": "last_edited_time", "last_edited_time": {"on_or_after": self.QUERY_LOOKBACK}
            } # Default query filter. Overrided by config.
        self.config = {}
        self.first_run = True
        self.base = 'meno'
    
        os.makedirs(self.STORAGE_DIRECTORY, exist_ok=True)
        
    def update_filter_time(self):
        self.query_filter['last_edited_time'] = {
            "on_or_after": (datetime.now() - timedelta(minutes=self.query_lookback_time)).strftime('%Y-%m-%dT%H:%M:%S')
            }

    def listen(self):
        """
        Listens to changes in the configured Notion databases and triggers actions based on the configuration.
        This method performs the following steps:
        1. Iterates over each database ID in the configuration.
        2. Normalizes the database ID by removing hyphens.
        3. Sets up query property filters to maximize efficiency.
        4. Loads the previous query results from storage or initializes storage if it doesn't exist.
        5. Queries the database for changes.
        6. Checks for changes between the previous and current query results.
        7. If changes are detected, checks for triggers in the configuration and takes appropriate actions.
        8. Saves the current query results to storage.
        9. Updates the query lookback time based on the script's runtime.
        Attributes:
            start_time (datetime): The start time of the listening process.
            db_id (str): The normalized database ID.
            filter_properties (list or None): List of properties to filter the query or None if no filters are found.
            active_properties (list): List of active properties in the database.
            content_filter (dict): The content filter for querying the database.
            last_query (dict): The previous query results loaded from storage.
            response (dict): The current query results from the database.
            difference (dict): The differences between the previous and current query results.
            activate_trigger (bool): Indicates if a trigger condition is met.
            package (dict): The package of changes to be processed by the action.
        Raises:
            Exception: If there is an issue with querying the database or processing the configuration.
        """
        
        start_time = datetime.now()
        for db_id in self.config:
            db_id = db_id.replace('-','') # Normalizing db ID now so it's not a problem again later.
            #print(f"Listening to database {db_id}")
            filter_properties = []
            
            for db_config_page in self.config[db_id]: # Set up query property filters. Trying to do as few queries as possible to maximize efficiency.
                #print(f"Checking config page {db_config_page['uid']}")
                if 'base' in db_config_page:
                    self.base = db_config_page['base']
                if 'filter_properties' in db_config_page:
                    #print(f"Filter properties found in config page {db_config_page['uid']}")
                    for property_id in db_config_page['filter_properties']:
                        #print(f"Adding property {property_id} to filter list.")
                        if property_id in filter_properties:
                            #print(f"Property {property_id} already in filter list.")
                            continue
                        filter_properties.append(property_id)
            if filter_properties == []: # If no filter properties are found, set to None.
                filter_properties = None
            
            active_properties = self.get_active_properties(db_id)
            self.logger.info(f"Listening to database {db_id}, with filter properties {filter_properties}")
            
            # Pull Content Filter from Config, defaults to self.query_filter if not found.
            if 'content_filter' in self.config[db_id]: 
                content_filter = self.config[db_id]['content_filter']
            else:
                content_filter = self.query_filter
            
            # Load previous query
            if not self.storage_exists(db_id) or self.first_run: # Storage does not exist, create it.
                self.logger.info(f"Storage does not exist for database {db_id}.")
                self.save_storage(db_id, self.query_database(db_id, filter_properties))
                continue
            
            last_query = self.load_storage(db_id)
            
            # Query DB
            response = self.query_database(db_id, filter_properties, content_filter)

            # Skips if returned no response because of a network issue or something.
            if response is None:
                self.logger.error(f"No response from queried database: {db_id}.")
                time.sleep(1)
                continue
                
            difference = self.check_change(last_query, response, active_properties)
            
            # If no bulk changes detected, continue to next db.
            if difference == {}:
                self.logger.info(f"No changes detected in database {db_id}.")
                time.sleep(.5)
                continue

            self.logger.info(f"Difference: {json.dumps(difference)}")
            
            for db_config_page in self.config[db_id]: # Check for triggers in the config file.
                for page in difference: # Check for triggers in the difference.
                    activate_trigger = self.check_triggers(difference[page], db_config_page['trigger'])
                    self.logger.info(f" Active Trigger: {activate_trigger}")
                    if activate_trigger: # If trigger is met, take action.
                        self.logger.info(f"Trigger met for page {page} in database {db_id}.")
                        
                        if 'action' in db_config_page: 
                            package = {page: difference[page]}
                            self.take_action(db_config_page['action'], package)
                            
                        else:
                            self.logger.error(f"No action found for page {page} in database {db_id}.")
            
            self.update_storage(db_id, response)
            time.sleep(5)
            
        self.query_lookback_time = (datetime.now() - start_time).total_seconds() / 60 + 10 # Update lookback time based on how long the script has been running.
        self.first_run = False # Set first run to false after the first run.
            
    def get_active_properties(self, db_id): # Returns a unique list of DB properties based off the config files for a given DB.
        """
        Returns a unique list of database properties based on the configuration files for a given database.
        This method parses the triggers defined in the configuration files and extracts the properties
        associated with those triggers. It supports nested 'and' and 'or' conditions within the triggers.
        Args:
            db_id (str): The ID of the database for which to retrieve active properties.
        Returns:
            list: A list of unique properties extracted from the configuration files for the specified database.
            If the database ID is not found in the configuration, an empty list is returned.
        Raises:
            KeyError: If the database ID is not present in the configuration.
        """
        
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
            self.logger.error(f"Database {db_id} not found in config.")
            return []

        for conf in self.config[db_id]:
            parse_trigger(conf['trigger'])
            
        return active_properties
            
    def check_triggers(self, data, config): # Recursively checks for triggers in the data.
        """
        Recursively checks for triggers in the data.
        This method evaluates the provided data against the specified configuration to determine if any triggers are activated. 
        It supports various property types and logical conditions such as 'and', 'or', and 'created'.
        Args:
            data (dict): The data to be checked for triggers. This typically includes information about property changes.
            config (dict): The configuration specifying the triggers to check. This can include logical conditions and property-specific checks.
        Returns:
            bool: True if any of the specified triggers are activated, False otherwise.
        Raises:
            Exception: If an error occurs during the trigger check, it logs the error and sends an email notification.
        """
        
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
                                
            if data['property changed']['new property'][property]['has_more']: # If there are more than 25 relations, replace data with full data (up to 100).
                if self.base == 'pts':
                    response = self.planet_helper.get_page_property(data, data['property changed']['new property'][property]['id'])
                else:
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
                            self.logger.info(f"Property: {property}, Type: {prop_type}")
                            prop_value = data['property changed']['new property'][property][prop_type]
                            package = prop_value if prop_value else {}
                            logging.info(f"Package: {package}")
                            router = { # This is a dictionary of functions that return the data in the correct format.
                                'select': is_selstat,
                                'status': is_selstat,
                                'formula': is_formula,
                                'rich_text': is_rich_text,
                                'relation': is_relation,
                                'date': is_date,
                                'files': is_files,
                                'last_edited_by': is_last_edited_by,
                                'multi_select': is_multi_select,
                                'rollup': is_rollup
                            }
                            for key, check_router in router.items():
                                if key == prop_type:
                                    package = check_router(data, property, prop_type) if prop_value else {}
                            logging.info(f"Package: {package}")
                        except Exception as e:
                            self.logger.error(f"Something in check_triggers failed: {property}\n{e}", exc_info=True)
                            self.automated_emails.send_email(self.EMAIL_ME_PATH, "NotionEventListener:Error in check_triggers", f"Error in check_triggers: {property}\n{e}")
                            return False
                        try:
                            return self.trigger_compare(package, config[prop_type]) 
                        except KeyError:
                            form_type = data['property changed']['new property'][property][prop_type]["type"]
                            return self.trigger_compare(package, config[form_type])
            else:
                return False
                
        else:
            return False
    
        return False 
    
    def trigger_compare(self, new_property, config): # Refactor this as a dictionary of functions.
        """ Summary
        Compares a new property value against a configuration using various comparison functions.
        Args:
            new_property (str): The new property value to be compared.
            config (dict): A dictionary containing the configuration for comparison. The keys in this dictionary
                           determine which comparison function to use.
        Returns:
            bool: The result of the comparison. Returns True if the comparison is successful, False otherwise.
        Comparison Functions:
            - 'contains': Checks if the config value is contained within the new property.
            - 'does_not_contain': Checks if the config value is not contained within the new property.
            - 'equals': Checks if the new property is equal to the config value.
            - 'does_not_equal': Checks if the new property is not equal to the config value.
            - 'ends_with': Checks if the new property ends with the config value.
            - 'starts_with': Checks if the new property starts with the config value.
            - 'is_empty': Checks if the new property is an empty string.
            - 'is_not_empty': Checks if the new property is not an empty string.
            - 'less_than': Checks if the new property is less than the config value.
            - 'greater_than': Checks if the new property is greater than the config value.
            - 'less_than_or_equal_to': Checks if the new property is less than or equal to the config value.
            - 'greater_than_or_equal_to': Checks if the new property is greater than or equal to the config value.
            - 'after': Checks if the new property date is after the config date.
            - 'before': Checks if the new property date is before the config date.
            - 'next_month': Checks if the new property date is within the next month.
            - 'next_year': Checks if the new property date is within the next year.
            - 'next_week': Checks if the new property date is within the next week.
            - 'on_or_after': Checks if the new property date is on or after the config date.
            - 'on_or_before': Checks if the new property date is on or before the config date.
            - 'past_month': Checks if the new property date is within the past month.
            - 'past_year': Checks if the new property date is within the past year.
            - 'past_week': Checks if the new property date is within the past week.
            - 'this_week': Checks if the new property date is within the current week.
        Raises:
            ValueError: If the date format of the new property or config value is invalid.
        """

        self.logger.info(f"trigger_compare: {new_property}, {config}")
        def date_convert(new_prop, config_prop=None): # Converts date strings to datetime objects.
            self.logger.info(f"date_convert: {new_prop}, {config_prop}")
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
                logging.info(f"@ key: {key}, check_function: {check_function}, new_property: {new_property}, config value: {config[key]}")
                if key not in ['this_week', 'next_week', 'past_week', 'next_month', 'past_month', 'next_year', 'past_year', "is_empty", "is_not_empty"]:
                    return check_function(new_property, config[key])
                else:
                    return check_function(new_property, config[key] if key in config else None)        
        
        self.logger.error(f"Trigger config error: {config}")
        return False

    def check_change(self, old_data, new_data, active_properties):
        """
        Checks for changes in the data, returns a dictionary of changes and additions.

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
        self.logger.info("Checking for changes in the data.")
        
        old_dictionary = {}
        new_dictionary = {}
        changes = {}

        
        for page in old_data:
            old_dictionary[page['id'].replace('-','')] = page['properties']
        
        for page in new_data:
            new_dictionary[page['id'].replace('-','')] = page['properties']
        
        for page_id in new_dictionary:
            print(f"Checking page {page_id}")
            
            if page_id not in old_dictionary:
                self.logger.info(f"New page detected: {page_id}")
                changes[page_id] = {"new page": page_id}
    
            else:
                for property in new_dictionary[page_id]:
                    if property in active_properties:
                        if property in old_dictionary[page_id]:
                            if DeepDiff(old_dictionary[page_id][property], new_dictionary[page_id][property]) != {}:
                                self.logger.info(f"Property {property} changed in page {page_id}")
                                if page_id not in changes:
                                    changes[page_id] = {'property changed': {'new property': {}, 'old property': {}}}
                                changes[page_id]['property changed']['old property'][property] = old_dictionary[page_id][property]
                                changes[page_id]['property changed']['new property'][property] = new_dictionary[page_id][property]
                                
                        else:
                            self.logger.info(f"New property {property} detected in page {page_id}")
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
        
        if "py_script" in action:
            for script in action['py_script']:
                self.start_script(script, data)
                
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
        self.logger.info("Loading config file.")
        with open(self.CONFIG_PATH, 'r') as config_file:
            self.config = json.load(config_file)

    def save_config(self):
        self.logger.info("Saving config file.")
        with open(self.CONFIG_PATH, 'w') as config_file:
            json.dump(self.config, config_file, indent=4)

    def load_storage(self, database_id):
        self.logger.info(f"Loading data from storage for database {database_id}")
        with open(f"{self.STORAGE_DIRECTORY}/{database_id}.json", 'r') as storage_file:
            return json.load(storage_file)

    def save_storage(self, database_id, data):
        self.logger.info(f"Saving data to storage for database {database_id}")
        with open(f"{self.STORAGE_DIRECTORY}/{database_id}.json", 'w') as storage_file:
            json.dump(data, storage_file, indent=4)
            
    def update_storage(self, database_id, data): 
        """
        Updates the local storage with the provided data for a specific database.
        This method compares the existing local data with the new data and updates the local storage accordingly.
        It ensures that any new pages are added and existing pages are updated, while preserving pages that are not in the new data.
        Args:
            database_id (str): The ID of the database whose storage needs to be updated.
            data (list): A list of dictionaries representing the new data to be stored. Each dictionary should contain an 'id' key.
        Returns:
            None
        """
        self.logger.info(f"Updating storage for database {database_id}")
        
        local_data = self.load_storage(database_id)
        page_index = {page['id']: page for page in data}
        
        updated_local_data = []
        processed_ids = set()
        
        for old_page in local_data:
            if old_page['id'] in page_index:
                updated_local_data.append(page_index[old_page['id']])
                processed_ids.add(old_page['id'])
            else:
                updated_local_data.append(old_page)
        
        for page_id, page in page_index.items():
            if page_id not in processed_ids:
                updated_local_data.append(page)
            
        self.save_storage(database_id, updated_local_data)

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

    def query_database(self, db_id, filter_properties = None, content_filter = None, pts = False):
        self.logger.info(f"Querying database {db_id}, filter properties: {filter_properties}\ncontent filter: {content_filter}")
        self.update_filter_time()
        
        if self.base == 'pts':
            return self.planet_helper.query(db_id, filter_properties, content_filter)
        else:
            return self.notion_helper.query(db_id, filter_properties, content_filter)
    
    def store_previous_query(self, data, db_id):
        self.logger.info(f"Storing previous query for database {db_id}")
        with open(os.path.join(self.STORAGE_DIRECTORY, f"{db_id}.json"), 'w') as file:
            json.dump(data, file, indent=4)
        pass

    def notify_email(self, email, config_directory = None, data = None):
        pass

    def notify_webhook(self, webhook_url, data = None):
        self.logger.info(f"Sending data to webhook: {webhook_url}")
        headers = {'Content-Type': 'application/json'}
        self.logger.info(f"Data: {json.dumps(data)}")
        try:
            response = requests.post(webhook_url, headers=headers, data=json.dumps(data))
            response.raise_for_status()
            self.logger.info(f"Successfully sent data to webhook: {webhook_url}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to send data to webhook: {webhook_url}, error: {e}")
        pass

    def notify_slack(self, data):
        pass

    def start_script(self, script_path, data):
        if data:
            page_id = list(data.keys())[0]
            self.logger.info(f"Starting script {script_path} for page {page_id}")
            
            try:
                subprocess.Popen(['python', script_path, page_id])
                self.logger.info(f"Script {script_path} started for page {page_id}")
            except Exception as e:
                self.logger.error(f"Failed to start script {script_path} for page {page_id}: {e}")
        else:
            self.logger.error(f"No data provided to start script {script_path}")
        pass
    
    def catch_variable(self):
        if len(sys.argv) > 1:
            argument = sys.argv[1]
            if argument == "-S" or argument == "--skip_startup":
                self.first_run = False
                self.logger.info(f"Starting script without database initializing")


    
if __name__ == "__main__":
    listener = NotionEventListener()
    counter = 0 # Tracking number of loops to trigger different events. ie. refreshing config, pinging monitoring service, clean memory, etc.
    
    listener.load_config()
    listener.catch_variable()
    MONITOR.ping(state='run')

    onoff = True

    listener.logger.info("Starting Notion Event Listener")

    try:
        while onoff:
            counter += 1    
            listener.listen()
            listener.logger.info(f"Sleeping...")
            time.sleep(SLEEP_TIMER)
            if counter % PING_CYCLE == 0:
                MONITOR.ping()
            if counter % GC_CYCLE == 0:
                gc.collect()
            if counter % CONFIG_RELOAD_CYCLE == 0:
                listener.load_config()
            if END_WINDOW[0] < datetime.now().strftime("%H:%M") < END_WINDOW[1]:
                onoff = False
    except KeyboardInterrupt:
        listener.logger.info("")
        MONITOR.ping(state='complete')
    except Exception as e:
        listener.logger.error(f"Error in Notion Event Listener: {e}", exc_info=True)
        listener.automated_emails.send_email(listener.EMAIL_ME_PATH, "NotionEventListener:Error", f"Error in Notion Event Listener: {e}")
        MONITOR.ping(state='fail')
        sys.exit(1)
    
    listener.logger.info("Stopping Notion Event Listener")
    MONITOR.ping(state='complete')