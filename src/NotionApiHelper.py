#!/usr/bin/env python3
# Aria Corona Sept 19th, 2024
# Notion API Helper
# This script is designed to assist in the interaction with the Notion API, providing a series of functions to query, create, and update pages in a Notion database.
# It has not been extensively tested and may require further development to be fully functional. (Specifically the create_page() function)

'''
Dependencies:
- None, but requires the headers.json file to be present in the same directory as the script. Notion API requires authentication and the Notion API version as headers.
'''


#  query(self, databaseID, filter_properties = None, content_filter = None, page_num = None):
"""
Sends a post request to a specified Notion database, returning the response as a dictionary. Will return {} if the request fails.
query(string, list(opt.), dict(opt.),int(opt.)) -> dict

    Args:
        databaseID (str): The ID of the Notion database.
        filter_properties (list): Filter properties as a list of strings. Optional.
            Can be used to filter which page properties are returned in the response.
            Example: ["%7ChE%7C", "NPnZ", "%3F%5BWr"]
        content_filter (dict): Content filter as a dictionary. Optional.
            Can be used to filter pages based on the specified properties.
            Example: 
                {
                    "and": [
                        {
                            "property": "Job status",
                            "select": {
                                "does_not_equal": "Canceled"
                            }
                        },
                        {
                            "property": "Created",
                            "date": {
                                "past_week": {}
                            }
                        }
                    ]
                }
        page_num (int): The number of pages to retrieve. Optional.
            If not specified, all pages will be retrieved.

    Returns:
        list of dictionary objects: The "results" of the JSON response from the Notion API. This will cut out the pagination information, returning only the page data.

Additional information on content filters can be found at https://developers.notion.com/reference/post-database-query-filter#the-filter-object
Additional information on Notion queries can be found at https://developers.notion.com/reference/post-database-query
"""

#  get_page(self, pageID):
"""
Sends a get request to a specified Notion page, returning the response as a dictionary. Will return {} if the request fails.
Relation properties are capped at 25 items, and will return a truncated list if the relation has more than 25 items. This is a limitation of the Notion API.
    Use the get_page_property method to retrieve the full list of relation items.

get_object(string) -> dict

    Args:
        databaseID (str): The ID of the Notion database.

    Returns:
        dict: The JSON response from the Notion API.
"""

#  get_page_property(self, pageID, propID):
"""
Sends a get request to a specified Notion page property, returning the response as a JSON property item object. Will return {} if the request fails.
https://developers.notion.com/reference/property-item-object

get_object(string) -> dict

    Args:
        pageID (str): The ID of the Notion database.
        propID (str): The ID of the property to retrieve.

    Returns:
        dict: The JSON response from the Notion API.
"""

#  create_page(self, databaseID, properties):
"""
Sends a post request to a specified Notion database, creating a new page with the specified properties. Returns the response as a dictionary. Will return {} if the request fails.

create_page(string, dict) -> dict

    Args:
        databaseID (str): The ID of the Notion database.
        properties (dict): The properties of the new page as a dictionary.

    Returns:
        dict: The dictionary response from the Notion API.
"""

#  update_page(self, pageID, properties, trash = False):
'''
Sends a patch request to a specified Notion page, updating the page with the specified properties. Returns the response as a dictionary. Will return {} if the request errors out.
Page property keys can be either the property name or property ID.

update_page(string, dict) -> dict
    Args:
        pageID (str): The ID of the Notion page.
        properties (dict): The properties of the page as a dictionary.
        trash (bool): Optional. If True, the page will be moved to the trash. Default is False.

    Returns:
        dict: The dictionary response from the Notion API.
'''

# generate_property_body(self, prop_name, prop_type, prop_value, prop_value2 = None, annotation = None):
'''
Accepts a range of property types and generates a dictionary based on the input.
    Accepted property types is a string from the following list:
        "checkbox" | "email" | "number" | "phone_number" | "url" | "select" | "status" | "date" | "files" | "multi_select" | "relation" | "people" | "rich_text" | "title"
    Args:
    - prop_name (string): The name of the property.
    - prop_type (string): The type of the property.
    - prop_value (string/number/bool/array of strings): The value of the property.
    - prop_value2 (string/array of strings): The second value of the property. Optional.
    - annotation (array of dict): The annotation of the property. Optional.
        - Dictionary format: [{"bold": bool, "italic": bool, "strikethrough": bool, "underline": bool, "code": bool, "color": string}]
        - default annotations: {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}
        - Acceptable Colors: Colors: "blue", "blue_background", "brown", "brown_background", "default", "gray", "gray_background", "green", "green_background", "orange", "orange_background", "pink", "pink_background", "purple", "purple_background", "red", "red_background", "yellow", "yellow_background"
    Returns:
    - dict: The python dictionary object of a property, formatted to fit as one of the properties in a page POST/PATCH request.

    Checkbox, Email, Number, Phone Number, URL:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "checkbox" | "email" | "number" | "phone_number" | "url"
        Property Value: string/number/bool to be uploaded.

    Select, Status:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "select" | "status"
        Property Value: string to be uploaded. Will create a new select/status if it does not exist.

    Date:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "date"
        Start Date Value: string (ISO 8601 date and optional time) as "2020-12-08T12:00:00Z" or "2020-12-08"
        End Date Value: optional string (ISO 8601 date and optional time) as "2020-12-08T12:00:00Z" or "2020-12-08"
            End date will default to None if not provided, meaning the date is not a range.

    Files:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "files"
        File Names: Array of string
        File URLs: Array of string

    Multi-Select:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "multi_select"
        Property Value: Array of strings

    Relation:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "relation"
        Property Value: Array of strings

    People:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "people"
        Property Value: Array of strings

    Rich Text:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "rich_text"
        Property Value: Array of strings 
        Property Value Link: Array of strings [opt.]
        Annotation: Array of dictionaries [opt.]
            Dictionary format: [{"bold": bool, "italic": bool, "strikethrough": bool, "underline": bool, "code": bool, "color": string}]
            default annotations: {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}
            Acceptable Colors: Colors: "blue", "blue_background", "brown", "brown_background", "default", "gray", "gray_background", "green", "green_background", "orange", "orange_background", "pink", "pink_background", "purple", "purple_background", "red", "red_background", "yellow", "yellow_background"

    Title:
        Property Name: string as the name of the property field in Notion
        Property Type: string as "title"
        Property Value: Array of strings
        Property Value Link: Array of strings
        Annotation: Array of dictionaries
            Dictionary format: [{"bold": bool, "italic": bool, "strikethrough": bool, "underline": bool, "code": bool, "color": string}]
            default annotations: {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}
            Acceptable Colors: Colors: "blue", "blue_background", "brown", "brown_background", "default", "gray", "gray_background", "green", "green_background", "orange", "orange_background", "pink", "pink_background", "purple", "purple_background", "red", "red_background", "yellow", "yellow_background"
'''

import requests, time, json, logging

class NotionApiHelper:
    MAX_RETRIES = 3
    RETRY_DELAY = 30  # seconds
    PAGE_SIZE = 100
    

    def __init__(self):
        # Load headers from the external JSON file
        with open('src/headers.json', 'r') as file:
            self.headers = json.load(file)
        
        self.endPoint = "https://api.notion.com/v1"
        self.counter = 0
    
    def query(self, databaseID, filter_properties = None, content_filter = None, page_num = None):

        databaseJson = {}
        get_all = page_num is None
        page_size = self.PAGE_SIZE if get_all else page_num
        bodyJson = {"page_size": page_size, "filter": content_filter} if content_filter else {"page_size": page_size}
        filter_properties = "?filter_properties=" + "&filter_properties=".join(filter_properties) if filter_properties else ""
        print(f"Body JSON: {json.dumps(bodyJson)}")
        databaseJson = self._make_query_request(databaseID, filter_properties, bodyJson)
        if not databaseJson:
            print("No data returned.")
            self.counter = 0
            return {}

        results = databaseJson["results"]
        print("Data returned.")
        while databaseJson["has_more"] and get_all:
            print("More data available.")
            time.sleep(0.5) # To avoid rate limiting
            print("Querying next page...")
            bodyJson = {"page_size": page_size, "start_cursor": databaseJson["next_cursor"], "filter": content_filter} if content_filter else {"page_size": page_size, "start_cursor": databaseJson["next_cursor"]}
            new_data = self._make_query_request(databaseID, filter_properties, bodyJson)
            if not new_data:
                self.counter = 0
                return {}
            databaseJson = new_data
            results.extend(databaseJson["results"])
        print("All data retrieved, returning results.")
        self.counter = 0
        return results

    def _make_query_request(self, databaseID, filter_properties, bodyJson):
        """
        Makes a POST request to the Notion API to query a database. Used by the query method to handle pagination.

        Args:
            databaseID (str): The ID of the Notion database.
            filter_properties (str): Filter properties as a query string.
            bodyJson (dict): The JSON body of the request.

        Returns:
            dict: The JSON response from the Notion API.
        """
        try:
            print("Sending post request...")
            print(f"{self.endPoint}/databases/{databaseID}/query{filter_properties}")
            response = requests.post(f"{self.endPoint}/databases/{databaseID}/query{filter_properties}", headers=self.headers, json=bodyJson)
            response.raise_for_status()
            print("Post request successful.")
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                print(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self._make_query_request(databaseID, filter_properties, bodyJson)
            else:
                print(f"Network error occurred too many times: {e}")
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                return {}

    def get_page(self, pageID):
        try:
            time.sleep(0.5) # To avoid rate limiting
            print(f"{self.endPoint}/pages/{pageID}")
            response = requests.get(f"{self.endPoint}/pages/{pageID}", headers=self.headers)
            response.raise_for_status()
            self.counter = 0
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self.get_page(pageID)
            else:    
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                self.counter = 0
                return {}
        
    def get_page_property(self, pageID, propID):
        try:
            time.sleep(0.5) # To avoid rate limiting
            print(f"{self.endPoint}/pages/{pageID}/properties/{propID}")
            response = requests.get(f"{self.endPoint}/pages/{pageID}/properties/{propID}", headers=self.headers)
            response.raise_for_status()
            self.counter = 0
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self.get_page_property(pageID, propID)
            else:    
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                self.counter = 0
                return {}

    def create_page(self, databaseID, properties): # Will update to allow icon and cover images later.
        jsonBody = {"parent": {"database_id": databaseID}, "properties": properties}
        try:
            print(f"{self.endPoint}/pages")
            response = requests.post(f"{self.endPoint}/pages", headers=self.headers, json=jsonBody)
            response.raise_for_status()
            self.counter = 0
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self.create_page(databaseID, properties)
            else:    
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                self.counter = 0
                return {}
            
    def update_page(self, pageID, properties, trash = False): # Will update to allow icon and cover images later.
        jsonBody = {"properties": properties}
        print(jsonBody)
        try:
            print("Sending patch request...")
            print(f"{self.endPoint}/pages/{pageID}")
            response = requests.patch(f"{self.endPoint}/pages/{pageID}", headers=self.headers, json=jsonBody)
            # print(response.text)
            response.raise_for_status()
            self.counter = 0
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self.update_page(pageID, properties)
            else:    
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                self.counter = 0
                return {}


    def simple_prop_gen(self, prop_name, prop_type, prop_value):
        '''
        Generates a simple property dictionary.
        "checkbox" | "email" | "number" | "phone_number" | "url"
        '''
        return {prop_name: {prop_type: prop_value}}
    
    def selstat_prop_gen(self, prop_name, prop_type, prop_value):
        '''
        Generates a select or status property dictionary.
        '''
        return {prop_name: {prop_type: {"name": prop_value}}}
    
    def date_prop_gen(self, prop_name, prop_type, prop_value, prop_value2):
        '''
        Generates a date property dictionary.
        '''
        if prop_value2 is None:
            return {prop_name: {prop_type: {"start": prop_value}}}
        else:
            return {prop_name: {prop_type: {"start": prop_value, "end": prop_value2}}}
        
    def files_prop_gen(self, prop_name, prop_type, file_names, file_urls):
        '''
        Generates a files property dictionary.
        '''
        if file_names is None or file_urls is None:
            return {}
        file_body = []
        for name, url in zip(file_names, file_urls):
            file_body.append({"name": name, "external": {"url": url}})
        return {prop_name: {prop_type: file_body}}

    def mulsel_prop_gen(self, prop_name, prop_type, prop_values):
        '''
        Generates a multi-select or relation property dictionary.
        '''
        prop_value_new = []
        for value in prop_values:
            prop_value_new.append({"name": value})
        return {prop_name: {prop_type: prop_value_new}}

    def relation_prop_gen(self, prop_name, prop_type, prop_values):
        '''
        Generates a relation property dictionary.
        '''
        prop_value_new = []
        for value in prop_values:
            prop_value_new.append({"id": value})
        return {prop_name: {prop_type: prop_value_new}}
    
    def people_prop_gen(self, prop_name, prop_type, prop_value):
        '''
        Generates a people property dictionary.
        '''
        prop_value_new = []
        for value in prop_value:
            prop_value_new.append({"object": "user","id": value})
        return {prop_name: {prop_type: prop_value_new}}
    
    def rich_text_prop_gen(self, prop_name, prop_type, prop_value, prop_value_link = None, annotation = None):
        '''
        Generates a rich text property dictionary.
        '''
        default_annotations = {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}
        rich_body = []
        if annotation and prop_value_link:
            for x, y, z in zip(prop_value, prop_value_link, annotation):
                rich_body.append({"type": "text", "text": {"content": x, "link": y}, "annotations": {"bold": z["bold"], "italic": z["italic"], "strikethrough": z["strikethrough"], "underline": z["underline"], "code": z["code"], "color": z["color"]}, "plain_text": x, "href": y})
        elif prop_value_link:
            for x, y in zip(prop_value, prop_value_link):
                rich_body.append({"type": "text", "text": {"content": x, "link": y}, "annotations": default_annotations, "plain_text": x, "href": y})
        elif annotation:
            for x, z in zip(prop_value, annotation):
                rich_body.append({"type": "text", "text": {"content": x, "link": y}, "annotations": {"bold": z["bold"], "italic": z["italic"], "strikethrough": z["strikethrough"], "underline": z["underline"], "code": z["code"], "color": z["color"]}, "plain_text": x, "href": y})
        else:
            for x in prop_value:
                rich_body.append({"type": "text", "text": {"content": x, "link": prop_value_link}, "annotations": default_annotations, "plain_text": x, "href": prop_value_link})
        return {prop_name: {prop_type: rich_body}}
    
    def title_prop_gen(self, prop_name, prop_type, prop_value, prop_value_link = None, annotation = None):
        '''
        Generates a title property dictionary.
        '''
        default_annotations = {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}
        rich_body = []
        if annotation and prop_value_link:
            for x, y, z in zip(prop_value, prop_value_link, annotation):
                rich_body.append({"type": "text", "text": {"content": x, "link": y}, "annotations": {"bold": z["bold"], "italic": z["italic"], "strikethrough": z["strikethrough"], "underline": z["underline"], "code": z["code"], "color": z["color"]}, "plain_text": x, "href": y})
        elif prop_value_link:
            for x, y in zip(prop_value, prop_value_link):
                rich_body.append({"type": "text", "text": {"content": x, "link": y}, "annotations": default_annotations, "plain_text": x, "href": y})
        elif annotation:
            for x, z in zip(prop_value, annotation):
                rich_body.append({"type": "text", "text": {"content": x, "link": y}, "annotations": {"bold": z["bold"], "italic": z["italic"], "strikethrough": z["strikethrough"], "underline": z["underline"], "code": z["code"], "color": z["color"]}, "plain_text": x, "href": y})
        else:
            for x in prop_value:
                rich_body.append({"type": "text", "text": {"content": x, "link": prop_value_link}, "annotations": default_annotations, "plain_text": x, "href": prop_value_link})
        return {prop_name: {"id": prop_type, "type": prop_type, prop_type: rich_body}}

    def generate_property_body(self, prop_name, prop_type, prop_value, prop_value2 = None, annotation = None): # Should have been named generate_body_property, will fix in future.

        type_dict = {
            'checkbox': self.simple_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'email': self.simple_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'number': self.simple_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'phone_number': self.simple_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'url': self.simple_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'select': self.selstat_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'status': self.selstat_prop_gen(prop_name, prop_type, prop_value), # string, string, string
            'date': self.date_prop_gen(prop_name, prop_type, prop_value, prop_value2), # string, string, string, string
            'files': self.files_prop_gen(prop_name, prop_type, prop_value, prop_value2), # string, string, array of string, array of string
            'multi_select': self.mulsel_prop_gen(prop_name, prop_type, prop_value), # string, string, array of strings
            'relation': self.relation_prop_gen(prop_name, prop_type, prop_value), # string, string, array of strings
            'people': self.people_prop_gen(prop_name, prop_type, prop_value), # string, string, array of strings
            'rich_text': self.rich_text_prop_gen(prop_name, prop_type, prop_value, prop_value2, annotation), # string, string, array of strings, array of strings, array of objects
            'title': self.title_prop_gen(prop_name, prop_type, prop_value, prop_value2, annotation) # string, string, array of strings, array of strings, array of objects
        }
        return type_dict[prop_type]
    
    def return_property_value(self, property, id):
        def is_simple(data, prop_type):
            return data[prop_type]
        
        def is_uid(data, prop_type):
            return data[prop_type]['prefix'] + data[prop_type]['number']
        
        def is_selstat(data, prop_type):
            return data[prop_type]['name']
        
        def is_formula(data, prop_type):
            form_type = data[prop_type]['type']
            if form_type == "date":
                return is_date(data[prop_type], form_type)
            else:
                return data[prop_type][form_type]     
        
        def is_rich_text(data, prop_type):
            text_list = []
            for text in data[prop_type]:
                text_list.append(text['plain_text'])
            return ", ".join(text_list)
        
        def is_relation(data, prop_type):
            print(f"Relation data: {data}")
            package = []
            if "has_more" in data:
                if data["has_more"]: # If there are more than 25 relations, replace data with full data (up to 100).
                    print(f"Gathering full relation data for {prop_type}")
                    response = self.get_page_property(id, data['id'])
                    data[prop_type] = response[prop_type]
            
            for id in data[prop_type]:
                package.append(id['id'])
                
            return package
        
        def is_date(data, prop_type):
            return data[prop_type]['start']
        
        def is_files(data, prop_type):
            file_list = []
            for file in data[prop_type]:
                file_list.append(file['external']['url'])
            return file_list
        
        def is_person(data, prop_type):
            return data[prop_type]['id']
        
        def is_multi_select(data, prop_type):
            package = []
            for select in data[prop_type]:
                package.append(select['name'])
            return package
        
        def is_rollup(data, prop_type):
            roll_type = data[prop_type]['type']
            if roll_type == "array":
                return_list = []
                for each in data[prop_type]['array']:
                    return_list.append(self.return_property_value(each, id))
                return return_list
            else:
                return self.return_property_value(data[prop_type][roll_type], id)
        
        router = { # This is a dictionary of functions that return the data in the correct format.
            'checkbox': is_simple,
            'created_by': is_person,
            'created_time': is_simple,
            'email': is_simple,
            'number': is_simple,
            'phone_number': is_simple,
            'people': is_multi_select, # This will return a list of names instead of IDs.
            'url': is_simple,
            'last_edited_time': is_simple,
            'select': is_selstat,
            'status': is_selstat,
            'formula': is_formula,
            'unique_id': is_uid,
            'rich_text': is_rich_text,
            'title': is_rich_text,
            'relation': is_relation,
            'date': is_date,
            'files': is_files,
            'last_edited_by': is_selstat, # This will return the name instead of the ID.
            'multi_select': is_multi_select,
            'rollup': is_rollup
        }
        try:
            prop_type = property['type']
            print(f"Property Type: {prop_type}")
            for key, check_router in router.items():
                if key == prop_type:
                    value = check_router(property, prop_type)
                    print(f"Found value: {value}")
            return value
        except Exception as e:
            print(f"Error returning property value: {e}")
            logging.error(f"Error returning property value: {e}")
            return None
