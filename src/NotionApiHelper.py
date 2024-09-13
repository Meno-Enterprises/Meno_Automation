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
        """
        Sends a post request to a specified Notion database, returning the response as a JSON object. Will return {} if the request fails.
        query(string, list(opt.), dict(opt.),int(opt.)) -> dict

            Args:
                databaseID (str): The ID of the Notion database.
                filter_properties (list): Filter properties as a list of strings. Optional.
                    Can be used to filter which page properties are returned in the response.
                    Example: ["%7ChE%7C", "NPnZ", "%3F%5BWr"]
                content_filter (dict): Content filter as a JSON object. Optional.
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
                dict: The "results" of the JSON response from the Notion API. This will cut out the pagination information, returning only the page data.

        Additional information on content filters can be found at https://developers.notion.com/reference/post-database-query-filter#the-filter-object
        Additional information on Notion queries can be found at https://developers.notion.com/reference/post-database-query
        """
        databaseJson = {}
        get_all = page_num is None
        page_size = self.PAGE_SIZE if get_all else page_num
        bodyJson = {"page_size": page_size, "filter": content_filter} if content_filter else {"page_size": page_size}
        filter_properties = "?filter_properties=" + "&filter_properties=".join(filter_properties) if filter_properties else ""
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
        """
        Sends a get request to a specified Notion page, returning the response as a JSON object. Will return {} if the request fails.
        
        
        get_object(string) -> dict

            Args:
                databaseID (str): The ID of the Notion database.

            Returns:
                dict: The JSON response from the Notion API.
        """
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
        try:
            time.sleep(0.5) # To avoid rate limiting
            print(f"{self.endPoint}/pages/{pageID}/properies/{propID}")
            response = requests.get(f"{self.endPoint}/pages/{pageID}/properies/{propID}", headers=self.headers)
            response.raise_for_status()
            self.counter = 0
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self.get_page(pageID, propID)
            else:    
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                self.counter = 0
                return {}


    def create_page(self, databaseID, properties): # Will update to allow icon and cover images later.
        """
        Sends a post request to a specified Notion database, creating a new page with the specified properties. Returns the response as a JSON object. Will return {} if the request fails.

        create_page(string, dict) -> dict

            Args:
                databaseID (str): The ID of the Notion database.
                properties (dict): The properties of the new page as a JSON object.

            Returns:
                dict: The JSON response from the Notion API.
        """
        jsonBody = json.dumps({"parent": {"database_id": databaseID}, "properties": properties})
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
        '''
        Sends a patch request to a specified Notion page, updating the page with the specified properties. Returns the response as a JSON object. Will return {} if the request
        '''
        jsonBody = json.dumps({"properties": properties})
        try:
            response = requests.post(f"{self.endPoint}/pages/{pageID}", headers=self.headers, json=jsonBody)
            response.raise_for_status()
            self.counter = 0
            return response.json()
        except requests.exceptions.RequestException as e:
            if self.counter < self.MAX_RETRIES:
                logging.error(f"Network error occurred: {e}. Trying again in {self.RETRY_DELAY} seconds.")
                time.sleep(self.RETRY_DELAY)
                self.counter += 1
                return self.create_page(pageID, properties)
            else:    
                logging.error(f"Network error occurred too many times: {e}")
                time.sleep(3)
                self.counter = 0
                return {}


    '''
    def properties_to_json(self):   #SHELVED FOR THE TIME BEING
        """
        Opens a GUI window with select and text fields to input the properties of a new Notion page. Returns the properties as a JSON object formatted for the NotionAPI.
        https://developers.notion.com/reference/page-property-values
        
        properties_to_json(list) -> dict

        Returns:
            dict: The properties as a JSON object.
        """
        properties = {}
        properties_list = []
        no_var = ["checkbox", "email", "number", "phone_number", "url"]
        one_var = ["select", "status"]
        two_var = ["date"]
        nested_var_multi = ["files"]
        one_var_multi = ["multi_select", "relation"]
        two_var_multi = ["people"]
        rich_text_multi = ["rich_text", "title"]

        def save_properties():
            properties["Property Name"] = property_name_entry.get()
            properties["Property Type"] = property_type.get()
            properties["Property Value"] = property_value_entry1.get()
            

            root.destroy()

        def property_pack(json_properties = []): 
            entry1 = property_name_entry.get() if property_name_entry.get() else ""
            entry2 = property_type.get() if property_type.get() else ""
            entry3 = property_value_entry1.get() if property_value_entry1.get() else ""
            if entry2 == "checkbox":
                json_properties.append({entry1: {entry2: entry3}})
            elif entry2 == "date":
                json_properties.append({entry1: {entry2: {"start": entry3}}})
            property_name_entry.delete(0, tk.END)
            property_type.set("Select One")
            property_value_entry1.delete(0, tk.END)
            return json_properties

        def propselect(*args):
            widget_destroy()
            widget_pack()

        def widget_pack():
            save_button.pack()
            add_button.pack()
            property_name_label.pack()
            property_name_entry.pack()
            porperty_type_label.pack()
            prop_type_dropdown.pack()
            if property_type.get() in no_var or property_type.get() in one_var:
                property_value_label1.pack()
                property_value_entry1.pack()

        def widget_destroy():
            property_name_label.destroy()
            property_name_entry.destroy()
            porperty_type_label.destroy()
            prop_type_dropdown.destroy()
            property_value_label1.destroy()
            property_value_entry1.destroy()
            datestart_label.destroy()
            datestart.destroy()
            dateend_label.destroy()
            dateend.destroy()

        def one_var_format():
            name = property_name_entry.get()
            prop = property_type.get()
            value = property_value_entry1.get()
            properties_list.append(str({prop: {name: { "name": value}}}))
            print(str({prop: {name: { "name": value}}}))

        root = tk.Tk()
        root.title("Notion Page Properties Generator")
        root.geometry("400x400")

        # Define all the widgets
        datestart = tk.Entry(root)
        datestart_label = tk.Label(root, text="Start Date")
        dateend = tk.Entry(root)
        dateend_label = tk.Label(root, text="End Date (Optional)")
        property_name_label = tk.Label(root, text="Property Name")
        property_name_entry = tk.Entry(root)
        porperty_type_label = tk.Label(root, text="Property Type")
        property_type = tk.StringVar(root)
        property_type.set("Select One")
        property_type.trace_add("write", propselect)
        prop_type_dropdown = tk.OptionMenu(root, property_type, "date", "number")
        property_value_label1 = tk.Label(root, text="Property Value")
        property_value_entry1 = tk.Entry(root)
        save_button = tk.Button(root, text="Save", command=save_properties)
        add_button = tk.Button(root, text="Add Another Property", command=property_pack)

        # Load the initial widgets
        widget_pack()


 

        root.mainloop()

        return properties
    '''

    def simple_prop_gen(self, prop_name, prop_type, prop_value):
        '''
        Generates a simple property JSON object.
        "checkbox" | "email" | "number" | "phone_number" | "url"
        '''
        return {prop_name: {prop_type: prop_value}}
    
    def selstat_prop_gen(self, prop_name, prop_type, prop_value):
        '''
        Generates a select or status property JSON object.
        '''
        return {prop_name: {prop_type: {"name": prop_value}}}
    
    def date_prop_gen(self, prop_name, prop_type, prop_value, prop_value2):
        '''
        Generates a date property JSON object.
        '''
        if prop_value2 is None:
            return {prop_name: {prop_type: {"start": prop_value}}}
        else:
            return {prop_name: {prop_type: {"start": prop_value, "end": prop_value2}}}
        
    def files_prop_gen(self, prop_name, prop_type, file_names, file_urls):
        '''
        Generates a files property JSON object.
        '''
        file_body = []
        for name, url in zip(file_names, file_urls):
            file_body.append({"name": name, "external": {"url": url}})
        return {prop_name: {prop_type: file_body}}

    def mulsel_prop_gen(self, prop_name, prop_type, prop_values):
        '''
        Generates a multi-select or relation property JSON object.
        '''
        prop_value_new = []
        for value in prop_values:
            prop_value_new.append({"name": value})
        return {prop_name: {prop_type: prop_value_new}}

    def relation_prop_gen(self, prop_name, prop_type, prop_values):
        '''
        Generates a relation property JSON object.
        '''
        prop_value_new = []
        for value in prop_values:
            prop_value_new.append({"id": value})
        return {prop_name: {prop_type: prop_value_new}}
    
    def people_prop_gen(self, prop_name, prop_type, prop_value):
        '''
        Generates a people property JSON object.
        '''
        prop_value_new = []
        for value in prop_value:
            prop_value_new.append({"object": "user","id": value})
        return {prop_name: {prop_type: prop_value_new}}
    
    def rich_text_prop_gen(self, prop_name, prop_type, prop_value, prop_value_link = None, annotation = None):
        '''
        Generates a rich text property JSON object.
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
        Generates a title property JSON object.
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

    def generate_property_body(self, prop_name, prop_type, prop_value, prop_value2 = None, annotation = None):
        '''
        Accepts a range of property types and generates a JSON object based on the input.
            Accepted property types is a string from the following list:
                "checkbox" | "email" | "number" | "phone_number" | "url" | "select" | "status" | "date" | "files" | "multi_select" | "relation" | "people" | "rich_text" | "title"
            Args:
                prop_name (string): The name of the property.
                prop_type (string): The type of the property.
                prop_value (string/number/bool/array of strings): The value of the property.
                prop_value2 (string/array of strings): The second value of the property. Optional.
                annotation (array of dict): The annotation of the property. Optional.
                    Dictionary format: [{"bold": bool, "italic": bool, "strikethrough": bool, "underline": bool, "code": bool, "color": string}]
                    default annotations: {"bold": False, "italic": False, "strikethrough": False, "underline": False, "code": False, "color": "default"}
                    Acceptable Colors: Colors: "blue", "blue_background", "brown", "brown_background", "default", "gray", "gray_background", "green", "green_background", "orange", "orange_background", "pink", "pink_background", "purple", "purple_background", "red", "red_background", "yellow", "yellow_background"
            Returns:
                dict: The JSON object of the property, formatted to fit as one of the properties in a page POST/PATCH request.

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
                Property Value Link: Array of strings
                Annotation: Array of dictionaries
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
        return json.dumps(type_dict[prop_type])

