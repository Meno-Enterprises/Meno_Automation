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
        dict: The "results" of the JSON response from the Notion API. This will cut out the pagination information, returning only the page data.

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