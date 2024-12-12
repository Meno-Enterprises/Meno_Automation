#!/usr/bin/env python3

'''
Aria Corona - November 20th, 2024
acorona@menoenterprises.com

This script is designed to monitor and manage print jobs from Caldera to Notion. It performs the following tasks:
1. **Initialization and Configuration:**
    - Imports necessary libraries and modules.
    - Sets up constants and configurations, including printer IDs, IP addresses, URLs, and logging.
2. **Helper Functions:**
    - `parse_filename(filename)`: Parses a filename to extract a specific pattern defined by `ID_REGEX`.
    - `catch_value(page, key)`: Retrieves the value associated with a given key from a dictionary-like object.
    - `check_for_nest(name, device, nest_db_data)`: Checks for a nest in the Notion database data.
    - `check_id_list(caldera_list, notion_list, update_check)`: Compares lists of IDs from Caldera and Notion.
    - `relation_packer(id_list, prop_name, package)`: Packs a relation property into a given package.
    - `repacker(prop_name, full_package, partial_package)`: Updates a dictionary with a value from another dictionary.
    - `process_overlimit_relation(job_list, reprint_list, nest_db_id, package, nest_name)`: Processes job and reprint lists that exceed a certain limit.
    - `fix_list(list)`: Processes a list of strings or dictionaries to remove hyphens.
    - `filter_bad_objects(list, device)`: Filters out bad objects from a list based on the device.
    - `create_notion_page(caldata, jobs_list_to_send, reps_list_to_send)`: Creates a new page in Notion for a given Caldera nest.
    - `parse_input(caldata)`: Parses input data to extract job and report IDs from filenames.
    - `process_data(data, nest_db_data)`: Processes data from Caldera and compares it with the nest database data from Notion.
    - `getRequest(urlRequest)`: Sends a GET request to a specified URL and returns the response.
    - `putRequest(urlRequest, body)`: Sends a PUT request to a specified URL with a given body.
    - `check_inactive_printers()`: Checks the status of printers and updates their state if necessary.
    - `pullPush(urlPrinter, lastPull, nest_db_data)`: Pulls data from a specified URL and processes it if there is new data.
3. **Main Loop:**
    - Continuously monitors and processes print jobs from Caldera.
    - Checks the status of printers and updates their state if necessary.
    - Pulls data from Caldera and processes it if there is new data.
    - Logs information and handles garbage collection at regular intervals.
    - Stops the script within a specified time window.
The script uses the `NotionApiHelper` class to interact with the Notion API and the `cronitor` library for monitoring.
'''



import gc, requests, time, cronitor, re, logging, os
from NotionApiHelper import NotionApiHelper
from datetime import datetime

STOP_TIME = ('23:52:00', '23:54:59') # Time window to stop the script
ID_REGEX = r'^.*_(\w*)__\d*\.'
PULL_TIMER = 60  # seconds
WEBHOOK_URL = ''

# idPrinter1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1B'  # Epson A
ID_PRINTER_4_OLD = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1C'  # Epson B
ID_PRINTER_4 = 'YlMweWZWZFlJMkZ2Tm1GbWJtYy1hbXM2TnlRfkVwc29uLVN1cmVDb2xvci1GMTAwMDAtQg'
ID_PRINTER_1_OLD = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1E' # Epson D
ID_PRINTER_1 = 'YlMweWZWZFlJMkZ2Tm1GbWJtYy1hbXM2TnlRfkVwc29uLVN1cmVDb2xvci1GMTAwMDAtRA'
ID_PRINTER_3_OLD = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1G' # Epson F
ID_PRINTER_3 = 'YlMweWZWZFlJMkZ2Tm1GbWJtYy1hbXM2TnlRfkVwc29uLVN1cmVDb2xvci1GMTAwMDAtRg'
ID_PRINTER_2_OLD = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1F'   # Epson E
ID_PRINTER_2 = 'YlMweWZWZFlJMkZ2Tm1GbWJtYy1hbXM2TnlRfkVwc29uLVN1cmVDb2xvci1GMTAwMDAtRQ'
IP2 = '192.168.0.151'  # Master Dell PC IP
IP1 = '192.168.0.52' # Secondary Dell PC IP
# IP2 = '192.168.0.134' # Temporary PC for Epson F
# ip2 = '192.168.0.151'  # Alienware, currently not relevant

ACTIVE_PRINTERS = [(ID_PRINTER_1, IP1), (ID_PRINTER_2, IP1), (ID_PRINTER_3, IP1)]

URL_PRINTER_1 = 'http://' + IP1 + ':45344/v1/jobs?idents.device=' + ID_PRINTER_1 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished&limit=20'
URL_PRINTER_2 = 'http://' + IP1 + ':45344/v1/jobs?idents.device=' + ID_PRINTER_2 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished&limit=20'
URL_PRINTER_3 = 'http://' + IP1 + ':45344/v1/jobs?idents.device=' + ID_PRINTER_3 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished&limit=15'
URL_PRINTER_4 = 'http://' + IP1 + ':45344/v1/jobs?idents.device=' + ID_PRINTER_4 + '&name=Autonest*&sort=idents.internal' \
                                                                                 ':desc&state=finished&limit=15'
                                                                               
URL_DEVICE = 'http://#IPADDRESS#:45344/v1/devices/'                                                        

NEST_DB_ID = '36f1f2e349e147a69468af461c31ab00'
NEST_DB_FILTER = {"timestamp": "created_time", "created_time": {"past_week": {}}}
    
pullStore1 = {}
pullStore2 = {}  # If adding additional printers, add more of these variables.
pullStore3 = {}    
pullStore4 = {}
                                                                               
loopCount = 0

with open("conf/Cronitor_API_Key.txt") as file:
    cronitor_api_key = file.read()
cronitor.api_key = cronitor_api_key
MONITOR = cronitor.Monitor('wsoCXX')

LOG_DIR = "logs"
log_path = os.path.join(LOG_DIR, "CalderaPullPush.log")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

file_handler = logging.FileHandler(log_path)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

notion_helper = NotionApiHelper()


def parse_filename(filename):
    """
    Parses the given filename to extract a specific pattern defined by ID_REGEX.
    Args:
        filename (str): The filename to be parsed.
    Returns:
        str or None: The extracted pattern if found, otherwise None.
    """
    
    if filename is None:
        return None
    
    match = re.search(ID_REGEX, filename)
    if match:
        return match.group(1)
    
    return None

def catch_value(page, key):
    """
    Retrieve the value associated with a given key from a dictionary-like object.
    Args:
        page (dict): The dictionary-like object from which to retrieve the value.
        key: The key whose associated value is to be retrieved.
    Returns:
        The value associated with the specified key if it exists, otherwise None.
    """
    
    if page is None or key is None:
        return None
    
    if key in page:
        value = page[key]
        
    else:
        value = None
        
    return value

def check_for_nest(name, device, service, nest_db_data, old_printer_id):
    """
    Check for a nest in the Notion database data.
    This function iterates through each page in the provided Notion database data to find a nest
    that matches the given name and device ID. It also checks the creation date and print status
    of the nest to ensure it meets certain criteria.
    Args:
        name (str): The name of the nest to search for.
        device (str): The device ID associated with the nest.
        caldera_id (str): The Caldera ID associated with the nest.
        nest_db_data (list): A list of dictionaries representing the Notion database data.
    Returns:
        tuple: A tuple containing the following elements:
            - page_id (str or None): The ID of the page where the nest was found, or None if not found.
            - jobs_list (list or None): A list of jobs associated with the nest, or None if not found.
            - reprints_list (list or None): A list of reprints associated with the nest, or None if not found.
            - found (bool): A boolean indicating whether the nest was found (True) or not (False).
    """

    def _get_rest(url, id):
        new_response = notion_helper.get_url_property(url, id)
        
        if 'has_more' in new_response:
            if new_response['has_more']:
                new_response += _get_rest(new_response['property_item']['next_url'], id)
                
                if new_response.status_code != 200:
                    return None
                
                if 'results' in new_response:
                    return new_response['results']
                
        return new_response['results']
    
    def _get_results(property, page_id):
        response = notion_helper.get_page_property(page_id, property['id'])
        
        if response is None:
            return None
        
        results = response['results']
        
        if 'has_more' in response:
            if response['has_more']:
                results += _get_rest(response['property_item']['next_url'], property['id'])
        
        logging.debug(f"_get_results(): {results}")
        return results
        
    # Iterate through each page in the Notion database data.
    for page in nest_db_data:
        page_id = catch_value(page, 'id').replace('-', '')
        properties = catch_value(page, 'properties')
        
        # Check if the Name and Device ID properties exist in the page.
        if all(key in properties for key in ['Name', 'Device ID']):
            nest_name = notion_helper.return_property_value(properties['Name'], page_id)
            device_id = notion_helper.return_property_value(properties['Device ID'], page_id)
            service_id = notion_helper.return_property_value(properties['Software service ID'], page_id)
            created = page['created_time']
            print_status = notion_helper.return_property_value(properties['Print Status'], page_id)
            
            # Check if the nest was created more than a week ago.
            created_date = datetime.strptime(created, '%Y-%m-%dT%H:%M:%S.%fZ')
            if (datetime.now() - created_date).days > 4:
                continue
            
            if print_status in ['Canceled']:
                continue
            
            # Check if the nest name and device ID match the provided values.
            if nest_name == name and (device_id == device or device_id == old_printer_id) and service_id == service:
                
                # Check for the Jobs and Reprints properties in the page.
                if all(key in properties for key in ['Jobs', 'Reprints']):
                    jobs_list = _get_results(properties['Jobs'], page_id)
                    reprints_list = _get_results(properties['Reprints'], page_id)
                    
                    logger.info(f"check_for_nest(): Found Nest {name} in Notion.")
                    print(f"check_for_nest() - Jobs: {jobs_list}\nRep: {reprints_list}")
                    
                return page_id, jobs_list, reprints_list, True
        
    return None, None, None, False

def check_id_list(caldera_list, notion_list, update_check):
    if notion_list:
        logger.info(f"check_id_list(): Notion list exists.")
    else:
        logger.info(f"check_id_list(): Notion list does not exist.")
        notion_list = []

    if caldera_list:
        logger.info(f"check_id_list(): Caldera list exists.")
        
        for id in caldera_list:
            if id in notion_list:
                return caldera_list, False
        
        return caldera_list, True
               
    return caldera_list, update_check
            
def relation_packer(id_list, prop_name, package):
    """
    Packs a relation property into a given package.
    This function generates a relation property using the provided `id_list` and `prop_name`, 
    and then adds this property to the `package`. If `id_list` is empty, it adds an empty list 
    to the `package` under the `prop_name` key.
    Args:
        id_list (list): A list of IDs to be used in generating the relation property.
        prop_name (str): The name of the property to be added to the package.
        package (dict): The package (dictionary) to which the property will be added.
    Returns:
        dict: The updated package with the new relation property added.
    """
    
    if id_list:
        prop = notion_helper.relation_prop_gen(prop_name, "relation", id_list)
        logging.info(f"relation_packer(): {prop}")
        package[prop_name] = prop[prop_name]

    else:
        package[prop_name] = []
        
    return package

def repacker(prop_name, full_package, partial_package):
    """
    Updates the full_package dictionary with the value of the specified property from the partial_package dictionary.
    Args:
        prop_name (str): The name of the property to be updated.
        full_package (dict): The dictionary to be updated.
        partial_package (dict): The dictionary containing the new value for the property.
    Returns:
        dict: The updated full_package dictionary.
    """
    
    full_package[prop_name] = partial_package[prop_name]
    return full_package
    
def process_overlimit_relation(job_list, reprint_list, nest_db_id, package, nest_name):
    """
    Processes job and reprint lists that exceed a certain limit by creating and updating packages in batches.
    Args:
        job_list (list): List of job items to be processed.
        reprint_list (list): List of reprint items to be processed.
        nest_db_id (str): The database ID where the package will be updated.
        package (dict): The initial package dictionary to be updated with job and reprint relations.
        nest_name (str): The name of the nest to be used in the package name.
    Returns:
        None
    """
    
    job_list_len = len(job_list)
    reprint_list_len = len(reprint_list)
    
    # Determine the maximum list size to process.
    max_list_size = job_list_len if job_list_len >= reprint_list_len else reprint_list_len
    logging.info(f"process_overlimit_relation()\n Max List Size: {max_list_size}, Job List: {job_list_len}, Reprint List: {reprint_list_len}")
    
    # Process the lists in batches of 100 items. Creates a new notion page for each batch.
    for i in range(100, max_list_size, 100):
        
        time.sleep(.5)

        index = (i/100)+1
        package['Name'] = notion_helper.rich_text_prop_gen('Name', "rich_text", [f"{nest_name}-{str(index)}"])['Name']
        
        # Remove the Jobs and Reprints properties from the package.
        if 'Jobs' in package:
            package.pop('Jobs')
        if 'Reprints' in package:
            package.pop('Reprints')
        
        if i + 100 > job_list_len:
            jobs_list_to_send = job_list[i:]
        else:
            jobs_list_to_send = job_list[i:i+100]
            
        if i + 100 > reprint_list_len:
            reps_list_to_send = reprint_list[i:]
        else:
            reps_list_to_send = reprint_list[i:i+100]
        
        if jobs_list_to_send:
            package = relation_packer(jobs_list_to_send, 'Jobs', package)
        if reps_list_to_send:
            package = relation_packer(reps_list_to_send, 'Reprints', package)
            
        logging.info(f"Package: {package}")
        response = notion_helper.create_page(nest_db_id, package)
        logger.info(f"Updated Nest {nest_db_id} with additional relations.")
    pass

def fix_list(list):
    """
    Processes a list of strings or dictionaries containing a specific key, 
    removing hyphens from the strings or the values associated with the key.
    Args:
        list (list): A list of strings or dictionaries. If the list contains 
                     dictionaries, each dictionary must have a key 'relation' 
                     with a value that is a string.
    Returns:
        list: A new list with hyphens removed from each string or from the 
              values associated with the 'relation' key in each dictionary.
    Example:
        >>> fix_list(['a-b-c', 'd-e-f'])
        ['abc', 'def']
        >>> fix_list([{'relation': 'a-b-c'}, {'relation': 'd-e-f'}])
        ['abc', 'def']
    """
    
    print(f"fix_list() - List: {list}")
    
    if not list:
        return []

    elif any('relation' in item for item in list):
        unpacked_list = []
        
        for relation in list:
            unpacked_list.append(relation['id'].replace('-', ''))
            
        return unpacked_list
    
    else:
        fixed_list = []
        
        for item in list:
            fixed_list.append(item.replace('-', ''))
        
        return fixed_list

def filter_bad_objects(list, device):
    """
    Filters out bad objects from the given list based on the device and certain conditions.
    Args:
        list (list): The list of objects to be filtered.
        device (str): The device identifier which determines the filtering criteria.
    Returns:
        list: The filtered list with bad objects removed.
    The function performs the following checks for each item in the list:
    1. If the item does not have associated data, it is removed from the list.
    2. If the item does not contain 'properties', it is removed from the list.
    3. If the 'Hot folder path' property is present:
       - If the path is 'BroadPillow', 'SuedePillow', or 'BroadRunner' and the device is ID_PRINTER_1, the item is removed.
       - If the path is not one of the above and the device is ID_PRINTER_2, the item is removed.
    """
    
    
    if list:
        for item in list:
            data = notion_helper.get_page(item)
            
            if not data:
                list.remove(item)
                continue
            
            if 'properties' not in data:
                list.remove(item)
                continue
            
            if 'Hot folder path' in data['properties']:
                if data['properties']['Hot folder path'] in ['BroadPillow', 'SuedePillow', 'BroadRunner']:
                    if device == ID_PRINTER_1: # If the device is Epson D, remove the item.
                        list.remove(item)
                else: # If the hot folder path is not one of the above
                    if device == ID_PRINTER_2: # If the device is Epson E, remove the item.
                        list.remove(item)
                        
    return list                   

def create_notion_page(caldata, jobs_list_to_send, reps_list_to_send):
    """
    Creates a new page in Notion for a given Caldera nest.
    Args:
        caldata (dict): A dictionary containing data about the Caldera nest.
            Expected keys:
                - 'name': The name of the nest.
                - 'idents_internal': Internal identifier for the Caldera.
                - 'idents_service': Service identifier for the Caldera.
                - 'device': Device ID associated with the Caldera.
                - 'nest_id': Identifier for the Caldera nest.
                - 'creation': Creation time of the nest.
        jobs_list_to_send (list): A list of job objects to be related to the Notion page.
        reps_list_to_send (list): A list of reprint objects to be related to the Notion page.
    Returns:
        None
    Raises:
        Any exceptions raised by the Notion API or helper functions will propagate.
    Notes:
        - Filters out bad objects from the jobs and reprints lists based on the device.
        - If the jobs or reprints lists exceed 100 items, they are split into multiple packages.
        - Adds various properties to the Notion page package.
        - Creates the Notion page and logs the creation.
        - If the relation lists are oversized, processes the remaining items.
    """
    
    
    oversized_relation = False
    logger.info(f"Creating package for Nest {caldata['name']}.")
    package = {}
    
    # Filter out bad objects from the lists. Ie. pillows on the piecegoods printer.
    #for each in [jobs_list_to_send, reps_list_to_send]:
    #    if each:
    #        each = filter_bad_objects(each, caldata['device'])
    
    # If the relation lists are over 100 items, split them into multiple packages.
    if len(jobs_list_to_send) >= 100 or len(reps_list_to_send) >= 100:
        oversized_relation = True
        whole_jobs_list = jobs_list_to_send.copy()
        whole_reps_list = reps_list_to_send.copy()
        jobs_list_to_send = whole_jobs_list[:100]
        reps_list_to_send = whole_reps_list[:100]
    
    # Add the relation properties to the package.
    if jobs_list_to_send:
        package = relation_packer(jobs_list_to_send, 'Jobs', package)
    if reps_list_to_send:
        package = relation_packer(reps_list_to_send, 'Reprints', package)
    
    # Add the remaining properties to the package.
    package = repacker('Caldera ID', package, notion_helper.rich_text_prop_gen('Caldera ID', "rich_text", [caldata['idents_internal']]))
    package = repacker('Software service ID', package, 
                        notion_helper.rich_text_prop_gen('Software service ID', "rich_text", [caldata['idents_service']]))
    package = repacker('Device ID', package, notion_helper.rich_text_prop_gen('Device ID', "rich_text", [caldata['device']]))
    package = repacker('System status', package, notion_helper.selstat_prop_gen('System status', "select", 'Active'))
    package = repacker('Caldera Nest ID', package, notion_helper.rich_text_prop_gen('Caldera Nest ID', "rich_text", [caldata['nest_id']]))
    package = repacker('Print Status', package, notion_helper.selstat_prop_gen('Print Status', "select", 'Queued'))
    package = repacker('Name', package, notion_helper.rich_text_prop_gen('Name', "rich_text", [caldata['name']]))
    package = repacker('Nest Creation Time', package, notion_helper.rich_text_prop_gen('Nest Creation Time', "rich_text", [str(caldata['creation'])]))
    
    # The nest does not exist in Notion, create a new page.
    print(package)
    response = notion_helper.create_page(NEST_DB_ID, package)
    logger.info(f"Created Nest {caldata['name']}")
        
    # If the relation lists are over 100 items, split them into multiple packages.
    if oversized_relation:
        process_overlimit_relation(whole_jobs_list, whole_reps_list, NEST_DB_ID, package, caldata['name'])

def parse_input(input_list):
    """
    Parses the input data to extract job and report IDs from filenames.
    Args:
        caldata (dict): A dictionary containing input data with filenames.
    Returns:
        tuple: A tuple containing two lists:
            - caldera_job_id_list (list): A list of job IDs extracted from filenames containing 'JOB-'.
            - caldera_rep_id_list (list): A list of report IDs extracted from filenames containing 'REP-'.
    """
    caldera_job_id_list = []
    caldera_rep_id_list = []
    for rip_file in input_list:
        file = catch_value(rip_file, 'file')
        db_id = parse_filename(file)
        
        if db_id:
            db_id = db_id.replace('-', '')
            if 'REP-' in file:
                caldera_rep_id_list.append(db_id)
            if 'JOB-' in file:
                caldera_job_id_list.append(db_id)
                
    return caldera_job_id_list, caldera_rep_id_list

def process_data(data, nest_db_data, old_printer_id):
    """
    Processes the data from Caldera and compares it with the nest database data from Notion.
    Args:
        data (list): The data retrieved from Caldera, expected to be a list of nests.
        nest_db_data (dict): The nest database data retrieved from Notion.
    Returns:
        None
    The function performs the following steps:
    1. Checks if the input data or nest database data is None and logs an appropriate message.
    2. Iterates through each nest in the Caldera data.
    3. Skips nests that do not contain 'Autonest' in their name.
    4. Extracts relevant information from each nest and stores it in a dictionary.
    5. Checks if the nest exists in the Notion database and retrieves related job and reprint lists.
    6. Removes hyphens from the IDs in the retrieved lists.
    7. Parses the input data to create lists of job and reprint IDs.
    8. Continues to the next nest if both job and reprint lists are empty.
    9. Logs the status of the nest in Notion and continues if the nest exists and matches the internal data.
    10. If the nest does not exist in Notion, prepares the data to create a new page in Notion and logs the details.
    """

    # Early exit if the query returns None for some reason.
    if nest_db_data is None:
        logger.info("No nest database data returned from Notion.")
        return
    
    if data is None:
        logger.info("No data returned from Caldera.")
        return
    
    # Iterate through each page in the Caldera data.
    for nest in data:
        name = catch_value(nest, 'name')
        
        # Skip if the name does not contain 'Autonest'.
        if 'Autonest' not in name:
            logger.info("{name} is not a nest. Continuing.")
            continue
        
        # Initialize variables for each nest.
        caldata = {}
        caldata['name'] = name
        caldata['form'] = catch_value(nest, 'form')
        caldata['origin'] = catch_value(caldata['form'], 'origin')
        caldata['input'] = catch_value(caldata['origin'], 'input')
        caldata['idents'] = catch_value(nest, 'idents')
        caldata['idents_internal'] = catch_value(caldata['idents'], 'internal')
        caldata['idents_service'] = catch_value(caldata['idents'], 'service')
        caldata['device'] = catch_value(caldata['idents'], 'device')
        caldata['nest_id'] = catch_value(nest, 'id')
        caldata['evolution'] = catch_value(caldata['form'], 'evolution')
        caldata['creation'] = catch_value(caldata['evolution'], 'creation')
        
        # Checks notion for nest data
        nest_page_id, nest_notion_jobs_list, nest_notion_reprints_list, matches_internal = check_for_nest(name, caldata['device'], caldata['idents_service'], nest_db_data, old_printer_id)
        
        # Remove the hyphens from the IDs in the lists.
        nest_notion_jobs_list = fix_list(nest_notion_jobs_list)
        nest_notion_reprints_list = fix_list(nest_notion_reprints_list)
        
        # Create lists of database IDs from the filenames in the Nest.
        caldera_job_id_list, caldera_rep_id_list = parse_input(caldata['input'])
        
        # Continue the loop of both lists in Caldera are empty.
        if all(not each for each in [caldera_job_id_list, caldera_rep_id_list]):
            logger.info(f"No files found for Nest {name}.\n")
            continue

        # Nest exists in Notion, checks that all the job relations have been added to the nest in Notion.
        logger.info(f"ID: {bool(nest_page_id)}, Jobs: {bool(nest_notion_jobs_list)}, Reprints: {bool(nest_notion_reprints_list)}, Matches: {matches_internal}")
        if nest_page_id and (nest_notion_jobs_list or nest_notion_reprints_list) and matches_internal:
            logger.info(f"Nest {name} exists in Notion.\n")
            continue
        
        # Nest does not exist in Notion, set variables to create a new page.
        else:
            logger.info(f"Nest {name} does not exist in Notion.")
            
            logger.info(f" {name}\nJobs: {caldera_job_id_list}\nReprints: {caldera_rep_id_list}\n")
            create_notion_page(caldata, caldera_job_id_list, caldera_rep_id_list)
        
    print("\n")

def getRequest(urlRequest):
    """
    Sends a GET request to the specified URL and returns the response.
    Args:
        urlRequest (str): The URL to which the GET request is sent.
    Returns:
        requests.Response: The response object from the GET request if successful.
        None: If an exception occurs during the request.
    Logs:
        Logs the status code of the response if the request is successful.
        Logs the exception message if an error occurs.
    """
    
    getReq = []
    
    try:
        getReq = requests.get(urlRequest)
        logger.info(f"getRequest(): {getReq.status_code}, {urlRequest}")
        getReq.raise_for_status()
    except Exception as e:
        logger.error(f'getRequest(): {e}')
        return None
    
    return getReq

def putRequest(urlRequest, body):
    """
    Sends a PUT request to the specified URL with the given body.
    Args:
        urlRequest (str): The URL to which the PUT request is sent.
        body (dict): The JSON body to be sent with the PUT request.
    Returns:
        requests.Response: The response object from the PUT request if successful.
        None: If an exception occurs during the request.
    Logs:
        Logs the status code of the response if the request is successful.
        Logs the exception message if an error occurs.
    """
    
    putReq = []
    
    try:
        putReq = requests.put(urlRequest, json=body)
        logger.info(f"putRequest(): {putReq.status_code}")
        putReq.raise_for_status()
    except Exception as e:
        logger.error(f'putRequest(): {e}')

def check_inactive_printers():
    """
    Checks the status of printers in the ACTIVE_PRINTERS list. For each printer, it sends a GET request to the 
    corresponding endpoint to retrieve its status. If the printer is not running, it logs an info message and 
    sends a PUT request to change the printer's state to 'running'.
    The function performs the following steps:
    1. Iterates over each printer in the ACTIVE_PRINTERS list.
    2. Replaces the placeholder in the URL_DEVICE with the printer's IP address.
    3. Sends a GET request to the endpoint to get the printer's status.
    4. Checks if the response contains an 'id' that matches the printer's ID.
    5. If the printer's status is not 'running', logs an info message and sends a PUT request to update the printer's state.
    Note:
    - ACTIVE_PRINTERS is expected to be a list of tuples, where each tuple contains the printer's ID and IP address.
    - URL_DEVICE is a string containing the endpoint URL with a placeholder for the IP address.
    - getRequest and putRequest are functions used to send GET and PUT requests, respectively.
    - logger is used to log information messages.
    """
    
    for printer in ACTIVE_PRINTERS:
        endpoint = URL_DEVICE.replace("#IPADDRESS#", printer[1])
        response = getRequest(endpoint)
        
        if response is None:
            continue
        
        for each in response.json():
            if 'id' in each:
                if each['id'] == printer[0]:
                    if each['state'] != 'running':
                        logger.info(f"Starting printer {printer[0]}.")
                        #putRequest(f"{endpoint}/{printer[0]}/state", "running")
                    else:
                        logger.info(f"Printer {printer[0]}: State: {each['state']}")
    

def pullPush(urlPrinter, lastPull, nest_db_data, old_printer_id):
    """
    Pulls data from a specified URL and processes it if there is new data.
    Args:
        urlPrinter (str): The URL to pull data from.
        lastPull (dict): The last pulled data to compare with the new data.
        nest_db_data (dict): The database data to be used in processing.
    Returns:
        dict: The latest pulled data from the URL.
    """
    
    caldera_response = getRequest(urlPrinter)
    
    if (caldera_response == []) or (caldera_response is None):
        return lastPull
    
    spoolerJson = caldera_response.json()
    
    if lastPull != spoolerJson:
        logger.info("New Caldera data detected, processing...")
        process_data(spoolerJson, nest_db_data, old_printer_id)
        
    else:
        logger.info("No data change.")
        
    return spoolerJson



MONITOR.ping(state='run')
gc.enable()

while True:
    nest_db_data = notion_helper.query(NEST_DB_ID, content_filter=NEST_DB_FILTER)
    check_inactive_printers()
    
    logger.info(f"Getting Data: {ID_PRINTER_2}")
    pullStore2 = pullPush(URL_PRINTER_2, pullStore2, nest_db_data, ID_PRINTER_2_OLD) 
    logger.info(f"Getting Data: {ID_PRINTER_1}")
    pullStore1 = pullPush(URL_PRINTER_1, pullStore1, nest_db_data, ID_PRINTER_1_OLD)
    logger.info(f"Getting Data: {ID_PRINTER_3}")
    pullStore3 = pullPush(URL_PRINTER_3, pullStore3, nest_db_data, ID_PRINTER_3_OLD)
    logger.info(f"Getting Data: {ID_PRINTER_4}")
    pullStore4 = pullPush(URL_PRINTER_4, pullStore4, nest_db_data, ID_PRINTER_4_OLD)

    time.sleep(PULL_TIMER)
    loopCount += 1
    
    if loopCount % 5 == 0:
        print(f"pong at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        MONITOR.ping()
        
    if loopCount % 180 == 0:
        gc.collect()
        logger.info(str(datetime.now().strftime("%H:%M:%S")) + " - It's trash day.\n")
        
    now = time.strftime('%H:%M:%S')
    
    if STOP_TIME[1] >= now and now >= STOP_TIME[0]:
        logger.info("Time is within the stop window. Stopping the observer.")
        MONITOR.ping(state='complete')
        break

MONITOR.ping(state='complete')
