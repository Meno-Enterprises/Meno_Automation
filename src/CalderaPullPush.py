#!/usr/bin/env python3
'''
Aria Corona
Nov. 8th, 2024

CalderaPullPush.py
This script is designed to interact with the Caldera RIP software and Notion API to manage and update print job data.
It periodically pulls data from Caldera, processes it, and updates or creates corresponding entries in Notion.
Modules:
    gc: Provides automatic garbage collection.
    requests: Allows sending HTTP requests.
    time: Provides various time-related functions.
    cronitor: Monitors the script's execution.
    re: Provides regular expression matching operations.
    logging: Provides a flexible framework for emitting log messages.
    NotionApiHelper: Custom module to interact with Notion API.
    datetime: Supplies classes for manipulating dates and times.
Constants:
    STOP_TIME (tuple): Time window to stop the script.
    ID_REGEX (str): Regular expression pattern to extract IDs from filenames.
    PULL_TIMER (int): Interval in seconds between data pulls.
    WEBHOOK_URL (str): URL for webhook notifications.
    ID_PRINTER_1, ID_PRINTER_2, ID_PRINTER_3 (str): Printer IDs.
    IP1, IP2 (str): IP addresses of the PCs.
    URL_PRINTER_1, URL_PRINTER_2, URL_PRINTER_3 (str): URLs to pull data from Caldera.
    NEST_DB_ID (str): Notion database ID for nests.
    NEST_DB_FILTER (dict): Filter for querying the Notion database.
Variables:
    pullStore1, pullStore2, pullStore3 (dict): Stores the last pulled data for each printer.
    loopCount (int): Counter for the main loop.
Functions:
    parse_filename(filename): Parses the given filename to extract a specific pattern defined by ID_REGEX.
    catch_value(page, key): Retrieves the value associated with a given key from a dictionary-like object.
    check_for_nest(name, device, nest_db_data): Checks for a nest in the provided Notion database data.
    check_id_list(caldera_list, notion_list, update_check): Compares two lists of IDs and updates the check list and update flag.
    relation_packer(id_list, prop_name, package): Packs a relation property into a given package.
    repacker(prop_name, full_package, partial_package): Updates the full_package dictionary with the value of the specified property from the partial_package dictionary.
    process_data(data, nest_db_data): Processes the given Caldera data and updates or creates corresponding entries in Notion.
    getRequest(urlRequest): Sends a GET request to the specified URL and returns the response.
    pullPush(urlPrinter, lastPull, nest_db_data): Pulls data from a specified URL and processes it if there is new data.
Execution:
    The script initializes the Cronitor monitor, enables garbage collection, and enters an infinite loop.
    In each iteration, it pulls data from Caldera for each printer, processes the data, and updates Notion.
    It also performs periodic garbage collection and checks if the current time is within the stop window to terminate the script.
'''


import gc, requests, time, cronitor, re, logging
from NotionApiHelper import NotionApiHelper
from datetime import datetime

STOP_TIME = ('23:52:00', '23:54:59') # Time window to stop the script
ID_REGEX = r'^.*_(\w*)__\d*\.'
PULL_TIMER = 60  # seconds
WEBHOOK_URL = ''

# idPrinter1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1B'  # Epson A
# idPrinter2 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1C'  # Epson B
ID_PRINTER_1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1E' # Epson D
ID_PRINTER_2 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1F' # Epson E
ID_PRINTER_3 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1G' # Epson F

# ip1 = '192.168.0.39'  # Master Dell PC IP
IP1 = '192.168.0.111' # Secondary Dell PC IP
IP2 = '192.168.0.134' # Temporary PC for Epson F
# ip2 = '192.168.0.151'  # Alienware, currently not relevant

ACTIVE_PRINTERS = [(ID_PRINTER_1, IP1), (ID_PRINTER_2, IP1), (ID_PRINTER_3, IP2)]

URL_PRINTER_1 = 'http://' + IP1 + ':12340/v1/jobs?idents.device=' + ID_PRINTER_1 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished&limit=15'
URL_PRINTER_2 = 'http://' + IP1 + ':12340/v1/jobs?idents.device=' + ID_PRINTER_2 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished&limit=15'
URL_PRINTER_3 = 'http://' + IP2 + ':12340/v1/jobs?idents.device=' + ID_PRINTER_3 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished&limit=15'
                                                                               
URL_DEVICE = 'http://#IPADDRESS#:12340/v1/devices/'                                                        

NEST_DB_ID = '36f1f2e349e147a69468af461c31ab00'
NEST_DB_FILTER = {"timestamp": "created_time", "created_time": {"past_week": {}}}
    
pullStore1 = {}
pullStore2 = {}  # If adding additional printers, add more of these variables.
pullStore3 = {}    
                                                                               
loopCount = 0

with open("conf/Cronitor_API_Key.txt") as file:
    cronitor_api_key = file.read()
cronitor.api_key = cronitor_api_key
MONITOR = cronitor.Monitor('Debian10C104 MOD-Caldera API Listener')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def check_for_nest(name, device, creation, nest_db_data):
    """
    Checks for a nest in the provided Notion database data.
    Args:
        name (str): The name of the nest to search for.
        device (str): The device ID associated with the nest.
        nest_db_data (list): A list of dictionaries representing the Notion database data.
    Returns:
        tuple: A tuple containing the page ID, jobs list, and reprints list if the nest is found.
               Returns (None, None, None) if the nest is not found.
    """
    
    for page in nest_db_data:
        page_id = catch_value(page, 'id').replace('-', '')
        properties = catch_value(page, 'properties')
        
        
        if all(key in properties for key in ['Name', 'Device ID', 'Nest Creation Time']):
            nest_name = notion_helper.return_property_value(properties['Name'], page_id)
            device_id = notion_helper.return_property_value(properties['Device ID'], page_id)
            notion_nest_creation_time = notion_helper.return_property_value(properties['Nest Creation Time'], page_id)
            
            if nest_name == name and device_id == device and notion_nest_creation_time == creation:
                if all(key in properties for key in ['Jobs', 'Reprints']):
                    jobs_list = notion_helper.return_property_value(properties['Jobs'], page_id)
                    reprints_list = notion_helper.return_property_value(properties['Reprints'], page_id)
                    logger.info(f"check_for_nest(): Found Nest {name} in Notion.")
                return page_id, jobs_list, reprints_list, True
        
    return None, None, None, False

def check_id_list(caldera_list, notion_list, update_check):
    """
    Compares two lists of IDs and updates the check list and update flag.
    This function takes two lists of IDs, `caldera_list` and `notion_list`, and checks if there are any IDs in 
    `caldera_list` that are not present in `notion_list`. If such IDs are found, they are added to the `check_list` 
    and the `update_check` flag is set to True.
    Args:
        caldera_list (list): List of IDs from Caldera.
        notion_list (list): List of IDs from Notion.
        update_check (bool): A flag indicating whether an update is needed.
    Returns:
        tuple: A tuple containing the updated check list and the update flag.
    """
    
    check_list = []
    
    if notion_list:
        logger.info(f"check_id_list(): Notion list exists.")
        check_list = notion_list.copy()
        
    if caldera_list:
        logger.info(f"check_id_list(): Caldera list exists.")
        
        for id in caldera_list:
            if id not in notion_list:
                check_list.append(id)
                update_check = True
               
    logger.info(f"check_id_list(): Returning {check_list}")
    
    return check_list, update_check
            
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
    
def process_overlimit_relation(job_list, reprint_list, page_id):
    job_list_len = len(job_list)
    reprint_list_len = len(reprint_list)
    
    max_list_size = job_list_len if job_list_len >= reprint_list_len else reprint_list_len
    
    for i in range(100, max_list_size, 100):
        if i + 100 > job_list_len:
            jobs_list_to_send = job_list[i:]
        else:
            jobs_list_to_send = job_list[i:i+100]
            
        if i + 100 > reprint_list_len:
            reps_list_to_send = reprint_list[i:]
        else:
            reps_list_to_send = reprint_list[i:i+100]
        
        package = {}
        
        if jobs_list_to_send:
            package = relation_packer(jobs_list_to_send, 'Jobs', package)
        if reps_list_to_send:
            package = relation_packer(reps_list_to_send, 'Reprints', package)
        
        response = notion_helper.update_page(page_id, package)
        logger.logging(f"Updated Nest {page_id} with additional relations.\nStatus Code: {response['status_code']}")
    pass

def process_data(data, nest_db_data):
    """
    Processes the given Caldera data and updates or creates corresponding entries in Notion.
    Args:
        data (list): A list of dictionaries containing Caldera data.
        nest_db_data (dict): A dictionary containing nest database data from Notion.
    Returns:
        None
    The function performs the following steps:
    1. Checks if the nest database data is None and logs an error if so.
    2. Iterates through each nest in the Caldera data.
    3. Skips nests that do not contain 'Autonest' in their name.
    4. Initializes variables for each nest.
    5. Checks Notion for existing nest data.
    6. Creates lists of database IDs from the filenames in the nest.
    7. Compares the job and reprint lists with those in Notion and sets flags for updating or creating a page.
    8. Creates a JSON package to send to Notion.
    9. Updates or creates a Notion page based on the flags.
    10. Logs the actions taken.
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
        caldera_job_id_list = []
        caldera_rep_id_list = []
        form = catch_value(nest, 'form')
        origin = catch_value(form, 'origin')
        input = catch_value(origin, 'input')
        idents = catch_value(nest, 'idents')
        idents_internal = catch_value(idents, 'internal')
        idents_service = catch_value(idents, 'service')
        device = catch_value(idents, 'device')
        caldera_nest_id = catch_value(nest, 'id')
        evolution = catch_value(form, 'evolution')
        creation = catch_value(evolution, 'creation')
        
        # Checks notion for nest data
        nest_notion_id, nest_notion_jobs_list, nest_notion_reprints_list, has_creation = check_for_nest(name, device, str(creation), nest_db_data)
        
        # Create a list of database IDs from the filenames in the Nest.
        for rip_file in input:
            file = catch_value(rip_file, 'file')
            db_id = parse_filename(file)
            
            if db_id:
                if 'REP-' in file:
                    caldera_rep_id_list.append(db_id)
                if 'JOB-' in file:
                    caldera_job_id_list.append(db_id)

        # Nest exists in Notion, checks that all the job relations have been added to the nest in Notion.
        if nest_notion_id and (nest_notion_jobs_list or nest_notion_reprints_list) and has_creation:
            logger.info(f"Nest {name} exists in Notion, comparing ID lists.")
            create_notion_page = False
            update_notion_page = False
            
            # Check if the job and reprint nest lists match the lists in Notion. Changes the update flag if needed.
            jobs_list_to_send, update_notion_page = check_id_list(caldera_job_id_list, nest_notion_jobs_list, update_notion_page)
            reps_list_to_send, update_notion_page = check_id_list(caldera_rep_id_list, nest_notion_reprints_list, update_notion_page)
        
        # Nest does not exist in Notion, set variables to create a new page.
        else:
            logger.info(f"Nest {name} does not exist in Notion.")
            create_notion_page = True
            update_notion_page = False
            jobs_list_to_send = caldera_job_id_list
            reps_list_to_send = caldera_rep_id_list
          
        # Create the json body to send to Notion.
        if update_notion_page or create_notion_page:
            oversided_relation = False
            logger.info(f"Creating package for Nest {name}.")
            package = {}
            
            if len(jobs_list_to_send) > 100 or len(reps_list_to_send) > 100:
                oversides_relation = True
                whole_jobs_list = jobs_list_to_send.copy()
                whole_reps_list = reps_list_to_send.copy()
                jobs_list_to_send = whole_jobs_list[:100]
                reps_list_to_send = whole_reps_list[:100]
            
            if jobs_list_to_send:
                package = relation_packer(jobs_list_to_send, 'Jobs', package)
            if reps_list_to_send:
                package = relation_packer(reps_list_to_send, 'Reprints', package)
                
            package = repacker('Nest', package, notion_helper.title_prop_gen('Nest', "title", [name]))
            package = repacker('Caldera ID', package, notion_helper.rich_text_prop_gen('Caldera ID', "rich_text", [idents_internal]))
            package = repacker('Software service ID', package, 
                               notion_helper.rich_text_prop_gen('Software service ID', "rich_text", [idents_service]))
            package = repacker('Device ID', package, notion_helper.rich_text_prop_gen('Device ID', "rich_text", [device]))
            package = repacker('System status', package, notion_helper.selstat_prop_gen('System status', "select", 'Active'))
            package = repacker('Caldera Nest ID', package, notion_helper.rich_text_prop_gen('Caldera Nest ID', "rich_text", [caldera_nest_id]))
            package = repacker('Print Status', package, notion_helper.selstat_prop_gen('Print Status', "select", 'Queued'))
            package = repacker('Name', package, notion_helper.rich_text_prop_gen('Name', "rich_text", [name]))
            package = repacker('Nest Creation Time', package, notion_helper.rich_text_prop_gen('Nest Creation Time', "rich_text", [str(creation)]))
            
            # The nest exists in Notion, update the page.
            if update_notion_page:
                response = notion_helper.update_page(nest_notion_id, package)
                logger.info(f"Updated Nest {name}: {nest_notion_id}")
            
            # The nest does not exist in Notion, create a new page.
            else:
                print(package)
                response = notion_helper.create_page(NEST_DB_ID, package)
                logger.info(f"Created Nest {name}")
                
            if oversided_relation:
                process_overlimit_relation(whole_jobs_list, whole_reps_list, response['id'])
                    
        else:
            logger.info(f"Nest {name} is already in Notion.")
            continue # No Action Needed
        
    print("\n")
                
# Do I add a way to check if a nest has already been processed recently without querying Notion?

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
                        putRequest(f"{endpoint}/{printer[0]}/state", "running")
                    else:
                        logger.info(f"Printer {printer[0]}: State: {each['state']}")
    

def pullPush(urlPrinter, lastPull, nest_db_data):
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
        process_data(spoolerJson, nest_db_data)
        
    else:
        logger.info("No data change.")
        
    return spoolerJson


MONITOR.ping(state='run')
gc.enable()

while True:
    nest_db_data = notion_helper.query(NEST_DB_ID, content_filter=NEST_DB_FILTER)
    check_inactive_printers()
    
    logger.info(f"Getting Data: {ID_PRINTER_2}")
    pullStore2 = pullPush(URL_PRINTER_2, pullStore2, nest_db_data) 
    logger.info(f"Getting Data: {ID_PRINTER_1}")
    pullStore1 = pullPush(URL_PRINTER_1, pullStore1, nest_db_data)
    logger.info(f"Getting Data: {ID_PRINTER_3}")
    pullStore3 = pullPush(URL_PRINTER_3, pullStore3, nest_db_data)

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
