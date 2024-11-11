#!/usr/bin/env python3

"""
Aria Corona - November 11, 2024

This script checks for stalled jobs in a Notion database and triggers webhooks or updates job statuses accordingly.
Modules:
    NotionApiHelper: A helper module for interacting with the Notion API.
    cronitor: A module for monitoring cron jobs.
    logging: A module for logging messages.
    json: A module for working with JSON data.
    requests: A module for making HTTP requests.
Constants:
    JOB_DB_ID (str): The ID of the Notion job database.
    JOB_DB_FILTER_LOCATION (str): The file path to the JSON filter for querying the job database.
    JOB_ID_STORAGE_LOCATION (str): The file path to store job IDs.
    MAKE_WEBHOOK_URL (str): The URL for the webhook to be triggered.
    CRONITOR_KEY_LOCATION (str): The file path to the Cronitor API key.
Functions:
    save_file(job_ids):
        Saves the job IDs to a JSON file.
    load_file():
        Loads the job IDs from a JSON file.
    get_job_data():
        Queries the Notion job database and returns the job data.
    format_job_file(job_data):
        Formats the job data into a dictionary for storage.
    compare_job_data(job_data, job_file):
        Compares the current job data with the stored job data and triggers webhooks or updates job statuses as needed.
    main():
        The main function that orchestrates the job checking process.
Execution:
    The script initializes the Notion API helper, logger, and Cronitor monitor. It then runs the main function and pings the Cronitor monitor to indicate the start and completion of the job checking process.
"""




from NotionApiHelper import NotionApiHelper
import cronitor, logging, json, requests

JOB_DB_ID = "f11c954da24143acb6e2bf0254b64079"
JOB_DB_FILTER_LOCATION = "conf/MOD_Check_Stalled_Jobs_Filter.json"
JOB_ID_STORAGE_LOCATION = "storage/MOD_Check_Stalled_Jobs_JobIDs.json"
MAKE_WEBHOOK_URL = ""
CRONITOR_KEY_LOCATION = "conf/Cronitor_API_Key.txt"

# Load the filter for the job database
with open(JOB_DB_FILTER_LOCATION) as f:
    JOB_DB_FILTER = json.load(f)

# Initialize the Notion API helper
notion = NotionApiHelper()

# Initialize the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load the Cronitor API key and initialize the monitor
with open(CRONITOR_KEY_LOCATION) as file:
    cronitor_api_key = file.read()
cronitor.api_key = cronitor_api_key
MONITOR = cronitor.Monitor('Debian10C104 MOD-Caldera API Listener')

def save_file(job_ids):
    with open(JOB_ID_STORAGE_LOCATION, "w") as f:
        json.dump(job_ids, f)
        
def load_file():
    try:
        with open(JOB_ID_STORAGE_LOCATION) as f:
            return json.load(f)
    except:
        print("No file found, returning empty list")
        return {"job_status": []}

def get_job_data():
    return notion.query(JOB_DB_ID, content_filter=JOB_DB_FILTER)

def format_job_file(job_data):
    """
    Formats job data into a structured dictionary.
    Args:
        job_data (list): A list of dictionaries where each dictionary represents a job page.
                         Each job page should contain an 'id' and a nested 'properties' dictionary
                         with 'Job status' which contains a 'select' dictionary with a 'name' key.
    Returns:
        dict: A dictionary with a single key 'job_status' which maps to a list of dictionaries.
              Each dictionary in the list contains 'id' and 'status' keys representing the job's ID
              and its status respectively.
    Raises:
        KeyError: If a job page does not contain the required keys, an error is logged and the job
                  is skipped.
    """
    
    
    job_file = {"job_status": []}
        
    # Arrange the job data into a list of dictionaries
    for page in job_data:
        try:
            job_file["job_status"].append(
                {"id": page["id"], "status": page["properties"]["Job status"]["select"]["name"]}
                )
        except KeyError:
            logger.error(f"Job {page['id']} does not have a status property.")
            continue

    return job_file

def compare_job_data(job_data, job_file):
    """
    Compares job data with a job file and performs actions based on job status.
    This function iterates through the job data and checks if each job is present in the job file.
    If a job is found in the job file, it performs the following actions based on the job status:
    - If the job status is "Nest", it sends a webhook with the job ID and status.
    - If the job status is "Queued", it updates the job status to "Nest" using the Notion API.
    Args:
        job_data (list): A list of dictionaries containing job information.
        job_file (dict): A dictionary containing job status information.
    Raises:
        Exception: If there is an error sending the webhook or updating the job status.
    """
    
    for page in job_data:
        try:
            # If the job is in the file, fire webhook.
            if page["id"] in [job["id"] for job in job_file["job_status"]]:
                status = page['properties']['Job status']['select']['name']
                if status == "Nest":
                    package = {page['id'].replace("-",""): status}
                    logger.info(f"Job {page['id']} is in the file, firing webhook.")
                    response = requests.post(MAKE_WEBHOOK_URL, json=package)
                    logger.info(f"Webhook response: {response.status_code}")
                if status == "Queued":
                    logger.info(f"Updating job {page['id']} to 'Nest'.")
                    package = notion.selstat_prop_gen("Job status", "status", "Nest")
                    response = notion.update(page["id"], package)
        except Exception as e:
            logger.error(f"Job {page['id']}: Error sending webhook.\n{e}")
            continue
    

def main():
    """
    Main function to check for stalled jobs in the queue.
    This function performs the following steps:
    1. Retrieves job data from Notion.
    2. If no jobs are found, logs the information, saves an empty job status file, and returns.
    3. Loads the job history file.
    4. If the job history file is empty, logs the information, writes the current jobs to the file, and returns.
    5. If the job history file is not empty, compares the current jobs to the jobs in the history file.
    6. Logs the comparison result, formats the current job data, and saves it to the job history file.
    """
    
    
    job_data = get_job_data()
    
    # If no jobs are found in Notion, return
    if job_data == []:
        logger.info("No jobs found in queue.")
        save_file({"job_status": []})
        return

    job_file = load_file()
    
    # If the job file is empty, write the current jobs to the file
    if job_file["job_status"] == []:
        logger.info("No jobs found in history, writing current jobs to file.")
        job_file = format_job_file(job_data)
        save_file(job_file)
        return
    
    # If the job file is not empty, compare the current jobs to the jobs in the file
    logger.info("Jobs found in history, comparing current jobs to history.")
    compare_job_data(job_data, job_file)
    
    logger.info("Jobs compared, writing current jobs to file.")
    job_file = format_job_file(job_data)
    save_file(job_file)
    
    
if __name__ == "__main__":
    MONITOR.ping(state='run')
    main()
    MONITOR.ping(state='complete')