#!/usr/bin/env python3

from AutomatedEmails import AutomatedEmails
from datetime import datetime
import logging, os, json, sys

automailer = AutomatedEmails()

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
   logging.FileHandler('logs/MOD_Track_Shipped.log'),
   logging.StreamHandler()
    ]
)

logger = logging.getLogger('MOD_Data_Tracker')
logger.info('Logging setup complete')

STORAGE_DIRECTORY = 'storage/MOD_Shipped_Tracker.json'

NOW = datetime.now().strftime('%Y-%m-%d')

def catch_variable():
    if len(sys.argv) == 2:
        page_id = sys.argv[1] # Command line argument
        logger.info(f"Shipment ID Recieved: {page_id}")
        return page_id
    sys.exit("No Page ID Provided")

def main():
    value = {'page_id': catch_variable(), 'date': NOW}
    
    if os.path.exists(STORAGE_DIRECTORY):
        with open(STORAGE_DIRECTORY, 'r') as file:
            data = json.load(file)
            logger.info(f"Loaded data from {STORAGE_DIRECTORY}")
    else:
        data = []
        logger.info(f"No existing data found, starting with an empty dictionary")
    
    data.append(value)
    
    with open(STORAGE_DIRECTORY, 'w') as file:
        json.dump(data, file, indent=4)
        
    logger.info(f"Data written to {STORAGE_DIRECTORY}")

if __name__ == '__main__':
    main()