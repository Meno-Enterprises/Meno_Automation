#!/usr/bin/env python3
# Aria Corona Sept 19th, 2024
# This script is designed to monitor a hotfolder for new files, check if they are images, and process them for preflight.

import time, os, re, shutil, cronitor, gc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from pathlib import Path
from PIL import Image
import json
import logging
import warnings

'''
Dependencies:
    pip install pillow
    pip install watchdog
    NotionApiHelper.py
    AutomatedEmails.py

Image correction logic:
    1. Check if file is an image.
    2. Check if database ID is present in file name.
    3. Check if file is a reprint. If so, send it straight to a hotfolder.
    4. Get job information from Notion.
    5. Get product information from Notion.
    6. Get image size and DPI.

    7. Preflight checks:
        1. Determine rotation based off which aspect ratio is closer to target aspect ratio.
        2. Check if image aspect ratio is within 5% of target aspect ratio. If not, trash the file and report the error.
        3. Check if image size matches target size within 38 pixels (1/4 inch) at 150 DPI.
            a. If image size matches target size, but DPI is wrong, change DPI EXIF Data and move to hotfolder.
            b. If image size and DPI matches target size and DPI, move to hotfolder.
        4. If image size does not match target size:
            a. If customer is on approved preflight list, scale image, crop to correct aspect ratio and move to hotfolder.
            b. If customer is not on approved preflight list, report error and trash file. Order and related jobs are canceled and the customer is notified.
            c. If customer is on the no-preflight list, crop to correct aspect ratio and size without scaling and move to hotfolder.
'''

class HotfolderHandler(FileSystemEventHandler):
    def __init__(self):
        self.notion_helper = NotionApiHelper()
        self.automated_emails = AutomatedEmails()
        self.EMAIL_CONFIG_PATH = r"conf/MOD_Preflight_Error_Conf.json"
        self.BLANK_CONFIG_PATH = r"conf/Blank_To_Email_Conf.json"
        self.TEMP_CONFIG_PATH = r"conf/Temp_To_Email_Conf.json"
        self.HOTFOLDER_PATH = "//192.168.0.178/meno/Hotfolders"
        self.CANCELED_ORDER_PATH = r"output/MOD_Canceled_Orders.txt"
        self.EXTENSION_REGEX = r"(.+)\.(jpg|jpeg|png)$"
        self.DATABASE_REGEX = r".*_(.*)__\d*"
        self.REPRINT_REGEX = r".*--(REP)-\d*_.*"
        self.EMAIL_ADDRESS_PATTERN = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
        self.ACCEPTED_EXTENSIONS = ['jpg', 'jpeg', 'png']
        self.DPI_CHANGE_ERROR = 1
        self.IMAGE_RESIZED_ERROR = 2
        self.IMAGE_CROPPED_ERROR = -1
        self.IMAGE_OOS_ERROR = 3
        self.UPDATE_LOG = 0
        self.STOP_JOB_ERROR = 4
        self.ORDER_PROP_JOB_IDS = r"iLNe"
        self.CUSTOMER_PROP_EMAIL_PRIMARY = r"jtAR"
        self.CUSTOMER_PROP_EMAIL_BACKUP = r"qfs~"
        with open(self.CANCELED_ORDER_PATH, 'r') as file:
            self.canceled_orders = file.readlines()
            
        logging.basicConfig(filename='logs/hotfolder_handler.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        warnings.simplefilter('ignore', Image.DecompressionBombWarning) # Suppresses DecompressionBombWarning
        Image.MAX_IMAGE_PIXELS = 600000000     # Ups the max image size to account for large 300DPI images.

    def on_created(self, event):
        if event.is_directory:
            return None
        else:
            print(f"New file detected: {event.src_path}")
            # Add your processing logic here
            self.process_new_file(event.src_path)

    def get_image_info(self, image_path):
        print(f"Getting image info for {image_path}.")
        try:
            image = self.open_image(image_path)
            size = image.size
            dpi = image.info.get('dpi')
        except Exception as e:
            print(f"Error getting image info: {e}")
            return None, -1
        return size, dpi
    
    def adjust_dpi_and_move(self, image, hotfolder, file_name):
        print(f"Adjusting DPI to 150 and moving to {hotfolder}.")
        new_path = f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
        if os.path.exists(new_path):
            print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
            self.remove_file(new_path)
            time.sleep(5) # Wait for Caldera to recognize the file is gone.        
        image.save(new_path, dpi=(150, 150)) # Force saves the image to 150 DPI. Doesn't actually do anything to the image, just changes the EXIF data
        try:
            shutil.copy(new_path, f"{self.HOTFOLDER_PATH}/tmp/{file_name}")
        except Exception as e:
            print(f"Error copying file to tmp folder: {e}")
        pass
    
    def resize_and_move(self, IMAGE, HOTFOLDER, IMAGE_FILE_NAME, TARGET_XPIX, TARGET_YPIX, ORIGINAL_SIZE, JOB_ID, EXISTING_JOB_LOG):
        try:
            print(f"Resizing image to {TARGET_XPIX},{TARGET_YPIX} and moving to {HOTFOLDER}.")
            HOTFOLDER_PATH = f"{self.HOTFOLDER_PATH}/{HOTFOLDER}/{IMAGE_FILE_NAME}"
            TEMP_PATH = f"{self.HOTFOLDER_PATH}/tmp/{IMAGE_FILE_NAME}"
            SCALE_FACTOR_WIDTH = TARGET_XPIX / ORIGINAL_SIZE[0]
            SCALE_FACTOR_HEIGHT = TARGET_YPIX / ORIGINAL_SIZE[1]
            SCALE_FACTOR = max(SCALE_FACTOR_WIDTH, SCALE_FACTOR_HEIGHT)
            ICC_PROFILE = IMAGE.info.get('icc_profile') if 'icc_profile' in IMAGE.info else None # Get ICC profile if it exists            
            
            print(f"Scaling image by {SCALE_FACTOR}.")
            SCALED_IMAGE = IMAGE.resize((int(ORIGINAL_SIZE[0] * SCALE_FACTOR), int(ORIGINAL_SIZE[1] * SCALE_FACTOR)), Image.LANCZOS) # Resize image to target size

            if os.path.exists(HOTFOLDER_PATH): # Check if file already exists in hotfolder
                print(f"File {IMAGE_FILE_NAME} already exists in {HOTFOLDER}. Removing old file.")
                logging.info(f"File {IMAGE_FILE_NAME} already exists in {HOTFOLDER}. Removing old file.")
                self.remove_file(HOTFOLDER_PATH)
                time.sleep(5) # Wait for Caldera to recognize the file is gone.

            # Determine the bounding box for the cropped image
            LEFT = (SCALED_IMAGE.size[0] - TARGET_XPIX) / 2
            TOP = (SCALED_IMAGE.size[1] - TARGET_YPIX) / 2
            RIGHT = (SCALED_IMAGE.size[0] + TARGET_XPIX) / 2
            BOTTOM = (SCALED_IMAGE.size[1] + TARGET_YPIX) / 2

            # Image does not need to be cropped. Save it to the hotfolder.
            if LEFT == 0 and TOP == 0 and RIGHT == SCALED_IMAGE.size[0] and BOTTOM == SCALED_IMAGE.size[1]: 
                self.save_image(JOB_ID, SCALED_IMAGE, HOTFOLDER_PATH, ICC_PROFILE)
                print(f"Scaled image to {TARGET_XPIX},{TARGET_YPIX} and saved to {HOTFOLDER}.")                
                self.copy_image(JOB_ID, TEMP_PATH, HOTFOLDER_PATH)
                return None
            
            # Crop the image and save it to the hotfolder.
            print(f"Cropping image to {TARGET_XPIX},{TARGET_YPIX}.")
            CROPPED_IMAGE = SCALED_IMAGE.crop((int(LEFT), int(TOP), int(RIGHT), int(BOTTOM)))
            self.save_image(JOB_ID, CROPPED_IMAGE, HOTFOLDER_PATH, ICC_PROFILE)
            print(f"Scaled and cropped image to {TARGET_XPIX},{TARGET_YPIX} and saved to {HOTFOLDER}.")
            self.copy_image(JOB_ID, TEMP_PATH, HOTFOLDER_PATH)

        except Exception as e:
            print(f"Critical error resizing image: {e}")
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            self.report_error(JOB_ID, f"{EXISTING_JOB_LOG}{now} - Critical Error resizing image: {e}\nPlease let someone know that CheckImageThenHotfolder.py had this error.", self.STOP_JOB_ERROR)
        logging.info(f"Resized {IMAGE_FILE_NAME} from {ORIGINAL_SIZE} to {TARGET_XPIX},{TARGET_YPIX} and moved to {HOTFOLDER}.")
        pass

    def crop_and_move(self, image, hotfolder, file_name, target_xpix, target_ypix, job_id, job_log):
        print(f"Cropping image to {target_xpix},{target_ypix} and moving to {hotfolder}.")
        try:
            icc_profile = image.info.get('icc_profile') if 'icc_profile' in image.info else None
            
            left = (image.size[0] - target_xpix) / 2
            top = (image.size[1] - target_ypix) / 2
            right = (image.size[0] + target_xpix) / 2
            bottom = (image.size[1] + target_ypix) / 2
            print(f"Cropping image to {target_xpix},{target_ypix}.")
            cropped_image = image.crop((int(left), int(top), int(right), int(bottom)))

            new_path = f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
            if os.path.exists(new_path):
                print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
                self.remove_file(new_path)
                time.sleep(5)
            
            if icc_profile:
                cropped_image.save(new_path, dpi=(150, 150), icc_profile=icc_profile)
            else:
                cropped_image.save(new_path, dpi=(150, 150))
            print(f"Corrected image to {target_xpix},{target_ypix} and moved to {hotfolder}.")   
            time.sleep(1) 
            shutil.copy(new_path, f"{self.HOTFOLDER_PATH}/tmp/{file_name}")
        except Exception as e:
            print(f"Error resizing image: {e}")
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            self.report_error(job_id, f"{job_log}{now} - Critical Error cropping image: {e}\nFind Aria and let her know this broke.", self.STOP_JOB_ERROR)

    def move_to_hotfolder(self, hotfolder, file_name):
        print(f"Moving file {file_name} to {hotfolder}.")
        new_path = f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
        current_path = f"{self.HOTFOLDER_PATH}/Hopper/{file_name}"
        if os.path.exists(new_path):
            print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
            self.remove_file(new_path)
            time.sleep(5) # Wait for Caldera to recognize the file is gone.
        os.rename(current_path, new_path)
        pass

    def save_image(self, JOB_ID, IMAGE, PATH, ICC_PROFILE = None):
        try:
            if ICC_PROFILE:
                IMAGE.save(PATH, dpi=(150, 150), icc_profile=ICC_PROFILE)
            else:
                IMAGE.save(PATH, dpi=(150, 150))
            print(f"Image saved to {PATH}.")
            time.sleep(1)
        except Exception as e:
            print(f"Error saving image: {e}")
            self.report_error(JOB_ID, f"Error saving image: {e}", self.STOP_JOB_ERROR)
        pass
    
    def copy_image(self, JOB_ID, NEW_PATH, CURRENT_PATH):
        try:
            print(f"Copying image to {NEW_PATH}.")
            shutil.copy(CURRENT_PATH, NEW_PATH)
            time.sleep(1)
        except Exception as e:
            print(f"Error copying image: {e}")
            self.report_error(JOB_ID, f"Error copying image: {e}", self.STOP_JOB_ERROR)
        pass

    # Report error to Notion and log file. Report_error will handle writing all errors to the log file.
    def report_error(self, job_id, error_message, level = 0):
        print(f"{job_id}: {error_message}")
        properties = {}
        
        logs = self.notion_helper.generate_property_body("Log", "rich_text", [error_message])
        
        if level == self.DPI_CHANGE_ERROR:
            tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["DPI Changed", "OOS"]) 
            properties = {"Log": logs["Log"], "Tags": tags["Tags"]}
            
        elif level == self.IMAGE_RESIZED_ERROR:
            tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["Resized", "OOS"])
            properties = {"Log": logs["Log"], "Tags": tags["Tags"]}
            
        elif level == self.IMAGE_CROPPED_ERROR:
            tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["Cropped", "OOS"])
            properties = {"Log": logs["Log"], "Tags": tags["Tags"]}
            
        elif level == self.STOP_JOB_ERROR:
            system_status = self.notion_helper.generate_property_body("System status", "select", "Error")
            properties = {"Log": logs["Log"], "System status": system_status["System status"]}            
            
        elif level == self.IMAGE_OOS_ERROR:
            tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["OOS"]) 
            system_status = self.notion_helper.generate_property_body("System status", "select", "Error")
            properties = {"Log": logs["Log"], "System status": system_status["System status"], "Tags": tags["Tags"]}
            
        elif level == self.UPDATE_LOG:
            properties = {"Log": logs["Log"]}
            
        else: # Default to critical error
            system_status = self.notion_helper.generate_property_body("System status", "select", "Error")
            properties = {"Log": logs["Log"], "System status": system_status["System status"]} 
            
        print(f"End Reporting error for job {job_id}: {error_message}")
        logging.error(f"Error for job {job_id}: {error_message}")
        self.notion_helper.update_page(job_id, properties)
        pass

    def remove_file(self, file_path):
        print(f"Removing file: {file_path}")
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File {file_path} has been removed.\n")
        else:
            print(f"File {file_path} does not exist.\n")
        pass

    def cancel_order(self, ORDER_ID, SKU, REASON_CANCELED, IMAGE_SOURCE): # Banking on there not being an order with over 100 jobs. If so, pagination needs to be implemented.
        print(f"Cancelling order: {ORDER_ID}")
        canceled_job_list = []

        # Generate canceled properties
        CANCELED_ORDER_PROP_BODY = self.notion_helper.generate_property_body("Status", "select", "Canceled")
        CANCELED_JOB_PROP_BODY = self.notion_helper.generate_property_body("Job status", "select", "Canceled")
        
        self.notion_helper.update_page(ORDER_ID, CANCELED_ORDER_PROP_BODY) # Mark order as canceled on Notion.
        time.sleep(.5)
        ORDER_JOB_ID_RESULTS = self.notion_helper.get_page_property(ORDER_ID, self.ORDER_PROP_JOB_IDS) # Get job IDs related to the order.
        time.sleep(.5)
        ORDER_RESULTS = self.notion_helper.get_page(ORDER_ID) # Get order information

        order_number = 'Order_Number_Not_Found'
        if ORDER_RESULTS['properties']['Order number']['rich_text']:
            order_number = ORDER_RESULTS['properties']['Order number']['rich_text'][0]['plain_text']

        try: # Get customer id
            CUSTOMER_NOTION_ID = ORDER_RESULTS['properties']['Customer']['relation'][0]['id']
        except Exception as e:
            self.report_error(ORDER_ID, f"Order Canceled. Error finding customer ID in order {ORDER_ID}: {e}", self.STOP_JOB_ERROR)
            return
        time.sleep(.5)
        
        # Get customer email
        customer_email_response = self.notion_helper.get_page_property(CUSTOMER_NOTION_ID, self.CUSTOMER_PROP_EMAIL_PRIMARY)
        try:
            CUSTOMER_EMAIL_STRING = customer_email_response['results'][0]['rich_text']['plain_text']
        except Exception as e:
            try:
                customer_email_response = self.notion_helper.get_page_property(CUSTOMER_NOTION_ID, self.CUSTOMER_PROP_EMAIL_BACKUP)
                CUSTOMER_EMAIL_STRING = customer_email_response['email']
            except Exception as e:
                self.report_error(ORDER_ID, f"Order Canceled. Error finding customer email in order {ORDER_ID}: {e}", self.STOP_JOB_ERROR)
                return
            
        CUSTOMER_EMAIL_LIST = re.findall(self.EMAIL_ADDRESS_PATTERN, CUSTOMER_EMAIL_STRING)

        # Process related job IDs, cancel jobs, and add to canceled job list.
        try:
            if ORDER_JOB_ID_RESULTS:
                if ORDER_JOB_ID_RESULTS['results']:
                    for each in ORDER_JOB_ID_RESULTS['results']:
                        id = each['relation']['id']
                        canceled_job_list.append(id)
                        self.notion_helper.update_page(id, CANCELED_JOB_PROP_BODY)
        except Exception as e:
            self.report_error(ORDER_ID, f"Order Canceled. Error canceling jobs for order {ORDER_ID}: {e}", self.STOP_JOB_ERROR)
            
        if len(canceled_job_list) == 0:
            print(f"No jobs found for order {ORDER_ID}.")
            ERROR_PROP_BODY = self.notion_helper.generate_property_body("System status", "select", "Error")
            self.notion_helper.update_page(ORDER_ID, ERROR_PROP_BODY)
        print(f"Canceled Jobs: {canceled_job_list}")

        # Send cancelation email
        print("Preparing cancelation email...")

        with open(self.BLANK_CONFIG_PATH, 'r') as file: # Load default config file.
            blank_to_email_conf = json.load(file)
        blank_to_email_conf['to_email'] = CUSTOMER_EMAIL_LIST # Add customer email to config.
        with open(self.TEMP_CONFIG_PATH, 'w') as temp_file:
            json.dump(blank_to_email_conf, temp_file, indent=4) # Write to temp config file.

        subject = f"Order Cancelation Notice: {order_number}"
        body = f"""
        Notice date: {time.strftime('%m-%d-%Y')} 
        Order: {order_number}
        Product: {SKU}
        Image Problem: {REASON_CANCELED}
        Image Source: {IMAGE_SOURCE}

        ---

        One or more images did not meet the required product specifications for production. We cannot proceed with the order as it stands, and have canceled order {order_number}. Please check that the submitted images comply with the product templates and make any necessary corrections prior to resubmitting the order.

        If you have any questions, please contact customer support at ondemand@menoenterprises.com.
        """
        self.automated_emails.send_email(self.TEMP_CONFIG_PATH, subject, body)

        # Record canceled order in file
        with open(self.CANCELED_ORDER_PATH, 'a') as file:
            file.write(f"{ORDER_ID}\n")
        self.canceled_orders.append(ORDER_ID)
        
        logging.info(f"Order {ORDER_ID} has been canceled. SKU: {SKU}")
        pass

    def open_image(self, image_path):
        try:
            image = Image.open(image_path)
        except Exception as e:
            print(f"Error opening image: {e}")
            return None
        return image

    def process_new_file(self, file_path):

        # Log the file path
        logging.info(f"Processing new file: {file_path}")
        if ".~#~" in file_path:  
            print(f"File {file_path} is a temporary file. Waiting.")
            old_path = file_path
            file_path = file_path.replace(".~#~", "")
            time.sleep(1)
            if os.path.exists(file_path) == False:
                self.process_new_file(old_path)
                return None
        # time.sleep(5) # Wait for file to finish copying
        allow_alter = 0
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"{now} - Processing file: {file_path}")
        file_name = os.path.basename(file_path)

        try:
            job_id = re.search(self.DATABASE_REGEX, file_name, re.IGNORECASE).group(1)
            print(f"Job ID: {job_id}")
            extension = re.search(self.EXTENSION_REGEX, file_name, re.IGNORECASE).group(2)
            print(f"Extension: {extension}")

            # Check if file is an image
            if extension is None or extension.lower() not in self.ACCEPTED_EXTENSIONS: 
                print(f"File {file_name} is not an accepted image type. Skipping.")
                return None
            
            # Check if database ID is present in file name
            if job_id == None: 
                print(f"Could not find database ID in {file_name}. Skipping.")
                return None
            
        except AttributeError:
            print(f"Could not find database ID or image extension in {file_name}. Skipping.")
            logging.error(f"Could not find database ID or image extension in {file_name}. Skipping.")
            return None        
       
        # Check if file is a reprint, sends it straight to a hotfolder.
        try:  
            reprint_match = re.search(self.REPRINT_REGEX, file_name, re.IGNORECASE).group(1)
        except AttributeError:
            reprint_match = None
        
        if reprint_match:
            print(f"File {file_name} is a reprint. Pushing to hotfolder.")
            reprint_output = self.notion_helper.get_page(job_id)
            job_log = ""
            
            if reprint_output['properties']['Log']['rich_text']: # Collecting Job Log
                job_log = reprint_output['properties']['Log']['rich_text'][0]['plain_text'] + "\n"
                
            try:   # Variables for reprints
                hotfolder = reprint_output['properties']['Hot folder path']['formula']['string']
            except Exception as e:
                self.report_error(job_id, f"{job_log}LOG - {now} - Missing reprint info in Notion {job_id}: {e}\nSkipping File.", self.STOP_JOB_ERROR)
                return None
            
            self.move_to_hotfolder(hotfolder, file_name)
            return None
        
        # Get job information from Notion
        print("Querying Notion API for job information...")
        job_output = self.notion_helper.get_page(job_id) 

        job_log = ""
        try:
            if job_output['properties']['Log']['rich_text']: # Collecting Job Log
                job_log = job_output['properties']['Log']['rich_text'][0]['plain_text'] + "\n"
        except Exception as e:
            print(f"Error collecting job log: {e}")
            logging.error(f"Error collecting job log: {e}")
            self.report_error(job_id, f"{job_log}{now} - Error collecting job log: {e}. Critical Error, please notify Aria.", 4)

        try:    # @Aria: Assign job variables here.
            product_id = job_output['properties']['Product']['relation'][0]['id']
            order_number = job_output['properties']['Order ID']['formula']['string']
            order_id = job_output['properties']['Order']['relation'][0]['id']
            image_source = 'Not Found'
            if job_output['properties']['Image source']['rich_text']:
                image_source = job_output['properties']['Image source']['rich_text'][0]['plain_text']
        except Exception as e: 
            logging.error(f"Could not find image_source, product ID or customer in job {job_id}. Skipping.")
            print(f"Could not find product ID or customer in job {job_id}. Skipping.")
            self.report_error(job_id, f"{job_log}{now} - Missing product or customer in Notion: {e}", 4)
            self.remove_file(file_path)
            return None

        if order_number in self.canceled_orders:
            print(f"Order {order_number} has been canceled. Skipping.")
            self.remove_file(file_path)
            return
        
        print(f"Getting customer ID.")
        customer_id_response = self.notion_helper.get_page_property(order_id, r"iegJ")
        try:
            customer_id = customer_id_response['results'][0]['relation']['id']
        except Exception as e:
            print(f"Error finding customer ID in order {order_id}: {e}")
            logging.error(f"Error finding customer ID in order {order_id}: {e}")
            self.report_error(job_id, f"{job_log}{now} - Error finding customer ID in order {order_id}: {e}", 4)
            self.remove_file(file_path)
            return None
        
        print(f"Getting customer preflight information.")
        customer_preflight_approval = self.notion_helper.get_page_property(customer_id, r"I%3E%7Cy")
        
        try:
            customer_preflight_approval = customer_preflight_approval['select']['name']
            print(customer_preflight_approval)
            allow_alter = int(customer_preflight_approval[0])
            print(f"Customer {customer_id}:{allow_alter}")
        except Exception as e:
            print(f"Error finding customer preflight approval status: {e}")
            logging.error(f"Error finding customer preflight approval status: {e}")
            self.report_error(job_id, f"{job_log}{now} - Error finding customer preflight approval status: {e}", 4)
            self.remove_file(file_path)
            return None
        
        '''
        try:    # Assigns preflight approval status for customer from JSON file
            allow_alter = self.customer_preflight_approval[customer]
        except KeyError:
            print(f"Customer {customer} not found in preflight approval list. Skipping.")
            logging.error(f"Customer {customer} not found in preflight approval list. Skipping.")
            self.report_error(job_id, f"{job_log}{now} - Customer not found in preflight approval list.", 4)
            self.remove_file(file_path)
            return None
        '''

        product_output = self.notion_helper.get_page(product_id) # Get product information from Notion

        try:    # @Aria: Assign product variables here.
            xpix = product_output['properties']['xpix']['number']
            ypix = product_output['properties']['ypix']['number']
            hotfolder = product_output['properties']['Hot Folder']['select']['name']
            sku = product_output['properties']['Product Code']['title'][0]['plain_text']
        except Exception as e:
            print(f"Missing info for {product_id} in Notion. Skipping. {e}")
            logging.error(f"Missing info for {product_id} in Notion. Skipping. {e}")
            self.report_error(job_id, f"{job_log}{now} - Missing product info in Notion: {e}", 4)
            self.remove_file(file_path)
            return None
        
        if xpix == None or ypix == None or hotfolder == None: # Check if product info is missing in Notion
            print(f"Missing product info for {product_id} in Notion. Skipping.")
            logging.error(f"Missing product info for {product_id} in Notion. Skipping.")
            self.report_error(job_id, f"{job_log}{now} - Missing product info in Notion.", 4)
            self.remove_file(file_path)
            return None

        print(f"Product ID: {product_id}, xpix: {xpix}, ypix: {ypix}, hotfolder: {hotfolder}")
        size, dpi = self.get_image_info(file_path) # Get image size and DPI
        if dpi == -1: # 
            print(f"Error getting image info for {file_path}. Skipping.")
            logging.error(f"Error getting image info for {file_path}. Skipping.")
            self.report_error(job_id, f"{job_log}{now} - Image file too large. Either an image issue (check this first) or the maximum image size needs to be increased in the preflighting script.", 4)
            self.remove_file(file_path)
            return None
        
        print(f"Image size: {size}, DPI: {dpi}")
        # Determine which aspect ratio is closer to target aspect ratio.
        if abs(xpix - size[0]) + abs(ypix - size[1]) > abs(xpix - size[1]) + abs(ypix - size[0]): 
            xpix, ypix = ypix, xpix     # Swap xpix and ypix to match rotation. Removes needing to account for rotation probably.
        image_aspect = size[0] / size[1]
        target_aspect = xpix / ypix

        print(f"Image size: {size} at {dpi} DPI, AR: {image_aspect}. Target size: {xpix},{ypix} at 150 DPI, AR: {target_aspect}.")

        # Check if image aspect ratio is within 5% of target aspect ratio. If not, trash the file and report the error.
        if image_aspect <= target_aspect * 0.95 or image_aspect >= target_aspect * 1.05: 
            print(f"Image aspect ratio is outsize acceptable fixable range. Reporting and trashing file.")
            logging.error(f"Image aspect ratio is outside acceptable fixable range.")
            self.report_error(job_id, f"{job_log}{now} - Image aspect ratio is outside acceptable fixable range.", 3)
            if allow_alter == 2:
                order_id = job_output['properties']['Order']['relation'][0]['id']
                reason_canceled = f"Image aspect ratio is outside fixable range."
                self.cancel_order(order_id, sku, reason_canceled, image_source)
            self.remove_file(file_path)
            return None
        
        # Check if image size matches target size within 38 pixels (1/4 inch) at 150 DPI. 
        if((xpix - 38 <= size[0] <= xpix + 38 and ypix - 38 <= size[1] <= ypix + 38) or 
            (xpix - 38 <= size[1] <= xpix + 38 and ypix - 38 <= size[0] <= ypix + 38)): 
            if(dpi != (150, 150)): # Correct Size, but DPI is wrong. Adjust DPI and move to hotfolder.
                print(f"Image size matches target size, but DPI does not. Adjusting DPI to 150 and moving to {hotfolder}.")
                image = self.open_image(file_path)
                self.adjust_dpi_and_move(image, hotfolder, file_name)
                self.report_error(job_id, f"{job_log}{now} - Image DPI {dpi} does not match target DPI. Adjusting to 150 DPI and moving to hotfolder.", 1)            
                self.remove_file(file_path) # Remove original file
                return None
            else: # Image is correct size and DPI
                print(f"Image size and DPI match target size. Moving to {hotfolder}.")
                self.move_to_hotfolder(hotfolder, file_name)
                return None
        elif(allow_alter == 1): # Image size does not match target size. Resize and move to hotfolder.
            print(f"Image size does not match target size. Resizing and moving to {hotfolder}.")
            image = self.open_image(file_path)
            self.resize_and_move(image, hotfolder, file_name, xpix, ypix, size, job_id, job_log)
            self.report_error(job_id, f"{job_log}{now} - Image size {size} does not match target size ({xpix},{ypix}). Customer is on approved preflight list, resizing.", 2)
            self.remove_file(file_path) # Remove original file
            return None
        elif(allow_alter == 3): # Image size does not match target size. Crop and move to hotfolder.
            print(f"Image size does not match target size. Cropping and moving to {hotfolder}.")
            image = self.open_image(file_path)
            self.crop_and_move(image, hotfolder, file_name, xpix, ypix, job_id, job_log)
            self.report_error(job_id, f"{job_log}{now} - Image size {size} does not match target size ({xpix},{ypix}). Customer on the let it run list, cropping.", -1)
        else: # Image size does not match target size at 150DPI. Cancel order and trash file.
            print(f"Image size does not match target size. Trashing file.")
            logging.info(f"Image size does not match target size. Cancelling order.")
            self.report_error(job_id, f"{job_log}{now} - Image size does not match target size at 150DPI. Customer not on approved preflight list. Canceling jobs and order.", 3)
            self.remove_file(file_path)
            if job_output['properties']['Order']['relation']:
                order_id = job_output['properties']['Order']['relation'][0]['id']
                reason_canceled = f"Image size {size} does not match target size ({xpix},{ypix}) at 150 DPI."
                self.cancel_order(order_id, sku, reason_canceled, image_source)
            else:
                self.report_error(job_id, f"{job_log}{now} - Could not find order ID in Notion. Order needs to be canceled due to improper art size.", 4)
            return None
        

if __name__ == "__main__":
    cronitor_key_path = "conf/Cronitor_API_Key.txt"
    email_config_path = "conf/MOD_Preflight_Launch_Conf.json"
    auto_emails = AutomatedEmails()
    path = r"\\192.168.0.178\meno\Hotfolders\Hopper"  # Replace with the path to your hotfolder
    event_handler = HotfolderHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    gc.enable()
    
    with open(cronitor_key_path, "r") as file:
        cronitor_api_key = file.read()
    cronitor.api_key = cronitor_api_key
    monitor = cronitor.Monitor("MOD Preflight Script")
    
    print(f"Monitoring directory: {path}")
    subject = "MOD Preflight Script Launch"
    body = f"MOD Preflight Script has been started at {time.strftime('%Y-%m-%d %H:%M:%S')}.\n\n\nThis is an automated email being sent on behalf of Aria Corona, please do not reply. If you have any questions or concerns, please contact Aria directly at acorona@menoenterprises.com"
    auto_emails.send_email(email_config_path, subject, body)
    tick = 0
    try:
        while True:
            time.sleep(1)
            if tick % 300 == 0:
                monitor.ping()
            if tick % 3600 == 0:
                gc.collect()
                tick = 0
            tick += 1
    except KeyboardInterrupt:
        observer.stop()
    except Exception as e:
        print(f"Critical Error: {e}")
        error_subject = "MOD Preflight Script Critical Error"
        error_body = f"MOD Preflight Script has encountered a critical error at {time.strftime('%Y-%m-%d %H:%M:%S')}. Please check the script."
        auto_emails.send_email(email_config_path, error_subject, error_body)
        observer.stop()
    observer.join()