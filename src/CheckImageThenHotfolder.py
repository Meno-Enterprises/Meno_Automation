#!/usr/bin/env python3
# Aria Corona Oct. 29th, 2024
# This script is designed to monitor a hotfolder for new files, check if they are images, and process them for preflight.

import time, os, re, shutil, cronitor, gc
# from watchdog.observers import Observer
# from watchdog.events import FileSystemEventHandler
from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from datetime import datetime
from pathlib import Path
from PIL import Image
import json
import logging
import warnings
import subprocess

'''
Dependencies:
    pip install pillow
    pip install cronitor
    NotionApiHelper.py
    AutomatedEmails.py
Property Dependencies:
    MOD Jobs Database:
        - Log
        - Product
        - Order ID
        - Order
        - Image source
    MOD Products Database:
        - xpix
        - ypix
        - Hot Folder
        - Product Code
    MOD Customers Database:
        - Email Primary
        - Email Backup
        - Preflight Approval
    MOD Orders Database:
        - Order number
        - Customer
        - Job IDs

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

CRONITOR_KEY_PATH = "conf/Cronitor_API_Key.txt"
PING_CYCLE = 100
GC_CYCLE = 3600
STOP_TIME = '23:59:00' # Time to stop the script
PATH = r"\\192.168.0.178\meno\Hotfolders\Hopper"  # Replace with the path to your hotfolder

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/hotfolder_handler.log'),
                        logging.StreamHandler()
                    ])


class HotfolderHandler():
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
        self.DUPLICATE_FILE_REGEX = r".*\(\d{1,4}\).*"
        self.ACCEPTED_EXTENSIONS = ['jpg', 'jpeg', 'png', 'tif', 'tiff']
        self.DPI_CHANGE_ERROR = 1
        self.IMAGE_RESIZED_ERROR = 2
        self.IMAGE_CROPPED_ERROR = -1
        self.IMAGE_OOS_ERROR = 3
        self.UPDATE_LOG = 0
        self.STOP_JOB_ERROR = 4
        self.ORDER_PROP_JOB_IDS = r"iLNe"
        self.CUSTOMER_PROP_EMAIL_PRIMARY = r"jtAR"
        self.CUSTOMER_PROP_EMAIL_BACKUP = r"qfs~"
        
        self.LABEL_CREATED_PACKAGE = {'Print Status': {'select': {'name': 'Label created'}}}
        self.JOB_NESTING_PACKAGE = {'Job status': {'select': {'name': 'Nesting'}}}
        self.REPRINT_NESTING_PACKAGE = {'Reprint status': {'select': {'name': 'Nesting'}}}
        
        with open(self.CANCELED_ORDER_PATH, 'r') as file:
            self.canceled_orders = file.readlines()
              
        warnings.simplefilter('ignore', Image.DecompressionBombWarning) # Suppresses DecompressionBombWarning
        Image.MAX_IMAGE_PIXELS = 600000000     # Ups the max image size to account for large 300DPI images.


    def check_directory(self, directory_path):
        try:
            # List all files in the directory
            files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
            return files
        except Exception as e:
            logging.error(f"Error accessing directory {directory_path}: {e}")
            return []


    def get_image_info(self, image_path):
        print(f"Getting image info for {image_path}.")
        try:
            image = self.open_image(image_path)
            size = image.size
            dpi = image.info.get('dpi')
        except Exception as e:
            logging.error(f"Error getting image info: {e}")
            return None, -1
        return size, dpi
    
    
    def adjust_dpi_and_move(self, image, hotfolder, file_name, job_id):
        """
        Adjusts the DPI of the given image to 150 and moves it to the specified hotfolder.
        Parameters:
        image (PIL.Image.Image): The image to be processed.
        hotfolder (str): The name of the hotfolder where the image will be moved.
        file_name (str): The name of the file to be saved.
        job_id (str): The job identifier associated with the image.
        Returns:
        None
        """
              
        logging.info(f"Adjusting DPI to 150 and moving to {hotfolder}.")
        new_path = f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
        if os.path.exists(new_path):
            logging.info(f"File {file_name} already exists in {hotfolder}. Removing old file.")
            self.remove_file(new_path)
            time.sleep(5) # Wait for Caldera to recognize the file is gone.        
        image.save(new_path, dpi=(150, 150)) # Force saves the image to 150 DPI. Doesn't actually do anything to the image, just changes the EXIF data
        self.copy_image(job_id, f"{self.HOTFOLDER_PATH}/tmp/{file_name}", new_path)
    
    
    def resize_image(self, image, hotfolder, image_file_name, target_xpix, target_ypix, original_size, job_id, existing_job_log):
        try:
            logging.info(f"Resizing image to {target_xpix},{target_ypix} and moving to {hotfolder}.")
            scale_factor_width = target_xpix / original_size[0]
            scale_factor_height = target_ypix / original_size[1]
            scale_factor = max(scale_factor_width, scale_factor_height)       
            
            logging.info(f"Scaling image by {scale_factor}.")
            scaled_image = image.resize(
                (int(original_size[0] * scale_factor), int(original_size[1] * scale_factor)), Image.LANCZOS
                ) # Resize image to target size

            # Crop the image and save it to the hotfolder.
            self.crop_and_move(
                scaled_image, hotfolder, image_file_name, target_xpix, target_ypix, job_id, existing_job_log
                )

        except Exception as e:
            logging.error(f"Critical error resizing image: {e}")
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            
            self.report_error(
                job_id, f"{existing_job_log}{now} - Critical Error resizing image: {e}\n" +
                f"Please let someone know that CheckImageThenHotfolder.py had this error.", self.STOP_JOB_ERROR
                )
            
        logging.info(
            f"Resized {image_file_name} from {original_size} to {target_xpix},{target_ypix} and moved to {hotfolder}."
            )
        pass


    def crop_and_move(self, image, hotfolder, file_name, target_xpix, target_ypix, job_id, job_log):
        print(f"Checking if image needs to be cropped.")
        try:
            icc_profile = image.info.get('icc_profile') if 'icc_profile' in image.info else None
            
            left = (image.size[0] - target_xpix) / 2
            top = (image.size[1] - target_ypix) / 2
            right = (image.size[0] + target_xpix) / 2
            bottom = (image.size[1] + target_ypix) / 2
            
            if left == 0 and top == 0 and right == image.size[0] and bottom == image.size[1]:
                logging.info(f"Image does not need to be cropped. Saving to {hotfolder}.")
                self.save_image(job_id, image, f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}", icc_profile)
                self.copy_image(
                    job_id, f"{self.HOTFOLDER_PATH}/tmp/{file_name}", f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
                    )
                
            else:
                logging.info(f"Image needs to be cropped. Cropping to {target_xpix},{target_ypix}.")
                cropped_image = image.crop((int(left), int(top), int(right), int(bottom)))
                new_path = f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
                
                self.save_image(job_id, cropped_image, new_path, icc_profile)
                logging.info(f"Corrected {file_name} image to {target_xpix},{target_ypix} and moved to {hotfolder}.")
                self.copy_image(job_id, f"{self.HOTFOLDER_PATH}/tmp/{file_name}", new_path)
                
        except Exception as e:
            
            print(f"Error resizing image: {e}")
            now = time.strftime('%Y-%m-%d %H:%M:%S')
            self.report_error(
                job_id, 
                f"{job_log}{now} - Critical Error cropping image: {e}\nFind Aria and let her know this broke.", 
                self.STOP_JOB_ERROR
                )


    def move_to_hotfolder(self, hotfolder, file_name):
        logging.info(f"Moving file {file_name} to {hotfolder}.")
        new_path = f"{self.HOTFOLDER_PATH}/{hotfolder}/{file_name}"
        current_path = f"{self.HOTFOLDER_PATH}/Hopper/{file_name}"
        
        if os.path.exists(new_path):
            print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
            self.remove_file(new_path)
            time.sleep(5) # Wait for Caldera to recognize the file is gone.
            
        os.rename(current_path, new_path)
        pass


    def save_image(self, job_id, image, path, icc_profile = None):
        if os.path.exists(path):
                    logging.info(f"File already exists in {path}. Removing old file.")
                    self.remove_file(path)
                    time.sleep(5)
        try:
            if icc_profile:
                image.save(path, dpi=(150, 150), icc_profile=icc_profile)
            else:
                image.save(path, dpi=(150, 150))
                
            logging.info(f"Image saved to {path}.")
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error saving image: {e}")
            self.report_error(job_id, f"Error saving image: {e}", self.STOP_JOB_ERROR)
        pass
    
    
    def copy_image(self, job_id, new_path, current_path):
        try:
            logging.info(f"Copying image to {new_path}.")
            time.sleep(2)
            shutil.copy(current_path, new_path)
        except Exception as e:
            logging.error(f"Error copying image: {e}")
            self.report_error(job_id, f"Error copying image: {e}", self.STOP_JOB_ERROR)
        pass


    # Report error to Notion and log file. Report_error will handle writing all errors to the log file.
    def report_error(self, job_id, error_message, level = 0):
        """
        Reports an error by logging it, printing it, and updating a Notion page with the error details.
        Args:
            job_id (str): The ID of the job where the error occurred.
            error_message (str): The error message to be reported.
            level (int, optional): The severity level of the error. Defaults to 0.
        Error Levels:
            - self.DPI_CHANGE_ERROR: Tags the error with "DPI Changed" and "OOS".
            - self.IMAGE_RESIZED_ERROR: Tags the error with "Resized" and "OOS".
            - self.IMAGE_CROPPED_ERROR: Tags the error with "Cropped" and "OOS".
            - self.STOP_JOB_ERROR: Sets the system status to "Error".
            - self.IMAGE_OOS_ERROR: Tags the error with "OOS" and sets the system status to "Error".
            - self.UPDATE_LOG: Only logs the error message.
            - Default: Sets the system status to "Error".
        The method generates appropriate properties based on the error level and updates the Notion page with these properties.
        """
        properties = {}
        
        NOW = datetime.now().strftime('%Y-%m-%d %H:%M')
        
        page_data = self.notion_helper.get_page(job_id)

        if not page_data:
            logging.error(f"Error finding job {job_id}.")
            return
        
        old_log = self.notion_helper.return_property_value(page_data['properties']['Log'], job_id)
        new_log = f"{old_log}\n{NOW} - {error_message}" if old_log else f"{NOW} - {error_message}"
        
        logs = self.notion_helper.generate_property_body("Log", "rich_text", [new_log])
        
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
            
        logging.error(f"Error for job {job_id}: {error_message}")
        self.notion_helper.update_page(job_id, properties)
        pass


    def remove_file(self, file_path):
        logging.info(f"Removing file: {file_path}")
        
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"File {file_path} has been removed.\n")
            
        else:
            print(f"File {file_path} does not exist.\n")
            
        pass


    def cancel_order(self, order_id, sku, reason_canceled, image_source): # Banking on there not being an order with over 100 jobs. If so, pagination needs to be implemented.
        logging.info(f"Cancelling order: {order_id}")
        canceled_job_list = []

        # Generate canceled properties
        canceled_order_prop_body = self.notion_helper.generate_property_body("Status", "select", "Canceled")
        canceled_job_prop_body = self.notion_helper.generate_property_body("Job status", "select", "Canceled")
        
        self.notion_helper.update_page(order_id, canceled_order_prop_body) # Mark order as canceled on Notion.
        time.sleep(.5)
        order_job_id_results = self.notion_helper.get_page_property(order_id, self.ORDER_PROP_JOB_IDS) # Get job IDs related to the order.
        time.sleep(.5)
        order_results = self.notion_helper.get_page(order_id) # Get order information

        order_number = 'Order_Number_Not_Found'
        if order_results['properties']['Order number']['rich_text']:
            order_number = order_results['properties']['Order number']['rich_text'][0]['plain_text']

        try: # Get customer id
            customer_notion_id = order_results['properties']['Customer']['relation'][0]['id']
        except Exception as e:
            self.report_error(order_id,
                f"Order Canceled. Error finding customer ID in order {order_id}: {e}", self.STOP_JOB_ERROR
                )
            return
        time.sleep(.5)
        
        # Get customer email
        customer_email_response = self.notion_helper.get_page_property(customer_notion_id, self.CUSTOMER_PROP_EMAIL_PRIMARY)
        try:
            customer_email_string = customer_email_response['results'][0]['rich_text']['plain_text']
        except Exception as e:
            try:
                customer_email_response = self.notion_helper.get_page_property(customer_notion_id, 
                                                                               self.CUSTOMER_PROP_EMAIL_BACKUP)
                customer_email_string = customer_email_response['email']
            except Exception as e:
                self.report_error(order_id, 
                                  f"Order Canceled. Error finding customer email in order {order_id}: {e}", 
                                  self.STOP_JOB_ERROR)
                return
            
        customer_email_list = re.findall(self.EMAIL_ADDRESS_PATTERN, customer_email_string)

        # Process related job IDs, cancel jobs, and add to canceled job list.
        try:
            if order_job_id_results:
                if order_job_id_results['results']:
                    for each in order_job_id_results['results']:
                        id = each['relation']['id']
                        canceled_job_list.append(id)
                        self.notion_helper.update_page(id, canceled_job_prop_body)
        except Exception as e:
            self.report_error(order_id, 
                              f"Order Canceled. Error canceling jobs for order {order_id}: {e}", 
                              self.STOP_JOB_ERROR)
            
        if len(canceled_job_list) == 0:
            logging.error(f"No jobs found for order {order_id}.")
            error_prop_body = self.notion_helper.generate_property_body("System status", "select", "Error")
            self.notion_helper.update_page(order_id, error_prop_body)
        logging.info(f"Canceled Jobs: {canceled_job_list}")

        # Send cancelation email
        print("Preparing cancelation email...")

        with open(self.BLANK_CONFIG_PATH, 'r') as file: # Load default config file.
            blank_to_email_conf = json.load(file)
            
        blank_to_email_conf['to_email'] = customer_email_list # Add customer email to config.
        
        with open(self.TEMP_CONFIG_PATH, 'w') as temp_file:
            json.dump(blank_to_email_conf, temp_file, indent=4) # Write to temp config file.

        subject = f"Order Cancelation Notice: {order_number}"
        body = f"""
        Notice date: {time.strftime('%m-%d-%Y')} 
        Order: {order_number}
        Product: {sku}
        Image Problem: {reason_canceled}
        Image Source: {image_source}

        One or more images did not meet the required product specifications for production. We cannot proceed with the order as it stands, and have canceled order {order_number}. Please check that the submitted images comply with the product templates and make any necessary corrections prior to resubmitting the order.

        If you have any questions, please contact customer support at ondemand@menoenterprises.com.
        """
        self.automated_emails.send_email(self.TEMP_CONFIG_PATH, subject, body)
        logging.info(f"Cancelation email sent to {customer_email_list}.")

        # Record canceled order in file
        with open(self.CANCELED_ORDER_PATH, 'a') as file:
            file.write(f"{order_id}\n")
        self.canceled_orders.append(order_id)
        
        logging.info(f"Order {order_id} has been canceled. SKU: {sku}")
        pass


    def open_image(self, image_path):
        try:
            image = Image.open(image_path)
        except Exception as e:
            logging.error(f"Error opening image: {e}")
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
            
            if os.path.exists(file_path) == False: # Loops until the file is finished copying.
                self.process_new_file(old_path)
                return None
            
        if "(1)" in file_path:
            logging.info(f"File {file_path} is a duplicate. Removing.")
            self.remove_file(file_path)
            return None

        allow_alter = 0
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        print(f"{now} - Processing file: {file_path}")
        file_name = os.path.basename(file_path)

        try:
            job_id = re.search(self.DATABASE_REGEX, file_name, re.IGNORECASE).group(1)
            print(f"Job ID: {job_id}")
            extension = re.search(self.EXTENSION_REGEX, file_name, re.IGNORECASE).group(2)
            print(f"Extension: {extension}")

            # Check if database ID is present in file name
            if job_id == None: 
                logging.error(f"Could not find database ID in {file_name}. Skipping.")
                return None

            # Check if file is an image
            if extension is None or extension.lower() not in self.ACCEPTED_EXTENSIONS: 
                logging.info(f"File {file_name} is not an accepted image type. Skipping.")
                self.report_error(job_id, f"{now} - File {file_name} is not an accepted image type. Skipping.", self.STOP_JOB_ERROR)
                return None
            
        except AttributeError:
            logging.error(f"Could not find database ID or image extension in {file_name}. Skipping.")
            return None        
       
        
        # Check if file is a reprint, sends it straight to a hotfolder.
        try:  
            reprint_match = re.search(self.REPRINT_REGEX, file_name, re.IGNORECASE).group(1)
        except AttributeError:
            reprint_match = None
        
        if reprint_match:
            logging.info(f"File {file_name} is a reprint. Pushing to hotfolder.")
            reprint_output = self.notion_helper.get_page(job_id)
            job_log = ""
            
            if reprint_output['properties']['Log']['rich_text']: # Collecting Job Log
                job_log = reprint_output['properties']['Log']['rich_text'][0]['plain_text'] + "\n"
                
            try:   # Variables for reprints
                hotfolder = reprint_output['properties']['Hot folder path']['formula']['string']
            except Exception as e:
                self.report_error(job_id, f"{job_log}LOG - {now} - Missing reprint info in Notion {job_id}: {e}\nSkipping File.", self.STOP_JOB_ERROR)
                return None
            
            # Update reprint status in Notion
            self.notion_helper.update_page(job_id, self.REPRINT_NESTING_PACKAGE)
            self.move_to_hotfolder(hotfolder, file_name)
            return None
        
        # Update job status in Notion
        self.notion_helper.update_page(job_id, self.JOB_NESTING_PACKAGE)
        
        # Get job information from Notion
        print("Querying Notion API for job information...")
        job_output = self.notion_helper.get_page(job_id) 

        job_log = ""
        try:
            if job_output['properties']['Log']['rich_text']: # Collecting Job Log
                job_log = job_output['properties']['Log']['rich_text'][0]['plain_text'] + "\n"
        except Exception as e:
            logging.error(f"Error collecting job log: {e}")
            self.report_error(job_id, f"{job_log}{now} - Error collecting job log: {e}. Critical Error, please notify Aria.", 4)

        try:    
            product_id = job_output['properties']['Product']['relation'][0]['id']
            order_number = job_output['properties']['Order ID']['formula']['string']
            order_id = job_output['properties']['Order']['relation'][0]['id']
            
            image_source = 'Not Found'
            if job_output['properties']['Image source']['rich_text']:
                image_source = job_output['properties']['Image source']['rich_text'][0]['plain_text']
                
        except Exception as e: 
            logging.error(f"Could not find image_source, product ID or customer in job {job_id}. Skipping.")
            self.report_error(job_id, f"{job_log}{now} - Missing product or customer in Notion: {e}", 4)
            self.remove_file(file_path)
            return None

        if order_number in self.canceled_orders:
            logging.info(f"Order {order_number} has been canceled. Skipping.")
            self.remove_file(file_path)
            return
        
        print(f"Getting customer ID.")
        customer_id_response = self.notion_helper.get_page_property(order_id, r"iegJ")
        try:
            customer_id = customer_id_response['results'][0]['relation']['id']
        except Exception as e:
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
            logging.info(f"Customer {customer_id}:{allow_alter}")
        except Exception as e:
            logging.error(f"Error finding customer preflight approval status: {e}")
            self.report_error(job_id, f"{job_log}{now} - Error finding customer preflight approval status: {e}", 4)
            self.remove_file(file_path)
            return None

        product_output = self.notion_helper.get_page(product_id) # Get product information from Notion

        try:    # @Aria: Assign product variables here.
            xpix = product_output['properties']['xpix']['number']
            ypix = product_output['properties']['ypix']['number']
            hotfolder = product_output['properties']['Hot Folder']['select']['name']
            sku = product_output['properties']['Product Code']['title'][0]['plain_text']
        except Exception as e:
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

        logging.info(f"Product ID: {product_id}, xpix: {xpix}, ypix: {ypix}, hotfolder: {hotfolder}")
        size, dpi = self.get_image_info(file_path) # Get image size and DPI
        
        if dpi == -1: # 
            logging.error(f"Error getting image info for {file_path}. Skipping.")
            self.report_error(job_id, f"{job_log}{now} - Image file too large. Either an image issue (check this first) or the maximum image size needs to be increased in the preflighting script.", 4)
            self.remove_file(file_path)
            return None
        
        # Determine which aspect ratio is closer to target aspect ratio.
        if abs(xpix - size[0]) + abs(ypix - size[1]) > abs(xpix - size[1]) + abs(ypix - size[0]): 
            xpix, ypix = ypix, xpix     # Swap xpix and ypix to match rotation. Removes needing to account for rotation probably.
        image_aspect = size[0] / size[1]
        target_aspect = xpix / ypix

        logging.info(f"Image size: {size} at {dpi} DPI, AR: {image_aspect}. Target size: {xpix},{ypix} at 150 DPI, AR: {target_aspect}.")

        # Check if image aspect ratio is within 5% of target aspect ratio. If not, trash the file and report the error.
        if image_aspect <= target_aspect * 0.95 or image_aspect >= target_aspect * 1.05: 
            logging.info(f"Image aspect ratio is outside acceptable fixable range.")
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
                logging.info(f"Image size matches target size, but DPI does not. Adjusting DPI to 150 and moving to {hotfolder}.")
                image = self.open_image(file_path)
                self.adjust_dpi_and_move(image, hotfolder, file_name, job_id)
                self.report_error(job_id, f"{job_log}{now} - Image DPI {dpi} does not match target DPI. Adjusting to 150 DPI and moving to hotfolder.", 1)            
                self.remove_file(file_path) # Remove original file
                return None
            
            else: # Image is correct size and DPI
                logging.info(f"Image size and DPI match target size. Moving to {hotfolder}.")
                self.move_to_hotfolder(hotfolder, file_name)
                return None
            
        elif(allow_alter == 1): # Image size does not match target size. Resize and move to hotfolder.
            logging.info(f"Image size does not match target size. Resizing and moving to {hotfolder}.")
            image = self.open_image(file_path)
            self.resize_image(image, hotfolder, file_name, xpix, ypix, size, job_id, job_log)
            self.report_error(job_id,
                              f"{job_log}{now} - Image size {size} does not match target size ({xpix},{ypix})."+
                              "Customer is on approved preflight list, resizing.", 2)
            self.remove_file(file_path) # Remove original file
            return None
        
        elif(allow_alter == 3): # Image size does not match target size. Crop and move to hotfolder.
            logging.info(f"Image size does not match target size. Cropping and moving to {hotfolder}.")
            image = self.open_image(file_path)
            self.crop_and_move(image, hotfolder, file_name, xpix, ypix, job_id, job_log)
            self.report_error(job_id, f"{job_log}{now} - Image size {size} does not match target size ({xpix},{ypix})."+
                              "Customer on the let it run list, cropping.", -1)
            self.remove_file(file_path) # Remove original file
        
        else: # Image size does not match target size at 150DPI. Cancel order and trash file.
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
    EVENT_HANDLER = HotfolderHandler()

    gc.enable()
    
    with open(CRONITOR_KEY_PATH, "r") as file:
        cronitor_api_key = file.read()

    cronitor.api_key = cronitor_api_key
    MONITOR = cronitor.Monitor("MOD Preflight Script")
    MONITOR.ping(state='run')
    logging.info(f"Monitoring directory: {PATH}")
    tick = 0
 
    try:
        while True:
            tick += 1
            file_list = EVENT_HANDLER.check_directory(PATH)
            if file_list:
                if file_list[0] != "Thumbs.db":
                    EVENT_HANDLER.process_new_file(f"{PATH}/{file_list[0]}")
            
            time.sleep(1)
            
            if tick % PING_CYCLE == 0:
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                print(f"{now} - pong")
                MONITOR.ping()
                
            if tick % GC_CYCLE == 0:
                gc.collect()
                tick = 0
            
            now = time.strftime('%H:%M:%S')
            if now >= STOP_TIME:
                logging.info(f"Time is after {STOP_TIME} EST. Stopping the observer.")
                MONITOR.ping(state='complete')
                break
     
    except KeyboardInterrupt:
        MONITOR.ping(state='complete')

    except Exception as e:
        logging.error(f"Critical Error: {e}")
        MONITOR.ping(state='fail')