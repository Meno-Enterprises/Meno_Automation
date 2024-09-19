#!/usr/bin/env python3
# Aria Corona Sept 19th, 2024
# This script is designed to monitor a hotfolder for new files, check if they are images, and process them for preflight.

import time, os, re, shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from NotionApiHelper import NotionApiHelper
from AutomatedEmails import AutomatedEmails
from pathlib import Path
from PIL import Image
import json

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
            b. If customer is not on approved preflight list, report error and trash file.
'''

class HotfolderHandler(FileSystemEventHandler):
    def __init__(self):
        self.notion_helper = NotionApiHelper()
        self.automated_emails = AutomatedEmails()
        self.email_config_path = r"conf/MOD_Preflight_Error_Conf.json"
        self.hotfolder_path = "//192.168.0.178/meno/Hotfolders"
        self.image_pattern = r"(.+)\.(jpg|jpeg|png)$"
        self.db_pattern = r".*_(.*)__\d*"
        self.reprint_pattern = r".*--(REP)-\d*_.*"
        self.accepted_extensions = ['jpg', 'jpeg', 'png']
        path = Path(r'conf/CustomerPreflightApproval.json')
        with open(path, 'r') as file:
            self.customer_preflight_approval = json.load(file)

    def on_created(self, event):
        if event.is_directory:
            return None
        else:
            print(f"New file detected: {event.src_path}")
            # Add your processing logic here
            self.process_new_file(event.src_path)

    def get_image_info(self, image_path):
        print(f"Getting image info for {image_path}.")
        image = Image.open(image_path)
        size = image.size
        dpi = image.info.get('dpi')
        return size, dpi
    
    def adjust_dpi_and_move(self, image, hotfolder, file_name):
        print(f"Adjusting DPI to 150 and moving to {hotfolder}.")
        new_path = f"{self.hotfolder_path}/{hotfolder}/{file_name}"
        if os.path.exists(new_path):
            print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
            self.remove_file(new_path)
            time.sleep(5) # Wait for Caldera to recognize the file is gone.        
        image.save(new_path, dpi=(150, 150)) # Force saves the image to 150 DPI. Doesn't actually do anything to the image, just changes the EXIF data
        try:
            shutil.copy(new_path, f"{self.hotfolder_path}/tmp/{file_name}")
        except Exception as e:
            print(f"Error copying file to tmp folder: {e}")
        pass
    
    def resize_and_move(self, image, hotfolder, file_name, target_xpix, target_ypix, original_size, job_id):
        try:
            print(f"Resizing image to {target_xpix},{target_ypix} and moving to {hotfolder}.")
            new_path = f"{self.hotfolder_path}/{hotfolder}/{file_name}"
            scale_factor_width = target_xpix / original_size[0]
            scale_factor_height = target_ypix / original_size[1]
            scale_factor = max(scale_factor_width, scale_factor_height)
            print(f"Scaling image by {scale_factor}.")

            icc_profile = image.info.get('icc_profile') if 'icc_profile' in image.info else None
            scaled_image = image.resize((int(original_size[0] * scale_factor), int(original_size[1] * scale_factor)), Image.LANCZOS)

            if os.path.exists(new_path):
                print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
                self.remove_file(new_path)
                time.sleep(5) # Wait for Caldera to recognize the file is gone.

            # Determine the bounding box for the cropped image
            left = (scaled_image.size[0] - target_xpix) / 2
            top = (scaled_image.size[1] - target_ypix) / 2
            right = (scaled_image.size[0] + target_xpix) / 2
            bottom = (scaled_image.size[1] + target_ypix) / 2

            if left == 0 and top == 0 and right == scaled_image.size[0] and bottom == scaled_image.size[1]:
                print(f"Image does not need to be cropped. Moving to {hotfolder}.")
                if icc_profile:
                    scaled_image.save(new_path, dpi=(150, 150), icc_profile=icc_profile)
                else:
                    scaled_image.save(new_path, dpi=(150, 150))
                time.sleep(1)
                try:
                    shutil.copy(new_path, f"{self.hotfolder_path}/tmp/{file_name}")
                except Exception as e:
                    print(f"Error copying file to tmp folder: {e}")
                return None
            # Crop the image and save it to the hotfolder.
            print(f"Cropping image to {target_xpix},{target_ypix}.")
            cropped_image = scaled_image.crop((int(left), int(top), int(right), int(bottom)))
            if icc_profile:
                cropped_image.save(new_path, dpi=(150, 150), icc_profile=icc_profile)
            else:
                cropped_image.save(new_path, dpi=(150, 150))
            print(f"Corrected image to {target_xpix},{target_ypix} and moved to {hotfolder}.")
            time.sleep(1)
            try:
                shutil.copy(new_path, f"{self.hotfolder_path}/tmp/{file_name}")
            except Exception as e:
                print(f"Error copying file to tmp folder: {e}")
        except Exception as e:
            print(f"Error resizing image: {e}")
            self.report_error(job_id, f"Error resizing image: {e}\nFind Aria and let her know this broke.", 4)
        pass

    def move_to_hotfolder(self, hotfolder, file_name):
        print(f"Moving file {file_name} to {hotfolder}.")
        new_path = f"{self.hotfolder_path}/{hotfolder}/{file_name}"
        current_path = f"{self.hotfolder_path}/Hopper/{file_name}"
        if os.path.exists(new_path):
            print(f"File {file_name} already exists in {hotfolder}. Removing old file.")
            self.remove_file(new_path)
            time.sleep(5) # Wait for Caldera to recognize the file is gone.
        os.rename(current_path, new_path)
        pass

    def report_error(self, job_id, error_message, level = 0): # level 0: update note only, level 1: DPI Change, lvevl 2: Resize, level 3: stop job, level 4: job/product info related or critical error.
        properties = {}
        subject = f"MOD Preflight Error: {job_id}, level {level}"
        body = f"An error has occurred during preflight for https://notion.so/{job_id}.\n\nError message: {error_message}\n\nThis is an automated email being sent on behalf of Aria Corona, please do not reply. If you have any questions or concerns, please contact Aria directly at"
        notes = self.notion_helper.generate_property_body("Notes", "rich_text", [error_message])
        if level == 1:
            tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["DPI Changed", "OOS"]) # Change job tags here.
        elif level == 2:
            tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["Resized", "OOS"]) # Change job tags here.
            #self.automated_emails.send_email(self.email_config_path, subject, body)
        if level >= 3:
            system_status = self.notion_helper.generate_property_body("System status", "select", "Error")
            #self.automated_emails.send_email(self.email_config_path, subject, body)
            if level == 3:
                tags = self.notion_helper.generate_property_body("Tags", "multi_select", ["OOS"]) 
            properties = {"Notes": notes["Notes"], "System status": system_status["System status"], "Tags": tags["Tags"]}
        else:
            properties = {"Notes": notes["Notes"], "Tags": tags["Tags"]}
        print(f"Reporting error for job {job_id}: {error_message}\n{properties}")
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

    def process_new_file(self, file_path):
        if ".~#~" in file_path:  
            print(f"File {file_path} is a temporary file. Waiting.")
            file_path = file_path.replace(".~#~", "")
            time.sleep(5)
            if os.path.exists(file_path) == False:
                print(f"File took too long to copy, skipping.")
                return None
        time.sleep(5) # Wait for file to finish copying
        allow_alter = 0
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - Processing file: {file_path}")
        file_name = os.path.basename(file_path)

        try:
            job_id = re.search(self.db_pattern, file_name, re.IGNORECASE).group(1)
            print(f"Job ID: {job_id}")
            extension = re.search(self.image_pattern, file_name, re.IGNORECASE).group(2)
            print(f"Extension: {extension}")

            # Check if file is an image
            if extension is None or extension.lower() not in self.accepted_extensions: 
                print(f"File {file_name} is not an accepted image type. Skipping.")
                return None
            
            # Check if database ID is present in file name
            if job_id == None: 
                print(f"Could not find database ID in {file_name}. Skipping.")
                return None
        except AttributeError:
            print(f"Could not find database ID or image extension in {file_name}. Skipping.")
            return None        
       
        # Check if file is a reprint, sends it straight to a hotfolder.
        try:  
            reprint_match = re.search(self.reprint_pattern, file_name, re.IGNORECASE).group(1)
        except AttributeError:
            reprint_match = None
        print(f"Reprint: {reprint_match}")
        if reprint_match: 
            print(f"File {file_name} is a reprint. Pushing to hotfolder.")
            reprint_output = self.notion_helper.get_page(job_id)
            try:    # @Aria: Assign reprint variables here.
                hotfolder = reprint_output['properties']['Hot folder path']['formula']['string']
            except Exception as e:
                print(f"Missing info for reprint {job_id} in Notion. Skipping.")
                self.report_error(job_id, f"Missing reprint info in Notion: {e}", 4)
                return None
            self.move_to_hotfolder(hotfolder, file_name)
            return None
        
        # Get job information from Notion
        print("Querying Notion API for job information...")
        job_output = self.notion_helper.get_page(job_id) 

        try:    # @Aria: Assign job variables here.
            product_id = job_output['properties']['Product']['relation'][0]['id']
            customer = job_output['properties']['Customer']['formula']['string']
        except Exception as e: 
            print(f"Could not find product ID in job {job_id}. Skipping.")
            self.report_error(job_id, f"Missing product or customer in Notion: {e}", 4)
            self.remove_file(file_path)
            return None

        try:    # Assigns preflight approval status for customer from JSON file
            allow_alter = self.customer_preflight_approval[customer]
        except KeyError:
            print(f"Customer {customer} not found in preflight approval list. Skipping.")
            self.report_error(job_id, f"Customer not found in preflight approval list.", 4)
            self.remove_file(file_path)
            return None

        product_output = self.notion_helper.get_page(product_id) # Get product information from Notion

        try:    # @Aria: Assign product variables here.
            xpix = product_output['properties']['xpix']['number']
            ypix = product_output['properties']['ypix']['number']
            hotfolder = product_output['properties']['Hot Folder']['select']['name']
        except Exception as e:
            print(f"Missing info for {product_id} in Notion. Skipping. {e}")
            self.report_error(job_id, f"Missing product info in Notion: {e}", 4)
            self.remove_file(file_path)
            return None
        
        if xpix == None or ypix == None or hotfolder == None: 
            print(f"Missing product info for {product_id} in Notion. Skipping.")
            self.report_error(job_id, "Missing product info in Notion.", 4)
            self.remove_file(file_path)
            return None

        print(f"Product ID: {product_id}, xpix: {xpix}, ypix: {ypix}, hotfolder: {hotfolder}")
        size, dpi = self.get_image_info(file_path) # Get image size and DPI
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
            self.report_error(job_id, "Image aspect ratio is outside acceptable fixable range.", 3)
            self.remove_file(file_path)
            return None
        
        # Check if image size matches target size within 38 pixels (1/4 inch) at 150 DPI. 
        if((xpix - 38 <= size[0] <= xpix + 38 and ypix - 38 <= size[1] <= ypix + 38) or 
            (xpix - 38 <= size[1] <= xpix + 38 and ypix - 38 <= size[0] <= ypix + 38)): 
            if(dpi != (150, 150)): # Correct Size, but DPI is wrong. Adjust DPI and move to hotfolder.
                print(f"Image size matches target size, but DPI does not. Adjusting DPI to 150 and moving to {hotfolder}.")
                image = Image.open(file_path)
                self.adjust_dpi_and_move(image, hotfolder, file_name)
                self.report_error(job_id, f"Image DPI {dpi} does not match target DPI. Adjusting to 150 DPI and moving to hotfolder.", 1)
                self.remove_file(file_path) # Remove original file
                return None
            else: # Image is correct size and DPI
                print(f"Image size and DPI match target size. Moving to {hotfolder}.")
                self.move_to_hotfolder(hotfolder, file_name)
                return None
        elif(allow_alter == 1): # Image size does not match target size. Resize and move to hotfolder.
            print(f"Image size does not match target size. Resizing and moving to {hotfolder}.")
            image = Image.open(file_path)
            self.resize_and_move(image, hotfolder, file_name, xpix, ypix, size, job_id)
            self.report_error(job_id, f"Image size {size} does not match target size ({xpix},{ypix}). Customer is on approved preflight list, resizing.", 2)
            self.remove_file(file_path) # Remove original file
            return None
        else: # Image size does not match target size at 150DPI. Report error and trash file.
            print(f"Image size does not match target size. Trashing file.")
            self.report_error(job_id, "Image size does not match target size at 150DPI. Customer not on approved preflight list. Not sent to hotfolder.", 3)
            self.remove_file(file_path)
            return None
        

if __name__ == "__main__":
    auto_emails = AutomatedEmails()
    path = r"\\192.168.0.178\meno\Hotfolders\Hopper"  # Replace with the path to your hotfolder
    event_handler = HotfolderHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()
    print(f"Monitoring directory: {path}")
    email_config_path = "conf/MOD_Preflight_Launch_Conf.json"
    subject = "MOD Preflight Script Launch"
    body = f"MOD Preflight Script has been started at {time.strftime('%Y-%m-%d %H:%M:%S')}.\n\n\nThis is an automated email being sent on behalf of Aria Corona, please do not reply. If you have any questions or concerns, please contact Aria directly at acorona@menoenterprises.com"
    auto_emails.send_email(email_config_path, subject, body)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    except Exception as e:
        print(f"Critical Error: {e}")
        error_subject = "MOD Preflight Script Critical Error"
        error_body = f"MOD Preflight Script has encountered a critical error at {time.strftime('%Y-%m-%d %H:%M:%S')}. Please check the script."
        auto_emails.send_email(email_config_path, error_subject, error_body)
        observer.stop()
    observer.join()