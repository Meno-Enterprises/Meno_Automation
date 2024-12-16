#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
import logging, sys, re, subprocess, requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from io import BytesIO
from PIL import Image as PILImage

notion_helper = NotionApiHelper(header_path="src/headers.json")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler('logs/MOD_Reprint_Mirror_System_Status.log'),
                        logging.StreamHandler()
                    ])

# Create a logger for this module
logger = logging.getLogger(__name__)

WEBHOOK_URL = "https://hook.us1.make.com/2waybxgtfc8utztl6dqi432go83iacj4"

SERVICE_ACCOUNT_FILE = 'cred/green-campaign-438119-v8-17ab715c7730.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

gdrive_credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

drive_service = build('drive', 'v3', credentials=gdrive_credentials)

UPDATE_PACKAGE = {'System status': {'select': {'name': 'Active'}}, 'Trigger': {'number': 0}}

INTERNAL_STORAGE_ID_REGEX = re.compile(r'\d*__(.*)')

def catch_variable():
    if len(sys.argv) == 2:
        page_id = sys.argv[1] # Command line argument
        logger.info(f"Page ID Recieved: {page_id}")
        return page_id
    sys.exit("No Page ID Provided")

def get_page_info(page_id):
    try:
        logger.info(f"Getting page info for {page_id}")
        page_info = notion_helper.get_page(page_id)
        return page_info
    except Exception as e:
        logger.error(f"Error in getting page info: {e}")
        return None

def get_image_mimetype(image_io):
    # Open the image using PIL
    with PILImage.open(image_io) as image:
        # Get the format of the image
        format = image.format
    
    # Map the format to the corresponding MIME type
    mimetype_map = {
        'JPEG': 'image/jpeg',
        'PNG': 'image/png',
        'GIF': 'image/gif',
        'BMP': 'image/bmp',
        'TIFF': 'image/tiff',
        'WEBP': 'image/webp'
    }
    
    try:
        # Return the MIME type
        return mimetype_map[format]
    except KeyError:
        error_message = f"Error in getting image MIME type: Unsupported image format {format}"
        logger.error(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])

def recache_artwork(url, internal_storage_id):
    response = requests.get(url, stream=True)
    
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        error_message = f"Error in recaching artwork: {e}"
        logger.error(error_message, exc_info=True)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
    
    file_io = BytesIO()
    
    for chunk in response.iter_content(chunk_size=8192):
        file_io.write(chunk)
        
    file_io.seek(0)
    
    mimetype = get_image_mimetype(file_io)
    
    media = MediaIoBaseUpload(file_io, mimetype=mimetype, resumable=True)
    updated_file = drive_service.files().update(
        fileId=internal_storage_id,
        media_body=media
    ).execute()
    return

def main():
    page_id = catch_variable()
    page_data = get_page_info(page_id)

    if page_data:
        logger.info(f"Page data found for {page_id}")
        isid = notion_helper.return_property_value(page_data['properties']['Internal storage ID'], page_id)
        source_url = notion_helper.return_property_value(page_data['properties']['Image source'], page_id)
        
        try:
            internal_storage_id = INTERNAL_STORAGE_ID_REGEX.match(isid).group(1)
        except AttributeError as e:
            error_message = f"Internal storage ID not found for page {page_id}."
            logger.error(error_message, exc_info=True)
            subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])

        log_message = notion_helper.return_property_value(page_data['properties']['Log'], page_id)
        
        if not log_message:
            log_message = ""
        else:
            log_message = log_message + "\n"
        
        try:
            recache_artwork(source_url, internal_storage_id)
        except:
            error_message = f"Error in recaching artwork for {page_id}"
            logger.error(error_message, exc_info=True)
            subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
        new_message = f"{log_message}{current_time} - MOD_Recache_Artwork.py - Recaching assets."

        log_message = notion_helper.rich_text_prop_gen("Log", "rich_text", [new_message])
        
        package = {**log_message, **UPDATE_PACKAGE}
        
        notion_helper.update_page(page_id, package)

    else:
        error_message = f"Page data not found for {page_id}"
        logger.info(error_message)
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        error_message = f"MOD_Rechache_Artwork.py - Error in main() recaching artwork: {e}"
        logger.error(error_message, exc_info=True)
        page_id = catch_variable()
        subprocess.run(["python", "src/Notion_Error_Reporter.py", page_id, error_message])
