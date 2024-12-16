#!/usr/bin/env python3
'''
Aria Corona December 9th, 2024

This script generates nest labels for jobs and reprints, processes the content, and uploads the generated labels to Google Drive.
It interacts with Notion API to fetch and update page information, and uses ReportLab to generate PDF labels.
Modules:
    - MOD_Generate_Nest_Labels_Logger: Custom logger for the script.
    - NotionApiHelper: Helper class for interacting with Notion API.
    - svglib.svglib: Converts SVG files to ReportLab drawing objects.
    - PIL: Python Imaging Library for image processing.
    - io: Core tools for working with streams.
    - google.oauth2.service_account: Google OAuth2 service account credentials.
    - googleapiclient.discovery: Google API client library.
    - googleapiclient.http: HTTP utilities for Google API client.
    - reportlab.lib.pagesizes: Page size definitions for ReportLab.
    - reportlab.lib.units: Unit definitions for ReportLab.
    - reportlab.graphics: Graphics utilities for ReportLab.
    - reportlab.lib.utils: Utility functions for ReportLab.
    - reportlab.lib.styles: Style definitions for ReportLab.
    - reportlab.platypus: High-level layout and document generation for ReportLab.
    - reportlab.pdfgen: PDF generation utilities for ReportLab.
    - reportlab.pdfbase: Base utilities for PDF generation in ReportLab.
    - math: Mathematical functions.
    - sys: System-specific parameters and functions.
    - logging: Logging utilities.
    - datetime: Basic date and time types.
    - json: JSON encoder and decoder.
    - qrcode: QR code generation library.
    - re: Regular expression operations.
    - uuid: UUID generation library.
Pip Dependencies:
    - Pillow
    - svglib
    - google-auth
    - google-auth-oauthlib
    - google-auth-httplib2
    - google-api-python-client
    - reportlab
    - qrcode[pil]
    - requests
    - python-dateutil
Constants:
    - Various constants for Notion database IDs, Google Drive folder IDs, PDF generation settings, and label layout settings.
Functions:
    - catch_variable(): Retrieves the nest ID from command-line arguments.
    - report_error(id, error_message): Logs an error message and updates the corresponding page in Notion with the error log.
    - get_page_info(id): Retrieves page information from Notion.
    - update_page_info(id, package): Updates page information in Notion.
    - update_nest_page_info(content_dict, label_dict, file_id): Updates the nest page information with the provided label URL and completion status.
    - process_nest_content(nest_id, jobs, reprints): Queries the jobs and reprints databases for their page content.
    - generate_qr_code(qr_value, fill_color, back_color): Generates a QR code with the given value and returns it as an SVG image in a BytesIO object.
    - generate_thumbnail(isid, page_id): Generates a thumbnail for the given internal storage ID and returns the image in memory and its height.
    - save_image_to_memory(image, format, quality): Saves an image to memory.
    - download_file_from_drive(file_id): Downloads a file from Google Drive.
    - upload_file_to_drive(file_io, file_name, mime_type, folder_id): Uploads a file to Google Drive.
    - process_jobrep_content(content_dict): Processes job and reprint content from a given content dictionary and generates a list of label dictionaries.
    - truncate_text(text, max_width, font_name, font_size): Truncates text to fit within a specified width.
    - draw_svg_on_canvas(c, svg_io, x, y, max_width, max_height): Draws an SVG image on a ReportLab canvas with specified maximum width and height.
    - draw_label(c, label, x, y, notion_logo, shipstation_logo): Draws a label on the given canvas at the specified coordinates.
    - load_logo(logo_path): Loads a logo image from the given path.
    - generate_labels(label_dict): Generates a PDF containing labels based on the provided label dictionary.
    - main(): Main function that orchestrates the label generation process.
Usage:
    Run the script with the nest ID as a command-line argument.
'''


from MOD_Generate_Nest_Labels_Logger import logger
from NotionApiHelper import NotionApiHelper
from svglib.svglib import svg2rlg
from PIL import Image, ImageEnhance
from io import BytesIO
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.graphics import renderPDF
from reportlab.lib.utils import ImageReader
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from math import floor
import sys, logging, datetime, json, qrcode, re, uuid
import qrcode.image.svg


notion = NotionApiHelper()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler('logs/MOD_Generate_Nest_Labels.log'),
    logging.StreamHandler()
])
logger = logging.getLogger(__name__)

SERVICE_ACCOUNT_FILE = 'cred/green-campaign-438119-v8-17ab715c7730.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

gdrive_credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

drive_service = build('drive', 'v3', credentials=gdrive_credentials)

LABEL_GEN_PACKAGE = {'Print Status': {'select': {'name': 'Label generating'}}, 'Regenerate Trigger': {'number': 0}}
LABEL_CREATED_PACKAGE = {'Print Status': {'select': {'name': 'Label created'}}}
LABEL_ERROR_PACKAGE = {'System status': {'select': {'name': 'Error'}}}
jobrep_label_url_package = {'Label Printed': {'select': {'name': 'Printed'}}, "Label URL": {'url': None}}

# Names of the related properties that need to be checked for label information.
LIST_NAMES = ['Jobs', 'Reprints']

NEST_LOG_PROP_ID = "%3BNMV"

# Notion database IDs.
JOB_DB_ID = 'f11c954da24143acb6e2bf0254b64079'
REPRINT_DB_ID = 'f631a4f09c27427dbe70f4d7a2e61e9c'

# Google Drive folder IDs.
THUMBNAIL_FOLDER_ID = '1hBeSlW4h56-BmygGdeNCeZF--1gTb9Zm'
PDF_FOLDER_ID = '1HuAFqh8ITutdjSOBxo-qKU73ujVNFzMB'
COPY_FOLDER_ID = '1BL6BxkJR7GV067DKk8z1-qXlnlOg4cTM'
FILE_VIEWER_URL = 'https://drive.google.com/file/d/#ID#/view'

EPSON_LIBRARY = {
    'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1C': 'EPSON_B',
    'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1E': 'EPSON_D',
    'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1F': 'EPSON_E',
    'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1G': 'EPSON_F'
}


# PDF generation constants.
PDF_OUTPUT_PATH = 'output/mod/labels.pdf'
NOTION_LOGO_PATH = 'assets/mod/generate_nest_labels/Notion_app_logo_thumbnail.png'
SHIPSTATION_LOGO_PATH = 'assets/mod/generate_nest_labels/ShipStation_app_logo_thumbnail.png'
INTERNAL_STORAGE_ID_REGEX = re.compile(r'\d*__(.*)')
NEST_NAME_REGEX = re.compile(r'(\w*\s\#\d*)')

PAGE_WIDTH = 8.5 * inch # pixels, letter size
PAGE_HEIGHT = 11 * inch # pixels, letter size

LABELS_PER_PAGE = 10
LABEL_COLLUMNS = 2
LABEL_ROWS = 5

LABEL_WIDTH = 4 * inch # pixels
LABEL_HEIGHT = 2 * inch # pixels

PAGE_LR_MARGIN = floor((PAGE_WIDTH - (LABEL_WIDTH * 2)) / 2)
PAGE_TB_MARGIN = floor((PAGE_HEIGHT - (LABEL_HEIGHT * 5)) /2)

LABEL_FONT = 'Courier'
FONT_SIZE = 10 # pt
LABEL_FONT_BOLD = 'Courier-Bold'
BOLD_FONT_SIZE = 12 # pt

PADDING = 3
CENTER_DEVISOR = 5

HEADER_PLACEMENT = (PAGE_LR_MARGIN, PAGE_HEIGHT - PAGE_TB_MARGIN + (PADDING*2))
HEADER_FONT_SIZE = 20

THUMBNAIL_MAX_SIZE = (110, 144) # pixels
THUMBNAIL_POS = (0, 0)

QR_CODE_MAX_SIZE = (floor(LABEL_HEIGHT * (2/5)), floor(LABEL_HEIGHT * (2/5))) # pixels
QR_CODE_1_POS = (THUMBNAIL_MAX_SIZE[0] + PADDING, 0) 
QR_CODE_2_POS = (LABEL_WIDTH - QR_CODE_MAX_SIZE[0] - PADDING, 0)

LOGO_MAX_SIZE = (20, 20)
NOTION_LOGO_POS = (QR_CODE_2_POS[0] - LOGO_MAX_SIZE[0] - PADDING, (QR_CODE_MAX_SIZE[1] - LOGO_MAX_SIZE[1]) / 2)
SHIPSTATION_LOGO_POS = (QR_CODE_1_POS[0] + QR_CODE_MAX_SIZE[0] + PADDING, (QR_CODE_MAX_SIZE[1] - LOGO_MAX_SIZE[1]) / 2)

ROW_NEST = (THUMBNAIL_MAX_SIZE[0] + PADDING, QR_CODE_MAX_SIZE[1]+PADDING)
ROW_NEST_MAX_WIDTH = 95 # pixels

SHIP_BY_ROW = (ROW_NEST[0] + ROW_NEST_MAX_WIDTH, QR_CODE_MAX_SIZE[1] + PADDING)

PROD_DESCRIPTION = (THUMBNAIL_MAX_SIZE[0] + PADDING, ROW_NEST[1] + BOLD_FONT_SIZE + PADDING)
PROD_DESCRIPTION_MAX_WIDTH = LABEL_WIDTH - THUMBNAIL_MAX_SIZE[0]
PROD_DESCRIPTION_MAX_HEIGHT = 30 # pixels

ORDER_NUMBER = (THUMBNAIL_MAX_SIZE[0] + PADDING, PROD_DESCRIPTION[1] + PROD_DESCRIPTION_MAX_HEIGHT + PADDING)
ORDER_NUMBER_MAX_WIDTH = LABEL_WIDTH - THUMBNAIL_MAX_SIZE[0]

ITEM_QUANT = (THUMBNAIL_MAX_SIZE[0] + PADDING, ORDER_NUMBER[1] + FONT_SIZE + PADDING)
ITEM_QUANT_MAX_WIDTH = floor((LABEL_WIDTH - THUMBNAIL_MAX_SIZE[0]) / 2)

JOB_QUANT = (ITEM_QUANT[0] + ITEM_QUANT_MAX_WIDTH, ORDER_NUMBER[1] + FONT_SIZE + PADDING)
JOB_QUANT_MAX_WIDTH = ITEM_QUANT_MAX_WIDTH

ROW_CUSTOMER = (THUMBNAIL_MAX_SIZE[0] + PADDING, ITEM_QUANT[1] + FONT_SIZE + PADDING)
ROW_CUSTOMER_MAX_WIDTH = floor((LABEL_WIDTH - THUMBNAIL_MAX_SIZE[0]) * (2/3))

ROW_UID = (ROW_CUSTOMER[0] + ROW_CUSTOMER_MAX_WIDTH, ITEM_QUANT[1] + FONT_SIZE + PADDING)


def catch_variable():
    try:
        nest_id = sys.argv[1]
        return nest_id
    except IndexError:
        logger.error("Nothing passed to MOD_Generate_Nest_Labels.py. Exiting.")
        sys.exit(1)


def report_error(id, error_message):
    """
    Logs an error message and updates the corresponding page in Notion with the error log.
    Args:
        id (str): The ID of the Notion page to update.
        error_message (str): The error message to log and update in the Notion page.
    Returns:
        None
    """
    
    logger.error(f"Error: {error_message}")
    
    # Get old log, add new log, update page
    now = datetime.datetime.now()
    
    old_log = notion.get_page_property(id, NEST_LOG_PROP_ID)
    error_log = f"{now}::{error_message}\n{old_log}" if old_log else f"{now}::{error_message}"
    log_package = notion.rich_text_prop_gen('Logs', 'rich_text', error_log)
    package = {**LABEL_ERROR_PACKAGE, **log_package}
    
    notion.update_page(id, package)
    return


def get_page_info(id):
    logger.info(f"Getting page info - {id}")
    response = notion.get_page(id)
    
    if not response:
        logger.error("No response returned. Exiting.")
        sys.exit(1)
        
    if 'properties' not in id:
        logger.error("No properties found in response. Exiting.")
        sys.exit(1)
    
    logger.info("Response returned.")
    return response


def update_page_info(id, package):
    logger.info(f"Updating page info - {id}\n{package}")
    response = notion.update_page(id, package)
    
    if not response:
        error_message = "No response returned when updating page info."
        report_error(id, error_message)
        sys.exit(1)
        
    if 'properties' not in response:
        error_message = "No properties found in response when updating page info."
        report_error(id, error_message)
        sys.exit(1)
    
    logger.info("Response returned.")
    return response


def update_nest_page_info(content_dict, label_dict, file_id):
    """
    Updates the nest page information with the provided label URL and completion status.
    Args:
        content_dict (dict): A dictionary containing the content information for various job representations.
        label_dict (dict): A dictionary containing the label information.
        file_id (str): The file identifier used to generate the label URL.
    Returns:
        None
    """
    
    
    logger.info("Updating nest page info.")
    
    label_url = f"{FILE_VIEWER_URL.replace('#ID#', file_id)}"
    logger.info(f"Label URL: {label_url}")
    
    for jobrep in LIST_NAMES:
        logger.info(f"Updating {jobrep} pages with label URL.")
        if jobrep in content_dict:
            for page in content_dict[jobrep]:
                page_id = page['id']
                
                logger.info(f"Updating page {page_id} with label URL.")
                if 'Label URL' not in page['properties']:
                    logger.error(f"Label URL property not found in page {page_id}.")
                    continue
                
                existing_labels = notion.return_property_value(page['properties']['Label URL'], page_id)
                labels = f"{label_url}, {existing_labels}" if existing_labels else label_url
                jobrep_label_url_package['Label URL']['url'] = labels
                
                logging.info(f"Updating page {page_id} with {label_url}.")
                logger.info(json.dumps(jobrep_label_url_package))
                response = notion.update_page(page_id, jobrep_label_url_package)
                
    logging.info("Updating nest page with completion status.")
    response = notion.update_page(content_dict['Nest']['id'], LABEL_CREATED_PACKAGE)            
                

def process_nest_content(nest_id, jobs, reprints):
    """
    Creates a filter including an or statement for each job/rep ID. Queries the jobs and 
    reprints databases for their page content

    Args:
        nest_id (str): The ID of the nest being processed.
        jobs (list): A list of job page IDs to be queried.
        reprints (list): A list of reprint page IDs to be queried.
    Returns:
        dict: A dictionary containing the queried jobs and reprints, with keys 'Jobs' and 'Reprints'.
    """
    
    logging.info(f"Processing nest {nest_id} content.")
    
    content_dict = {}
    
    # Need to query jobs and reprints separately
    for prop_name, list in [('Jobs', jobs), ('Reprints', reprints)]:
        content_filter = {'or': []}
        
        if list:
            # Create filter for each page_id in list, one query per db as opposed to one get request per page_id.
            for page_id in list:
                filter_template = {'property': 'Notion record', 'formula': {'string': {'contains': page_id.replace("-", "")}}}
                content_filter['or'].append(filter_template)        
            
            # Get jobs or reprints page content
            db_id = JOB_DB_ID if prop_name == 'Jobs' else REPRINT_DB_ID
            logger.info(json.dumps(content_filter))
            response = notion.query(db_id, content_filter=content_filter)
            
            # Add page data to content_dict
            content_dict[prop_name] = response

    return content_dict


def generate_qr_code(qr_value, fill_color = 'black', back_color = 'white'):
    """
    Generates a QR code with the given value and returns it as an SVG image in a BytesIO object.
    Args:
        qr_value (str): The value to encode in the QR code.
        fill_color (str, optional): The color of the QR code. Defaults to 'black'.
        back_color (str, optional): The background color of the QR code. Defaults to 'white'.
    Returns:
        BytesIO: A BytesIO object containing the SVG image of the generated QR code.
    """
    
    logger.info(f"Generating QR code for {qr_value}")
    
    qr = qrcode.QRCode(
        version=3,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=6,
        border=0,
    )
    
    qr.add_data(qr_value)
    qr.make(fit=True)
    
    factory = qrcode.image.svg.SvgImage
    qr_code_io = BytesIO()
    qr_code_image = qr.make_image(image_factory=factory, fill_color=fill_color, back_color=back_color)
    qr_code_image.save(qr_code_io)
    
    return qr_code_io


def generate_thumbnail(isid, page_id): #isid:internal_storage_id
    """
    Generates a thumbnail for the given internal storage ID (isid) and returns the image in memory and its height.
    Args:
        isid (str): The internal storage ID of the file to generate a thumbnail for.
        page_id (str): The page ID associated with the thumbnail.
    Returns:
        tuple: A tuple containing:
            - image_io (BytesIO): The in-memory image file of the generated thumbnail.
            - image_height (int): The height of the generated thumbnail image.
    Raises:
        Exception: If there is an error in downloading the file, opening the image, or saving the image to memory.
    """
    
    logger.info(f"Generating thumbnail for {isid}")
    
    source_fh = download_file_from_drive(isid)
    
    with Image.open(BytesIO(source_fh.read())) as image:
            enhancer = ImageEnhance.Sharpness(image)
            image = enhancer.enhance(2)
            
            image_height = image.size[1]
            image_width = image.size[0]
            
            
            
            if image_height < image_width:
                thumbnail_size = (THUMBNAIL_MAX_SIZE[0], int(image_height * (THUMBNAIL_MAX_SIZE[0] / image_width)))
            else:
                thumbnail_size = (int(image_width * (THUMBNAIL_MAX_SIZE[0] / image_width)), THUMBNAIL_MAX_SIZE[1])
            
            image.thumbnail((thumbnail_size[0]*3, thumbnail_size[1]*3), Image.LANCZOS)
            
            image_io = save_image_to_memory(image)

    
    return image_io, thumbnail_size


def save_image_to_memory(image, format='PNG', quality=100):
    logger.info("Saving image to memory.")
    
    if image.mode != 'RGB':
        image = image.convert('RGB')
    
    image_io = BytesIO()
    image.save(image_io, format=format, quality=quality)
    image_io.seek(0)
    
    return image_io


def download_file_from_drive(file_id):
    """
    Downloads a file from Google Drive using the given file ID.
    Args:
        file_id (str): The ID of the file to be downloaded from Google Drive.
    Returns:
        BytesIO: A BytesIO object containing the downloaded file's content.
    """
    
    logger.info(f"Downloading file {file_id} from Google Drive.")
    
    request = drive_service.files().get_media(fileId=file_id)
    fh = BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logger.info(f"Download {int(status.progress() * 100)}%.")
    
    fh.seek(0)
    return fh


def upload_file_to_drive(file_io, file_name, mime_type, folder_id):
    """
    Uploads a file to Google Drive.
    Args:
        file_io (io.BytesIO): The file-like object to upload.
        file_name (str): The name of the file to be uploaded.
        mime_type (str): The MIME type of the file.
        folder_id (str): The ID of the Google Drive folder where the file will be uploaded.
    Returns:
        str: The ID of the uploaded file if successful, None otherwise.
    Raises:
        Exception: If there is an error during the file upload process.
    """
    
    logger.info(f"Uploading file {file_name} to Google Drive.")
    
    file_io.seek(0)
    
    file_metadata = {
        'name': file_name,
        'parents': [folder_id]
        }
    
    media = MediaIoBaseUpload(file_io, mimetype=mime_type)
    
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    )
    
    try:
        response = file.execute()
    except Exception as e:
        logger.error(f"Error uploading file to Google Drive: {e}", exc_info=True)
        return None
    
    logger.info(f"File ID: {response.get('id')}")
    return response.get('id')


def process_jobrep_content(content_dict):
    """
    Processes job and reprint content from a given content dictionary and generates a list of label dictionaries.
    Args:
        content_dict (dict): A dictionary containing job and reprint content. The dictionary is expected to have 
                                keys corresponding to job and reprint names, each containing a list of pages with 
                                properties.
    Returns:
        list: A list of dictionaries, each representing a label with various properties such as page_id, ship_date, 
                order_number, nest_name, quantity, product_description, customer, shipstation_qr_code, qr_code, 
                thumbnail, thumbnail_size, line_code, uid, and label_urls.
    Raises:
        Exception: If there is an error parsing the ship date or any other unexpected error occurs during processing.
    """    
    
    logger.info("Processing job and reprint content.")
    
    # Get nest name from nest page
    nest_name = notion.return_property_value(
        content_dict['Nest']['properties']['Name'], content_dict['Nest']['id'])
    nest_name = NEST_NAME_REGEX.match(nest_name).group(1)
    
    printer_id = notion.return_property_value(
        content_dict['Nest']['properties']['Device ID'], content_dict['Nest']['id'])
    if printer_id in EPSON_LIBRARY:
        printer_name = EPSON_LIBRARY[printer_id]
    else:
        printer_name = "N/a"
    
    # Initialize label_dict with template
    label_dict = []
    label_dict_template = {
        'page_id': None,
        'ship_date': None,
        'order_number': None,
        'nest_name': nest_name,
        'printer_name': printer_name,
        'quantity': None,
        'product_description': None,
        'customer': None,
        'shipstation_qr_code': None,
        'qr_code': None,
        'thumbnail': None,
        'thumbnail_size': THUMBNAIL_MAX_SIZE[1],
        'line_code': None,
        'uid': None,
        'label_urls': []
    }

    # Iterate through jobs and reprints
    for jobrep in LIST_NAMES:
        if jobrep in content_dict:
            for page in content_dict[jobrep]:
    
                
                single_label_dict = label_dict_template.copy()
                page_props = page['properties']
                page_id = page['id']
                notion_link = f'https://www.notion.so/menoenterprises/{page_id.replace("-", "")}'
                
                # Get internal storage ID to get artwork for thumbnail.
                internal_storage_id = notion.return_property_value(page_props['Internal storage ID'], page_id)
                
                try:
                    internal_storage_id = INTERNAL_STORAGE_ID_REGEX.match(internal_storage_id).group(1)
                except AttributeError as e:
                    logger.error(f"Internal storage ID not found for page {page_id}.", exc_info=True)
                    internal_storage_id = "1WaC0UYGN5gSk8gxDPBfSlWUlNhOtXGYo"
                
                # Add properties to single_label_dict
                single_label_dict['page_id'] = page_id
                single_label_dict['order_number'] = notion.return_property_value(page_props['Order ID'], page_id)
                single_label_dict['customer'] = notion.return_property_value(page_props['Customer ID'], page_id)
                
                try:
                    single_label_dict['line_code'] = notion.return_property_value(page_props['Line item'], page_id)
                except:
                    single_label_dict['line_code'] = "N/a"
                
                try:
                    if 'Ship date' in page_props:
                        ship_date = notion.return_property_value(page_props['Ship date'], page_id)
                    else:
                        ship_date = notion.return_property_value(page_props['Ship Date'], page_id)
                        
                    if ship_date:
                        single_label_dict['ship_date'] = datetime.datetime.strptime(
                            ship_date, '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%m-%d-%Y')
                    else:
                        logger.error(f"Ship date not found for page {page_id}.")
                        single_label_dict['ship_date'] = "--"    
                    
                except Exception as e:
                    logger.error(f"Error parsing ship date for page {page_id}: {e}", exc_info=True)
                    single_label_dict['ship_date'] = "--"
                    
                single_label_dict['product_description'] = notion.return_property_value(
                    page_props['Product Description'], page_id)
                
                single_label_dict['uid'] = f"{
                    page_props['ID']['unique_id']['prefix']}-{str(page_props['ID']['unique_id']['number'])}"
                
                single_label_dict['label_urls'] = notion.return_property_value(page_props['Label URL'], page_id)
                
                # Images and QR codes
                single_label_dict['thumbnail'], single_label_dict['thumbnail_size'] = generate_thumbnail(
                    internal_storage_id, page_id)
                single_label_dict['qr_code'] = generate_qr_code(notion_link)
                single_label_dict['shipstation_qr_code'] = generate_qr_code(
                    notion.return_property_value(page_props['Order Title'], page_id), fill_color='green')
                
                # Quantity handling
                try:
                    quantity = int(notion.return_property_value(page_props['Quantity'], page_id))
                except:
                    quantity = int(notion.return_property_value(page_props['Reprint quantity'], page_id))
                
                # Generates one label per quantity
                for i in range(1, quantity+1):
                    single_label_dict['quantity'] = f"{i}-{quantity}"
                    label_dict.append(single_label_dict.copy())
            
    return label_dict


def truncate_text(text, max_width, font_name, font_size):
    while pdfmetrics.stringWidth(text, font_name, font_size) > max_width:
        text = text[:-1]
    
    return text


def draw_svg_on_canvas(c, svg_io, x, y, max_width, max_height):
    """
    Draws an SVG image on a ReportLab canvas with specified maximum width and height.
    Args:
        c (Canvas): The ReportLab canvas object where the SVG will be drawn.
        svg_io (BytesIO): A BytesIO object containing the SVG data.
        x (float): The x-coordinate on the canvas where the SVG will be drawn.
        y (float): The y-coordinate on the canvas where the SVG will be drawn.
        max_width (float): The maximum width for the SVG on the canvas.
        max_height (float): The maximum height for the SVG on the canvas.
    Returns:
        None
    """
    
    # Convert the SVG to a ReportLab drawing object
    drawing = svg2rlg(svg_io)
    
    # Calculate the scaling factor
    scale_x = max_width / drawing.width
    scale_y = max_height / drawing.height
    scale = min(scale_x, scale_y)
    
    # Apply the scaling factor
    drawing.width *= scale
    drawing.height *= scale
    drawing.scale(scale, scale)
    
    # Draw the scaled drawing on the canvas
    renderPDF.draw(drawing, c, x, y)


def draw_label(c, label, x, y, notion_logo, shipstation_logo):
    """
    Draws a label on the given canvas at the specified coordinates.
    Args:
        c (Canvas): The canvas object to draw on.
        label (dict): A dictionary containing label information with the following keys:
            - 'uid' (str): Unique identifier for the label.
            - 'nest_name' (str): Name of the nest.
            - 'customer' (str): Customer name.
            - 'quantity' (int): Item quantity.
            - 'order_number' (str): Order number.
            - 'line_code' (str): Job quantity.
            - 'product_description' (str): Description of the product.
            - 'ship_date' (str): Shipping date.
            - 'qr_code' (BytesIO): QR code image data.
            - 'shipstation_qr_code' (BytesIO): Shipstation QR code image data.
            - 'thumbnail' (BytesIO): Thumbnail image data.
            - 'thumbnail_size' (touple): Size of the thumbnail image.
        x (float): The x-coordinate to start drawing the label.
        y (float): The y-coordinate to start drawing the label.
    Returns:
        None
    """
    
    styles = getSampleStyleSheet()
    styleN = ParagraphStyle(
        'CustomStyle',
        parent=styles['Normal'],
        fontName=LABEL_FONT,
        fontSize=FONT_SIZE,
        leading=11
    )
    
    c.setFont(LABEL_FONT_BOLD, BOLD_FONT_SIZE)
    c.drawString(x+ROW_UID[0], y+ROW_UID[1], f"{label['uid']}")

    truncated_text = truncate_text(label['nest_name'], ROW_NEST_MAX_WIDTH, f"{LABEL_FONT}-Bold", BOLD_FONT_SIZE)
    c.drawString(x+ROW_NEST[0], y+ROW_NEST[1], truncated_text)
    
    c.setFont(LABEL_FONT, FONT_SIZE)
    c.drawString(x+ROW_CUSTOMER[0], y+ROW_CUSTOMER[1], f"{label['customer']}")
    c.drawString(x+ITEM_QUANT[0], y+ITEM_QUANT[1], f"Item Qty:{label['quantity']}")
    
    truncated_text = truncate_text(f"{label['customer']}", ROW_CUSTOMER_MAX_WIDTH, LABEL_FONT, FONT_SIZE)
    c.drawString(x+ROW_CUSTOMER[0], y+ROW_CUSTOMER[1], truncated_text)
    
    truncated_text = truncate_text(f"Order#:{label['order_number']}", ORDER_NUMBER_MAX_WIDTH, LABEL_FONT, FONT_SIZE)
    c.drawString(x+ORDER_NUMBER[0], y+ORDER_NUMBER[1], truncated_text)
    
    truncated_text = truncate_text(f"Job Qty:{label['line_code']}", JOB_QUANT_MAX_WIDTH, LABEL_FONT, FONT_SIZE)
    c.drawString(x+JOB_QUANT[0], y+JOB_QUANT[1], truncated_text)
    
    product_description = Paragraph(label['product_description'], styleN)
    product_description.wrapOn(c, PROD_DESCRIPTION_MAX_WIDTH, PROD_DESCRIPTION_MAX_HEIGHT)
    product_description.drawOn(c, x + PROD_DESCRIPTION[0], y + PROD_DESCRIPTION[1])
    
    c.drawString(x + SHIP_BY_ROW[0], y + SHIP_BY_ROW[1], f"SHIP-BY:{label['ship_date']}")

    if label['qr_code']:
        qr_code_io = BytesIO(label['qr_code'].getvalue())
        qr_code_io.seek(0)
        draw_svg_on_canvas(
            c, qr_code_io, x + QR_CODE_2_POS[0], y + QR_CODE_2_POS[1], QR_CODE_MAX_SIZE[0], QR_CODE_MAX_SIZE[1])
        qr_code_io.close()

    if label['shipstation_qr_code']:
        shipstation_qr_code_io = BytesIO(label['shipstation_qr_code'].getvalue())
        shipstation_qr_code_io.seek(0) 
        draw_svg_on_canvas(
            c, shipstation_qr_code_io, x + QR_CODE_1_POS[0], y + QR_CODE_1_POS[1], QR_CODE_MAX_SIZE[0], QR_CODE_MAX_SIZE[1])
        shipstation_qr_code_io.close()

    if label['thumbnail']:
        thumbnail_io = BytesIO(label['thumbnail'].getvalue())
        thumbnail_io.seek(0)
        
        ypos = y + THUMBNAIL_POS[1] if label['thumbnail_size'][1] == THUMBNAIL_MAX_SIZE[1] else y + int((LABEL_HEIGHT - label['thumbnail_size'][1]) / 2)
        
        c.drawImage(ImageReader(thumbnail_io), x + THUMBNAIL_POS[0], ypos, width=label['thumbnail_size'][0], height=label['thumbnail_size'][1])
        thumbnail_io.close()
        
    c.drawImage(notion_logo, x + NOTION_LOGO_POS[0], y + NOTION_LOGO_POS[1], width=LOGO_MAX_SIZE[0], height=LOGO_MAX_SIZE[1])
    c.drawImage(shipstation_logo,x + SHIPSTATION_LOGO_POS[0], y + SHIPSTATION_LOGO_POS[1], width=LOGO_MAX_SIZE[0], height=LOGO_MAX_SIZE[1])


def load_logo(logo_path):
    with Image.open(logo_path) as logo:
        logo = logo.convert('RGBA')
        return ImageReader(logo)
    

def generate_labels(label_dict):
    """
    Generates a PDF containing labels based on the provided label dictionary.
    Args:
        label_dict (dict): A dictionary where each key-value pair represents a label's data.
    Returns:
        BytesIO: A BytesIO object containing the generated PDF with labels.
    The function performs the following steps:
    1. Initializes a BytesIO object to store the PDF in memory.
    2. Creates a canvas object for drawing the PDF.
    3. Iterates over the label dictionary to draw each label on the PDF.
    4. Calculates the position of each label on the page.
    5. Draws the label on the canvas at the calculated position.
    6. Adds a header to each page and logs the completion of each page.
    7. Saves the canvas to the BytesIO object and logs the completion of the PDF generation.
    """
    
    logger.info("Generating labels.")
    
    pdf_io = BytesIO()
    
    c = canvas.Canvas(pdf_io, pagesize=letter)
    
    notion_logo = load_logo(NOTION_LOGO_PATH)
    shipstation_logo = load_logo(SHIPSTATION_LOGO_PATH)
    
    for index, label in enumerate(label_dict):
        label_counter = index % LABELS_PER_PAGE
        side_indicator = index % LABEL_COLLUMNS
        
        x_pos = PAGE_LR_MARGIN + ((side_indicator) * LABEL_WIDTH)
        x_pos = x_pos - CENTER_DEVISOR if side_indicator == 0 else x_pos + CENTER_DEVISOR
        
        y_pos = PAGE_TB_MARGIN + ((index % LABEL_ROWS) * LABEL_HEIGHT)

        draw_label(c, label, x_pos, y_pos, notion_logo, shipstation_logo)
       
        if label_counter == 0:
            c.setFont(LABEL_FONT_BOLD, HEADER_FONT_SIZE)
            c.drawString(HEADER_PLACEMENT[0], HEADER_PLACEMENT[1], f"{label['nest_name']} {label['printer_name']}")
        if label_counter == 9:
            logging.info(f"Page {index//LABELS_PER_PAGE} complete.")
            c.showPage()
       
    c.save()
    logger.info(f"Labels saved to PDF in memory.")
    return pdf_io


def main():
    unique_id = str(uuid.uuid4())    
    nest_id = catch_variable()
    logger.info(f"[START] - {unique_id} - Nest ID: {nest_id}")
    
    # Update page to Label generating, gets page properties as a response.
    nest_page = update_page_info(nest_id, LABEL_GEN_PACKAGE)
    
    # Get properties from page response
    nest_properties = nest_page['properties']
    
    jobs = []
    reps = []
    
    # Get jobs and reprints for nest
    for each, variable in [('Jobs', jobs), ('Reprints', reps)]:
        if each in nest_properties:
            id_list = notion.return_property_value(nest_properties[each], nest_id)
            logger.info(f"main(): {each} -- {id_list}")
            if id_list:
                variable.extend(id_list)
            else:
                logger.error(f"No {each} found for nest {nest_id}.")
    
    # If no jobs or reprints found, log error and exit.
    if all([not jobs, not reps]):
        log_message = f"No jobs or reprints found for nest {nest_id}."
        report_error(nest_id, log_message)
        sys.exit(1)
        
    # Get content for nest, jobs, and reprints
    content_dict = process_nest_content(nest_id, jobs, reps)
    
    # Add nest data to the content_dict, which includes job and reprint data
    content_dict['Nest'] = nest_page
        
    # Process content for labels
    label_dict = process_jobrep_content(content_dict)
    
    # Generate labels and upload to Google Drive
    pdf_io = generate_labels(label_dict)
    
    # Upload PDF to Google Drive
    filename = f'MOD-{unique_id}_{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")}.pdf'
    file_id = upload_file_to_drive(pdf_io, filename, 'application/pdf', PDF_FOLDER_ID)
    #copied_id = upload_file_to_drive(pdf_io, filename, 'application/pdf', COPY_FOLDER_ID)
    # Close PDF in memory
    pdf_io.close()
    
    # Update nest, jobs, and reps with label URL
    update_nest_page_info(content_dict, label_dict, file_id)
    
    logger.info(f"[END] - {unique_id} - Nest ID: {nest_id}")
    
    
if __name__ == '__main__':
    logger.info("MOD_Generate_Nest_Labels.py started")
    main()