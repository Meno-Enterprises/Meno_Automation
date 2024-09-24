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