from NotionApiHelper import NotionApiHelper
from datetime import datetime, timezone

notion_helper = NotionApiHelper()
content_filter = {"and": [{"property": "Job status", "select": {"does_not_equal": "Canceled"}}, {"property": "Created", "date": {"past_week": {}}}]}
current_time = datetime.now(timezone.utc)
customer_dict = {}
product_dict = {}
total_jobs = 0  # I could do a len(notion_response) but I want to be explicit to check for any errors.
total_items = 0 # I could do a sum of the product_quantity but I want to be explicit to check for any errors.

notion_response = notion_helper.query("f11c954da24143acb6e2bf0254b64079", [r"%7CVjk", r"Ye%40l", r"Mgz%3F", r"KQKT", r"zUY%3F", r"%3AL%5EW"], content_filter)
# print(notion_response)
for page in notion_response:
    # print(page["properties"]["Product ID"]["formula"]["string"])
    customer = page["properties"]["Customer"]["formula"]["string"]
    product_description = page["properties"]["Product Description"]["formula"]["string"]
    created = datetime.fromisoformat(page["created_time"].replace('Z', '+00:00'))
    last_edited = datetime.fromisoformat(page["last_edited_time"].replace('Z', '+00:00'))
    job_status = page["properties"]["Job status"]["select"]["name"]
    reprint_count = page["properties"]["Reprint count"]["formula"]["number"]
    product_quantity = page["properties"]["Quantity"]["number"]
    product_id = page["properties"]["Product ID"]["formula"]["string"]

    job_age = (current_time - created).days
    age_label = f"Day {job_age}" if job_age < 6 else "Day 6+"

    total_jobs += 1
    total_items += product_quantity

    # Add a check for "shipped today"

    if customer not in customer_dict:
        customer_dict[customer] = {
            "Total Jobs": 0,
            "Day 0": 0,
            "Day 1": 0,
            "Day 2": 0,
            "Day 3": 0,
            "Day 4": 0,
            "Day 5": 0,
            "Day 6+": 0,
            "Shipped Today": 0,
            "Queued": 0,
            "Nest": 0,
            "Print": 0,
            "Production": 0,
            "Packout": 0,
            "Completed": 0,
        }

    if product_id not in product_dict:
        product_dict[product_id] = {
            "Total Jobs": 0,
            "Total Items": 0,
            "Day 0": 0,
            "Day 1": 0,
            "Day 2": 0,
            "Day 3": 0,
            "Day 4": 0,
            "Day 5": 0,
            "Day 6+": 0,
            "Total Reprints": 0
        }   

    # Will finish the rest tomorrow. Needs to update the dictionaries with the correct values.

    