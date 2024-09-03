from NotionApiHelper import NotionApiHelper
import json

notion_helper = NotionApiHelper()
content_filter = {"and": [{"property": "Job status", "select": {"does_not_equal": "Canceled"}}, {"property": "Created", "date": {"past_week": {}}}]}
notion_response = notion_helper.query("f11c954da24143acb6e2bf0254b64079", [r"%7CVjk", r"_~%7Bv", r"Ye%40l", r"Mgz%3F", r"KQKT", r"zUY%3F", r"%3AL%5EW"], content_filter)
print(json.dumps(notion_response, indent=4))