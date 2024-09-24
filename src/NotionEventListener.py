#!/usr/bin/env python3

from NotionApiHelper import NotionApiHelper
import time, os, requests

class NotionEventListener:
    def __init__(self):
        self.notion_helper = NotionApiHelper()

    def listen(self, db_id = [], trigger = {}):
        first_pass = True
        while True:
            for key in db_id:
                pass
            time.sleep(5)
            first_run = False
            print("Database updated.")

    def notify_webhook(self, webhook_url, data = []):
        pass

    def notify_slack(self, data):
        pass

'''
{
    :and": [
        {
            "property": "Status",
            "select": {
                "equals": "In Progress"
            }
        },
        {
            "property": "Priority",
            "select": {
                "equals": "High"
            }
        }
    ],
    "or": [
        {
            "property": "Status",
            "select": {
                "equals": "In Progress"
            }
        },
        {
            "property": "Priority",
            "select": {
                "equals": "High"
            }
        }
    ]
}
'''


if __name__ == "__main__":
    listener = NotionEventListener()
    listener.listen()