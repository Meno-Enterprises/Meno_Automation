# V1.0 - Aria Corona
# Script to pull Spooler data from a local Caldera server, and sending it to a dedicated webhook.
#
#
import gc
import requests
from datetime import datetime
import time

pullTimer = 60  # seconds
# idPrinter1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1B'  # Epson A
# idPrinter2 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1C'  # Epson B
idPrinter1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1E' # Epson D
idPrinter2 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1F' # Epson E
# ip1 = '192.168.0.39'  # Master Dell PC IP
ip1 = '192.168.0.90' # Secondary Dell PC IP
# ip2 = '192.168.0.151'  # Alienware, currently not relevant
pullStore1 = {}
pullStore2 = {}  # If adding additional printers, add more of these variables.
webhookURL = 'webhookURL'
urlPrinter1 = 'http://' + ip1 + ':12340/v1/jobs?idents.device=' + idPrinter1 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished'
urlPrinter2 = 'http://' + ip1 + ':12340/v1/jobs?idents.device=' + idPrinter2 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=finished'
loopCount = 0


# This will soon be made redundant. We will soon skip sending data to the webhook and instead process the data and send it directly to Notion.
# Sends pulled JSON data to the provided webhook. Bad requests will continue to retry until successful.
def sendRequest(webhookR, i):
    postReq = []
    error = False  # try/catch flag for post timeout.
    request = [x for x in webhookR if  # Filter nests that are older than 3 days.
               (datetime.today() - datetime.fromisoformat(x["form"]["evolution"]["creation"][0:10])).days < 3]
    # request = [y for y in request if "/MENO/Hotfolders/" in y["form"]["origin"]["input"]["file"]]

    print(str(datetime.now().strftime("%H:%M:%S")) + " - Sending data to webhook...")
    try:
        postReq = requests.post(webhookURL, json=request, timeout=60)  # Makes the post request to the webhook.
        postReq.raise_for_status()
    except Exception:
        error = True

    # If an error is returned, wait a bit before retrying.
    if (i <= 5 and error):
        print(str(datetime.now().strftime("%H:%M:%S")) + ": Attempt " + str(i) + ". Trying again in " + str(i * 15) + " seconds.\n")
        time.sleep(i * 15)
        print("Retrying...")
        i = i + 1
        sendRequest(webhookR, i)
    elif (i > 5 and error):
        print(str(datetime.now().strftime("%H:%M:%S")) + ": Could not send data to webhook. :(\nExiting.\n")
        time.sleep(5)
        i = 1
    else:
        print(str(postReq.status_code) + " Sent successfully.\n")



# Makes a get request to the provided URL. Bad requests will exit after 5 retries.
def getRequest(urlRequest, i):
    getReq = []
    error = False  # try/catch flag for get timeout.
    print("Getting data.")
    try:
        getReq = requests.get(urlRequest)  # Makes the get request to the Caldera API.
        getReq.raise_for_status()
    except Exception as e:
        error = True

    # If an error is returned, wait a bit before retrying.
    if (i <= 5 and error):
        print(str(datetime.now().strftime("%H:%M:%S")) + ": Fetch Attempt " + str(i) + ". Trying again in " + str(i * 5) + " seconds.\n")
        time.sleep(i * 5)
        print("Retrying...")
        i = i + 1
        getReq = getRequest(urlRequest, i)
    elif (i > 5 and error):
        print(str(datetime.now().strftime("%H:%M:%S")) + ": Fetching failed. Check to see if the Caldera API is running.\nExiting.\n")
        time.sleep(5)
        i = 1
        return []
    else:
        return getReq


# Pulls spooler data from Caldera server, checks it against the last pull and sends to Make is it is different.
def pullPush(urlPrinter, lastPull):
    r = getRequest(urlPrinter, 1)
    if (r == []) or (r is None):
        return lastPull
    spoolerJson = r.json()
    if lastPull != spoolerJson:
        # print(spoolerJson)
        print("New data detected, sending to server.\n")
        sendRequest(spoolerJson, 1)
    else:
        print("No data change.")
    return spoolerJson


time.sleep(1)  # Gives a little time for the Caldera API to launch.
gc.enable()
# Loops endlessly, delayed by whatever pullTimer is set to. Add new lines here for new Printers
while True:
    print(idPrinter2)
    pullStore2 = pullPush(urlPrinter2, pullStore2) 
    print(idPrinter1)
    time.sleep(1)
    pullStore1 = pullPush(urlPrinter1, pullStore1) 
    print(str(datetime.now().strftime("%H:%M:%S")) + " - Sleeping Loop")
    time.sleep(pullTimer)
    loopCount = loopCount + 1
    if loopCount > 180:
        gc.collect()
        loopCount = 0
        print(str(datetime.now().strftime("%H:%M:%S")) + " - It's trash day.\n")
