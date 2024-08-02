# V1.0 - Aria Corona
# Script to pull Spooler data from a local Caldera server, and sending it to a dedicated webhook.
#
#
import gc

import requests
from datetime import datetime
import time

pullTimer = 60  # seconds
idPrinter1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1B'  # Epson A
idPrinter2 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1C'  # Epson B
ip1 = '192.168.0.39'  # Dell PC IP
# ip2 = '192.168.0.151'  # Alienware, currently not relevant
pullStore1 = {}
pullStore2 = {}  # If adding additional printers, add more of these variables.
webhookURL = 'https://hook.us1.make.com/7c5fomyqgiuaodfakepg13ty3rxxikmz'
urlPrinter1 = 'http://' + ip1 + ':12340/v1/jobs?idents.device=' + idPrinter1 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=pending|finished&limit=5'
urlPrinter2 = 'http://' + ip1 + ':12340/v1/jobs?idents.device=' + idPrinter2 + '&name=Autonest*&sort=idents.internal' \
                                                                               ':desc&state=pending|finished&limit=5'
loopCount = 0


# Sends pulled JSON data to the provided webhook. Bad requests will continue to retry until successful.
def sendRequest(webhookR, i):
    postReq = []
    error = False  # try/catch flag for post timeout.
    request = [x for x in webhookR if  # Filter nests that are older than 3 days.
               (datetime.today() - datetime.fromisoformat(x["form"]["evolution"]["creation"][0:10])).days < 3]
    # request = [y for y in request if "/MENO/Hotfolders/" in y["form"]["origin"]["input"]["file"]]

    print("Sending data to webhook...")
    try:
        postReq = requests.post(webhookURL, json=request, timeout=30)  # Makes the post request to the webhook.
    except Exception as e:
        error = True

    try:
        # If an error is returned, wait a bit before retrying.
        if ((postReq.status_code != requests.codes.ok) and (i <= 5)) or error:
            print(str(postReq.status_code) + ": Attempt " + str(i) + ". Trying again in " + str(i * 15) + " seconds.\n")
            time.sleep(i * 15)
            print("Retrying...")
            i = i + 1
            sendRequest(webhookR, i)
        elif ((postReq.status_code != requests.codes.ok) and (i > 5)) or error:
            print(
                str(postReq.status_code) + ": Attempt " + str(
                    i) + ". Check to see if the Caldera API is running.\nTrying again "
                         "in 120 seconds.\n")
            time.sleep(120)
            print("Retrying...")
            i = i + 1
            sendRequest(webhookR, i)
        else:
            print(str(postReq.status_code) + " Sent successfully.")
    except AttributeError:
        time.sleep(i * 15)
        i = i + 1
        sendRequest(webhookR, i)


# Makes a get request to the provided URL. Bad requests will continue to retry until successful.
def getRequest(urlRequest, i):
    getReq = []
    error = False  # try/catch flag for get timeout.

    try:
        getReq = requests.get(urlRequest)  # Makes the get request to the Caldera API.
    except Exception as e:
        error = True

    # If an error is returned, wait a bit before retrying.
    if ((getReq.status_code != requests.codes.ok) and (i <= 5)) or error:
        print(str(getReq.status_code) + ": Attempt " + i + ". Trying again in " + i * 15 + " seconds.\n")
        time.sleep(i * 15)
        print("Retrying...")
        i = i + 1
        getReq = getRequest(urlRequest, i)
    elif ((getReq.status_code != requests.codes.ok) and (i > 5)) or error:
        print(
            str(getReq.status_code) + ": Attempt " + i + ". Check to see if the Caldera API is running.\nTrying again "
                                                         "in 120 seconds.\n")
        time.sleep(120)
        print("Retrying...")
        i = i + 1
        getReq = getRequest(urlRequest, i)

    return getReq


# Pulls spooler data from Caldera server, checks it against the last pull and sends to Make is it is different.
def pullPush(urlPrinter, lastPull):
    r = getRequest(urlPrinter, 1)
    spoolerJson = r.json()
    if lastPull != spoolerJson:
        # print(spoolerJson)
        print("New data detected, sending to server.\n")
        sendRequest(spoolerJson, 1)
    return spoolerJson


time.sleep(1)  # Gives a little time for the Caldera API to launch.
gc.enable()
# Loops endlessly, delayed by whatever pullTimer is set to. Add new lines here for new Printers
while True:
    pullStore2 = pullPush(urlPrinter2, pullStore2)  # Epson B
    pullStore1 = pullPush(urlPrinter1, pullStore1)  # Epson A
    time.sleep(pullTimer)
    loopCount = loopCount + 1
    if loopCount > 180:
        gc.collect()
        loopCount = 0
