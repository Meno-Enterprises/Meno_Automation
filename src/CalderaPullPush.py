# V1.0 - Aria Corona
# Script to pull Spooler data from a local Caldera server, and sending it to a dedicated webhook.
#
#

import requests
import time

pullTimer = 60  # seconds
idPrinter1 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1B'  # Epson A
idPrinter2 = 'bG9jYWxob3N0OjQ1MzQzfmNhbGRlcmFyaXB-RXBzb24tU3VyZUNvbG9yLUYxMDAwMC1C'  # Epson B
ip1 = '192.168.0.39'  # Dell PC IP
# ip2 = '192.168.0.151'  # Alienware, currently not relevant
hostIP = ip1  # Change which Caldera API gets pulled from here.
pullStore1 = {}
pullStore2 = {} # If adding additional printers, add more of these variables.
webhookURL = 'https://hook.us1.make.com/7c5fomyqgiuaodfakepg13ty3rxxikmz'
urlPrinter1 = 'http://' + hostIP + ':12340/v1/jobs?idents.device=' + idPrinter1 + '&name=Autonest*&sort=idents.internal' \
                                                                                  ':desc&state=pending|finished'
urlPrinter2 = 'http://' + hostIP + ':12340/v1/jobs?idents.device=' + idPrinter2 + '&name=Autonest*&sort=idents.internal' \
                                                                                  ':desc&state=pending|finished'


# Sends pulled JSON data to the provided webhook. Bad requests will continue to retry until successful.
def sendRequest(webhookR, i):
    print("Sending data to webhook...")
    postReq = requests.post(webhookURL, json=webhookR, timeout=30)
    if (postReq.status_code != requests.codes.ok) and (i <= 5):
        print(str(postReq.status_code) + ": Attempt " + i + ". Trying again in " + i * 15 + " seconds.\n")
        time.sleep(i * 15)
        print("Retrying...")
        i = i + 1
        sendRequest(webhookR, i)
    elif (postReq.status_code != requests.codes.ok) and (i > 5):
        print(
            str(postReq.status_code) + ": Attempt " + i + ". Check to see if the Caldera API is running.\nTrying again "
                                                          "in 120 seconds.\n")
        time.sleep(120)
        print("Retrying...")
        i = i + 1
        sendRequest(webhookR, i)
    else:
        print(str(postReq.status_code) + " Sent successfully.")


# Makes a get request to the provided URL. Bad requests will continue to retry until successful.
def getRequest(urlRequest, i):
    print("Pulling " + urlRequest)
    getReq = requests.get(urlRequest)
    if (getReq.status_code != requests.codes.ok) and (i <= 5):
        print(str(getReq.status_code) + ": Attempt " + i + ". Trying again in " + i * 15 + " seconds.\n")
        time.sleep(i * 15)
        print("Retrying...")
        i = i + 1
        getReq = getRequest(urlRequest, i)
    elif (getReq.status_code != requests.codes.ok) and (i > 5):
        print(
            str(getReq.status_code) + ": Attempt " + i + ". Check to see if the Caldera API is running.\nTrying again "
                                                         "in 120 seconds.\n")
        time.sleep(120)
        print("Retrying...")
        i = i + 1
        getReq = getRequest(urlRequest, i)
    else:
        print("Pull successful.")
    return getReq


# Pulls spooler data from Caldera server, checks it against the last pull and sends to Make is it is different.
def pullPush(urlPrinter, lastPull):
    r = getRequest(urlPrinter, 1)
    spoolerJson = r.json()
    if lastPull != spoolerJson:
        print("New data detected, sending to server.\n")
        sendRequest(spoolerJson, 1)
    else:
        print("No update.\n")
    return spoolerJson


time.sleep(30)

# Loops endlessly, delayed by whatever pullTimer is set to. Add new lines here for new Printers
while True:
    pullStore1 = pullPush(urlPrinter1, pullStore1) # Epson A
    pullStore2 = pullPush(urlPrinter2, pullStore2) # Epson B
    time.sleep(pullTimer)

