import requests
import os
import json, ast
from pymongo import MongoClient
import utils

def login_api(api_info):
    url = api_info["login"]
    creds = api_info["creds"]

    r = requests.post(url, json=creds)

    return ast.literal_eval(json.dumps(r.json()))

def get_objectIDs(url, token):
    r = requests.get(url.format(token))
    objects = r.json()["data"]["objects"]
    objectIDs = []

    for obj in objects:
        objectIDs.append(obj["objectId"])
	

    print(objectIDs)
    return objectIDs

def get_routes(url, token, objectID, start_date, end_date):
    #client = MongoClient(os.environ["DB_PORT_27017_TCP_ADDR"], 27017)
    #client = MongoClient()
    #db = client.cairo_crawler ## change this in order to change the DB place
    #routes, dists = db.device_writer, db.device_dists
    #routes.drop()
    url = url.format(token, objectID, start_date, end_date)
    with open("log.txt", "a+") as log:
        log.write("ObjectID: " + str(objectID) + " | startDate: " + str(start_date) + " | endDate: " + str(end_date) + "\n")
    r = requests.get(url).json()
    #routes.insert(r)
