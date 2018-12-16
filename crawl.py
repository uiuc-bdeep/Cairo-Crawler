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

    return objectIDs

def get_routes(url, token, objectID, start_date, end_date):
    #client = MongoClient(os.environ["DB_PORT_27017_TCP_ADDR"], 27017)
    client = MongoClient()
    db = client.cairo_crawler
    routes, dists = db.device_routes, db.device_dists
    url = url.format(token, objectID, start_date, end_date)

    r = requests.get(url).json()
    r["objectID"] = objectID
    routes.insert(r)
