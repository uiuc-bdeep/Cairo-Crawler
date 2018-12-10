import requests
import json, ast
from pymongo import MongoClient

def login_api(api_info):
    url = api_info["login"]
    creds = api_info["creds"]

    r = requests.post(url, json=creds)

    return ast.literal_eval(json.dumps(r.json()))

def get_routes(url, token, objectID, startDate, endDate):
    client = MongoClient(os.environ["DB_PORT_27017_TCP_ADDR"], 27017)
    db = client.cairo_crawler
    cc_device_routes, cc_device_list = db.device_routes, db.device_dist

    url = url.format(token, objectID, startDate, endDate)

    r = requests.get(url)

    cc_device_routes.insert(r.json())
