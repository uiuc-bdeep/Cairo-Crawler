import json
import sys
import requests
import crawl
import utils
from datetime import datetime
from pymongo import MongoClient

def crawl_all(objectIDs, url, token, start_date, end_date):
    client = MongoClient()
    client.drop_database("cairo_crawler")
    for objectID in objectIDs:
        print(objectID)
        crawl.get_routes(url, token, objectID, 1544900000, 1544930694)

def main():
    api_info = sys.argv[1]

    with open(api_info) as f:
            api_info = json.load(f)

    api_data = crawl.login_api(api_info)
    token = api_data["data"]["token"]
    object_url = api_info["objects"]

    objectIDs = crawl.get_objectIDs(object_url, token)
    print(objectIDs)
    curr_time = datetime.now()
    start_date = utils.datetime_to_unix(curr_time, 6-1/3)
    end_date = utils.datetime_to_unix(curr_time, 6)
    crawl_all(objectIDs, api_info["routes"], token, start_date, end_date)

if __name__ == "__main__":
    main()
