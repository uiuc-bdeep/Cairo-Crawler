from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient

def unix_to_datetime(time):
    utc_time = datetime.utcfromtimestamp(time)
    return utc_time - timedelta(hours=6)

def datetime_to_unix(time):
    time = time - datetime(1970, 1, 1)
    return int(time.total_seconds())

def get_last_k(collection, objectID, k):
    last_k = collection.find({"objectID": objectID},
                             sort=[("_id", pymongo.DESCENDING)]).limit(k)

    return list(last_k)

def check_offline(route_data):
    total = len(routes_data)
    off_count = 0
    for i in range(total):
        if not routes_data["routes"]:
            off_count += 1

    return (off_count / total) > 0.5
