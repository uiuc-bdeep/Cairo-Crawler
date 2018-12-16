from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient

def unix_to_datetime(time, delta=0):
    utc_time = datetime.utcfromtimestamp(time)
    return utc_time - timedelta(hours=delta)

def datetime_to_unix(time, delta=0):
    time = time - datetime(1970, 1, 1) + timedelta(hours=delta)
    return int(time.total_seconds())

def get_last_k(collection, objectID, k):
    last_k = collection.find({"objectID": objectID},
                             sort=[("_id", pymongo.DESCENDING)]).limit(k)

    return list(last_k)

def slack_notification(slack_msg):
    payload = {"text": slack_msg}

    try:
        r = requests.post(slack_url, data=json.dumps(payload))
    except requests.exceptions.RequestionException as e:
        logger.info("Cairo Crawler: Error while sending Slack notification")
        logger.info(e)
