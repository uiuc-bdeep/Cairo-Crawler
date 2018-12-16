import json
import sys
import requests
import crawl
import schedule
import utils
from pymong import MongoClient

slack_url = ""
running = 1

def slack_notification(slack_msg):
    payload = {"text": slack_msg}

    try:
        r = requests.post(slack_url, data=json.dumps(payload))
    except requests.exceptions.RequestionException as e:
        logger.info("Cairo Crawler: Error while sending Slack notification")
        logger.info(e)

def crawl(url, token, objectID, starttime, endtime):
    crawl.get_routes(url, token, objectID, starttime, endtime)
    return schedule.CancelJob

def schedule_trips():
    global running
    running = 1

    timestamps = ["00:00", "00:20", "00:40", "01:00", "01:20", "01:40",
                  "02:00", "02:20", "02:40", "03:00", "03:20", "03:40",
                  "04:00", "04:20", "04:40", "05:00", "05:20", "05:40",
                  "06:00", "06:20", "06:40", "07:00", "07:20", "07:40",
                  "08:00", "08:20", "08:40", "09:00", "09:20", "09:40",
                  "10:00", "10:20", "10:40", "11:00", "11:20", "11:40",
                  "12:00", "12:20", "12:40", "13:00", "13:20", "13:40",
                  "14:00", "14:20", "14:40", "15:00", "15:20", "15:40",
                  "16:00", "16:20", "16:40", "17:00", "17:20", "17:40",
                  "18:00", "18:20", "18:40", "19:00", "19:20", "19:40",
                  "20:00", "20:20", "20:40", "21:00", "21:20", "21:40",
                  "22:00", "22:20", "22:40", "23:00", "23:20", "23:40",
                  ]
    for timestamp in timestamps:
        schedule.every().monday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))
    for timestamp in timestamps:
        schedule.every().tuesday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))
    for timestamp in timestamps:
        schedule.every().wednesday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))
    for timestamp in timestamps:
        schedule.every().thursday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))
    for timestamp in timestamps:
        schedule.every().friday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))
    for timestamp in timestamps:
        schedule.every().saturday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))
    for timestamp in timestamps:
        schedule.every().sunday.at(timestamp).do(crawl(url, token, objectID, starttime, endtime))

def end_scheduler():
    global running
    running = 0
    return schedule.CancelJob

def main():
    api_info = sys.argv[1]

    with open(api_info) as f:
            api_info = json.load(f)

    api_data = crawl.login_api(api_info)

    token = api_data["data"]["token"]
    slack_url = api_data["slack_url"]

    client = MongoClient(os.environ["DB_PORT_27017_TCP_ADDR"], 27017)
    db = client.cairo_crawler

    db.device_routes.drop()
    db.device_dist.drop()

    while True:
        schedule_trips()

        schedule.every().monday.at("21:30").do(write_csv)
        schedule.every().tuesday.at("21:30").do(write_csv)
        schedule.every().wednesday.at("21:30").do(write_csv)
        schedule.every().thursday.at("21:30").do(write_csv)
        schedule.every().friday.at("21:30").do(write_csv)
        schedule.every().saturday.at("21:30").do(write_csv)
        schedule.every().sunday.at("21:30").do(write_csv)

        while True and running:
            schedule.run_pending()
            time.sleep(0.5)

if __name__ == "__main__":
    main()
