import json
import sys
import requests
import crawl
import utils
import time
import datetime
import calendar
import csv
import pytz
import MySQLdb
from slack_log import slack_notification 

num_null_phones = 0
num_active_users = 0

def insertTripSQL(db, phoneNumber, distance, startDate, duration, avgSpeed, moved, maxSpeed, routesID):
    cur = db.cursor()
    if(maxSpeed is None): 
        maxSpeed = 0
    command = 'INSERT INTO Trips(phoneNumber, distance, startDate, readableDate, duration, avgSpeed, moved, maxSpeed, routesID) VALUES (\"'+ str(phoneNumber) + "\", " + str(distance) + ", " + str(startDate) + ", \"" + str(readableTime(startDate, phoneNumber)) + "\", " + str(duration) + ", " + str(avgSpeed) + ", " + str(moved) + ", " + str(maxSpeed) + ", " + str(routesID) + ");"
    try: 
        cur.execute(command)
	db.commit()
	return 1
    except: 
        db.rollback()
    	print(phoneNumber + ": FAILED INSERTING TRIP")
	return 0

def insertRouteSQL(db, phoneNumber, routesID, latitude, longitude, endDate, speed):
    cur = db.cursor()
    command = "INSERT INTO Routes(routesID, latitude, longitude, endDate, readableDate, speed) VALUES(" + str(routesID) + ", " + str(latitude) + ", " + str(longitude) + ", " + str(endDate) + ", \"" + readableTime(endDate, phoneNumber) + "\", " + str(speed) + ");"
    try:
        cur.execute(command)
        db.commit()
	return 1
    except:
       db.rollback()
       print(phoneNumber + ": FAILED INSERTING ROUTE")
       return 0

def insertTotalSQL(db, date, phoneNumber, total):
    cur = db.cursor()
    command = "INSERT INTO DailyTotals(date, phoneNumber, total) VALUES(\"" + date + "\", \"" + phoneNumber + "\", " + str(total) + ");"
    try:
    	cur.execute(command)
	db.commit()
	return 0
    except:
	db.rollback()
    print(phoneNumber + ": FAILED INSERTING TOTAL")
    return -1


def insertTripCSV(filename, phoneNumber, distance, startDate, duration, avgSpeed, moved, maxSpeed, routesID):
	with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/trips/trips-" + filename + ".csv", "a+") as csv_file:
		writer = csv.writer(csv_file, delimiter=",")
		row = [phoneNumber, str(distance), str(startDate), readableTime(startDate, phoneNumber), str(duration), str(avgSpeed), str(moved), str(maxSpeed), str(routesID)]
		writer.writerow(row)

def insertRouteCSV(filename, phoneNumber, routesID, latitude, longitude, endDate, speed):
	with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/routes/routes-" + filename + ".csv", "a+") as csv_file:
		writer = csv.writer(csv_file, delimiter=',')
		row = [str(routesID), str(latitude), str(longitude), str(endDate), readableTime(endDate, phoneNumber), str(speed)]
		writer.writerow(row)

def insertTotalCSV(filename, date, phoneNumber, total):
        with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/daily_totals/totals-" + filename + ".csv", "a+") as csv_file:
                writer = csv.writer(csv_file, delimiter=",")
	        row = [date, phoneNumber, str(total)]
		writer.writerow(row)

## Convert unix time to a human readable time, which differs depending on the timezone (Cairo or Chicago)

def readableTime(start_time, phoneNumber="None"):
    if(phoneNumber == "2243818480" or phoneNumber == "7026775781"):
    	value = datetime.datetime.fromtimestamp(start_time,tz=pytz.timezone('America/Chicago')) #Users Daniel and Adam are in Chicago time
	return value.strftime('%d %B %Y %H:%M:%S')
    else:
    	value = datetime.datetime.fromtimestamp(start_time,tz=pytz.timezone('Africa/Cairo')) #All other users are in Cairo time
    	return value.strftime('%d %B %Y %H:%M:%S')

## Convert unix time to the form day-month-year used for file naming

def filename_from_time(date):
    return datetime.datetime.fromtimestamp(date).strftime('%d-%B-%Y')

## Convert unix time to the form (day month) to be used in the SQL database

def date_from_time(date):
    month = datetime.datetime.fromtimestamp(date).strftime('%m')
    month_str = calendar.month_name[int(month)]
    return datetime.datetime.fromtimestamp(date).strftime('%d') + " " + month_str

def crawl_all(db, objectIDs, url, token, start_date, end_date):
    file_name = filename_from_time(end_date)
    write_header(file_name)
    tripID = 0
    for objectID in objectIDs:
	crawl.get_routes(url, token, objectID, start_date, end_date)
	tripID = crawl_user(objectID, token, start_date, end_date, url, file_name, db, tripID)

def crawl_user(objectID, token, start_date, end_date, site, filename, db, tripID):
	global num_null_phones
	global num_active_users
	url = site.format(token, objectID, start_date, end_date) # log this
	r = requests.get(url).json()
	phoneNumber = r["data"]["phoneNumber"]
	if(r["data"]["routes"] is None):
		print(phoneNumber + ": Route is NULL")
		num_null_phones += 1
		with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/null_phones/phones-" + filename + ".csv", "a+") as csv_file:
			writer = csv.writer(csv_file, delimiter= ',')
			phone_row = [phoneNumber, readableTime(start_date), readableTime(end_date)]
			writer.writerow(phone_row)
		return tripID
	num_active_users += 1
	tcount = 0
	rcount = 0
	for trip in r["data"]["routes"]:
		tcount += insertTripSQL(db, phoneNumber, trip["distance"], trip["startdate"], trip["duration"], trip["avgSpeed"], trip["moved"], trip["maxSpeed"], tripID)
		insertTripCSV(filename, phoneNumber, trip["distance"], trip["startdate"], trip["duration"], trip["avgSpeed"], trip["moved"], trip["maxSpeed"], tripID)
		for route in trip["coordinates"]:
			rcount += insertRouteSQL(db, phoneNumber, tripID, route["latitude"], route["longitude"], route["datetime"], route["speed"])
			insertRouteCSV(filename, phoneNumber, tripID, route["latitude"], route["longitude"], route["datetime"], route["speed"])
			tripID += 1
	print(phoneNumber + ": Inserted " + str(tcount) + " trip(s) with " + str(rcount) + " route(s)")
	return tripID

def write_header(filename):
	with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/trips/trips-" + filename + ".csv", "w") as csv_file:
		writer = csv.writer(csv_file, delimiter=',')
		header_row = ['phoneNumber', 'distance', 'startDate', 'startDateReadable', 'duration', 'avgSpeed', 'moved', 'maxSpeed', 'routesID']
		writer.writerow(header_row)
	with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/routes/routes-" + filename + ".csv", "w") as csv_file:
	        writer = csv.writer(csv_file, delimiter=',')
		header_row = ['routesID', 'latitude', 'longitude', 'endDate', 'endDateReadable', 'speed']
		writer.writerow(header_row)
	with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/null_phones/phones-" + filename + ".csv", "w") as number_file:
		writer = csv.writer(number_file, delimiter= ',')
		header_row = ['phoneNumber', 'crawl_start_time', 'crawl_end_time']
		writer.writerow(header_row)
	with open("/home/ubuntu/Cairo-Crawler/Cairo-writer/csv_data/daily_totals/totals-" + filename + ".csv", "w") as number_file:
                writer = csv.writer(number_file, delimiter= ',')
		header_row = ['date', 'phoneNumber', 'distance']
		writer.writerow(header_row)

def calculate_totals(db, end_date):
    cur = db.cursor()
    date = date_from_time(end_date)
    query = "SELECT phoneNumber, SUM(distance) FROM Trips WHERE readableDate LIKE \"%" + date + "%\" GROUP BY phoneNumber;"
    cur.execute(query)
    data = cur.fetchall()
    length = len(data)
    file_name = filename_from_time(end_date)
    print("Calculating Daily Totals for " + str(length) + " users")
    for entry in data:
        print(entry[0] + ": travelled " + str(entry[1]) + " meters")
	result = insertTotalSQL(db, date, entry[0], entry[1]) #entry[0] = phoneNumber, entry[1] = total
	insertTotalCSV(file_name, date, entry[0], entry[1])
	length += result
    print("Calculated Daily Totals for " + str(length) + " users on \"" + date + "\"")
    slack_notification("Calculated Daily Totals for " + str(length) + " users")

def main():
    #while True:
        api_info = sys.argv[1]

        with open(api_info) as f:
             api_info = json.load(f)
	
        api_data = crawl.login_api(api_info)
        print(api_data)
        token = api_data["data"]["token"]
        object_url = api_info["objects"]

        objectIDs = crawl.get_objectIDs(object_url, token)
        #print(objectIDs)

	end_date = datetime.datetime.now()
	start_date = end_date - datetime.timedelta(days = 1)
	start_date = utils.datetime_to_unix(start_date)
	end_date = utils.datetime_to_unix(end_date)
        db = MySQLdb.connect(host="localhost", user="ubuntu", db="Cairo")
	print("---------------------------- Starting Crawler --------------------------------")
	print("Current time (Chicago time): " + readableTime(end_date, "2243818480"))
	print("Start date (Cairo time): " + readableTime(start_date))
	print("End date (Cairo time): " + readableTime(end_date))
	slack_notification("Beginning Uboro Crawl\nStart time (Cairo time) = " + readableTime(start_date) + "\nEnd time (Cairo time) = " + readableTime(end_date))
        crawl_all(db, objectIDs, api_info["routes"], token, start_date, end_date)
	print("Total Null Numbers = " + str(num_null_phones) + "\nTotal Active Users = " + str(num_active_users))
	slack_notification("Total Null Numbers = " + str(num_null_phones) + "\nTotal Active Users = " + str(num_active_users))
	calculate_totals(db, end_date)
	print("Finished Crawling: " + date_from_time(end_date))
	db.close()

if __name__ == "__main__":
    main()
