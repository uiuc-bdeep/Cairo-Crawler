import json
import sys
import requests
import crawl
import utils
import time
import datetime
import csv
import pytz
import MySQLdb
#from datetime import datetime

def insertTrip(db, phoneNumber, distance, startDate, duration, avgSpeed, moved, maxSpeed, routesID):
    cur = db.cursor()
    if(maxSpeed is None): 
        maxSpeed = 0
    command = 'INSERT INTO Trips(phoneNumber, distance, startDate, readableDate, duration, avgSpeed, moved, maxSpeed, routesID) VALUES (\"'+ str(phoneNumber) + "\", " + str(distance) + ", " + str(startDate) + ", \"" + str(readableTime(startDate)) + "\", " + str(duration) + ", " + str(avgSpeed) + ", " + str(moved) + ", " + str(maxSpeed) + ", " + str(routesID) + ");"
    try: 
        cur.execute(command)
	db.commit()
    except: 
        db.rollback()
    	print("FAILED INSERTING TRIP")

def insertRoute(db, routesID, latitude, longitude, endDate, speed):
    cur = db.cursor()
    command = "INSERT INTO Routes(routesID, latitude, longitude, endDate, readableDate, speed) VALUES(" + str(routesID) + ", " + str(latitude) + ", " + str(longitude) + ", " + str(endDate) + ", \"" + readableTime(endDate) + "\", " + str(speed) + ");"
    cur.execute(command)
    try:
        cur.execute(command)
        db.commit()
    except:
       db.rollback()
       print("FAILED INSERTING ROUTE")

def readableTime(start_time):
    value = datetime.datetime.fromtimestamp(start_time,tz=pytz.timezone('Africa/Cairo'))
    return value.strftime('%d %B %Y %H:%M:%S')

def filename_from_time(date):
    return datetime.datetime.fromtimestamp(date).strftime('%m-%d-%Y')

def crawl_all(objectIDs, url, token, start_date, end_date):
    db = MySQLdb.connect(host="localhost", user="ubuntu", db="Cairo")
    file_name = filename_from_time(end_date)
    write_header(file_name)
    tripID = 0
    for objectID in objectIDs:
	crawl.get_routes(url, token, objectID, start_date, end_date)
	tripID = write_csv(objectID, token, start_date, end_date, url, file_name, db, tripID)
    db.close()

def write_csv(objectID, token, start_date, end_date, site, filename, db, tripID):
	url = site.format(token, objectID, start_date, end_date) # log this
	r = requests.get(url).json()
	#with open("data_json/json-" + filename + ".json", "a+") as output:
	#	json.dump(r, output)
	if(r["data"]["routes"] is None):
		print(r["data"]["phoneNumber"] + ": ROUTE NULL")
		with open("csv_data/null_phones/phones-" + filename + ".csv", "a+") as number_file:
			writer = csv.writer(number_file, delimiter= ',')
			start_date = readableTime(start_date)
			end_date = readableTime(end_date)
			phone_row = [r["data"]["phoneNumber"], start_date, end_date]
			writer.writerow(phone_row)
		return tripID
	print(r["data"]["phoneNumber"] + ": ROUTE VALID")
	for route in r["data"]["routes"]:
		insertTrip(db, r["data"]["phoneNumber"], route["distance"], route["startdate"], route["duration"], route["avgSpeed"], route["moved"], route["maxSpeed"], tripID)
		with open("csv_data/routes/routes-" + filename + ".csv", "a+") as csv_file:
			writer = csv.writer(csv_file, delimiter=",")
			row = [str(r["data"]["phoneNumber"]), str(route["distance"]), str(route["startdate"]), readableTime(route["startdate"]), str(route["duration"]), str(route["avgSpeed"]), str(route["moved"]), str(route["maxSpeed"]), str(tripID)]
			writer.writerow(row)
			csv_file.close()
		for trip in route["coordinates"]:
			insertRoute(db, tripID, trip["latitude"], trip["longitude"], trip["datetime"], trip["speed"])
			with open("csv_data/trips/trips-" + filename + ".csv", "a+") as csv_file:
				writer = csv.writer(csv_file, delimiter=',')
				row = [str(tripID), str(trip["latitude"]), str(trip["longitude"]), str(trip["datetime"]), readableTime(trip["datetime"]), str(trip["speed"])]
				writer.writerow(row)
				csv_file.close()
			tripID += 1
	return tripID

def write_header(filename):
	with open("csv_data/routes/routes-" + filename + ".csv", "w") as csv_file:
		writer = csv.writer(csv_file, delimiter=',')
		header_row = ['phoneNumber', 'distance', 'startDate', 'startDateReadable', 'duration', 'avgSpeed', 'moved', 'maxSpeed', 'routesID']
		writer.writerow(header_row)
		csv_file.close()
	with open("csv_data/trips/trips-" + filename + ".csv", "w") as csv_file:
	        writer = csv.writer(csv_file, delimiter=',')
		header_row = ['routesID', 'latitude', 'longitude', 'endDate', 'endDateReadable', 'speed']
		writer.writerow(header_row)
		csv_file.close()
	with open("csv_data/dist_totals/totals-" + filename + ".csv", "w") as csv_file:
	        writer = csv.writer(csv_file, delimiter=',')
		header_row = ['phoneNumber', 'date', 'distance']
		writer.writerow(header_row)
		csv_file.close()
	with open("csv_data/null_phones/phones-" + filename + ".csv", "w") as number_file:
		writer = csv.writer(number_file, delimiter= ',')
		header_row = ['phoneNumber', 'crawl_start_time', 'crawl_end_time']
		writer.writerow(header_row)
		number_file.close()

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

        start_date = datetime.date.today() - datetime.timedelta(5)
	start_date = datetime.datetime.combine(start_date, datetime.datetime.min.time())
	end_date = datetime.datetime.now()
	start_date = utils.datetime_to_unix(start_date)
	end_date = utils.datetime_to_unix(end_date)

        #curr_time = datetime.now()
        #start_date = utils.datetime_to_unix(curr_time, 6-1/3)
        #end_date = utils.datetime_to_unix(curr_time, 6)
	print("start date | end date")
	print([datetime.datetime.fromtimestamp(start_date).strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.fromtimestamp(end_date).strftime('%Y-%m-%d %H:%M:%S')])
	#print([start_date, end_date])
        crawl_all(objectIDs, api_info["routes"], token, start_date, end_date)
	
	#time.sleep(1200)

if __name__ == "__main__":
    main()
