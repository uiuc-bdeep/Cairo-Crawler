import requests
import json
import ast

def login_api(api_info):
	url = api_info["login"]
	creds = api_info["creds"]

	r = requests.post(url, json=creds)

	return ast.literal_eval(json.dumps(r.json()))

def get_routes(url, token, objectID, startDate, endDate):
	url = url.format(token, objectID, startDate, endDate)

	print(url)

	r = requests.get(url)

	return json.dumps(r.json())
