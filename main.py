import json
import sys
import call
from gmplot import gmplot

def main():
	api_info = sys.argv[1]

	with open(api_info) as f:
		api_info = json.load(f)

	api_data = call.login_api(api_info)

	print(api_data)

	token = api_data["data"]["token"]

	print(token)

	route = call.get_routes(api_info["routes"], token, 12923, 1541262523, 1541397600) 

	map()	


def map(route=None):
	gmap = gmplot.GoogleMapPlotter(41.7196764,-87.7477973, 13)
	gmap.draw("map.html")



if __name__ == "__main__":
	main()
