#!/usr/bin/env python3

'''
Copyright (c) 2024 drolx Labs

Licensed under the MIT License
you may not use this file except in compliance with the License.
    https://opensource.org/license/mit
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Godwin peter .O (me@godwin.dev)
Created At: Thursday, 7th Nov 2024
Modified By: Godwin peter .O
Modified At: Sat Nov 09 2024
'''

import csv
import sys
import http.client
import json
from typing import TypedDict

## Default Values
defult_file_path: str = "bhn_trip_routes.csv"
nominatim_url: str = "nominatim.openstreetmap.org"
routing_url: str = "valhalla1.openstreetmap.de"

## Shared Types
Coord = TypedDict("Coord", {"lat": float, "lon": float})
Location = TypedDict("Location", { "name": str, "lat": float, "lon": float})
RouteInfo = TypedDict("RouteInfo", {
    "trip_length": str,
    "trip_time": str,
    "source": Coord,
    "destination": Coord,
})
TripInfo = TypedDict("TripInfo", {
    "trip_code": str,
    "trip_distance": str,
    "trip_time": str,
    "trip_source": str,
    "source_coords": str,
    "trip_destination": str,
    "destination_coords": str,
})

class coord_service:
    geocoding_url: str
    routing_url: str
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Chrome"
    }

    def __init__(self, geocoding_url: str, routing_url: str) -> None:
        self.geocoding_url = geocoding_url
        self.routing_url = routing_url

    def get_coords(self, address: str) -> Coord:
        try:
            conn = http.client.HTTPSConnection(self.geocoding_url)
            url_path = "/search?format=json&limit=1&addressdetails=0&email=mx@drolx.com&q="
            payload = ""

            conn.request("GET", url_path + address, payload, self.headers)
            res = conn.getresponse()
            data = res.read()
            result_items = data.decode("utf-8")
            item  = json.loads(result_items)[0]

            return { "lat": item["lat"], "lon": item["lon"]}
        except http.client.HTTPException:
           print("There was an error with the HTTP request")
           sys.exit()


    def get_route_attributes(self, source: Coord, destination: Coord) -> RouteInfo:
        try:
            conn = http.client.HTTPSConnection(self.routing_url)
            payload = json.dumps({
              "format": "json",
              "shape_format": "polyline6",
              "costing": "truck",
              "units": "kilometers",
              "alternates": 0,
              "search_filter": {
                "exclude_closures": True
              },
              "locations": [
                {
                  "lat": source["lat"],
                  "lon": source["lon"]
                },
                {
                  "lat": destination["lat"],
                  "lon": destination["lon"]
                }
              ]
            })

            conn.request("POST", "/route", payload, self.headers)
            res = conn.getresponse()
            data = res.read()
            result = data.decode("utf-8")

            item  = json.loads(result)
            return {
                "length": item["trip"]["summary"]["length"],
                "time": item["trip"]["summary"]["time"],
                "source": { "lat": source["lat"], "lon": source["lon"] },
                "destination": { "lat": destination["lat"], "lon": destination["lon"] },
            }
        except http.client.HTTPException:
           print("There was an error with the HTTP request")
           sys.exit()



class data_loader:
    file_path = ""
    source_data = []
    locations: list[str] = []
    locations_coord: list[Location] = []

    def __init__(self, path: str) -> None:
        self.file_path = path

    def get_chunked(self, count: int) -> list:
        return []

    def load(self) -> None:
        try:
            self.file_path
            with open(self.file_path, "r") as source_file:
                local_source = list(csv.DictReader(source_file, delimiter=","))
                self.source_data.extend(local_source)

                ## Loop source data
                for line_item in self.source_data:
                    trip_code = line_item["trip_code"]
                    trip_source = line_item["source"]
                    trip_destination = line_item["destination"]

                    # Add source if missing in location list
                    if self.locations.count(trip_source) < 1:
                        self.locations.append(trip_source)

                    # Add source if missing in location list
                    if self.locations.count(trip_destination) < 1:
                        self.locations.append(trip_destination)

                    print("Processing => Source: {0} | Destination: {1}".format(trip_source, trip_destination))
        except IOError:
           print("There was an error writing to", self.file_path)
           sys.exit()

        # ## Get location data
        # for location_text in self.locations:
        #     coords = get_address_coords(location_text)
        #     print(coords)


        # test = next(item for item in source_data if item["destination"] == "ASABA")
        # print(test)



class data_processing:
    data: data_loader
    service: coord_service

    def __init__(self, data: data_loader, service: coord_service) -> None:
        self.data = data
        self.service = service

    def geocode_coords(self) -> list[Location]:
        return []

    def process_routes(self) -> list[TripInfo]:
        return []


if __name__ == "__main__":
    processing = data_processing(data_loader(defult_file_path), coord_service(nominatim_url, routing_url))
    print("...")
