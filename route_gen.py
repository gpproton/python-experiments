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
import urllib.parse
import json
import re
import asyncio
import pprint
from typing import TypedDict
from itertools import batched

## Default Values
defult_file_path: str = "bhn_trip_routes.csv"
nominatim_url: str = "nominatim.openstreetmap.org"
routing_host: str = "valhalla1.openstreetmap.de"
global_http_chunks = 2
global_http_delay = 1

## Shared Types
Coord = TypedDict("Coord", {"lat": float, "lon": float, "address": str })
Location = TypedDict("Location", { "name": str, "address": str, "lat": float, "lon": float})
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
    geocoding_host: str
    routing_host: str
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Chrome"
    }

    def __init__(self, geocoding_host: str, routing_host: str) -> None:
        self.geocoding_host = geocoding_host
        self.routing_host = routing_host

    def get_coords(self, address: str) -> Location:
        try:
            conn = http.client.HTTPSConnection(self.geocoding_host)
            url_path = "/search?format=json&limit=1&addressdetails=0&email=dev@drolx.com&q=" + urllib.parse.quote(address)
            payload = ""

            conn.request("GET", url_path, payload, self.headers)
            res = conn.getresponse()
            data = res.read()
            result_items = data.decode("utf-8")
            item  = json.loads(result_items)[0]

            return { "name": address, "lat": item["lat"], "lon": item["lon"], "address": item["display_name"]}
        except http.client.HTTPException:
            print("There was an error with the HTTP request")
            sys.exit()


    def get_route_attributes(self, source: Coord, destination: Coord) -> RouteInfo:
        try:
            conn = http.client.HTTPSConnection(self.routing_host)
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
    locations: list[Location] = []

    def __init__(self, path: str) -> None:
        self.file_path = path
        self.__load()
        self.__load_locations()
    
    def __load(self) -> None:
        try:
            with open(self.file_path, "r") as source_file:
                local_source = list(csv.DictReader(source_file, delimiter=","))
                self.source_data.extend(local_source)
                
                # Correct locations name
                for line_item in self.source_data:
                    source_value = re.sub(r'[^a-zA-Z0-9\s]', ' ', line_item["source"]).capitalize()
                    destination_value = re.sub(r'[^a-zA-Z0-9\s]', ' ', line_item["destination"]).capitalize()
                    
                    line_item["source"] = source_value
                    line_item["destination"] = destination_value

        except IOError:
           print("There was an error writing to", self.file_path)
           sys.exit()
        
    def get_location_object(self, name: str) -> Location | None:
        return next((item for item in self.locations if item['name'] == name), None)


    def __load_locations(self) -> None:
        for line_item in self.source_data:
                trip_source: str = line_item["source"]
                trip_destination: str = line_item["destination"]
                
                check_source = self.get_location_object(trip_source)
                check_destination = self.get_location_object(trip_destination)
                
                if(check_source is None or len(check_source) < 1):
                    self.locations.append({ 'name': trip_source })
                if(check_destination is None or len(check_destination) < 1):
                    self.locations.append({ 'name': trip_destination })

        print("Loaded {0} locations for processing...".format(len(self.locations)))



class data_processing:
    data: data_loader
    service: coord_service

    def __init__(self, data: data_loader, service: coord_service) -> None:
        self.data = data
        self.service = service

    async def geocode_coords(self) -> list[Location]:
        # Local coord resolver functions
        async def resolve_location(name: str, delay: float = 0.15) -> Location:
            await asyncio.sleep(delay)
            
            return self.service.get_coords(name)

        async def exec_chunked(location_chunk: list[Location]):
            chunk_tasks = [resolve_location(loc["name"], global_http_delay) for loc in location_chunk]
            result = await asyncio.gather(*chunk_tasks)
            
            return result

        try:
            chunked_locations = list(batched(self.data.locations, global_http_chunks))
            tasks = [exec_chunked(loc_chunks) for loc_chunks in chunked_locations]
            all_location: list[list[Location]] = await asyncio.gather(*tasks)
            location_results = [item for sublist in all_location for item in sublist]
            
            return location_results
        except Exception:
            print('Error loading coordinates...')
            return []

    def process_routes(self) -> list[TripInfo]:
        return []

async def main():
    processing = data_processing(data_loader(defult_file_path), coord_service(nominatim_url, routing_host))

    locations = await processing.geocode_coords()
    
    pprint.pprint(locations)

        

if __name__ == "__main__":
    asyncio.run(main())
