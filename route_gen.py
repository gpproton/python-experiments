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
from datetime import datetime

## Default Values
global_file_path: str = "input.csv"
global_nominatim_url: str = "nominatim.openstreetmap.org"
global_routing_host: str = "valhalla1.openstreetmap.de"
global_http_chunks = 2
global_http_delay = 0.15
global_fixed_speed = 41
global_top_speed = 59

## Shared Types
Coord = TypedDict("Coord", {"lat": float, "lon": float})
Location = TypedDict("Location", { "name": str, "address": str, "lat": float, "lon": float})
RouteInfo = TypedDict("RouteInfo", {
    "trip_code": str,
    "length": str,
    "time": str,
    "source": Coord,
    "destination": Coord,
})
TripInfo = TypedDict("TripInfo", {
    "trip_code": str,
    "length": str,
    "time": str,
    "source": str,
    "source_coords": str,
    "destination": str,
    "destination_coords": str,
})

def current_time() -> str:
    now = datetime.now()
    return now.strftime("%H:%M:%S")

def log_info(message: str)-> None:
    print("{0} | {1}".format(current_time(), message))

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
              "units": "kilometers",
              "alternates": 0,
              "search_filter": {
                "exclude_closures": True
              },
              "costing": "auto",
              "costing_options": {
                  "auto": { "fixed_speed": global_fixed_speed, "top_speed": global_top_speed }
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



class data_handler:
    file_path = ""
    source_data = []
    locations: list[Location] = []
    routes: list[RouteInfo] = []
    trips: list[TripInfo] = []

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
                    trip_code: str = line_item["trip_code"]

                    line_item["source"] = source_value
                    line_item["destination"] = destination_value
                    line_item["trip_code"] = trip_code.lower()

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

        log_info("Loaded {0} locations for processing...".format(len(self.locations)))



class data_processing:
    data: data_handler
    service: coord_service
    location_pool: list[Location]
    
    def __init__(self, data: data_handler, service: coord_service) -> None:
        self.data = data
        self.service = service
    
    @classmethod
    async def bootstrap(cls, data: data_handler, service: coord_service):
        self = cls(data, service)
        
        ## Execute constrcutor async method
        self.location_pool = await self.__geocode_coords();
        self.data.routes = await self.__process_routes();
        
        return self


    async def __geocode_coords(self) -> list[Location]:
        # Local coord resolver functions
        async def resolve_location(name: str, delay: float = 0.15) -> Location:
            await asyncio.sleep(delay)
            
            response = self.service.get_coords(name)
            log_info("Name: {0}, Lat: {1}, Lon: {2}".format(response["name"], response["lat"], response["lon"]))
            
            return response

        async def exec_chunked(location_chunk: list[Location]):
            chunk_tasks = [resolve_location(loc["name"], global_http_delay) for loc in location_chunk]
            result = await asyncio.gather(*chunk_tasks)
            
            return result

        try:
            chunked_locations = list(batched(self.data.locations, global_http_chunks))
            tasks = [exec_chunked(loc_chunks) for loc_chunks in chunked_locations]
            
            log_info("Started resolving {0} locations for coordinates...".format(len(self.data.locations)))
            grouped_location: list[list[Location]] = await asyncio.gather(*tasks)
            
            log_info("Completed resolving locations...")

            all = [item for sublist in grouped_location for item in sublist]
            
            # update tagged data
            for local_item in self.data.locations:
                loc: Location = next((item for item in all if item['name'] == local_item["name"]), None)
                local_item.update(loc)
                    
            return all
            
        except Exception:
            log_info('Error processing coordinates...')
            return []

    async def __process_routes(self) -> list[TripInfo]:
        try:
            if(len(self.location_pool) > 0):
                query_data: list = self.data.source_data
                
                async def exec_delayed(trip_code: str, source: Coord, destination: Coord) -> RouteInfo:
                    await asyncio.sleep(global_http_delay)
                    response: RouteInfo = self.service.get_route_attributes(source, destination)
                    distance = "{0}KM".format(response["length"])
                    
                    log_info("Load route ID:{0}, Time: {1}, Distance: {2}".format(
                        trip_code,
                        response["time"],
                        distance
                    ))
                    
                    return response
                
                tasks = []
                for item in query_data:
                    data_source: Location = self.data.get_location_object(item["source"])
                    data_destination: Location = self.data.get_location_object(item["destination"])
                    
                    trip_code = item["trip_code"]
                    source: Coord = { "lat": data_source["lat"], "lon": data_source["lon"] }
                    destination: Coord = { "lat": data_destination["lat"], "lon": data_destination["lon"] }
                    
                    tasks.append(exec_delayed(trip_code, source, destination))
                    
                return await asyncio.gather(*tasks)
            
        except Exception as error:
            log_info('Error processing route information'.format(error))
            
        return []

async def main():
    proc = await data_processing.bootstrap(data_handler(global_file_path), coord_service(global_nominatim_url, global_routing_host))
    
    # pprint.pprint(proc.data.trips)

if __name__ == "__main__":
    asyncio.run(main())
