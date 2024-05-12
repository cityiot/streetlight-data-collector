# -*- coding: utf-8 -*-

# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

"""Module for getting latitude and longitude for a street address.
   Uses https://nominatim.openstreetmap.org/
"""

import json

import requests


def stored_locations(street_address, city, country):
    """Some predefined locations (source: Google Maps)."""
    # For a location in Tampere without street address.
    if street_address == "" and city == "Tampere" and (country == "FI" or country == "Finland"):
        return [61.498302, 23.726467]
    # For a location in Tampere for which the openstreetmap API doesn't found coordinates.
    elif street_address == "Viklanpolku 5" and city == "Tampere" and (country == "FI" or country == "Finland"):
        return [61.492742, 23.805784]
    else:
        return None


def get_latlon(street_address, city="Tampere", country="Finland"):
    """Returns the latitude and the longitude as a two-element list for the given address."""
    stored_location = stored_locations(street_address, city, country)
    if stored_location is not None:
        return stored_location

    geocode_host = "https://nominatim.openstreetmap.org/search?format=json"
    street_param = "street=" + street_address
    city_param = "city=" + city
    country_param = "country=" + country
    query = "&".join([geocode_host, street_param, city_param, country_param])

    try:
        req = requests.get(query)
        data = json.loads(req.text)
        location = data[0]

        latitude = round(float(location["lat"]), 6)
        longitude = round(float(location["lon"]), 6)
        return [latitude, longitude]

    except:
        return None


def save_coordinates(coordinates, filename):
    """Saves the given coordinates to a file named filename. The coordinates are expected to be a dict with
       keys being (address, city, country) tuples and the values being the latitude and the longitude as a list.
       The file content will be a json list of objects with 'address', 'city', 'country' and 'coordinates' fields."""
    location_list = []
    for (address, city, country), location in coordinates.items():
        location_list.append({
            "address": address,
            "city": city,
            "country": country,
            "coordinates": location
        })

    with open(filename, "w", encoding="utf-8") as file:
        file.write(json.dumps(location_list, indent=2, ensure_ascii=False))


def load_coordinates(filename):
    """Loads and returns coordinates from a file named filename. The file content is expected to be a json list of objects
       with 'address', 'city', 'country' and 'coordinates' fields. The returned coordinates are a dict with
       keys being (address, city, country) tuples and the values being the latitude and the longitude as a list."""
    with open(filename, "r", encoding="utf-8") as file:
        location_list = json.load(file)

    coordinates = {}
    for location in location_list:
        coordinates[(location["address"], location["city"], location["country"])] = location["coordinates"]
    return coordinates
