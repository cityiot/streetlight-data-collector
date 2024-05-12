# -*- coding: utf-8 -*-

# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

"""Module for loading Tampere city street light data."""

import copy
import csv
import datetime
import json

import requests

import common_utils
import fiware_tools
import geocode
import streetlight_models


def items_for_update_data(entity, updates, attribute_names, attribute_types, attribute_values, timestamp,
                          extra_check=None, allow_same_value=False, allow_same_time=False):
    """Checks whether the given attribute values are new updates for the given entity.
       Any new updates that are found are added to the given updates list.
       - attribute_names is a list of the attribute names
       - attribute_types is a list of the attribute types
       - attribute_values is a list of the new attribute values
       - timestamp is the UNIX timestamp of the measurements (given in ms)
       - if extra_check is not None, the update is only accepted if it the attribute value != extra_check
    """
    entity_copy = copy.deepcopy(entity)
    attribute_checklist = zip(attribute_names, attribute_types, attribute_values)
    for attribute_name, attribute_type, attribute_value in attribute_checklist:
        identifier = (entity_copy["id"], entity_copy["type"], attribute_name)
        if identifier not in updates:
            updates[identifier] = []
        if len(updates[identifier]) > 0:
            entity_copy[attribute_name] = fiware_tools.get_attribute(updates[identifier][-1], timestamp_as_str=False)

        if (attribute_value is not None and
            (extra_check is None or attribute_value != extra_check) and
            fiware_tools.check_for_update(
                entity=entity_copy,
                attribute_name=attribute_name,
                value=attribute_value,
                timestamp=timestamp,
                allow_same_value=allow_same_value,
                allow_same_time=allow_same_time)):

            updates[identifier].append({
                "entity_id": entity_copy["id"],
                "entity_type": entity_copy["type"],
                "attribute": attribute_name,
                "type": attribute_type,
                "value": attribute_value,
                "timestamp": timestamp
            })
            updates[identifier].sort(key=lambda x: x["timestamp"])


def sort_update_list(update_list):
    """Sorts the given update list."""
    update_list.sort(key=lambda x: (x["timestamp"], x["entity_type"], x["entity_id"], x["attribute"]))


def clean_update_data(update_data):
    """Cleans the collection of updates by combining the subattribute values that have the same timestamps
       for attributes that have the type "StructuredType". (used for electric intensity and voltage)"""
    clean_updates = {}
    if len(update_data) == 0:
        return clean_updates

    updates = copy.deepcopy(update_data)
    for identifier in updates:
        if len(updates[identifier]) == 0:
            continue
        clean_updates[identifier] = []
        sort_update_list(updates[identifier])
        updates[identifier] = common_utils.remove_duplicates(updates[identifier])

        previous_update = updates[identifier][0]
        clean_updates[identifier].append(previous_update)
        for update in updates[identifier][1:]:
            if (update["timestamp"] == previous_update["timestamp"] and
                    update["entity_type"] == previous_update["entity_type"] and
                    update["entity_id"] == previous_update["entity_id"] and
                    update["attribute"] == previous_update["attribute"] and
                    update["type"] == "StructuredValue" and
                    previous_update["type"] == "StructuredValue"):

                for item in previous_update["value"]:
                    if previous_update["value"][item] is None and update["value"].get(item, None) is not None:
                        previous_update["value"][item] = update["value"][item]
                for item in update["value"]:
                    if item not in previous_update["value"] and update["value"][item] is not None:
                        previous_update["value"][item] = update["value"][item]

            else:
                clean_updates[identifier].append(update)
                previous_update = update

        sort_update_list(clean_updates[identifier])
        clean_updates[identifier] = common_utils.remove_duplicates(clean_updates[identifier])

    return clean_updates


def add_entity_locations(entities, coordinates={}):
    """Adds location information to the given entities. If the relevant address is not included in the given
       coordinates, it is fetched from internet using the component geocode."""
    for entity in entities:
        # Entities of type Device are handled separately
        if "location" in entity or entity["type"] == "Device":
            continue
        if "address" not in entity:
            entity["address"] = streetlight_models.get_postal_address("")

        streetaddress = entity["address"]["value"]["streetAddress"]
        city = entity["address"]["value"]["addressLocality"]
        country = entity["address"]["value"]["addressCountry"]
        full_address = (streetaddress, city, country)

        if full_address not in coordinates:
            location = geocode.get_latlon(*full_address)
            if location is not None:
                coordinates[full_address] = location
            else:
                print("Failed to get location for", full_address)
                continue
        else:
            location = coordinates[full_address]

        entity["location"] = streetlight_models.get_location_attribute(*location)

    # Handle the entities of type Device by using the reference given in the attribute owner.
    for entity in entities:
        if "location" in entity:
            continue

        if entity["type"] == "Device":
            owner_entity_type, owner_entity_id = entity["owner"]["value"].split(":")
            owner_entity_list = [x for x in entities if x["type"] == owner_entity_type and x["id"] == owner_entity_id]
            if len(owner_entity_list) == 0:
                continue
            else:
                owner_entity = owner_entity_list[0]
                if "location" in owner_entity:
                    entity["location"] = owner_entity["location"]


def load_illuminance_data(data=None, entities=[], updates={}):
    """Parses the given illuminance data and using the given previous entities and updates.
       Returns the parsed entity list and update data."""
    if data is None or len(data) == 0:
        return entities, updates

    # sort the input data by the timestamps
    time_column = "Aika"
    data.sort(key=lambda x: common_utils.to_timestamp(x[time_column]))

    entity_list = copy.deepcopy(entities)
    update_data = copy.deepcopy(updates)
    for item in data:
        try:
            cabinet_id = common_utils.to_str(item["Ohjauskeskus"]).replace(" ", "_")
            timestamp = common_utils.to_timestamp(item[time_column])
            illuminance = common_utils.to_int(item["valoisuusarvo"])
            lux_limit_on = common_utils.to_int(item["lux_limit_on"])
            lux_limit_off = common_utils.to_int(item["lux_limit_off"])

            cabinet_entity_id = streetlight_models.get_entity_id("StreetlightControlCabinet", cabinet_id)
            try:
                cabinet_index = [entity["id"] for entity in entity_list].index(cabinet_entity_id)
            except ValueError:
                cabinet_index = -1

            if cabinet_index < 0:
                # add new cabinet to the entity_list
                entity_list.append(
                    streetlight_models.control_cabinet_entity(cabinet_entity_id, timestamp))
                cabinet_index = len(entity_list) - 1

            # add cabinet's illuminance parameters to the update_data if necessary
            items_for_update_data(
                entity_list[cabinet_index],
                update_data,
                ["illuminanceOn", "illuminanceOff"],
                ["Number", "Number"],
                [lux_limit_on, lux_limit_off],
                timestamp
            )

            sensor_entity_id = streetlight_models.get_entity_id("Device", "illuminance_" + cabinet_id)
            try:
                sensor_index = [entity["id"] for entity in entity_list].index(sensor_entity_id)
            except ValueError:
                sensor_index = -1

            if sensor_index < 0:
                # add new illuminance sensor entity to the entity_list
                entity_list.append(
                    streetlight_models.illuminance_sensor_entity(sensor_entity_id, entity_list[cabinet_index]))
                sensor_index = len(entity_list) - 1

            measurement_entity_id = streetlight_models.get_entity_id("WeatherObserved", cabinet_id)
            try:
                measurement_index = [entity["id"] for entity in entity_list].index(measurement_entity_id)
            except ValueError:
                measurement_index = -1

            if measurement_index < 0:
                # add new illuminance measurement entity to the entity_list
                entity_list.append(
                    streetlight_models.illuminance_measurement_entity(
                        measurement_entity_id, sensor_entity_id, timestamp))
                measurement_index = len(entity_list) - 1

            # add illuminance to the update_data if necessary
            items_for_update_data(
                entity_list[measurement_index],
                update_data,
                ["illuminance", "dateObserved"],
                ["Number", "DateTime"],
                [illuminance, timestamp],
                timestamp,
                allow_same_value=True
            )

        except Exception as error:
            print("load_illuminance_data:", error)

    return entity_list, update_data


def load_electricity_data(data=None, entities=[], updates={}):
    """Parses the given electricity data and using the given previous entities and updates.
       Returns the parsed entity list and update data."""
    if data is None or len(data) == 0:
        return entities, updates

    # sort the input data by the timestamps
    time_column = "Aika"
    data.sort(key=lambda x: common_utils.to_timestamp(x[time_column]))

    entity_list = copy.deepcopy(entities)
    update_data = copy.deepcopy(updates)
    for item in data:
        try:
            group_id = common_utils.to_str(item["KV_keskus"]).replace(" ", "_")
            measurement_id = common_utils.to_int(item["Vaiheet"])
            measurement_type = common_utils.to_str(item["Virta_Jännite"])
            raw_value = common_utils.to_float(item["lukema_raw"])
            timestamp = common_utils.to_timestamp(item[time_column])
            address = common_utils.handle_address(item["Katuosoite"])
            relays = common_utils.to_list(item["Releet"])
            cabinet_id = common_utils.to_str(item["Ohjauskeskus"])
            if cabinet_id is not None:
                cabinet_id = cabinet_id.replace(" ", "_")
            lux_limit_on = common_utils.to_int(item["lux_limit_on"])
            lux_limit_off = common_utils.to_int(item["lux_limit_off"])

            intensity = streetlight_models.get_phase_struct()
            voltage = streetlight_models.get_phase_struct()

            # NOTE: the following trusts that the phases L1, L2 and L3 corresponds to measurement_ids 33, 34 and 35
            value = raw_value / 10
            phase = str(measurement_id - 32)
            if measurement_type == "CurrentCluster.currentPresentValue":
                intensity["L" + phase] = value
            elif measurement_type == "VoltageCluster.voltagePresentValue":
                voltage["L" + phase] = value

            streetlight_entity_id = streetlight_models.get_entity_id("StreetlightGroup", group_id)
            if cabinet_id is not None:
                cabinet_entity_id = streetlight_models.get_entity_id("StreetlightControlCabinet", cabinet_id)
                try:
                    cabinet_index = [entity["id"] for entity in entity_list].index(cabinet_entity_id)
                except ValueError:
                    cabinet_index = -1

                if cabinet_index < 0:
                    # add new cabinet to the entity_list
                    entity_list.append(
                        streetlight_models.control_cabinet_entity(cabinet_entity_id, timestamp))
                    cabinet_index = len(entity_list) - 1

                # add cabinet's illuminance parameters to the update_data if necessary
                items_for_update_data(
                    entity=entity_list[cabinet_index],
                    updates=update_data,
                    attribute_names=["illuminanceOn", "illuminanceOff"],
                    attribute_types=["Number", "Number"],
                    attribute_values=[lux_limit_on, lux_limit_off],
                    timestamp=timestamp
                )

                if streetlight_entity_id not in entity_list[cabinet_index]["refStreetlightGroup"]["value"]:
                    # add the streetlight group to the control cabinet entity
                    entity_list[cabinet_index]["refStreetlightGroup"]["value"].append(streetlight_entity_id)

                if group_id == cabinet_id and address is not None and "address" not in entity_list[cabinet_index]:
                    # add address to the control cabinet entity (assume the control cabinet's location
                    # is the same as the streetlight group's that have the same identifier)
                    entity_list[cabinet_index]["address"] = streetlight_models.get_postal_address(address)
            else:
                cabinet_entity_id = None

            try:
                streetlight_index = [entity["id"] for entity in entity_list].index(streetlight_entity_id)
            except ValueError:
                streetlight_index = -1

            if streetlight_index < 0:
                # add new cabinet to the entity_list
                entity_list.append(
                    streetlight_models.streetlight_group_entity(
                        streetlight_group_id=streetlight_entity_id,
                        cabinet_id=cabinet_entity_id,
                        relays=relays,
                        timestamp=timestamp))
                if address is not None:
                    entity_list[streetlight_index]["address"] = streetlight_models.get_postal_address(address)
                streetlight_index = len(entity_list) - 1

            # add the control cabinet to the streetlight group entity
            streetlight_models.add_cabinet_to_streetlight_group(
                streetlight_group_entity=entity_list[streetlight_index],
                cabinet_id=cabinet_entity_id,
                relays=relays)

            if address is not None and "address" not in entity_list[streetlight_index]:
                entity_list[streetlight_index]["address"] = streetlight_models.get_postal_address(address)

            # add cabinet's illuminance parameters to the update_data if necessary
            items_for_update_data(
                entity=entity_list[streetlight_index],
                updates=update_data,
                attribute_names=["intensity", "voltage"],
                attribute_types=["StructuredValue", "StructuredValue"],
                attribute_values=[intensity, voltage],
                timestamp=timestamp,
                extra_check=streetlight_models.get_phase_struct(),
                allow_same_time=True
            )

        except Exception as error:
            print("load_electricity_data:", error)

    return entity_list, update_data


def load_doorsensor_data(data=None, entities=[], updates={}):
    """Parses the given door sensor data and using the given previous entities and updates.
       Returns the parsed entity list and update data."""
    if data is None or len(data) == 0:
        return entities, updates

    # sort the input data by the timestamps
    time_column = "time"
    data.sort(key=lambda x: common_utils.to_timestamp(x[time_column], localtime=False))

    entity_list = copy.deepcopy(entities)
    update_data = copy.deepcopy(updates)
    for item in data:
        try:
            group_id = common_utils.to_str(item["name"]).replace(" ", "_")
            attribute_input = common_utils.to_str(item["attribute"])
            timestamp = common_utils.to_timestamp(item[time_column], localtime=False)

            # find the whether the door is open or closed
            attribute_text = "BinaryInputCluster.binaryPresentValue="
            value_int = common_utils.to_int(attribute_input[len(attribute_text):])
            if value_int == 1:
                value = "closed"
            else:
                value = "open"

            # find the streetlight group entity
            streetlight_entity_id = streetlight_models.get_entity_id("StreetlightGroup", group_id)
            try:
                streetlight_index = [entity["id"] for entity in entity_list].index(streetlight_entity_id)
            except ValueError:
                streetlight_index = -1
            if streetlight_index < 0:
                # add new streetlight group entity
                entity_list.append(
                    streetlight_models.streetlight_group_entity(
                        streetlight_group_id=streetlight_entity_id,
                        cabinet_id=None,
                        relays=None,
                        timestamp=timestamp))
                streetlight_index = len(entity_list) - 1

            doorsensor_id = streetlight_models.get_entity_id("Device", "doorsensor_" + group_id)
            try:
                doorsensor_index = [entity["id"] for entity in entity_list].index(doorsensor_id)
            except ValueError:
                doorsensor_index = -1

            if doorsensor_index < 0:
                # add new door sensor to the entity_list
                entity_list.append(
                    streetlight_models.doorsensor_entity(
                        sensor_id=doorsensor_id,
                        streetlight_group_entity=entity_list[streetlight_index],
                        timestamp=timestamp))
                doorsensor_index = len(entity_list) - 1

            # add door sensor's value parameter to the update_data if necessary
            items_for_update_data(
                entity=entity_list[doorsensor_index],
                updates=update_data,
                attribute_names=["value"],
                attribute_types=["Text"],
                attribute_values=[value],
                timestamp=timestamp,
                allow_same_value=True
            )

        except Exception as error:
            print("load_doorsensor_data:", error)

    return entity_list, update_data


def load_data_from_files(illuminance_files=[], electricity_files=[], doorsensor_files=[], coordinates={}):
    """Loads street light data from the given files. Returns the resulting entities and update data."""
    entities = []
    updates = {}

    file_lists = (illuminance_files, electricity_files, doorsensor_files)
    load_functions = (load_illuminance_data, load_electricity_data, load_doorsensor_data)

    for file_list, load_function in zip(file_lists, load_functions):
        data = []
        for filename in file_list:
            print("Reading:", filename)
            with open(filename, mode="r", encoding="utf-8") as file:
                new_data = json.load(file)

            data += new_data

        entities, updates = load_function(
            data=data,
            entities=entities,
            updates=updates)
        print(len(entities), "entities and", sum([len(updates[key]) for key in updates]), "updates.")

    print("Cleaning the entities and updates")
    updates = clean_update_data(updates)
    add_entity_locations(entities, coordinates)
    fiware_tools.timestamps_to_isoformat(entities)
    print(len(entities), "entities and", sum([len(updates[key]) for key in updates]), "updates.")

    return entities, updates


def load_data_from_api(api_file, begin_date, end_date, coordinates={}, save_to_file=False):
    """Loads street light from a given API using the given begin_data and end_data as query parameters.
       If save_to_file is True, stores the loaded json objects to files.
       Returns the resulting entities and update data."""
    entities = []
    updates = {}

    with open(api_file, mode="r", encoding="utf-8") as file:
        api_data = json.load(file)

    api_types = ("illuminance_apis", "electricity_apis", "doorsensor_apis")
    load_functions = (load_illuminance_data, load_electricity_data, load_doorsensor_data)
    filenames = ("illuminance_{date:}.json", "electricity_{date:}.json", "doorsensor_{date:}.json")

    headers = {
        api_data["header_attr"]: api_data["api_key"]
    }

    for api_type, load_function, filename in zip(api_types, load_functions, filenames):
        data = []
        status_code = 0
        req = None
        for api in api_data[api_type]:
            query = api_data["host"] + api.format(begin=begin_date, end=end_date)
            try:
                req = requests.get(query, headers=headers)
            except Exception as error:
                # TODO: add proper error handling
                print(query, headers)
                print("Error:", error)
                continue

            status_code = getattr(req, "status_code", 0)
            print(query, "=>", status_code)

            if status_code == 200 and req.text != "":
                new_data = req.json()
            else:
                new_data = []
                if req.text != "":
                    print(req.text)

            data += new_data

        if save_to_file and status_code == 200 and getattr(req, "text", "") != "":
            with open(filename.format(date=end_date), mode="w", encoding="utf-8") as file:
                file.write(json.dumps(data, indent=2, ensure_ascii=False))

        entities, updates = load_function(
            data=data,
            entities=entities,
            updates=updates)
        print(len(entities), "entities and", sum([len(updates[key]) for key in updates]), "updates.")

    print("Cleaning the entities and updates")
    updates = clean_update_data(updates)
    add_entity_locations(entities, coordinates)
    fiware_tools.timestamps_to_isoformat(entities)
    print(len(entities), "entities and", sum([len(updates[key]) for key in updates]), "updates.")

    return entities, updates


# NOTE: unused values in the illuminance data:
# "pvm"
# "kloaika"
# "apip"

# NOTE: unused values in the electricity data:
# "pvm"
# "kloaika"
# "apip"
# "Virta"
# "Jännite"
# "Virta_1"
# "Virta_2"
# "Virta_3"
# "Jännite_1"
# "Jännite_2"
# "Jännite_3"
# "Ohj_ryhmia"
# "Ohjausryhma"
# "Lat"
# "Long"

# NOTE: unused values in the door sensor data:
# "endpointid"
# "time_eest"
