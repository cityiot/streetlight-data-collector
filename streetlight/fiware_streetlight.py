# -*- coding: utf-8 -*-

# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

"""Module for putting the Tampere street light data into Fiware."""

import copy
import datetime
import json
import sys
import threading
import time

import common_utils
import fiware_tools
import geocode
import streetlight_models
import tampere_streetlight


def create_entities(entity_list, use_patch=True, fiware_service=None, fiware_servicepath=None):
    """Creates new entities and updates the existing ones according to the given entity list.
       If use_patch is True, uses patch updates. Otherwise each entity is updated by a separate HTTP call."""
    new_entities = []
    changed_entities = []

    for entity in entity_list:
        old_entity = fiware_tools.read_entity(
            entity_id=entity["id"],
            entity_type=entity["type"],
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)
        if old_entity is None:
            new_entities.append(entity)
            continue

        changed_attributes = {}
        found_change_attribute = False
        dynamic_attributes = common_utils.flat_list(
            streetlight_models.get_entity_attributes(entity["type"])[1])

        for attribute_name, attribute_value in entity.items():
            type_check = isinstance(attribute_value, str)
            if type_check:
                changed_attributes[attribute_name] = attribute_value
                continue
            elif attribute_name in dynamic_attributes or attribute_value["value"] is None:
                continue

            # check whether the metadata has new or changed attributes
            new_metadata = attribute_value.get("metadata", {})
            old_metadata = old_entity[attribute_name].get("metadata", {})
            metadata_check = False
            for meta_attr_name, meta_attr_value in new_metadata.items():
                if meta_attr_name not in old_metadata or meta_attr_value != old_metadata[meta_attr_name]:
                    if meta_attr_name != "relays":
                        metadata_check = True
                    else:
                        if meta_attr_name not in old_metadata:
                            metadata_check = True
                        else:
                            # add new relays to the end of the old list
                            relay_list = copy.deepcopy(old_metadata[meta_attr_name])
                            for relay in new_metadata[meta_attr_name]:
                                if relay not in relay_list:
                                    relay_list.append(relay)
                            attribute_value["metadata"][meta_attr_name] = relay_list
                            if relay_list != old_metadata[meta_attr_name]:
                                metadata_check = True

            if attribute_name == "refStreetlightCabinetController" and attribute_name in old_entity:
                # combine the street light control cabinet list from the old and new values
                delimiter = "___"
                controller_list = old_entity[attribute_name]["value"].split(delimiter)
                new_controllers = attribute_value["value"].split(delimiter)
                for new_controller in new_controllers:
                    if new_controller not in controller_list:
                        controller_list.append(new_controller)
                attribute_value["value"] = delimiter.join(controller_list)

            if attribute_name == "refStreetlightGroup" and attribute_name in old_entity:
                # add new street light group references to the old list
                new_group_list = copy.deepcopy(old_entity[attribute_name]["value"])
                for group in attribute_value["value"]:
                    if group not in new_group_list:
                        new_group_list.append(group)
                attribute_value["value"] = new_group_list

            if (attribute_name not in old_entity or
                    attribute_value["type"] != old_entity[attribute_name]["type"] or
                    attribute_value["value"] != old_entity[attribute_name]["value"] or
                    metadata_check):
                # add old metadata attributes to the new attribute metadata
                if "metadata" not in attribute_value:
                    attribute_value["metadata"] = {}
                for meta_attr_name, meta_attr_value in old_metadata.items():
                    if (meta_attr_name not in new_metadata or
                            meta_attr_name not in ["dateCreated", "dateModified"]):
                        attribute_value["metadata"][meta_attr_name] = meta_attr_value

                changed_attributes[attribute_name] = attribute_value
                found_change_attribute = True

        if found_change_attribute:
            changed_entities.append(changed_attributes)

    if len(new_entities) > 0:
        fiware_tools.create_new_entities(
            entity_list=new_entities,
            use_patch=use_patch,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)
    if len(changed_entities) > 0:
        fiware_tools.append_to_entities(
            entity_list=changed_entities,
            use_patch=use_patch,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)


def find_latest_timestamp(update_data):
    """Returns the latest timestamp from the given update data or None if the update data is empty."""
    latest_timestamp = None
    for identifier in update_data:
        if len(update_data[identifier]) > 0:
            new_timestamp = update_data[identifier][-1]["timestamp"]
            if latest_timestamp is None or new_timestamp > latest_timestamp:
                latest_timestamp = new_timestamp
    return latest_timestamp


def remove_old_updates(update_data, fiware_service=None, fiware_servicepath=None):
    """Returns a list of updates where all update timestamps are newer than the current ones in Orion."""
    new_updates = {}
    for index, (identifier, update_list) in enumerate(update_data.items()):
        entity_id, entity_type, attribute_name = identifier

        old_value = fiware_tools.read_attribute(
            entity_id=entity_id,
            entity_type=entity_type,
            attribute_name=attribute_name,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)

        if old_value is None:
            old_timestamp_iso = None
        else:
            old_timestamp_iso = old_value.get("metadata", {}).get("timestamp", {}).get("value", None)

        if old_timestamp_iso is None:
            new_updates[identifier] = update_list
        else:
            old_timestamp = common_utils.to_timestamp(
                time_string=old_timestamp_iso, decimal_count=fiware_tools.orion_time_decimals, localtime=False)

            for update in update_list:
                new_timestamp = update.get("timestamp", None)
                if new_timestamp is not None and new_timestamp > old_timestamp:
                    if identifier not in new_updates:
                        new_updates[identifier] = []
                    new_updates[identifier].append(update)

    return new_updates


def get_update_patches(update_data):
    """Divides and returns the given update data to update patches, so that each patch contain only
       one update for each identifier. Identifiers are (entity type, entity id, attribute name) tuples."""
    update_collection = []
    updates_copy = copy.deepcopy(update_data)
    index = 0
    while len(updates_copy) > 0:
        update_collection.append([])
        identifiers = list(updates_copy)
        for identifier in identifiers:
            update_collection[index].append(updates_copy[identifier][0])
            updates_copy[identifier][:] = updates_copy[identifier][1:]
            if len(updates_copy[identifier]) == 0:
                updates_copy.pop(identifier, None)

        index += 1

    return update_collection


def setup_subscriptions(fiware_service=None, fiware_servicepath=None, target_host=None, platform_key=None):
    """Sets up and creates the needed subscriptions for Quantum Leap for the street light data."""
    subs = fiware_tools.fetch_subscriptions(
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)
    # simple check for existing subscriptions (could be made a lot better)
    if len(subs) > 0:
        return

    attributes = {}
    for entity_type in streetlight_models.get_entity_types():
        attributes[entity_type] = streetlight_models.get_entity_attributes(entity_type)

    fiware_tools.create_subscriptions(
        attributes=attributes,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath,
        target_host=target_host,
        platform_key=platform_key)
    print(datetime.datetime.now(), "- Created subscriptions.")


def send_streetlight_data(entity_list, update_data, fiware_service=None,
                          fiware_servicepath=None, use_patch=True):
    """Sends the given entity and update data to FIWARE."""
    create_entities(
        entity_list=entity_list,
        use_patch=use_patch,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)
    print(datetime.datetime.now(), "- Checked updates for", len(entity_list), "entities.")

    print("Removing old updates from the update list.")
    update_data = remove_old_updates(
        update_data=update_data,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)
    update_patch_collection = get_update_patches(update_data)
    print(sum([len(x) for x in update_patch_collection]), "new updates found.")

    for index, update_patch in enumerate(update_patch_collection):
        fiware_tools.update_entities(
            update_list=update_patch,
            use_patch=use_patch,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)
        print(datetime.datetime.now(), "-", len(update_patch), "updates sent to Fiware.",
              sum([len(x) for x in update_patch_collection[index+1:]]), "updates remaining.")
        time.sleep(15.0)

    print(sum([len(x) for x in update_patch_collection]), "updates sent to Fiware.")


def send_saved_data(data_file, coordinate_file, fiware_service=None, fiware_servicepath=None):
    """Loads the data from the files given in the data_file parameter."""
    try:
        # Load the data file names
        with open(data_file, mode="r", encoding="utf-8") as open_file:
            data_filenames = json.load(open_file)
        illuminance_files = data_filenames["illumination_files"]
        electricity_files = data_filenames["electricity_files"]
        doorsensor_files = data_filenames["doorsensor_files"]
    except:
        print("Failed to parse data file names from:", data_file)
        illuminance_files = []
        electricity_files = []
        doorsensor_files = []

    # Load stored coordinates
    try:
        coordinates = geocode.load_coordinates(coordinate_file)
    except:
        coordinates = {}
    old_coordinates = copy.deepcopy(coordinates)

    # Parse the data from the files
    entities, updates = tampere_streetlight.load_data_from_files(
        illuminance_files, electricity_files, doorsensor_files, coordinates)
    latest_timestamp = find_latest_timestamp(updates)

    # Store the the coordinates if there are new entries
    coordinate_check = False
    for coordinate in coordinates:
        if coordinate not in old_coordinates:
            old_coordinates[coordinate] = coordinates[coordinate]
            coordinate_check = True
    if coordinate_check:
        try:
            geocode.save_coordinates(old_coordinates, coordinate_file)
        except Exception as error:
            print("Failed to store coordinates:", error)

    # Send the new data to FIWARE
    if latest_timestamp is not None:
        send_streetlight_data(
            entity_list=entities,
            update_data=updates,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)

    return latest_timestamp


def fetch_and_send(n_days, api_file, coordinate_file, store_data,
                   fiware_service=None, fiware_servicepath=None):
    """Fetches street light data from the given API and sends it to FIWARE.
       n_days is the number of previous days for which data is queried. If the current date is 2018-11-23
       and n_days=3, the API is queried with the begin date as 2018-11-20 and the end date as 2018-11-23.
       Returns the latest timestamp for an update received from the API or None if the query failed."""
    max_days = 7
    min_days = 0
    if n_days > max_days:
        n_days = max_days
    if n_days < min_days:
        n_days = min_days

    day_string = "{year:}-{month:}-{day:}"
    today = datetime.datetime.now()
    n_days_ago = today - datetime.timedelta(days=n_days)
    start_date = day_string.format(year=n_days_ago.year, month=n_days_ago.month, day=n_days_ago.day)
    end_date = day_string.format(year=today.year, month=today.month, day=today.day)

    # Load stored coordinates
    try:
        coordinates = geocode.load_coordinates(coordinate_file)
    except:
        coordinates = {}
    old_coordinates = copy.deepcopy(coordinates)

    # Fetch new data from the API
    try:
        entities, updates = tampere_streetlight.load_data_from_api(
            api_file=api_file,
            begin_date=start_date,
            end_date=end_date,
            coordinates=coordinates,
            save_to_file=store_data)
        latest_timestamp = find_latest_timestamp(updates)
    except Exception as error:
        print("Failed to fetch data from API:", error)
        entities = []
        updates = {}
        latest_timestamp = None

    # Store the the coordinates if there are new entries
    coordinate_check = False
    for coordinate in coordinates:
        if coordinate not in old_coordinates:
            old_coordinates[coordinate] = coordinates[coordinate]
            coordinate_check = True
    if coordinate_check:
        try:
            geocode.save_coordinates(old_coordinates, coordinate_file)
        except Exception as error:
            print("Failed to store coordinates:", error)

    # Send the new data to FIWARE
    if latest_timestamp is not None:
        send_streetlight_data(
            entity_list=entities,
            update_data=updates,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)

    return latest_timestamp


def get_days_for_fetch(timestamp):
    """Returns how many number of days of data will be used in the next data fetch from API."""
    start_dt = datetime.datetime.fromtimestamp(timestamp / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
    today_dt = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return int((today_dt - start_dt).total_seconds()) // int(datetime.timedelta(days=1).total_seconds())


def sleep_until(wake_up_hour, wake_up_minute):
    """Makes the thread go to sleep until the next wake up hour and minute."""
    current = datetime.datetime.now()
    target = datetime.datetime(year=current.year, month=current.month, day=current.day,
                               hour=wake_up_hour, minute=wake_up_minute)
    if (target - current).total_seconds() < 0:
        target += datetime.timedelta(days=1)
    sleep_seconds = (target - current).total_seconds()
    print(datetime.datetime.now(), "- Going to sleep for:", datetime.timedelta(seconds=sleep_seconds), "seconds")
    time.sleep(sleep_seconds)


def data_fetcher(config):
    """Setups the environment, sends the stored data to FIWARE, and then
       periodically fetches new data from the API and sends it to FIWARE."""
    # Set the addresses for Orion and Quantum Leap.
    if config["keyrock_address"] is not None:
        fiware_tools.keyrock_address = config["keyrock_address"]
    if config["orion_address"] is not None:
        fiware_tools.orion_address = config["orion_address"]
    if config["quantumleap_address"] is not None:
        fiware_tools.quantumleap_address = config["quantumleap_address"]

    # Get the update time from the configurations.
    update_hour, update_minute = [int(x) for x in config["update_time"].split(":")]
    # Get the minimum wait time before new queries to the API and the maximum number of fails per day
    retry_wait = config["retry_wait_s"]
    max_fails = config["max_fails"]
    # Get the configurations file names
    coordinate_file = config["coordinate_file"]
    data_file = config["data_file"]
    api_file = config["api_file"]
    # Get the configuration for whether to save to fetched data to file or nor
    store_data = config["store_data"]

    # Set the FIWARE service and service path
    fiware_service = config.get("fiware_service", None)
    fiware_servicepath = config.get("fiware_servicepath", None)

    # Get the apikey token and the platform key and host for notifications
    try:
        with open(api_file, mode="r", encoding="utf-8") as config_file:
            config_json = json.load(config_file)
        apikey_header = config_json.get("apikey_header", "apikey")
        secret_apikey = config_json.get("fiware_apikey", None)
        platform_key = config_json.get("fiware_platform_key", None)
        notification_target = config_json.get("fiware_notification_target", None)
        auth_secret_key = config_json.get("auth_secret", "")
        access_token = config_json.get("access_token", None)
        refresh_token = config_json.get("refresh_token", "")
    except:
        apikey_header = "apikey"
        secret_apikey = ""
        platform_key = None
        notification_target = None
        auth_secret_key = ""
        access_token = None
        refresh_token = ""

    if secret_apikey is None:
        use_tokens = True
        access_key = access_token
    else:
        use_tokens = False
        access_key = secret_apikey

    fiware_tools.request_maker.insert_keys(
        use_tokens=use_tokens,
        secret_key=auth_secret_key,
        apikey_header=apikey_header,
        access_token=access_key,
        refresh_token=refresh_token)

    create_subscriptions = config.get("create_subscriptions", True)
    # Setup the subscriptions for Quantum Leap from Orion.
    if create_subscriptions:
        setup_subscriptions(
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath,
            target_host=notification_target,
            platform_key=platform_key)

    # Send the stored data to FIWARE
    timestamp = send_saved_data(
        data_file=data_file,
        coordinate_file=coordinate_file,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    last_round_success = False
    fails = 0

    while True:
        if timestamp is None:
            # no data received last time => just fetch the previous weeks data on the next try
            days = 7
        else:
            days = get_days_for_fetch(timestamp)
        if timestamp is None:
            print("timestamp=None, days=", days, sep="")
        else:
            print("timestamp=", timestamp, " (", datetime.datetime.fromtimestamp(timestamp / 1000),
                  "), days=", days, sep="")

        if days <= 0 or last_round_success or fails >= max_fails:
            sleep_until(update_hour, update_minute)
            last_round_success = False
            fails = 0
        else:
            new_timestamp = fetch_and_send(
                n_days=days,
                api_file=api_file,
                coordinate_file=coordinate_file,
                store_data=store_data,
                fiware_service=fiware_service,
                fiware_servicepath=fiware_servicepath)

            last_round_success = new_timestamp is not None and (timestamp is None or new_timestamp > timestamp)
            if last_round_success:
                timestamp = new_timestamp
                fails = 0
            else:
                fails += 1

            wait_time = retry_wait * (fails + 1)
            print(datetime.datetime.now(), "- Going to sleep for:", wait_time, "seconds")
            time.sleep(wait_time)


def default_parameters():
    """Returns the default parameters for the street light data fetcher."""
    return {
        "data_file": "data_files.json",
        "coordinate_file": "coordinates.json",
        "api_file": "streetlight_api.json",
        "update_time": "08:15",
        "retry_wait_s": 300,
        "max_fails": 5,
        "store_data": False,
        "fiware_service": None,
        "fiware_servicepath": None,
        "keyrock_address": None,
        "orion_address": None,
        "quantumleap_address": None,
    }


if __name__ == "__main__":
    """Main program that reads the config file and starts the data fetcher."""
    config = {}
    default_config = default_parameters()

    if len(sys.argv) == 2:
        config_file = sys.argv[1]
        with open(sys.argv[1], mode="r", encoding="utf-8") as file:
            config = json.load(file)
    elif len(sys.argv) != 1:
        print("Start this program with 'python", sys.argv[0], "config_file.json' command")
        print("or use 'python ", sys.argv[0], "' to use the default configuration parameters.", sep="")
        quit()

    for field in default_config:
        if field not in config:
            config[field] = default_config[field]

    # Start the data fetcher in another thread
    data_thread = threading.Thread(target=data_fetcher, name="streetlight_data_fetcher", args=(config,))
    data_thread.start()
    data_thread.join()
