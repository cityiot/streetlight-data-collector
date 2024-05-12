# -*- coding: utf-8 -*-

# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

"""Module for a collection of helper function for dealing with Fiware."""

import datetime
import json

import requests

import common_utils

keyrock_address = "http://keyrock:3005"
orion_address = "http://orion:1027/v2/"
quantumleap_address = "http://quantumleap:8668/v2/"
max_payload_size = 400000
orion_timeformat = "%Y-%m-%dT%H:%M:%S.%fZ"
orion_time_decimals = 2


TOKEN_ERROR_MESSAGES = [
    "invalid_token",
    "unauthorized_request",
    "invalid_grant"
]


class RequestMaker():
    def __init__(self):
        self.use_tokens = False
        self.secret_key = ""
        self.apikey_header = "apikey"
        self.access_token = ""
        self.refresh_token = ""
        self.token_type = ""

    def insert_keys(self, use_tokens=None, secret_key=None, apikey_header=None,
                    access_token=None, refresh_token=None, token_type=None):
        if use_tokens is not None:
            self.use_tokens = use_tokens
        if secret_key is not None:
            self.secret_key = secret_key
        if apikey_header is not None:
            self.apikey_header = apikey_header
        if access_token is not None:
            self.access_token = access_token
        if refresh_token is not None:
            self.refresh_token = refresh_token
        if token_type is not None:
            self.token_type = token_type

    def update_tokens(self):
        print("{} ".format(datetime.datetime.now()), end="")
        try:
            req = requests.post(
                "/".join([keyrock_address, "oauth2", "token"]),
                headers={
                    "Authorization": "Basic {}".format(self.secret_key),
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json"
                },
                data="&".join([
                    "=".join(["grant_type", "refresh_token"]),
                    "=".join(["refresh_token", self.refresh_token])
                ])
            )

            if req.status_code == 200:
                data = req.json()
                self.access_token = data["access_token"]
                self.refresh_token = data["refresh_token"]
                self.token_type = data["token_type"]
                print("Tokens updated successfully.")
            else:
                print("While updating tokens received code {} ({})".format(req.status_code, req.text))

        except Exception as error:
            print("Error while updating tokens: ({})".format(str(error)))

    def apikeys_to_headers(self, headers):
        if self.apikey_header is not None:
            headers[self.apikey_header] = self.access_token

    def request(self, method, address, headers, json=None, try_again=True):
        self.apikeys_to_headers(headers)

        if json is None:
            req = getattr(requests, method.lower())(address, headers=headers)
        else:
            req = getattr(requests, method.lower())(address, headers=headers, json=json)

        if self.use_tokens and try_again and req.status_code // 100 == 4:
            for error_message in TOKEN_ERROR_MESSAGES:
                if error_message in req.text:
                    self.update_tokens()
                    return self.request(method, address, headers, json, False)
        return req

    def get(self, address, headers, json=None):
        return self.request("GET", address, headers, json)

    def post(self, address, headers, json=None):
        return self.request("POST", address, headers, json)

    def put(self, address, headers, json=None):
        return self.request("PUT", address, headers, json)

    def patch(self, address, headers, json=None):
        return self.request("PATCH", address, headers, json)

    def delete(self, address, headers, json=None):
        return self.request("DELETE", address, headers, json)


request_maker = RequestMaker()


def timestamp_to_isoformat(attribute):
    """Changes the attribute value to ISO format if the attribute type is DateTime."""
    if attribute["type"] == "DateTime" and not isinstance(attribute["value"], str):
        attribute["value"] = common_utils.timestamp_to_isoformat(attribute["value"])
    for meta_attribute in attribute.get("metadata", {}):
        timestamp_to_isoformat(attribute["metadata"][meta_attribute])


def timestamps_to_isoformat(entities):
    """Changes all the DateTime attribute values to ISO format for all the entities."""
    if isinstance(entities, list):
        entity_list = entities
    else:
        entity_list = [entities]

    for entity in entity_list:
        for attribute in entity:
            if isinstance(entity[attribute], dict):
                timestamp_to_isoformat(entity[attribute])


def check_for_update(entity, attribute_name, value, timestamp=None, allow_same_value=False, allow_same_time=False):
    """Checks whether the given value for the given attribute_name is a new update for the given entity.
       If timestamp is not None, an update is not accepted unless it is newer than the previous timestamp.
       If allow_same_value is True, then the update can be accepted even if the value has not changed.
       If allow_same_time is True, then the update can be accepted even if the timestamp has remained the same."""
    return ((entity[attribute_name]["value"] != value and
             ((entity[attribute_name]["type"] != "DateTime" and
               (timestamp is None or
                "timestamp" not in entity[attribute_name]["metadata"] or
                entity[attribute_name]["metadata"]["timestamp"]["value"] < timestamp or
                (allow_same_time and entity[attribute_name]["metadata"]["timestamp"]["value"] == timestamp))) or
              (entity[attribute_name]["type"] == "DateTime" and
               entity[attribute_name]["value"] < value))) or

            (allow_same_value and
             entity[attribute_name]["value"] == value and
             timestamp is not None and
             "timestamp" in entity[attribute_name]["metadata"] and
             entity[attribute_name]["metadata"]["timestamp"]["value"] < timestamp))


def get_attribute(value, timestamp_as_str=True):
    """Returns a constructed attribute for a Fiware entity from the given value."""
    if value["type"] == "DateTime" and type(value["value"]) is not str and timestamp_as_str:
        attr_value = common_utils.timestamp_to_isoformat(value["value"])
    else:
        attr_value = value["value"]

    attribute = {
        "type": value["type"],
        "value": attr_value,
        "metadata": {}
    }

    if "unitCode" in value:
        attribute["metadata"]["unitCode"] = {
            "type": "Text",
            "value": value["unitCode"]
        }
    if "timestamp" in value:
        if type(value["timestamp"]) is not str and timestamp_as_str:
            timestamp = common_utils.timestamp_to_isoformat(value["timestamp"])
        else:
            timestamp = value["timestamp"]

        attribute["metadata"]["timestamp"] = {
            "type": "DateTime",
            "value": timestamp
        }

    return attribute


def get_entity(entity_data):
    """Returns a Fiware entity constructed from the given entity_data."""
    entity = {}
    for attribute_id, attribute_data in entity_data.items():
        if attribute_id == "id" or attribute_id == "type":
            entity[attribute_id] = attribute_data
        else:
            entity[attribute_id] = get_attribute(attribute_data)

    return entity


def get_subscription_notification_http(target_host=None, platform_key=None):
    if target_host is None:
        target_url = quantumleap_address + "notify"
    else:
        target_url = target_host

    if platform_key is None:
        return (
            "http",
            {
                "url": target_url
            })
    else:
        return (
            "httpCustom",
            {
                "url": target_host,
                "headers": {
                    "platform-key": platform_key
                }
            }
        )


def get_subscription(entity_type, input_attributes, output_attributes, reverse_output=False,
                     target_host=None, platform_key=None):
    """Returns a Fiware Orion subscription for Quantum Leap for any changes in one of the input_attributes for entity_type.
       If reverse_output is False, the notified attributes for Quantum Leap are output_attributes.
       Otherwise, the notified attributes for Quantum Leap are all attributes except output_attributes."""
    if not isinstance(input_attributes, list):
        input_attributes = [input_attributes]
    if not isinstance(output_attributes, list):
        output_attributes = [output_attributes]

    if reverse_output:
        output_attr_field = "exceptAttrs"
    else:
        output_attr_field = "attrs"

    http_field, http_value = get_subscription_notification_http(
        target_host=target_host,
        platform_key=platform_key)

    return {
        "description": "Subscription for attributes {} for entity type {}.".format(
            "_".join(input_attributes), entity_type),
        "subject": {
            "entities": [
                {
                    "idPattern": ".*",
                    "type": entity_type
                }
            ],
            "condition": {
                "attrs": input_attributes
            }
        },
        "notification": {
            output_attr_field: output_attributes,
            http_field: http_value,
            "metadata": [
                "dateModified",
                "timestamp"
            ]
        },
        "expires": "2050-12-31T23:59:59.00Z",
        "status": "active"
    }


def add_fiware_service(header, fiware_service=None, fiware_servicepath=None):
    if fiware_service is not None:
        header["Fiware-Service"] = fiware_service
    if fiware_servicepath is not None:
        header["Fiware-ServicePath"] = fiware_servicepath


def fetch_subscriptions(fiware_service=None, fiware_servicepath=None):
    header = {}
    add_fiware_service(
        header=header,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    try:
        req = request_maker.get(orion_address + "subscriptions", headers=header)
        if req.status_code != 200:
            return []
        else:
            return req.json()
    except:
        return []


def create_subscriptions(attributes, fiware_service=None, fiware_servicepath=None,
                         target_host=None, platform_key=None):
    """Creates Orion subscriptions for Quantum Leap depending on the input attributes.
       Attributes is dictionary with the entity type as keys and the attribute names as the values.
       The attribute names are given as a two-element list with the static attributes as the first
       elements and the dynamic attributes as the second elements."""
    subscriptions = {}
    for entity_type, (static_attributes, dynamic_attributes) in attributes.items():
        subscriptions[(entity_type, str(static_attributes))] = get_subscription(
            entity_type=entity_type,
            input_attributes=static_attributes,
            output_attributes=common_utils.flat_list(dynamic_attributes),
            reverse_output=True,
            target_host=target_host,
            platform_key=platform_key)
        for dynamic_attribute in dynamic_attributes:
            subscriptions[(entity_type, str(dynamic_attribute))] = get_subscription(
                entity_type=entity_type,
                input_attributes=dynamic_attribute,
                output_attributes=dynamic_attribute,
                reverse_output=False,
                target_host=target_host,
                platform_key=platform_key)

    address = orion_address + "subscriptions"
    header = {"Content-Type": "application/json"}
    add_fiware_service(
        header=header,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    for (entity_type, input_attributes), subscription in subscriptions.items():
        req = request_maker.post(address, headers=header, json=subscription)
        print("making subscription for", (entity_type, input_attributes), "=>", req.status_code)
        if req.status_code != 201:
            if req.encoding is None:
                req.encoding = "UTF-8"
            print("  ", req.content.decode(req.encoding))


def delete_subscriptions(fiware_service=None, fiware_servicepath=None):
    """Deletes all subscriptions from Orion."""
    list_address = orion_address + "subscriptions?options=count&offset={}"
    header = {}
    add_fiware_service(
        header=header,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    sub_list = []
    offset = 0
    count = 1

    while offset < count:
        req = request_maker.get(list_address.format(offset), headers=header)
        if req.status_code != 200:
            break
        sub_list += [sub["id"] for sub in req.json()]
        offset = len(sub_list)
        count = int(req.headers["Fiware-Total-Count"])

    print("getting subscription list =>", req.status_code)

    delete_address = orion_address + "subscriptions"
    for sub_id in sub_list:
        req = request_maker.delete("/".join([delete_address, sub_id]), headers=header)
        print("deleting subscription", sub_id, "=>", req.status_code)


def read_attribute(entity_id, entity_type, attribute_name, fiware_service=None,
                   fiware_servicepath=None, verbose=False):
    """Reads and returns and attribute for an entity from Orion."""
    address = orion_address + "entities/{id:}/attrs/{attr:}?type={type:}".format(
        id=entity_id, attr=attribute_name, type=entity_type)
    header = {}
    add_fiware_service(
        header=header,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    req = request_maker.get(address, headers=header)
    if req.status_code != 200:
        if verbose:
            print("Reading attribute", attribute_name, "from entity", entity_id, "=> status code:", req.status_code)
            if req.encoding is None:
                req.encoding = "UTF-8"
            print("  ", req.content.decode(req.encoding))
        return None
    else:
        return req.json()


def read_entity(entity_id, entity_type, fiware_service=None, fiware_servicepath=None, verbose=False):
    """Reads and returns and entity from Orion."""
    address = orion_address + "entities/{id:}?type={type:}".format(id=entity_id, type=entity_type)
    header = {}
    add_fiware_service(
        header=header,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    req = request_maker.get(address, headers=header)
    if req.status_code != 200:
        if verbose:
            print("Reading entity", entity_id, "=> status code:", req.status_code)
            if req.encoding is None:
                req.encoding = "UTF-8"
            print("  ", req.content.decode(req.encoding))
        return None
    else:
        return req.json()


def create_new_entities(entity_list, use_patch=True, fiware_service=None, fiware_servicepath=None):
    """Creates new entities to Orion.
       If use_patch is True uses the patch operations, otherwise sends each entity individually."""
    if use_patch:
        address = orion_address + "op/update"
        header = {"Content-Type": "application/json"}
        add_fiware_service(
            header=header,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)

        for part_entity_list in common_utils.get_part_list(entity_list, max_payload_size):
            entity_patch = {
                "actionType": "append",
                "entities": part_entity_list
            }

            req = request_maker.post(address, headers=header, json=entity_patch)
            print("Creating", len(part_entity_list), "entities", "=> status code:", req.status_code)
            if req.status_code // 100 != 2:
                if req.encoding is None:
                    req.encoding = "UTF-8"
                print("  ", req.content.decode(req.encoding))

    else:
        for entity in entity_list:
            address = orion_address + "entities"
            header = {"Content-Type": "application/json"}
            add_fiware_service(
                header=header,
                fiware_service=fiware_service,
                fiware_servicepath=fiware_servicepath)

            req = request_maker.post(address, headers=header, json=entity)
            print("Creating entity", entity["id"], "=> status code:", req.status_code)
            if req.status_code != 201:
                if req.encoding is None:
                    req.encoding = "UTF-8"
                print("  ", req.content.decode(req.encoding))


def append_to_entities(entity_list, use_patch=True, fiware_service=None, fiware_servicepath=None):
    """Adds new attributes or replaces the values of existing attributes for the given entities.
        If use_patch is True uses the patch operations, otherwise sends each entity individually."""
    if use_patch:
        address = orion_address + "op/update"
        header = {"Content-Type": "application/json"}
        add_fiware_service(
            header=header,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)

        for part_entity_list in common_utils.get_part_list(entity_list, max_payload_size):
            entity_patch = {
                "actionType": "append",
                "entities": part_entity_list
            }

            req = request_maker.post(address, headers=header, json=entity_patch)
            print("Updating", len(part_entity_list), "entities", "=> status code:", req.status_code)
            if req.status_code // 100 != 2:
                if req.encoding is None:
                    req.encoding = "UTF-8"
                print("  ", req.content.decode(req.encoding))

    else:
        for entity in entity_list:
            address = "{host:}entities/{id:}/attrs?type={type:}".format(
                host=orion_address, id=entity["id"], type=entity["type"])
            header = {"Content-Type": "application/json"}
            add_fiware_service(
                header=header,
                fiware_service=fiware_service,
                fiware_servicepath=fiware_servicepath)

            new_attributes = {}
            for attribute_name, attribute_value in entity.items():
                if not isinstance(attribute_value, str):
                    new_attributes[attribute_name] = attribute_value

            req = request_maker.post(address, headers=header, json=new_attributes)
            print("Updating entity", entity["id"], "=> status code:", req.status_code)
            if req.status_code != 201:
                if req.encoding is None:
                    req.encoding = "UTF-8"
                print("  ", req.content.decode(req.encoding))


def update_entities(update_list, use_patch=True, fiware_service=None, fiware_servicepath=None):
    """Updates attribute values with the given updates.
        If use_patch is True uses the patch operations, otherwise sends each update individually.
        Assumes that there is no more than one update for each attribute for each entity."""
    entity_updates = {}
    for update in update_list:
        entity_id = update["entity_id"]
        entity_type = update["entity_type"]
        if (entity_id, entity_type) not in entity_updates:
            if use_patch:
                entity_updates[(entity_id, entity_type)] = {
                    "id": entity_id,
                    "type": update["entity_type"]
                }
            else:
                entity_updates[(entity_id, entity_type)] = {}

        entity_updates[(entity_id, entity_type)][update["attribute"]] = get_attribute(update)

    if use_patch:
        full_entity_list = []
        for (entity_id, entity_type), entity_data in entity_updates.items():
            full_entity_list.append(entity_data)

        for part_entity_list in common_utils.get_part_list(full_entity_list, max_payload_size):
            entity_patch = {
                "actionType": "update",
                "entities": part_entity_list
            }

            address = orion_address + "op/update"
            header = {"Content-Type": "application/json"}
            add_fiware_service(
                header=header,
                fiware_service=fiware_service,
                fiware_servicepath=fiware_servicepath)

            req = request_maker.post(address, headers=header, json=entity_patch)
            print("Updating", len(part_entity_list), "entities => status code:", req.status_code)
            if req.status_code // 100 != 2:
                if req.encoding is None:
                    req.encoding = "UTF-8"
                print("  ", req.content.decode(req.encoding))

    else:
        address = orion_address + "entities/{}/attrs?type={}"
        header = {"Content-Type": "application/json"}
        add_fiware_service(
            header=header,
            fiware_service=fiware_service,
            fiware_servicepath=fiware_servicepath)

        for (entity_id, entity_type), data in entity_updates.items():
            req = request_maker.patch(address.format(entity_id, entity_type), headers=header, json=data)
            print("Updating entity", entity_id, "=> status code:", req.status_code)
            if req.status_code // 100 != 2:
                if req.encoding is None:
                    req.encoding = "UTF-8"
                print("  ", req.content.decode(req.encoding))


def delete_entities(fiware_service=None, fiware_servicepath=None):
    """Deletes all entities from Orion."""
    list_address = orion_address + "entities?options=count&offset={}"
    header = {}
    add_fiware_service(
        header=header,
        fiware_service=fiware_service,
        fiware_servicepath=fiware_servicepath)

    entity_list = []
    offset = 0
    count = 1

    while offset < count:
        req = request_maker.get(list_address.format(offset), headers=header)
        if req.status_code != 200:
            break
        if req.encoding is None:
            req.encoding = "UTF-8"
        entity_list += [(entity["id"], entity["type"]) for entity in json.loads(req.content.decode(req.encoding))]
        offset = len(entity_list)
        count = int(req.headers["Fiware-Total-Count"])

    print("getting entity list =>", req.status_code)

    delete_address = orion_address + "entities/{}?type={}"
    for (entity_id, entity_type) in entity_list:
        req = request_maker.delete(delete_address.format(entity_id, entity_type), headers=header)
        print("deleting entity", (entity_id, entity_type), "=>", req.status_code)
