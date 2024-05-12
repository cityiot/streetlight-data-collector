# -*- coding: utf-8 -*-

# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

"""Module for a collection of general helper functions."""

import datetime
import json
import math


def to_int(value):
    """Returns the given value as an int."""
    if value is None or value == "NULL":
        return None
    else:
        return int(value)


def to_float(value):
    """Returns the given value as a float."""
    if value is None or value == "NULL":
        return None
    else:
        return float(value)


def to_str(value):
    """Returns the given value as a string."""
    if value is None or value == "NULL":
        return None
    else:
        return str(value)


def to_list(value_str, separator=" "):
    """Returns the given value_str as a list using the given separator."""
    if value_str is None or value_str == "NULL":
        return []
    else:
        return value_str.strip().split(separator)


def get_part_list(full_list, max_payload_size):
    """Returns the list as chunks such that each chunks should not be any larger than the max_payload_size.
       Assumes that the items in the list are relatively equal in size."""
    if len(full_list) == 0:
        return full_list

    chunks = math.ceil(len(json.dumps(full_list)) / max_payload_size)
    chunk_length = math.ceil(len(full_list) / chunks)

    for index in range(0, len(full_list), chunk_length):
        yield full_list[index:index + chunk_length]


def flat_list(nested_list):
    """Returns a flattened list constructed from the given nested_list."""
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            for sub_item in flat_list(item):
                flattened_list.append(sub_item)
        else:
            flattened_list.append(item)
    return flattened_list


def timestamp_to_isoformat(timestamp):
    """Returns the ISO 8601 string for the UNIX timestamp given in ms."""
    return datetime.datetime.fromtimestamp(timestamp / 1000, datetime.timezone.utc).isoformat()


def get_local_timezone(timestamp):
    """Returns the local time zone string for the given UNIX timestamp."""
    # TODO: this should be done better, now assumes Finnish time
    #       and only checks the clock change at 2018-10-28, 2019-03-31 and 2019-10-29
    summer_time_before_1 = datetime.datetime(year=2018, month=10, day=28, hour=1, tzinfo=datetime.timezone.utc)
    winter_time_before_1 = datetime.datetime(year=2019, month=3, day=31, hour=1, tzinfo=datetime.timezone.utc)
    summer_time_before_2 = datetime.datetime(year=2019, month=10, day=29, hour=1, tzinfo=datetime.timezone.utc)

    if datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc) <= summer_time_before_1:
        return "+0300"
    elif datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc) <= winter_time_before_1:
        return "+0200"
    if datetime.datetime.utcfromtimestamp(timestamp).replace(tzinfo=datetime.timezone.utc) <= summer_time_before_2:
        return "+0300"
    else:
        return "+0200"


def to_timestamp(time_string, datetime_format="%Y-%m-%dT%H:%M:%S.%f", decimal_count=6, localtime=True):
    """Returns the UNIX timestamp in ms for the given time_string."""
    decimal_place = time_string.find(".")
    if decimal_place < 0:
        full_string = time_string + "." + "0" * decimal_count
    else:
        decimals = len(time_string) - (decimal_place + 1)
        if decimals <= decimal_count:
            full_string = time_string + "0" * (decimal_count - decimals)
        else:
            full_string = time_string[:-(decimals - decimal_count)]

    if localtime:
        timezone = get_local_timezone(datetime.datetime.strptime(full_string, datetime_format).timestamp())
    else:
        timezone = "+0000"

    datetime_format_tz = datetime_format + "%z"
    ts = datetime.datetime.strptime(full_string + timezone, datetime_format_tz).timestamp()
    return int(ts * 1000)


def handle_address(address_str):
    """Returns a string where each word in address_str starts with capital letter and
       all other letters are in lower case."""
    if address_str is None or address_str.strip() == "NULL":
        return None
    original_words = address_str.strip().split(" ")
    handled_words = []
    for word in original_words:
        if len(word) > 0:
            handled_words.append(word[0].upper() + word[1:].lower())
    return " ".join(handled_words)


def remove_duplicates(sorted_list):
    """Returns a list constructed from sorted_list with all duplicates removed.
       The sorted_list attribute is assumed to be sorted."""
    unique_list = []
    if len(sorted_list) == 0:
        return unique_list

    previous_item = sorted_list[0]
    unique_list.append(previous_item)
    for item in sorted_list[1:]:
        if item != previous_item:
            unique_list.append(item)
            previous_item = item

    return unique_list
