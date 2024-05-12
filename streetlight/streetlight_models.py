# -*- coding: utf-8 -*-

# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

"""Module for the entity data models and data model utilities for Tampere city street light data."""


def get_postal_address(street_address, city="Tampere", country="FI"):
    """Returns a postal address attribute for a FIWARE entity."""
    return {
        "type": "StructuredValue",
        "value": {
            "addressCountry": country,
            "addressLocality": city,
            "streetAddress": street_address
        },
        "metadata": {}
    }


def get_location_attribute(latitude, longitude):
    """Returns a location attribute for a FIWARE entity."""
    return {
        "type": "geo:json",
        "value": {
            "type": "Point",
            "coordinates": [latitude, longitude]
        },
        "metadata": {}
    }


def get_phase_struct(phases=3):
    """Returns a struct for storing values with different phases. Used for electric intensity and voltage."""
    struct = {}
    for phase in range(1, phases+1):
        struct["L" + str(phase)] = None
    return struct


def add_cabinet_to_streetlight_group(streetlight_group_entity, cabinet_id, relays, delimiter="___"):
    """Adds a reference to a new cabinet to the given streetlight group entity."""
    if cabinet_id is None:
        return

    current_cabinet_ids = streetlight_group_entity["refStreetlightCabinetController"]["value"].split(delimiter)
    if cabinet_id not in current_cabinet_ids:
        current_cabinet_ids.append(cabinet_id)
        streetlight_group_entity["refStreetlightCabinetController"]["value"] = delimiter.join(current_cabinet_ids)
        streetlight_group_entity["refStreetlightCabinetController"]["metadata"]["relays"]["value"].append(relays)


def get_entity_types():
    """Returns a list of the used entity types for the Tampere street light data."""
    return [
        "Device",
        "WeatherObserved",
        "StreetlightControlCabinet",
        "StreetlightGroup"
    ]


def get_entity_id(entity_type, identifier):
    """Returns a entity id for the given entity type and identifier."""
    return ":".join([entity_type, identifier])


def get_entity_attributes(entity_type):
    """Returns a list containing the static and dynamic attribute names for the given entity type."""
    if entity_type == "StreetlightControlCabinet":
        return ["address", "location", "refStreetlightGroup", "workingMode"], ["illuminanceOn", "illuminanceOff"]
    elif entity_type == "StreetlightGroup":
        return ["address", "location", "refStreetlightCabinetController"], ["intensity", "voltage"]
    elif entity_type == "Device":
        return ["category", "controlledProperty", "location", "owner"], ["value"]
    elif entity_type == "WeatherObserved":
        return ["address", "location", "refDevice"], [["dateObserved", "illuminance"]]
    else:
        return [], []


def control_cabinet_entity(entity_id, timestamp):
    """Returns a FIWARE entity for a street light control cabinet."""
    return {
        "id": entity_id,
        "type": "StreetlightControlCabinet",
        "refStreetlightGroup": {
            "type": "StructuredValue",
            "value": [],
            "metadata": {}
        },
        "workingMode": {
            "type": "Text",
            "value": "automatic",
            "metadata": {}
        },
        "illuminanceOn": {
            "type": "Number",
            "value": None,
            "metadata": {
                "timestamp": {
                    "type": "DateTime",
                    "value": timestamp
                }
            }
        },
        "illuminanceOff": {
            "type": "Number",
            "value": None,
            "metadata": {
                "timestamp": {
                    "type": "DateTime",
                    "value": timestamp
                }
            }
        }
    }


def illuminance_sensor_entity(sensor_id, cabinet_entity):
    """Returns a FIWARE entity for an illuminance sensor."""
    return {
        "id": sensor_id,
        "type": "Device",
        "category": {
            "type": "StructuredValue",
            "value": ["sensor"],
            "metadata": {}
        },
        "controlledProperty": {
            "type": "StructuredValue",
            "value": ["light"],
            "metadata": {}
        },
        "owner": {
            "type": "Text",
            "value": cabinet_entity["id"],
            "metadata": {}
        }
    }


def illuminance_measurement_entity(measurement_id, sensor_id, timestamp):
    """Returns a FIWARE entity for an illuminance measurement."""
    return {
        "id": measurement_id,
        "type": "WeatherObserved",
        "dateObserved": {
            "type": "DateTime",
            "value": timestamp,
            "metadata": {}
        },
        "refDevice": {
            "type": "Text",
            "value": sensor_id,
            "metadata": {}
        },
        "illuminance": {
            "type": "Number",
            "value": None,
            "metadata": {
                "timestamp": {
                    "type": "DateTime",
                    "value": timestamp
                }
            }
        }
    }


def streetlight_group_entity(streetlight_group_id, cabinet_id, relays, timestamp):
    """Returns a FIWARE entity for a street light group."""
    entity = {
        "id": streetlight_group_id,
        "type": "StreetlightGroup",
        "refStreetlightCabinetController": {
            "type": "Text",
            "value": "",
            "metadata": {
                "relays": {
                    "type": "StructuredValue",
                    "value": []
                }
            }
        },
        "intensity": {
            "type": "StructuredValue",
            "value": get_phase_struct(),
            "metadata": {
                "timestamp": {
                    "type": "DateTime",
                    "value": timestamp
                }
            }
        },
        "voltage": {
            "type": "StructuredValue",
            "value": get_phase_struct(),
            "metadata": {
                "timestamp": {
                    "type": "DateTime",
                    "value": timestamp
                }
            }
        }
    }

    if cabinet_id is not None:
        entity["refStreetlightCabinetController"]["value"] = cabinet_id
        entity["refStreetlightCabinetController"]["metadata"]["relays"]["value"] = [relays]

    return entity


def doorsensor_entity(sensor_id, streetlight_group_entity, timestamp):
    """Returns a FIWARE entity for a door sensor."""
    return {
        "id": sensor_id,
        "type": "Device",
        "category": {
            "type": "StructuredValue",
            "value": ["sensor"],
            "metadata": {}
        },
        "controlledProperty": {
            "type": "StructuredValue",
            "value": ["motion"],
            "metadata": {}
        },
        "value": {
            "type": "Text",
            "value": "unknown",
            "metadata": {
                "timestamp": {
                    "type": "DateTime",
                    "value": timestamp
                }
            }
        },
        "owner": {
            "type": "Text",
            "value": streetlight_group_entity["id"],
            "metadata": {}
        }
    }
