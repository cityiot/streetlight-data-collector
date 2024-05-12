# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

FROM python:3.6

RUN mkdir -p /streetlight

COPY requirements.txt /streetlight/requirements.txt

RUN pip install -r /streetlight/requirements.txt

COPY *.py /streetlight/
COPY *.json /streetlight/

WORKDIR /streetlight

CMD [ "python3", "-u", "-m", "fiware_streetlight", "main_config.json" ]
