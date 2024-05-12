# Copyright 2019 Tampere University
# This software was developed as a part of the CityIoT project: https://www.cityiot.fi/english
# This source code is licensed under the 3-clause BSD license. See license.txt in the repository root directory.
# Author(s): Ville Heikkilä <ville.heikkila@tuni.fi>

KEYROCK_ADDRESS=http://localhost:3005/oauth2/token
USERNAME=
USERPASSWORD=
APPLICATIONID=
APPLICATIONSECRET=

AUTH_SECRET=$(echo -n "$APPLICATIONID:$APPLICATIONSECRET" | base64 -w 255)
TOKENS=$(curl -X POST "$KEYROCK_ADDRESS" \
  --header "Authorization: Basic $AUTH_SECRET" \
  --header "Content-Type: application/x-www-form-urlencoded" \
  --data "grant_type=password&username=$USERNAME&password=$USERPASSWORD" | tail -n 1)
ACCESS_TOKEN=$(echo "$TOKENS" | cut -d '"' -f4)
REFRESH_TOKEN=$(echo "$TOKENS" | cut -d '"' -f14)

echo "AUTH_SECRET: $AUTH_SECRET"
echo "ACCESS_TOKEN: $ACCESS_TOKEN"
echo "REFRESH_TOKEN: $REFRESH_TOKEN"
