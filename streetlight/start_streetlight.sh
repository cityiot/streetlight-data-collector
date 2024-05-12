#!/bin/sh

# starts the streetlight data gatherer in a screen window
screen -d -m -L -S streetlight -t streetlight python3 -u -m fiware_streetlight main_config.json
