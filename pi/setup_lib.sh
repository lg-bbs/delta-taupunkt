#!/bin/bash
set +e
cd "$(dirname "$0")"

source env2/bin/activate
pip install dht11
pip install adafruit-blinka
pip install adafruit-circuitpython-character-lcd

echo
read -p "Ende"