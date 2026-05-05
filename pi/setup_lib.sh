#!/bin/bash
set +e
cd "$(dirname "$0")"

source env/bin/activate
pip install dht11
pip install adafruit-blinka
pip install adafruit-circuitpython-character-lcd

echo
read -p "Programmende - Enter zum SchlieÃen"