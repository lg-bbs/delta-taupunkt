#!/bin/bash
set +e
cd "$(dirname "$0")"

source env/bin/activate
sudo python3 dht_11.py