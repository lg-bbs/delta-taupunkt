#!/bin/bash
set +e
cd "$(dirname "$0")"

source env2/bin/activate
python main.py

echo
read -p "Programmende - Enter zum Schließen"