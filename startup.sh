#!/bin/bash
set -e
cd "$(dirname "$0")"

source env/bin/activate
python main.py

echo
read -p "Programmende - Enter zum Schließen"