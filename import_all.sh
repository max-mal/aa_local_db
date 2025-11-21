#!/bin/bash

DIR=$1

source venv/bin/activate
python3 -m tools.import_torrents ./torrents.example.json

find "$DIR" -type f -name 'aarecords__*.json.gz' -exec bash -c 'pv --name "$(basename "{}")" "{}" | gzip -d | python3 -m tools.import_json > /dev/null' \;
