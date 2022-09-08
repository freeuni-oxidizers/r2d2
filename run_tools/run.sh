#!/usr/bin/env bash

FILE=~/am_master
if [ -f "$FILE" ]; then
	./r2d2_bin --id `./get_id.sh` --master --port 6969
else 
	./r2d2_bin --id `./get_id.sh` --port 6969
fi
