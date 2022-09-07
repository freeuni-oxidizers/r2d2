#!/usr/bin/env bash

FILE=~/am_master
if [ -f "$FILE" ]; then
    echo "$FILE exists."
else 
    echo "$FILE does not exist."
fi
