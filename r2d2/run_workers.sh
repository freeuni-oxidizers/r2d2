#!/usr/bin/env bash

../target/$2/examples/$1 --id 0 --port 8880 &
../target/$2/examples/$1 --id 1 --port 8881 &
../target/$2/examples/$1 --id 2 --port 8882 &
