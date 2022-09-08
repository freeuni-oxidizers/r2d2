#!/usr/bin/env bash

../target/debug/examples/$1 --id 0 --port 8880 &
../target/debug/examples/$1 --id 1 --port 8881 &
../target/debug/examples/$1 --id 2 --port 8882 &
