#!/usr/bin/env bash

../target/release/examples/$1 --id 0 --port 8880 &
../target/release/examples/$1 --id 1 --port 8881 &
../target/release/examples/$1 --id 2 --port 8882 &
