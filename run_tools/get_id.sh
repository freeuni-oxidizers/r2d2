#!/usr/bin/env bash

priv_ip=`hostname -i`
cat ~/ip_mapping | grep $priv_ip | cut -d' ' -f2-
