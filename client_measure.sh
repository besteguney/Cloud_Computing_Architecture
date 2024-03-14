#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Insert the memcached_ip and internal_agent_ip"
    exit 1
fi

current_time=$(date +"%T")
./mcperf -s $1 --loadonly
./mcperf -s $1 -a $2 \
    --noload -T 16 -C 4 -D 4 -Q 1000 -c 4 -t 5 -w 2\
    --scan 5000:55000:5000 > "output_$3_$current_time.txt"