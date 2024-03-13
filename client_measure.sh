#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Insert the memcached_ip and internal_agent_ip"
    exit 1
fi

# Creating the inference
if [ $3 -eq 1]; then
    kubectl create -f interference/ibench-cpu.yaml
elif [ $3 eq 2]; then
    kubectl create -f interference/ibench-l1d.yaml
elif [ $3 eq 3]; then
    kubectl create -f interference/ibench-l1i.yaml
elif [ $3 eq 4]; then
    kubectl create -f interference/ibench-l2.yaml
elif [ $3 eq 5]; then
    kubectl create -f interference/ibench-llc.yaml
elif [ $3 eq 6]; then
    kubectl create -f interference/ibench-membw.yaml

./mcperf -s $1 --loadonly
./mcperf -s $1 -a $2 \
    --noload -T 16 -C 4 -D 4 -Q 1000 -c 4 -t 5 -w 2\
    --scan 5000:55000:5000 > results.txt

# Deleting the inference pod
if [ $3 -eq 1]; then
    kubectl delete pods ibench-cpu
elif [ $3 eq 2]; then
    kubectl delete pods ibench-l1d
elif [ $3 eq 3]; then
    kubectl delete pods ibench-l1i
elif [ $3 eq 4]; then
    kubectl delete pods ibench-l2.yaml
elif [ $3 eq 5]; then
    kubectl delete pods ibench-llc.yaml
elif [ $3 eq 6]; then
    kubectl delete pods ibench-membw.yaml
