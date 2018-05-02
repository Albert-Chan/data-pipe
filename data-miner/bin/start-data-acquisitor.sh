#!/usr/bin/env bash

if [ -f passengerflow_env.sh ]; then
    . passengerflow_env.sh
fi
mkdir -p ${PF_ROOT}logs/ftp/

nohup java -jar ${PF_ROOT}lib/data-acquisitor.jar \
-brokers 10.68.4.34:9092,10.68.4.35:9092,10.68.4.45:9092 \
-remoteBase /home/roamer/gsmFile/ \
-ftpServer 192.168.100.14 \
-ftpUser roamer \
-ftpPassword roamer > ${PF_ROOT}logs/ftp/data-acquisitor.out 2>&1 &
