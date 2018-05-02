#!/usr/bin/env bash

nohup java -jar data-acquisitor-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
-brokers 192.168.111.191:9092,192.168.111.192:9092,192.168.111.193:9092 \
-remoteBase /to \
-ftpServer 192.168.111.107 \
-ftpUser root \
-ftpPassword 123456 > data-acquisitor.out 2>&1 &