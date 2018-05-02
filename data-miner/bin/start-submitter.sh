#!/usr/bin/env bash

if [ -f passengerflow_env.sh ]; then
    . passengerflow_env.sh
fi

if [ ! -d ${PF_ROOT}logs/jobs/ ]; then
    mkdir ${PF_ROOT}logs/jobs/
fi

nohup java -jar ${PF_ROOT}lib/job-submitter.jar \
-topic jobSubmission -group jobSubmitter \
-z 192.168.111.191:2181,192.168.111.192:2181,192.168.111.193:2181 >>${PF_ROOT}logs/jobs/submitter.out 2>&1 &
