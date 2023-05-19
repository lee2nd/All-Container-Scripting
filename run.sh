#!/bin/bash

docker stop python_dailymail
docker rm python_dailymail
docker run -t -d  --name python_dailymail -p 2023:8080 -v /etc/localtime:/etc/localtime:ro -v /app_1/wma/Dailymail:/Dailymail python_dailymail python dailyreport.py
