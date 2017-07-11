#!/bin/bash

exec spark-submit --verbose --master yarn --deploy-mode cluster \
             --class com.adazza.example.spark.job.Job \
             build/libs/*-all.jar ${@:2}
