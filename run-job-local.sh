#!/bin/bash

exec spark-submit --verbose --master yarn --deploy-mode cluster \
             --class com.adazza.example.spark.job.Job \
             s3://adazza-maven-repo/release/com/adazza/spark/example-job/example-spark-job_2.11/0.4.0/example-spark-job_2.11-0.4.0-all.jar
             a b

<workflow-app name="Spark Example Job" xmlns="uri:oozie:workflow:0.5">
    <start to="spark-6352"/>
    <kill name="Kill">
        <message>Action failed, error message[${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <action name="spark-6352">
        <spark xmlns="uri:oozie:spark-action:0.2">
            <job-tracker>${jobTracker}</job-tracker>
            <name-node>${nameNode}</name-node>
            <master>yarn</master>
            <mode>cluster</mode>
            <name>MySpark</name>
            <class>com.adazza.example.spark.job.Job</class>
            <jar>s3://adazza-maven-repo/release/com/adazza/spark/example-job/example-spark-job_2.11/0.4.0/example-spark-job_2.11-0.4.0-all.jar</jar>
        </spark>
        <ok to="End"/>
        <error to="Kill"/>
    </action>
    <end name="End"/>
</workflow-app>