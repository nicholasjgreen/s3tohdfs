#!/bin/bash

# Set up some vars
HDFS_EXEC=$HADOOP_PREFIX/bin/hdfs
HDP_EXEC=$HADOOP_PREFIX/bin/hadoop
HDFS_APPS_PATH=apps
APP_NAME=s3tohdfs
LOCAL_APP_PATH=./target
JAR_NAME=s3tohdfs.jar
CLIENT_CLASS=com.github.nicholasjgreen.s3tohdfs.Client

# Set up clean folder, copy in image
$HDFS_EXEC dfs -rm /$HDFS_APPS_PATH/$APP_NAME/*
$HDFS_EXEC dfs -mkdir /$HDFS_APPS_PATH
$HDFS_EXEC dfs -mkdir /$HDFS_APPS_PATH/$APP_NAME
$HDFS_EXEC dfs -copyFromLocal $LOCAL_APP_PATH/$JAR_NAME /$HDFS_APPS_PATH/$APP_NAME

# Schedule it with YARN
$HDP_EXEC jar $LOCAL_APP_PATH/$JAR_NAME $CLIENT_CLASS 1
