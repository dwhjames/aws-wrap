#!/bin/bash

WORKING_DIR="dynamodb"

mkdir -p $WORKING_DIR
cd $WORKING_DIR

VERSION="dynamodb_local_2013-09-12"
ARCHIVE="${VERSION}.tar.gz"
URL="https://s3-us-west-2.amazonaws.com/dynamodb-local/${ARCHIVE}"

if [ ! -f $ARCHIVE ]
then
  echo "Downloading DynamoDB Local"
  curl -O $URL
fi

if [ ! -d $VERSION ]
then
  echo "Extracting DynamoDB Local"
  tar -xzf $ARCHIVE
fi

NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
LOG_FILE="dynamodb-${NOW}.log"
LOG_DIR="logs"

mkdir -p $LOG_DIR

echo "DynamoDB Local output will save to ${WORKING_DIR}/${LOG_DIR}/${LOG_FILE}"
nohup java -Djava.library.path=${VERSION} -jar ${VERSION}/DynamoDBLocal.jar >"${LOG_DIR}/${LOG_FILE}" 2>&1 &
PID=$!

echo "DynamoDB Local started with pid ${PID}"
echo $PID >"PID"

echo "Pausing for 2 seconds..."
sleep 2
