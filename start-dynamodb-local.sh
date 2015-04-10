#!/usr/bin/env bash

# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

WORKING_DIR="dynamodb-local"

mkdir -p $WORKING_DIR
cd $WORKING_DIR

VERSION="dynamodb_local_latest"
ARCHIVE="${VERSION}.tar.gz"
URL="http://dynamodb-local.s3-website-us-west-2.amazonaws.com/${ARCHIVE}"

if [ ! -f $ARCHIVE ]
then
  echo "Downloading DynamoDB Local"
  curl -LO $URL
  echo "Extracting DynamoDB Local"
  tar -xzf $ARCHIVE
fi

LOG_DIR="logs"
mkdir -p $LOG_DIR
echo "DynamoDB Local output will save to ${WORKING_DIR}/${LOG_DIR}/"

NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
exec java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -port 8000 -inMemory 1>"${LOG_DIR}/${NOW}.out.log" 2>"${LOG_DIR}/${NOW}.err.log" &
PID=$!

echo "DynamoDB Local started with pid ${PID}"
echo $PID >"PID"

echo "Pausing for 2 seconds..."
sleep 2
