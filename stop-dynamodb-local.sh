#!/bin/bash

WORKING_DIR="dynamodb"

PID_FILE="${WORKING_DIR}/PID"

if [ ! -f $PID_FILE ]
then
  echo "No PID file found!"
  exit 1
else
  PID=$(cat $PID_FILE)
  kill -s KILL $PID
  rm $PID_FILE
  echo "DynamoDB Local stopped"
fi
