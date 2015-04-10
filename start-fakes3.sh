#!/usr/bin/env bash

# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

bundle check

WORKING_DIR="fakes3"
DATA_DIR="${WORKING_DIR}/data"
LOG_DIR="${WORKING_DIR}/logs"
mkdir -p "$DATA_DIR" "$LOG_DIR"

PORT="4000"
NOW=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
exec bundle exec fakes3 -r "$DATA_DIR" -p "$PORT" 1>"${LOG_DIR}/${NOW}.out.log" 2>"${LOG_DIR}/${NOW}.err.log" &
PID=$!

echo "fakes3 started with pid ${PID}"
echo $PID >"${WORKING_DIR}/PID"

echo "Pausing for 2 seconds..."
sleep 2
