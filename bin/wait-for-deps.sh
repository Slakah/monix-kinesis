#!/bin/bash
set -euo pipefail

# wait for dependencies to be ready https://docs.docker.com/compose/startup-order/

echoerr() { echo "$@" 1>&2; }

if [[ $# -ne 1 ]]; then
  echoerr "Usage: $0 localstackHost"
  exit 1
fi

readonly localstackHost="$1"

readonly timeout=120 # seconds

function maybeTimeout {
  if [[ $SECONDS -gt $timeout ]]; then
    echoerr "waited to be ready for $SECONDS seconds, timing out"
    exit 1
  fi
}

echo "waiting for dependencies to be ready..."

function waitForAws {
  url="$1"
  name="$2"
  until curl --insecure --output /dev/null --silent --head "$url"; do
    maybeTimeout
    echoerr "$url ($name) is unavailable, sleeping..."
    sleep 1
  done
  echo "$name is ready"
}

waitForAws "https://$localstackHost:4568" "kinesis"
waitForAws "https://$localstackHost:4569" "dynamodb"
waitForAws "https://$localstackHost:4582" "cloudwatch"

echo "all dependencies are ready"
