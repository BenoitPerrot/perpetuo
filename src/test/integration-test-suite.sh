#!/usr/bin/env bash
set -eu
# it's not supposed to be run by hand, it's run by integration.sh with the right setup

cd $(dirname $0)/../..

mvn clean package -DpackageForDeploy -DskipTests
perpetuo_jar=$(ls -t target/perpetuo-app-*-uber.jar | head -1)
echo Using ${perpetuo_jar}
trap_stack="" # used by `start_temporarily`
start_temporarily "Perpetuo" "Startup complete, server ready" java -Dtokens.rundeck=token -jar ${perpetuo_jar}
echo

function api_query() {
    echo "==> /api/$@" 1>&2
    echo $(curl -sfS -H 'Content-Type: application/json' "http://localhost:8989/api/$@")
}

function expects() {
    read line
    grep -Ex "$1" <<< "${line}" || { echo "! Expected $1"; echo ". Got ${line}"; return 1; }
}

# setup
api_query products -d '{"name": "itest-project"}'


### scenarios

api_query deployment-requests -d '{
    "productName": "itest-project",
    "version": "v42",
    "target": "everywhere please"
}' | expects '\{"id":[0-9]+}'
