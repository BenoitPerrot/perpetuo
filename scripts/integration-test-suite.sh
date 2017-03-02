#!/usr/bin/env bash
set -eu
# it's not supposed to be run by hand, it's run by integration.sh with the right setup

cd $(dirname $0)/..

PORT=$((8989 - ${EXECUTOR_NUMBER:-0}))

function api_query() {
    echo "==> /api/$@" 1>&2
    echo $(curl -sfS -H 'Content-Type: application/json' "http://localhost:${PORT}/api/$@")
}

function expects() {
    read line
    grep -Ex "$1" <<< "${line}" || { echo "! Expected $1"; echo ". Got ${line}"; return 1; }
}


### setup
if ! curl -fs localhost:${PORT} -o /dev/null
then
    function get_uber_jar_if_exists() {
        ls -t target/perpetuo-app-*-uber.jar 2> /dev/null | head -1
    }
    perpetuo_jar=$(get_uber_jar_if_exists)
    if [[ -z "${perpetuo_jar}" || $(find src -newer ${perpetuo_jar} 2> /dev/null | wc -l) -ne 0 ]]
    then
        # the current version of the code has not been built yet (as an uber-jar)
        mvn clean package -DpackageForDeploy -DskipTests
        perpetuo_jar=$(get_uber_jar_if_exists)
    fi
    echo Using ${perpetuo_jar}
    trap_stack="" # used by `start_temporarily`
    start_temporarily "Perpetuo" "Startup complete, server ready" java \
        -Dtokens.rundeck="${RD_TEST_TOKEN}" \
        -Dmarathon.user="Pheidippides" \
        -Dmarathon.password="Nenikekamen!" \
        -Dhttp.port=${PORT} \
        -DselfUrl="http://localhost:${PORT}" \
        -jar ${perpetuo_jar}
    echo

    api_query products -d '{"name": "itest-project"}'
else
    echo "Perpetuo seems to run already. Using it."
    api_query products -d '{"name": "itest-project"}' 2> /dev/null || true
fi


### scenarios

api_query deployment-requests -d '{
    "productName": "itest-project",
    "version": "v42",
    "target": ["par", "am5"]
}' | expects '\{"id":[0-9]+}'
