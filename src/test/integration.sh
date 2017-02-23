#!/usr/bin/env bash
set -eu
set -o pipefail
# You can use option `--fast` to not rebuild, run unit-tests, package and setup jobs every time

RUNDECK_REPO=incubator/criteo-rundeck-resources

cd $(dirname $0)/../..

if [ "${1:-}" == "--fast" ]; then fast=true; else fast=false; fi
export fast

${fast} || mvn clean package -DpackageForDeploy
export perpetuo_jar=$(ls -t $PWD/target/perpetuo-app-*-uber.jar | head -1)
echo Using ${perpetuo_jar}
echo


### get into an up-to-date clone of criteo-rundeck-resources
sandbox_dir=sandbox
if [ -d ${sandbox_dir}/.git ]
then
    cd ${sandbox_dir}
    git clean -f
    git fetch -q origin master
    git checkout -q FETCH_HEAD
else
    rm -rf ${sandbox_dir}
    echo Fetching ${RUNDECK_REPO}
    git clone -q http://review.criteois.lan/p/${RUNDECK_REPO}.git ${sandbox_dir}
    cd ${sandbox_dir}
fi


### define the test run
function run_tests() {
    ${fast} || ./gradlew clean setupForTest

    start_temporarily "Perpetuo" "Startup complete, server ready" java -Dtokens.rundeck=token -jar ${perpetuo_jar}
    echo

    function api_query() {
        echo "==> /api/$@" 1>&2
        curl -sfS -H 'Content-Type: application/json' "http://localhost:8989/api/$@"
    }
    # setup
    api_query products -d '{"name": "test-project"}'

    scenarios
}


### define the integration test scenarios (that use the local Rundeck instance)
function scenarios() {
    function expects() {
        read line
        grep -Ex "$1" <<< "${line}" || { echo "! Expected $1"; echo ". Got ${line}"; return 1; }
    }

    { api_query deployment-requests -d '{
        "productName": "test-project",
        "version": "v42",
        "target": "everywhere please"
    }' && echo; } | expects '\{"id":[0-9]+}'
}


### run the tests using a local Rundeck instance running
export -f run_tests scenarios
./using-local-rundeck.sh run_tests
