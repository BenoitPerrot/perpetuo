#!/usr/bin/env bash
set -eu
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

    echo "all good!"
}


### run the tests using a local Rundeck instance running
export -f run_tests
./using-local-rundeck.sh run_tests
