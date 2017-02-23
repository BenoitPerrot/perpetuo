#!/usr/bin/env bash
set -eu

RUNDECK_REPO=incubator/criteo-rundeck-resources

cd $(dirname $0)/../..


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
    ./gradlew clean setupForTest
    echo
    echo "all good!"
}


### run the tests using a local Rundeck instance running
export -f run_tests
./using-local-rundeck.sh run_tests
