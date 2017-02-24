#!/usr/bin/env bash
set -eu
set -o pipefail

RUNDECK_REPO=incubator/criteo-rundeck-resources

cd $(dirname $0)
export test_dir=$PWD
cd ${test_dir}/../..


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
    cd ${test_dir}
    export -f start_temporarily wait_for # for integration-test-suite.sh

    while true
    do
        ./integration-test-suite.sh || { echo FAILED; status=1; }

        if [[ -t 0 && -t 1 ]] # interactive mode, in a tty with no pipes in input and output
        then
            echo
            read -p "Re-run tests (y)? " ans
            [ "${ans}" == "y" ] || break
        else
            break
        fi
    done
    return ${status:-0}
}


### run the tests using a local Rundeck instance running
export -f run_tests
./using-local-rundeck.sh run_tests
