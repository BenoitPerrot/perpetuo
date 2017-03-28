#!/usr/bin/env bash
set -eu
set -o pipefail
# it's not supposed to be run by hand, it's run by integration.sh with the right setup

cd $(dirname $0)/..

PORT=$((8989 - ${EXECUTOR_NUMBER:-0}))
ADMIN_PORT=$((9990 - ${EXECUTOR_NUMBER:-0}))
query_tmp_file=$(mktemp)
out_tmp_file=$(mktemp)
err_tmp_file=$(mktemp)
trap_stack="rm -f ${query_tmp_file} ${out_tmp_file} ${err_tmp_file}" # used by `start_temporarily`
trap "${trap_stack}" EXIT

### common helpers

function api_query() {
    # only the response is written on stdout, the tmp files are here to distinguish the query from the error on stderr
    {
        echo
        echo "==> /api/$@"
    } | tee ${query_tmp_file} 1>&2

    # Ensure the response output is written on a single line ended with a newline.
    # In case of error, let the error go on stderr, and copy the error in a dedicated file.
    # The JWT specifies the user "qabot", and is encoded with the secret "itest-jwt-secret"
    echo $(curl -sfS --cookie "jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoicWFib3QifQ.uvI7EGBgJS6uJ2H998av6ROGi_9qXp2xw0uOtDlG2KY" -H 'Content-Type: application/json' "http://localhost:${PORT}/api/$@") | \
        tee ${out_tmp_file} 2> ${err_tmp_file} \
        || { cat ${err_tmp_file} 1>&2; }
}

function expects() {
    read line
    grep $@ <<< "${line}" || {
        { echo "! Expected $1"; echo ". Got ${line}"; } 1>&2
        return 1
    }
}

function 10_tries_with_sleep() {
    sleep_time=$1
    query=$2
    begin=$(date +%s)

    { ${query} > /dev/null 2>&1; status=$?; } || true # store the result if success, don't print anything
    cat ${query_tmp_file} 1>&2
    for i in $(seq 9)
    do
        [ ${status} -ne 0 ] || break
        sleep ${sleep_time}
        { ${query} > /dev/null 2>&1; status=$?; } || true
    done
    echo "(in $(($(date +%s) - ${begin}))s)"
    cat ${out_tmp_file}
    cat ${err_tmp_file} 1>&2
    return ${status}
}


### setup

if ! curl -fs localhost:${PORT} -o /dev/null
then
    function get_uber_jar_if_exists() {
        ls -t target/perpetuo-app-*-uber.jar 2> /dev/null | head -1
    }
    perpetuo_jar=$(get_uber_jar_if_exists || echo)
    if [[ -z "${perpetuo_jar}" || $(find src -newer ${perpetuo_jar} 2> /dev/null | wc -l) -ne 0 ]]
    then
        # the current version of the code has not been built yet (as an uber-jar)
        mvn clean package -DpackageForDeploy -DskipTests
        perpetuo_jar=$(get_uber_jar_if_exists)
    fi
    echo Using ${perpetuo_jar}
    start_temporarily "Perpetuo" "Startup complete, server ready" java \
        -Dtokens.rundeck="${RD_TEST_TOKEN}" \
        -Dauth.jwt.secret="itest-jwt-secret" \
        -Dmarathon.user="Pheidippides" \
        -Dmarathon.password="Nenikekamen!" \
        -Dhttp.port=${PORT} \
        -DselfUrl="http://localhost:${PORT}" \
        -jar ${perpetuo_jar} \
        -admin.port=:${ADMIN_PORT}
    echo

    api_query products -d '{"name": "itest-project"}'
else
    echo "Perpetuo seems to run already. Using it."
    api_query products -d '{"name": "itest-project"}' 2> /dev/null || true
fi


### helpers

function is_started() {
  dep_id=$1
  function rundeck_answers_a_permalink() {
      api_query execution-traces/by-deployment-request/${dep_id} | \
          expects '"state":"running"' | \
          expects '"logHref":"http://'
  }
  10_tries_with_sleep .2 rundeck_answers_a_permalink
}


### scenarios

# delayed start
api_query deployment-requests -d '{
    "productName": "itest-project",
    "version": "v42",
    "target": ["par", "am5"]
}' | expects '\{"id":[0-9]+}' -Ex | {
    read ans
    id1=$(grep -oE "[0-9]+" <<< "${ans}")
    echo "${ans}"

    # no execution
    api_query execution-traces/by-deployment-request/${id1} | \
        expects '\[]' -x

    api_query deployment-requests/${id1} -X PUT

    is_started ${id1}
}


# immediate start
api_query deployment-requests?start=true -d '{
    "productName": "itest-project",
    "version": "v42",
    "target": ["par", "am5"]
}' | expects '\{"id":[0-9]+}' -Ex | {
    read ans
    id2=$(grep -oE "[0-9]+" <<< "${ans}")
    echo "${ans}"

    function trigger_executions() {
        api_query execution-traces/by-deployment-request/${id2} | \
            expects '"state":"pending"' | \
            expects '"logHref":null'
    }
    10_tries_with_sleep .1 trigger_executions

    is_started ${id2}

    function rundeck_completes_the_job() {
        api_query execution-traces/by-deployment-request/${id2} | \
            expects '"state":"initFailed"' | \
            expects '"logHref":"http://'
    }
    10_tries_with_sleep .8 rundeck_completes_the_job
}
