#!/usr/bin/env bash
set -eu

if [ "${1:-}" == "end" ]
then
    m2_link=/tmp/m2_$(echo ${WORKSPACE} | sha1sum | cut -c-40)
    m2_repo=$([ -e ${m2_link} ] && echo ${m2_link} || echo ${WORKSPACE}/.m2)
    mvn -e -Dmaven.repo.local=${m2_repo} --batch-mode clean package verify -DpackageForDeploy -DskipTests

    $(dirname $0)/integration.sh
fi
