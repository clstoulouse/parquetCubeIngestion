#!/usr/bin/env bash

set -e

source "${JAVA_BIN_DIR}/functions.sh"

if [[ ! -z "$DOCKER_NAME" ]]; then
    append_jvm_opts "-DDockerName=$DOCKER_NAME"
fi
if [[ ! -z "$NAME_SPACE" ]]; then
    append_jvm_opts "-DNameSpace=$NAME_SPACE"
fi
if [[ -f "${JAVA_CONF_DIR}/application.conf" ]]; then
    append_jvm_opts "-Dconfig.file=${JAVA_CONF_DIR}/application.conf"
fi
if [[ -f "${JAVA_CONF_DIR}/logback.xml" ]]; then
    append_jvm_opts "-Dlogback.configurationFile=${JAVA_CONF_DIR}/logback.xml"
fi