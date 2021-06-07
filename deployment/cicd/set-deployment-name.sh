#!/usr/bin/env bash

if [[ -z "${DEPLOYMENT_NAME}" ]]; then
    if [[ -z "${CI_ENVIRONMENT_NAME}" ]]; then
        echo "[ERROR] Environment variables DEPLOYMENT_NAME and CI_ENVIRONMENT_NAME are both undefined" >&2
        exit 1
    fi
    echo "[INFO ] DEPLOYMENT_NAME will be inferred from CI_ENVIRONMENT_NAME"
    export DEPLOYMENT_NAME=$(echo "${CI_ENVIRONMENT_NAME}" | tr -c '[[:alnum:]]\n._-' '-' | tr '[:upper:]' '[:lower:]')
else
    echo "[INFO ] DEPLOYMENT_NAME is set to '${DEPLOYMENT_NAME}'"
fi