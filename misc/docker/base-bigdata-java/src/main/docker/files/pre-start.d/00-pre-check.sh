#!/usr/bin/env bash

if [[ -z "${JAVA_MAIN_CLASS}" ]]; then
    echo "ERROR: Missing required env variable JAVA_MAIN_CLASS" >&2
    exit 1
fi

if [[ -z "${JAVA_MAIN_JAR_NAME}" ]]; then
    echo "ERROR: Missing required env variable JAVA_MAIN_JAR_NAME" >&2
    exit 1
fi