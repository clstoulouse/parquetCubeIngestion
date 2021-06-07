#!/usr/bin/env bash

set -e

PASSWD_FILE="/etc/passwd"
GROUP_FILE="/etc/group"
DEFAULT_USER_NAME="default"
DEFAULT_GROUP_NAME="default"

if ! groups &> /dev/null; then
    if [[ -z "${GROUP_NAME}" ]]; then
        echo "WARNING: name-less group defined but no GROUP_NAME environment variable was specified, falling back to: ${DEFAULT_GROUP_NAME}" >&2
        export GROUP_NAME="${DEFAULT_GROUP_NAME}"
    fi

    if [[ -w "${GROUP_FILE}" ]]; then
        echo "${GROUP_NAME}:x:$(id -g):" >> "${GROUP_FILE}"
    else
        echo "WARNING: Cannot add an entry to ${GROUP_FILE} due to non sufficient write permissions" >&2
    fi
fi

if ! whoami &> /dev/null; then
    if [[ -z "${USER_NAME}" ]]; then
        echo "WARNING: name-less user defined but no USER_NAME environment variable was specified, falling back to: ${DEFAULT_USER_NAME}" >&2
        export USER_NAME="${DEFAULT_USER_NAME}"
    fi

    if [[ -w "${PASSWD_FILE}" ]]; then
        echo "${USER_NAME}:x:$(id -u):$(id -g):auto created user:${HOME}:/bin/bash" >> "${PASSWD_FILE}"
    else
        echo "WARNING: Cannot add an entry to ${PASSWD_FILE} due to non sufficient write permissions" >&2
    fi
fi