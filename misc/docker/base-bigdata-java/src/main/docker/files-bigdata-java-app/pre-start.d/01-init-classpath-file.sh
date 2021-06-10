#!/usr/bin/env bash

set -e

source "${JAVA_BIN_DIR}/functions.sh"

if [[ -z "${JAVA_MAIN_JAR_NAME}" ]]; then
    echo "ERROR: Missing env variable JAVA_MAIN_JAR_NAME" >&2
    exit 1
fi
if [[ ! -f "${JAVA_LIB_DIR}/${JAVA_MAIN_JAR_NAME}" ]]; then
    echo "ERROR: Missing main jar file: ${JAVA_LIB_DIR}/${JAVA_MAIN_JAR_NAME}" >&2
    exit 1
fi

echo -n > "${CLASSPATH_FILE}"

MAIN_JAR_PATH=$(realpath "${JAVA_LIB_DIR}/${JAVA_MAIN_JAR_NAME}")
CLASSPATH_FILE_PATH=$(realpath "${CLASSPATH_FILE}")

if [[ -z ${JAVA_CONF_DIR_ADD_TO_CLASSPATH+x} ]]; then
    append_to_classpath "${JAVA_CONF_DIR}"
fi

append_to_classpath "${MAIN_JAR_PATH}"
for f in $(find "${JAVA_LIB_DIR}" -type f) ; do
    f=$(realpath "$f")
	if [[ "$f" != "${MAIN_JAR_PATH}" ]] && [[ "$f" != "${CLASSPATH_FILE_PATH}" ]]; then
	    append_to_classpath "${f}"
	fi
done
