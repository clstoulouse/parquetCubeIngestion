#!/usr/bin/env bash

#######################################################################
# Appends one or multiple path to the application classpath
#
# USAGE: append_to_classpath PATH...
# ARGS:
#   PATH...: One or multiple path to add to the application classpath
#######################################################################
append_to_classpath() {
    while [[ $# -gt 0 ]]; do
        realpath --relative-to="${JAVA_LIB_DIR}" "${1}" >> "${CLASSPATH_FILE}"
        shift
    done
}

#######################################################################
# Prepends one or multiple path to the application classpath
#
# USAGE: prepend_to_classpath PATH...
# ARGS:
#   PATH...: One or multiple path to preadd to the application classpath
#######################################################################
prepend_to_classpath() {
    PREEND_TMP=$(mktemp)
    while [[ $# -gt 0 ]]; do
        realpath --relative-to="${JAVA_LIB_DIR}" "${1}" >> "${PREEND_TMP}"
        shift
    done
    cat "${CLASSPATH_FILE}" >> "${PREEND_TMP}"
    cat "${PREEND_TMP}" > "${CLASSPATH_FILE}"
    rm -f "${PREEND_TMP}"
}

#######################################################################
# Appends a jvm property to the application's java command
#
# USAGE:   append_jvm_opts JVM_OPTIONS...
# EXAMPLE: append_jvm_opts -Dkey=value
# EXAMPLE: append_jvm_opts -server -Dkey2=value2
# ARGS:
#   JVM_OPTIONS: JVM options to add.
#######################################################################
append_jvm_opts() {
    while [[ $# -gt 0 ]]; do
        if [[ -z "${JAVA_OPTIONS}" ]]; then
            export JAVA_OPTIONS="$1"
        else
            export JAVA_OPTIONS="$JAVA_OPTIONS $1"
        fi
        shift
    done
}