#!/usr/bin/env bash

set -e

. "${JAVA_BIN_DIR}/run-prestart-hooks.sh"

exec "${JAVA_BIN_DIR}/run-java.sh" "$@"