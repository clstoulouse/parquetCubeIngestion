#!/usr/bin/env bash

set-namespace() {
    if [[ ! -z "$NAMESPACE" ]]; then
        echo "[WARN ] The declared namespace will be ignored, this value will be used instead: $NAMESPACE" >&2
        return 0
    fi
    NAMESPACE="$1"
    if [[ -z "$NAMESPACE" ]]; then
        echo "[ERROR] Missing namespace name" >&2
        show_usage >&2
        return 1
    fi
    export NAMESPACE
}