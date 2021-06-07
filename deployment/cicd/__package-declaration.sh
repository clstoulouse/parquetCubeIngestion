#!/usr/bin/env bash

package() {
    show_usage() {
        echo "USAGE: ${0} [OPTIONS]"
        echo "DESCRIPTION: Registers a package"
        echo "OPTIONS:"
        echo "  -t|--test:  Flag to tag a package as to an integration tests package"
        echo "  -n|--name:  App name"
        echo "  -c|--chart: Chart url"
        echo "  -V|--no-check-version: No check on the version of this chart will be performed"
    }

    local args=
    for arg
    do
        delim=""
        case "$arg" in
           --test) args="${args}-t ";;
           --name) args="${args}-n ";;
           --chart) args="${args}-c ";;
           --no-check-version) args="${args}-V ";;
           # pass through anything else
           *) [[ "${arg:0:1}" == "-" ]] || delim="\""
               args="${args}${delim}${arg}${delim} ";;
        esac
    done
    # reset the translated args
    eval set -- $args

    local OPTIND=1         # Reset in case getopts has been used previously in the shell.

    PACKAGE_NAME=""
    PACKAGE_CHART=""
    IS_TEST="false"
    IS_NO_CHECK_VERSION="false"

    while getopts "n:c:t" opt; do
        case "$opt" in
        n)
            PACKAGE_NAME="${OPTARG}"
            ;;
        c)
            PACKAGE_CHART="${OPTARG}"
            ;;
        t)
            IS_TEST="true"
            ;;
        V)
            IS_NO_CHECK_VERSION="true"
            ;;
        esac
    done

    shift $((OPTIND-1))

    [[ "$1" = "--" ]] && shift

    if [[ -z "${PACKAGE_NAME}" ]]; then
        echo "[ERROR] Missing package name" >&2
        show_usage >&2
        return 1
    fi

    if [[ -z "${PACKAGE_CHART}" ]]; then
        echo "[ERROR] Missing package chart url" >&2
        show_usage >&2
        return 1
    fi

    on_package "${PACKAGE_NAME}" "${PACKAGE_CHART}" "${IS_TEST}" "${IS_NO_CHECK_VERSION}"
}