#!/usr/bin/env bash

CURRENT_DIR=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)

source "${CURRENT_DIR}/__defaults.sh"
source "${CURRENT_DIR}/__namespace-declaration.sh"
source "${CURRENT_DIR}/__package-declaration.sh"

###################################################################
#### Parsing arguments
###################################################################

show_usage() {
    echo "USAGE: ${0} [OPERATIONS...] [OPTIONS]"
    echo "DESCRIPTION: Checks that all the packages correspond to a specific version"
    echo "OPTIONS:"
    echo "  -v|--version:   version to check"
    echo "  -h|--help:      Prints the command usage and then exists"
}

for arg
do
    delim=""
    case "$arg" in
       --help) args="${args}-h ";;
       --version) args="${args}-v ";;

       # pass through anything else
       *) [[ "${arg:0:1}" == "-" ]] || delim="\""
           args="${args}${delim}${arg}${delim} ";;
    esac
done
# reset the translated args
eval set -- ${args}

OPTIND=1         # Reset in case getopts has been used previously in the shell.

while getopts "v:h" opt; do
    case "$opt" in
    v)
        VERSION="${OPTARG}"
        ;;
    h)
        show_usage
        exit 0
        ;;
    esac
done

shift $((OPTIND-1))

[[ "$1" = "--" ]] && shift

###################################################################
#### Check pre-requisites
###################################################################

if [[ -z "${VERSION}" ]]; then
    echo "ERROR: Missing required argument -v" >&2
    show_usage >&2
    exit 1
fi

if [[ ! -f "${PACKAGES_FILE}" ]]; then
    echo "ERROR: could not find packages file: ${PACKAGES_FILE}" >&2
    show_usage >&2
    exit 1
fi

###################################################################
#### Loading package file
###################################################################

nb_failures=0

on_package() {
    PACKAGE_NAME="$1"
    PACKAGE_CHART="$2"
    IS_NO_CHECK_VERSION="$4"

    PACKAGE_VERSION=$(helm inspect chart ${PACKAGE_CHART} | awk 'BEGIN { FS=":" } /1/ { if($1 == "version") print $2 }' | xargs -n 1 echo)
    
    if [[ "${IS_NO_CHECK_VERSION}" == "true" ]]; then
        if [[ -z "${PACKAGE_VERSION}" ]]; then
            echo "[error] could not get version of package '${PACKAGE_NAME}'" >&2
            ((nb_failures++))
        elif [[ "${PACKAGE_VERSION}" != "${VERSION}" ]]; then
            echo "[error] package '${PACKAGE_NAME}' has version '${PACKAGE_VERSION}', expected version is '${VERSION}'" >&2
            ((nb_failures++))
        fi
    else
        echo "[info] skip check: package '${PACKAGE_NAME}' with version '${PACKAGE_VERSION}'"
    fi
}

. ${PACKAGES_FILE}

exit ${nb_failures}