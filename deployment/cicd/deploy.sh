#!/usr/bin/env bash

CURRENT_DIR=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)

source "${CURRENT_DIR}/__defaults.sh"
source "${CURRENT_DIR}/__namespace-declaration.sh"
source "${CURRENT_DIR}/__package-declaration.sh"
source "${CURRENT_DIR}/__printing.sh"
source "${CURRENT_DIR}/__operations.sh"

HELM_OPTIONS=""
NAMESPACE=""
declare -a OPERATIONS

declare -a PACKAGE_NAMES
declare -a PACKAGE_CHARTS
declare -a PACKAGE_IS_TESTS

###################################################################
#### Parsing arguments
###################################################################

show_usage() {
    echo "USAGE: ${0} [OPERATIONS...] [OPTIONS]"
    echo "DESCRIPTION: Deployment script"
    echo "OPERATIONS:"
    show_operations_usage | sed 's/^/  /g'
    echo "OPTIONS:"
    echo "  -n|--namespace NAMESPACE:   Deployment namespace"
    echo "  -f|--values:                values file"
    echo "  -w|--wait:                  if added, the script will wait for each package to finish the deployment before moving to the next"
    echo "  -d|--debug:                 if added, the script will call helm in debug mode"
    echo "  -h|--help:                  Prints the command usage and then exists"
    echo "ENV VAR:"
    echo "  DEPLOYMENT_NAME: (required) Deployment name"
}

for arg
do
    delim=""
    case "$arg" in
       --help) args="${args}-h ";;
       --namespace) args="${args}-n ";;
       --values) args="${args}-f ";;
       --wait) args="${args}-w ";;
       --debug) args="${args}-d ";;

       # pass through anything else
       *) [[ "${arg:0:1}" == "-" ]] || delim="\""
           args="${args}${delim}${arg}${delim} ";;
    esac
done
# reset the translated args
eval set -- ${args}

OPTIND=1         # Reset in case getopts has been used previously in the shell.

while [[ "$#" -gt 0 ]] && [[ "$1" != "-"* ]]; do
    OPERATIONS+=("$1")
    shift
done

while getopts "f:n:wdh" opt; do
    case "$opt" in
    f)
        VALUES_FILE="${OPTARG}"
        ;;
    n)
        NAMESPACE="${OPTARG}"
        ;;
    w)
        HELM_OPTIONS="${HELM_OPTIONS} --wait"
        ;;
    d)
        HELM_OPTIONS="${HELM_OPTIONS} --debug"
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
#### Loading package file
###################################################################

if [[ ! -f "${PACKAGES_FILE}" ]]; then
    echo "ERROR: could not find packages file: ${PACKAGES_FILE}" >&2
    show_usage >&2
    exit 1
fi

on_package() {
    PACKAGE_NAME="$1"
    PACKAGE_CHART="$2"
    PACKAGE_IS_TEST="$3"

    PACKAGE_NAMES+=(${PACKAGE_NAME})
    PACKAGE_CHARTS+=(${PACKAGE_CHART})
    PACKAGE_IS_TESTS+=(${PACKAGE_IS_TEST})
}

. ${PACKAGES_FILE}

###################################################################
#### Checks
###################################################################

if [[ "${#OPERATIONS[*]}" -eq 0 ]]; then
    echo "[ERROR] at least one operation should be specified" >&2
    show_usage >&2
    exit 1
fi

for op in "${OPERATIONS[@]}"; do
    if ! validate_operation "$op"; then
        show_usage >&2
        exit 1
    fi
done

###################################################################
#### Main
###################################################################
print_line
echo "Bigdata Netcdf Deployment [${DEPLOYMENT_NAME}]"
echo "Operations:"
for op in ${OPERATIONS[*]}; do
    echo "  $op"
done
echo "Packages:"
for name in ${PACKAGE_NAMES[*]}; do
    echo "  $name"
done

declare -a outcomes

for (( i=0; i<${#PACKAGE_NAMES[*]}; i++ )); do
    name=${PACKAGE_NAMES[$i]}
    chart=${PACKAGE_CHARTS[$i]}
    is_test=${PACKAGE_IS_TESTS[$i]}

    print_line
    echo "Package $name"
    print_line
    outcome="OK"
    for op in "${OPERATIONS[@]}"
    do
        COMMAND=$(generate_operation_command "$op" "${name}" "${chart}" "${is_test}")

        echo
        echo "--- ${op}: ${COMMAND}"
        echo
        eval "${COMMAND}" 2>&1 && ret=0 || ret=1
        if [[ ${ret} -eq 0 ]]; then
            echo "[INFO ] Operation '$op' succeeded on package '$name'"
        else
            outcome="KO"
            echo "[WARN ] Operation '$op' failed on package '$name', skipping to the next package"
            break
        fi
    done
    outcomes+=("${outcome}")
done

print_line
global_outcome="OK"
for (( i=0; i<${#PACKAGE_NAMES[*]}; i++ )); do
    name=${PACKAGE_NAMES[$i]}
    outcome=${outcomes[$i]}
    print_line . "$name" "$outcome"
    if [[ "$outcome" != "OK" ]]; then
        global_outcome="KO"
    fi
done
print_line
if [[ "$global_outcome" == "OK" ]]; then
    echo "DEPLOYMENT SUCCESS"
else
    echo "DEPLOYMENT FAILURE"
fi
print_line
if [[ "$global_outcome" != "OK" ]]; then
    exit 1
fi