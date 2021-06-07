#!/usr/bin/env bash

###################################################################
#### Constants
###################################################################

OP_NAME_INSTALL="install"
OP_NAME_UNINSTALL="uninstall"
OP_NAME_UPSTALL="upstall"
OP_NAME_UPGRADE="upgrade"

###################################################################
#### Public functions
###################################################################

show_operations_usage() {
    echo "${OP_NAME_INSTALL}:    Install the packages (fails if the packages are already installed)"
    echo "${OP_NAME_UPSTALL}:    Install the packages if they are not, otherwise upgrades these packages"
    echo "${OP_NAME_UPGRADE}:    Upgrades the packages (fails if the packages are not already installed)"
    echo "${OP_NAME_UNINSTALL}:  Uninstall the packages (fails if the packages are not already installed)"
    echo "${OP_NAME_CHECK_VERSION}:  Uninstall the packages (fails if the packages are not already installed)"
}

validate_operation() {
    op="$1"
    if [[ -z "${DEPLOYMENT_NAME}" ]]; then
        echo "[ERROR] The deployment name is required" >&2
        return 1
    fi
    case "$op" in
        ${OP_NAME_INSTALL}|${OP_NAME_UPSTALL}|${OP_NAME_UPGRADE})
            if [[ -z "${VALUES_FILE}" ]]; then
                echo "[ERROR] Missing values files (required for operation '$op')" >&2
                return 1
            fi
            if [[ ! -f "${VALUES_FILE}" ]]; then
                echo "[ERROR] Values file does not exist (required for operation '$op'): ${VALUES_FILE}" >&2
                return 1
            fi
            if [[ -z "${NAMESPACE}" ]]; then
                echo "[ERROR] Missing namespace (required for operation '$op')" >&2
                return 1
            fi
            ;;
        ${OP_NAME_UNINSTALL}) ;;
        *)
            echo "[ERROR] unknown operation '$1'" >&2
            return 1
            ;;
    esac
}

generate_operation_command() {
    op="$1"
    name="$2"
    chart="$3"
    is_test="$4"

    case "$op" in
        ${OP_NAME_INSTALL}) __install "${name}" "${chart}";;
        ${OP_NAME_UNINSTALL}) __uninstall "${name}";;
        ${OP_NAME_UPSTALL}) __upstall "${name}" "${chart}";;
        ${OP_NAME_UPGRADE}) __upgrade "${name}" "${chart}";;
    esac
}

###################################################################
#### Internal functions
###################################################################

__install() {
    PACKAGE_NAME="$1"
    PACKAGE_CHART="$2"

    RELEASE_NAME=$(__get_release_name "${PACKAGE_NAME}")

    echo helm install \
        "${RELEASE_NAME}" \
        "${PACKAGE_CHART}" \
        -f "${VALUES_FILE}" \
        --namespace "${NAMESPACE}" \
        --set deploymentName="${DEPLOYMENT_NAME}" \
        ${HELM_OPTIONS}
}

__uninstall() {
    PACKAGE_NAME="$1"

    RELEASE_NAME=$(__get_release_name "${PACKAGE_NAME}")

    echo helm delete \
        --namespace "${NAMESPACE}" \
        "${RELEASE_NAME}"
}

__upstall() {
    PACKAGE_NAME="$1"
    PACKAGE_CHART="$2"

    RELEASE_NAME=$(__get_release_name "${PACKAGE_NAME}")

    echo helm upgrade \
        "${RELEASE_NAME}" \
        "${PACKAGE_CHART}" \
        --install \
        --recreate-pods \
        -f "${VALUES_FILE}" \
        --namespace "${NAMESPACE}" \
        --set deploymentName="${DEPLOYMENT_NAME}" \
        ${HELM_OPTIONS}
}

__upgrade() {
    PACKAGE_NAME="$1"
    PACKAGE_CHART="$2"

    RELEASE_NAME=$(__get_release_name "${PACKAGE_NAME}")

    echo helm upgrade \
        "${RELEASE_NAME}" \
        "${PACKAGE_CHART}" \
        --recreate-pods \
        -f "${VALUES_FILE}" \
        --namespace "${NAMESPACE}" \
        --set deploymentName="${DEPLOYMENT_NAME}" \
        ${HELM_OPTIONS}
}

__get_release_name() {
    # truncate at 53 chars and remove trailing '-' to please Helm/Kubernetes
    echo "${1}-${DEPLOYMENT_NAME}" | cut -c1-53 | sed -e 's/-$//'
}