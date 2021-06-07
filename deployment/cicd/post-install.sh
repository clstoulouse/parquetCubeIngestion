#!/usr/bin/env bash

CURRENT_DIR=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)

source "${CURRENT_DIR}/__defaults.sh"
source "${CURRENT_DIR}/__namespace-declaration.sh"

expose_ports() {
    show_usage() {
        echo "USAGE: ${0} [SERVICE NAME]"
        echo "DESCRIPTION: Expose all ports of the kube service"
        echo "SERVICE NAME: The name of the service whose ports must be exposed"
    }

    SERVICE_NAME="$1"

    if [[ -z "$NAMESPACE" ]]; then
        echo "[ERROR] Missing namespace name" >&2
        show_usage >&2
        return 1
    fi

    if [[ -z "$SERVICE_NAME" ]]; then
        echo "[ERROR] Missing service name" >&2
        show_usage >&2
        return 1
    fi

    SERVICE_PORT_GOTPL='{{range .spec.ports}}{{$.metadata.name}}:{{.port}}{{"\n"}}{{end}}'

    # Expose FTP ports
    for svcport in $(kubectl -n ${NAMESPACE} get svc ${SERVICE_NAME} -o go-template="${SERVICE_PORT_GOTPL}"); do
        port=$(echo "${svcport}" | cut -d: -f2)
        echo ">> <private>:${port} -> ${NAMESPACE}/${svcport}"
        ${CURRENT_DIR}/external-ports.sh map --tcp --private --port "${port}" --to "${NAMESPACE}/${svcport}"
    done
}

. ${POST_INSTALL_FILE}