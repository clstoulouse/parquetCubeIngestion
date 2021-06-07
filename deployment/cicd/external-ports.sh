#!/usr/bin/env bash

CURRENT_DIR=$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)
INGRESS_NAMESPACE="ingress-nginx"


ACTION_LS="ls"
ACTION_MAP="map"
ACTION_RM="rm"

PROTO_TCP="t"
PROTO_UDP="u"
NETWORK_PRIVATE="p"
NETWORK_PUBLIC="P"

###################################################################
#### Parsing arguments
###################################################################

show_usage() {
    echo "USAGE: ${0} ${ACTION_LS}  [--tcp|--udp] [--public|--private]"
    echo "       ${0} ${ACTION_MAP} [--tcp|--udp] [--public|--private] --port PORT --to SERVICE"
    echo "       ${0} ${ACTION_RM}  [--tcp|--udp] [--public|--private] --port PORT"
    echo "DESCRIPTION: Manages the exposed ports on the cluster"
    echo "ACTIONS:"
    echo "  ${ACTION_LS}:   Lists the exposed ports"
    echo "  ${ACTION_MAP}:    Map an external port to kubernetes service"
    echo "  ${ACTION_RM}:     Deletes the port mapping associated to a service"
    echo "OPTIONS:"
    echo "  --tcp|--udp:        (actions: all) Protocol of the exposed port(s) (if not specified, --tcp is implied)"
    echo "  --public|--private: (actions: all) private or the public network (if not specified, --private is implied)"
    echo "  --port PORT:        (actions: ${ACTION_MAP},${ACTION_RM}) External port to add/delete to/from the mapping list"
    echo "  --to SERVICE:       (actions: ${ACTION_MAP}) kubernetes service/port to be mapped to the external port"
    echo "                                     syntax: <namespace>/<service>:<port>"
    echo "  -h|--help:          Prints the command usage and then exists"
    echo "EXAMPLES:"
    echo "  ${0} ${ACTION_LS} --tcp --private:"
    echo "      Lists all the TCP ports exposed on the private network"
    echo "  ${0} ${ACTION_LS} --udp --public:"
    echo "      Lists all the UDP ports exposed on the public network"
    echo "  ${0} ${ACTION_MAP} --tcp --private --port 12345 --to aisqo/uda-light-svc:80"
    echo "      Maps the external TCP port 12345, in the private network,"
    echo "      to the port 80 of the kubernetes service uda-light-svc from the namespace aisqo"
    echo "  ${0} ${ACTION_RM} --udp --public --port 12345"
    echo "      Removes any mapping to the external UDP port 12345 in the public network"
}

ACTION="$1"
shift

for arg
do
    delim=""
    case "$arg" in
       --tcp) args="${args}-T ";;
       --udp) args="${args}-U ";;
       --private) args="${args}-n ";;
       --public) args="${args}-N ";;
       --port) args="${args}-p ";;
       --to) args="${args}-t ";;
       --help) args="${args}-h ";;

       # pass through anything else
       *) [[ "${arg:0:1}" == "-" ]] || delim="\""
           args="${args}${delim}${arg}${delim} ";;
    esac
done
# reset the translated args
eval set -- ${args}

PROTO="${PROTO_TCP}"
NETWORK="p"
PORT=""
SERVICE=""

OPTIND=1         # Reset in case getopts has been used previously in the shell.

while getopts "TUnNp:t:h" opt; do
    case "$opt" in
    T) PROTO="${PROTO_TCP}" ;;
    U) PROTO="${PROTO_UDP}" ;;
    n) NETWORK="${NETWORK_PRIVATE}" ;;
    N) NETWORK="${NETWORK_PUBLIC}" ;;
    p) PORT="${OPTARG}" ;;
    t) SERVICE="${OPTARG}" ;;
    h)
        show_usage
        exit 0
        ;;
    esac
done

shift $((OPTIND-1))

[[ "$1" = "--" ]] && shift

###################################################################
#### Checking
###################################################################

case "$ACTION" in
   ${ACTION_LS}|${ACTION_MAP}|${ACTION_RM}) ;;
   *) echo "ERROR: Unknown action '${ACTION}'" >&2
      show_usage >&2
      exit 1;;
esac

if [[ "$ACTION" == "${ACTION_LS}" ]]; then
    if [[ -n "${SERVICE}" ]]; then
        echo "ERROR: option '--to' is incompatible with action '${ACTION_LS}'" >&2
        show_usage >&2
        exit 1
    fi
    if [[ -n "${PORT}" ]]; then
        echo "ERROR: option '--port' is incompatible with action '${ACTION_LS}'" >&2
        show_usage >&2
        exit 1
    fi
fi

if [[ "$ACTION" == "${ACTION_RM}" ]]; then
    if [[ -n "${SERVICE}" ]]; then
        echo "ERROR: option '--to' is incompatible with action '${ACTION_RM}'" >&2
        show_usage >&2
        exit 1
    fi
fi

case "${PROTO}${NETWORK}" in
    "${PROTO_TCP}${NETWORK_PRIVATE}")   CM_NAME="tcp-private-services" ;;
    "${PROTO_TCP}${NETWORK_PUBLIC}")    CM_NAME="tcp-services" ;;
    "${PROTO_UDP}${NETWORK_PRIVATE}")   CM_NAME="udp-private-services" ;;
    "${PROTO_UDP}${NETWORK_PUBLIC}")    CM_NAME="udp-services" ;;
esac

###################################################################
#### Functions
###################################################################

list() {
    cm_name="$1"
    SCRIPT=$(cat <<EOF
import sys, json

jcm = json.load(sys.stdin)

print("PORT;SERVICE")
for port in jcm['data']:
    svc = jcm['data'][port]
    print(port + ";" + svc)
EOF
)

    kubectl get configmaps -n "${INGRESS_NAMESPACE}" "${cm_name}" -o json | python -c "${SCRIPT}" | column -t -s';'
}

map() {
    cm_name="$1"
    external_port="$2"
    target_service="$3"
    SCRIPT=$(cat <<EOF
import sys, json

jcm = json.load(sys.stdin)

jcm['data']['${external_port}'] = '${target_service}'

del jcm['metadata']['annotations']
del jcm['metadata']['creationTimestamp']
del jcm['metadata']['resourceVersion']
del jcm['metadata']['selfLink']
del jcm['metadata']['uid']

print(json.dumps(jcm))
EOF
)

    kubectl get configmaps -n "${INGRESS_NAMESPACE}" "${cm_name}" -o json | python -c "${SCRIPT}" | kubectl apply -n "${INGRESS_NAMESPACE}" -f -
}

del() {
    cm_name="$1"
    external_port="$2"
    SCRIPT=$(cat <<EOF
import sys, json

jcm = json.load(sys.stdin)

del jcm['data']['${external_port}']

del jcm['metadata']['annotations']
del jcm['metadata']['creationTimestamp']
del jcm['metadata']['resourceVersion']
del jcm['metadata']['selfLink']
del jcm['metadata']['uid']

print(json.dumps(jcm))
EOF
)

    kubectl get configmaps -n "${INGRESS_NAMESPACE}" "${cm_name}" -o json | python -c "${SCRIPT}" | kubectl apply -n "${INGRESS_NAMESPACE}" -f -
}

###################################################################
#### Doing the job
###################################################################

case "$ACTION" in
   ${ACTION_LS}) list "${CM_NAME}" ;;
   ${ACTION_MAP}) map "${CM_NAME}" "${PORT}" "${SERVICE}" ;;
   ${ACTION_RM}) del "${CM_NAME}" "${PORT}" ;;
esac