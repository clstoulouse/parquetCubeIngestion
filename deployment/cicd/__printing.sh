#!/usr/bin/env bash

CONSOLE_WIDTH=${COLUMNS}
if [[ -z "${CONSOLE_WIDTH}" ]]; then
    CONSOLE_WIDTH=$(tput cols 2> /dev/null || echo 72)
fi

print_line() {
    sep="$1"
    prefix="$2"
    suffix="$3"
    if [[ -z "$sep" ]]; then
        sep="-"
    fi
    len_prefix="${#prefix}"
    len_suffix="${#suffix}"
    if [[ "${len_prefix}" -gt 0 ]]; then
        len_prefix=$((${len_prefix} + 1))
    fi
    if [[ "${len_suffix}" -gt 0 ]]; then
        len_suffix=$((${len_suffix} + 1))
    fi
    nb=$(($CONSOLE_WIDTH - $len_prefix - $len_suffix))
    echo ${prefix} $(__line "$nb" "${sep}") ${suffix}
}

__line() {
    nb=$1
    sep=$2
    for ((x = 0; x < "$nb"; x++)); do
      printf %s "${sep}"
    done
}