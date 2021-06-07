#!/usr/bin/env bash

for f in /docker-entry-point.d/*; do
    case "$f" in
        *.sh)
            # https://github.com/docker-library/postgres/issues/450#issuecomment-393167936
            # https://github.com/docker-library/postgres/pull/452
            echo "$0: sourcing $f"
            . "$f"
            ;;
        *)
            echo "$0: ignoring $f" ;;
    esac
    echo
done

echo "working dir: $(pwd)"
echo "executing: $@"

exec "$@"