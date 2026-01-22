#!/bin/sh

# Runs the integration tests for quickmq
# NOTICE: Don't invoke script directly, instead call through `hatch run integration:test`
#
# Inspired from:
# https://medium.com/twodigits/testcontainers-on-podman-a090c348b9d8
# https://podman-desktop.io/docs/migrating-from-docker/using-the-docker_host-environment-variable

get_podman_path(){
    case "$(uname)" in
        [dD]arwin*)
            podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}'
            ;;
        [lL]inux*)
            podman info --format '{{.Host.RemoteSocket.Path}}'
            ;;
        *)
            echo "$0 ERR: Unknown/unsupported uname for podman."
            ;;
    esac
}

if command -v podman >/dev/null 2>&1; then
    # Use podman if it exists
    DOCKER_HOST=unix://"$(get_podman_path)"
    TESTCONTAINERS_RYUK_DISABLED=true
    export DOCKER_HOST TESTCONTAINERS_RYUK_DISABLED
elif ! command -v docker >/dev/null 2>&1; then
    # If podman and docker don't exist, error
    echo "$0 ERR: Cannot run integration tests without podman or docker"
    exit 1
fi

pytest --ignore='tests/unit/' tests/integration "$@"
