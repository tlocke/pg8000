#!/bin/bash

# This script will fire up a local postgresql test server using podman

base_path=$(dirname "$0")

podman run \
       --interactive \
       --tty \
       --rm \
       --publish 127.0.0.1:5432:5432 \
       --env POSTGRES_PASSWORD=pw \
       --env OUTSIDE_USER="$USER" \
       --mount type=bind,source="$base_path/init-scripts",target=/docker-entrypoint-initdb.d \
       --name pg8000-test-server \
       docker.io/library/postgres:latest "$@"
