#! /usr/bin/env bash 
set -x
set -eo pipefail

CONTAINER_NAME="s3"
docker run \
  --publish 4566:4566 \
  --detach \
  --name "${CONTAINER_NAME}" \
   localstack/localstack:s3-latest
until [ \
  "$(docker inspect -f "{{.State.Health.Status}}" ${CONTAINER_NAME})" == \
  "healthy" \
]; do
  >&2 echo "localstack:s3-latest is still unavailable - sleeping"
  sleep 1
done
