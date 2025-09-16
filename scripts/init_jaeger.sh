#! /usr/bin/env bash 
set -x
set -eo pipefail

CONTAINER_NAME="jaeger"
docker run --rm --name ${CONTAINER_NAME} --detach \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 5778:5778 \
  -p 9411:9411 \
  cr.jaegertracing.io/jaegertracing/jaeger:2.10.0
until [ \
  "$(docker inspect -f "{{.State.Health.Status}}" ${CONTAINER_NAME})" == \
  "healthy" \
]; do
  >&2 echo "${CONTAINER_NAME} is still unavailable - sleeping"
  sleep 1
done
