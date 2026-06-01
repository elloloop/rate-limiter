#!/usr/bin/env bash
#
# run-coverage.sh — produce a merged coverage profile (cover.out) for the
# coverage gate. Redis-backed integration tests run inline when
# QUOTA_TEST_REDIS_URL is set. Postgres-backed event sink tests run when
# QUOTA_TEST_POSTGRES_URL is set. CI sets both; locally start the services
# and export the URLs printed by `make redis-up` and `make postgres-up`.

set -euo pipefail

rm -f cover.out cover.*.out

go test -count=1 -race -timeout=600s \
  -coverprofile=cover.out \
  -coverpkg=./internal/...,./cmd/...,./ratelimiterserver/... \
  ./...
