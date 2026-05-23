#!/usr/bin/env bash
#
# run-coverage.sh — produce a merged coverage profile (cover.out) for the
# coverage gate. Redis-backed integration tests run inline when
# QUOTA_TEST_REDIS_URL is set (CI sets it; locally start a Redis and export
# it). Without it the Redis-dependent paths simply report lower coverage.

set -euo pipefail

rm -f cover.out cover.*.out

go test -count=1 -race -timeout=600s \
  -coverprofile=cover.out \
  -coverpkg=./internal/...,./cmd/... \
  ./...
