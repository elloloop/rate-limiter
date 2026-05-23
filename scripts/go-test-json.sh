#!/usr/bin/env bash

set -euo pipefail

if [[ $# -lt 3 || "$2" != "--" ]]; then
  echo "usage: $0 <report-title> -- <go-test-args...>" >&2
  exit 2
fi

title="$1"
shift 2

report_dir="${GO_TEST_REPORT_DIR:-go-test-reports}"
mkdir -p "$report_dir"

slug="$(printf '%s' "$title" \
  | tr '[:upper:]' '[:lower:]' \
  | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')"
if [[ -z "$slug" ]]; then
  slug="go-test"
fi

json_log="$report_dir/${slug}.json"
report="$report_dir/${slug}.md"

set +e
go test -json "$@" 2>&1 | tee "$json_log"
status=${PIPESTATUS[0]}
set -e

{
  echo "### $title"
  echo
  printf 'Command: `go test'
  for arg in "$@"; do
    printf ' %q' "$arg"
  done
  echo '`'
  echo
  if [[ "$status" -eq 0 ]]; then
    echo "Result: passed"
  else
    echo "Result: failed"
  fi
  echo
  python3 scripts/go-test-json-report.py "$json_log"
} > "$report"

cat "$report"
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  cat "$report" >> "$GITHUB_STEP_SUMMARY"
fi

exit "$status"
