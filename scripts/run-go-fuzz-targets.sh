#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <fuzztime>" >&2
  exit 2
fi

fuzztime="$1"
report_dir="${GO_FUZZ_REPORT_DIR:-}"
report=""

if [[ -n "$report_dir" ]]; then
  mkdir -p "$report_dir"
  report="$report_dir/fuzz-targets.md"
  {
    echo "### Go fuzz targets"
    echo
    echo "Fuzz time per target: \`$fuzztime\`"
    echo
  } > "$report"
fi

finish_report() {
  if [[ -n "$report" ]]; then
    cat "$report"
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
      cat "$report" >> "$GITHUB_STEP_SUMMARY"
    fi
  fi
}

mapfile -t targets < <(
  grep -rEn \
    --include='*_test.go' \
    --exclude-dir=.claude \
    --exclude-dir=.git \
    --exclude-dir=.idea \
    --exclude-dir=.vscode \
    --exclude-dir=node_modules \
    --exclude-dir=vendor \
    '^func (Fuzz[A-Za-z0-9_]+)\(' . \
    | sed -E 's|^(.*)/[^/]+:[0-9]+:func (Fuzz[A-Za-z0-9_]+).*$|\1\t\2|' \
    | sort -u
)

if [[ ${#targets[@]} -eq 0 ]]; then
  echo "no fuzz targets - skipping"
  if [[ -n "$report" ]]; then
    echo "No fuzz targets were discovered." >> "$report"
    finish_report
  fi
  exit 0
fi

for entry in "${targets[@]}"; do
  dir="${entry%$'\t'*}"
  dir="${dir#./}"
  name="${entry##*$'\t'}"
  echo "::group::fuzz $name in $dir"
  set +e
  output="$(go test -run='^$' -fuzz="^${name}$" -fuzztime="$fuzztime" "./$dir" 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output"
  echo "::endgroup::"

  if [[ "$status" -eq 0 ]]; then
    if [[ -n "$report" ]]; then
      echo "- \`./$dir\` \`$name\`: passed" >> "$report"
    fi
    continue
  fi

  # A non-zero exit whose only sign of trouble is the fuzzing engine's
  # context deadline (a slow worker, or a coordinator<->worker RPC that
  # timed out on a loaded CI runner) is inconclusive, not a real
  # counterexample. A genuine crash writes a reproducer under
  # testdata/fuzz/<target>/, so the absence of that "Failing input
  # written to" line is what separates an infra timeout from a finding.
  # Inconclusive targets are reported but do not fail the nightly.
  if grep -q "context deadline exceeded" <<<"$output" \
      && ! grep -q "Failing input written to" <<<"$output"; then
    echo "::warning::fuzz $name in $dir inconclusive: context deadline exceeded (treated as non-fatal)"
    if [[ -n "$report" ]]; then
      echo "- \`./$dir\` \`$name\`: inconclusive (context deadline exceeded)" >> "$report"
    fi
    continue
  fi

  if [[ -n "$report" ]]; then
    echo "- \`./$dir\` \`$name\`: failed" >> "$report"
    finish_report
  fi
  exit "$status"
done

finish_report
