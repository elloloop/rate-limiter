#!/usr/bin/env python3

import json
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <go-test-json-log>", file=sys.stderr)
        return 2

    failed_tests: list[tuple[str, str]] = []
    failed_packages: list[str] = []
    failed_test_set: set[tuple[str, str]] = set()
    failed_package_set: set[str] = set()
    non_json_lines = 0

    with open(sys.argv[1], encoding="utf-8", errors="replace") as log:
        for line in log:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                if line.strip():
                    non_json_lines += 1
                continue

            if event.get("Action") != "fail":
                continue

            package = event.get("Package", "")
            test = event.get("Test", "")
            if test:
                key = (package, test)
                if key not in failed_test_set:
                    failed_test_set.add(key)
                    failed_tests.append(key)
                continue

            if package and package not in failed_package_set:
                failed_package_set.add(package)
                failed_packages.append(package)

    packages_with_failed_tests = {package for package, _ in failed_tests}
    package_only_failures = [
        package for package in failed_packages if package not in packages_with_failed_tests
    ]

    if failed_tests:
        print("Failing tests:")
        for package, test in failed_tests[:200]:
            print(f"- `{package}` `{test}`")
        if len(failed_tests) > 200:
            print(f"- ... {len(failed_tests) - 200} more failing tests omitted")
        print()

    if package_only_failures:
        print("Package-level failures:")
        for package in package_only_failures[:200]:
            print(f"- `{package}`")
        if len(package_only_failures) > 200:
            print(
                f"- ... {len(package_only_failures) - 200} more package failures omitted"
            )
        print()

    if not failed_tests and not package_only_failures:
        print("No failing tests were reported in the JSON stream.")
        print()

    if non_json_lines:
        print(f"Skipped {non_json_lines} non-JSON log lines.")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
