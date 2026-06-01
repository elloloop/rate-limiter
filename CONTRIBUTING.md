# Contributing

Thanks for considering a contribution to Rate Limiter. This project is small
on purpose: changes should keep the quota service generic, typed, tested, and
easy to operate.

## Before you open a pull request

- Read `AGENTS.md`. Those engineering rules apply to all changes.
- Keep the change scoped to one complete behavior, bug fix, or documentation
  improvement.
- Add or update tests for every behavior change. Redis behavior must be tested
  against a real Redis instance by setting `QUOTA_TEST_REDIS_URL`.
- Do not include generated-by or model-attribution trailers in commit messages.
- Sign the CLA when the pull request asks for it.

## Local setup

Install Go, Docker, pnpm, buf, golangci-lint, and govulncheck. The Makefile
pins the tool versions used by CI.

```bash
make install-tools
make redis-up
make postgres-up
```

The service-level tests use these optional environment variables:

```bash
export QUOTA_TEST_REDIS_URL=redis://localhost:6379/0
export QUOTA_TEST_POSTGRES_URL=postgres://quota:quota@localhost:15432/quota?sslmode=disable
```

## Expected verification

Run the smallest useful command while iterating, then run the full gate before
requesting review:

```bash
make ci-full
make test-cover
make docs
```

`make ci-full` covers linting, module tidiness, vulnerability scanning, build,
race-enabled tests, smoke tests, fuzz smoke, and Docker Compose e2e. `make
test-cover` enforces the aggregate and per-package coverage gates.

## Pull request expectations

- Explain the behavior change and why it belongs in the generic quota service.
- List the exact verification commands you ran.
- Include e2e or integration coverage when the change affects Redis behavior,
  gRPC behavior, Docker packaging, config loading, event persistence, or
  release workflows.
- Update user-facing docs when config, API behavior, deployment, or operations
  guidance changes.

Security reports should not be opened as public issues. Use the process in
`.github/SECURITY.md`.
