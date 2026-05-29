# Project guidance for Claude

The full engineering rules live in [AGENTS.md](./AGENTS.md). They apply
to every change — agent-driven or human. Read them before making any
non-trivial edit.

Highlights for quick recall:

- **No patch fixes.** Change the wrong shape, don't wrap it. No shims,
  no compatibility layers, no translation tables for "old callers."
- **Delete dead code.** When a refactor lands, the old files, types,
  constants, and re-exports go with it.
- **No half-finished implementations.** Land complete features
  (impl + tests + wiring) or split along feature boundaries.
- **Tests are part of the change.** Bug fixes get regression tests.
  Redis-backed behaviour is tested against a real Redis; coverage gates
  go up, never down.
- **Clean commit messages.** Imperative mood, no AI attribution, no
  references to inaccessible context.

If existing code violates these rules and your change touches it, fix
the violation as part of your change. Do not preserve the wrong pattern.

---

## How I expect you to write code

**No shortcuts. "Simple" never means "sloppy."** A small diff that hardcodes,
duplicates, or skips a test isn't simpler — it's deferred cost.

1. **Fix causes, not symptoms.** Find the root cause before fixing. If you're
   applying a workaround, say so explicitly and explain why. Never swallow an
   exception or silence an error to make a problem disappear.

2. **Think about consequences.** Before changing shared or widely-used code,
   trace its callers and the invariants they rely on. A fix that's locally
   correct but breaks something elsewhere — now or later — is not a fix.

3. **SOLID, sensibly.** One responsibility per class/widget/function. Separate
   pure logic from I/O so it can be tested. Inject dependencies that cross a
   boundary so they're mockable. Don't add abstractions for things that don't
   cross a boundary.

4. **DRY about knowledge, not appearance.** Don't duplicate a rule or decision.
   Code that merely looks similar but changes for different reasons stays
   separate. When unsure, prefer duplication over a premature/wrong abstraction.

5. **No hardcoded values.** No magic numbers or strings inline — give them
   names. Environment/tenant/feature-specific values go in typed config in
   application code, never scattered literals, never the database.

6. **Readable & maintainable.** Clear names, short flat functions, early
   returns over deep nesting. Comments explain *why*, not *what*. Match the
   existing style of the file you're editing.

7. **Testable, and prove it.** Ship a test for behavior you add or change. If
   something is hard to test, that's a design smell — restructure until it
   isn't. "Works but can't be tested" means it isn't done.

A change is done only when: the cause (not a symptom) is fixed, no new hardcoded
values, a test covers it, and the analyzer/formatter are clean.

## Project facts

> Keep these current as the repo evolves; only write what you've confirmed.

- **Setup command:** `make install-tools` (installs pinned golangci-lint + govulncheck); deps via `go mod download`
- **Analyze/lint command:** `make lint` (golangci-lint, only new issues vs `origin/main`) or `make lint-all` for the whole tree
- **Test command (all):** `make test` (`go test -count=1 -race -timeout=600s ./...`); set `QUOTA_TEST_REDIS_URL` for Redis-backed paths
- **Test command (single file/test):** `go test -race -run '^TestName$' ./path/to/pkg/`
- **Format command:** `make fmt` (`golangci-lint fmt`, applies gofumpt)
- **Run an app:** `make dev` (docker-compose: Redis + quota service) or `go run ./cmd/quota-service`; CLI also has `validate-limits` and `print-config`
- **Repo layout:** `cmd/quota-service` (binary entrypoint); `ratelimiterserver` (embeddable gRPC server + `backend/` drivers); `internal/` (config, events, keys, limits, metrics); `quota/v1` (proto source); `gen/quota/v1` (generated stubs); `examples/`, `docs/`, `docs-site/`, `scripts/`, `test/e2e`, `tests/smoke`
- **State management / data layer conventions:** Redis-only backend (algorithms rely on Redis Lua atomicity); accessed through the `ratelimiterserver/backend.Backend` interface; Postgres used only as an optional async event sink. Pure quota math inside, business rules outside
- **Generated files NOT to hand-edit:** `gen/quota/v1/*.pb.go` and `gen/quota/v1/*_grpc.pb.go` — regenerate via `make proto` (`buf generate`) from `quota/v1/quota.proto`
- **Other gotchas worth recording:** Pin every external version exactly — no floating tags (AGENTS.md rule 10). Coverage gates (`.coverage-gates.yml`) only go up, never down. Run `make ci` (or `make ci-full` for the docker e2e) before pushing. Redis-backed behavior must be tested against a real Redis, not mocks
