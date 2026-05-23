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
