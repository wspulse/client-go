# AGENTS.md — wspulse/client-go

This file is the entry point for all AI coding agents (GitHub Copilot, Codex,
Cursor, Claude, etc.). Full working rules are in
`.github/copilot-instructions.md` — read it completely before
making any changes.

---

## Quick Reference

**Module**: `github.com/wspulse/client-go` | **Package**: `client`

**Key files**:

- `client.go` — `Client` interface, `Dial` constructor, `readPump` /
  `writePump` / `reconnectLoop` goroutines, `backoff` function
- `options.go` — `ClientOption` builders

**Pre-commit gate**: `make check` (fmt → lint → test)

---

## Non-negotiable Rules

1. **Read before write** — read the target file before any edit.
2. **Thread safety** — `Send` and `Close` must be safe for concurrent use.
3. **Goroutine lifecycle** — every goroutine must have an explicit exit
   condition. `Close()` must guarantee all internal goroutines have exited.
4. **No breaking changes without version bump.**
5. **No hardcoded secrets.**
6. **Minimal changes** — one concern per edit; no drive-by refactors.

---

## Session Protocol

> `doc/local/` is git-ignored. Never commit files under it.

- **Start of session**: read `doc/local/ai-learning.md` in full (create with header if missing) and check `doc/local/plan/` for any in-progress plan.
- **Feature work**: save plan to `doc/local/plan/<feature-name>.md` before starting.
- **End of session**: append at least one entry to `doc/local/ai-learning.md` — **mandatory even if no mistakes were made**. An empty file proves the session protocol was ignored.
  Format: `Date` / `Issue or Learning` / `Root Cause` / `Prevention Rule`.
