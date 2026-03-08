# Copilot Instructions — wspulse/client-go

## Project Overview

wspulse/client-go is a **WebSocket client library for Go** with automatic reconnection and exponential backoff. Module path: `github.com/wspulse/client-go`. Package name: `client`. Depends on `github.com/wspulse/core` for shared types (`Frame`, `Codec`).

## Architecture

- **`client.go`** — `Client` interface (public API: `Send`, `Close`, `Done`) and `Dial` constructor. Internal goroutines: `readPump`, `writePump`, `reconnectLoop`. Includes the `backoff` function for exponential delay calculation.
- **`options.go`** — `ClientOption` functional options and all `WithXxx` option builders (`WithOnMessage`, `WithAutoReconnect`, `WithCodec`, etc.).

## Development Workflow

```bash
# Run all tests with race detector
go test -race -count=3 ./...

# Vet
go vet ./...

# Lint (requires golangci-lint)
golangci-lint run ./...

# Format
goimports -w .
```

## Conventions

- **Go style**: `gofmt`/`goimports`, snake_case filenames, GoDoc on all public symbols, `if err != nil` error handling (never `panic`), secrets from env vars only.
- **Naming — readability is the highest priority**:
  - Use full words for all identifiers. Code is AI-generated; there is no excuse for cryptic names.
  - **Allowed abbreviations** (universally recognized only): ID, URL, HTTP, API, JSON, Msg, Err, Ctx, Buf, Cfg, Fn, Opt, Req, Resp, Src, Dst, Addr, Auth, Init, Exec, Cmd, Env, Pkg, Fmt, Doc, Spec, Sync, Async, Max, Min, Len, Cap, Idx, Tmp, Ref, Val, Str, Int, Bool, Impl, Repo.
  - **Banned** — half-word truncations that harm readability: `sess`, `conn`, `svc`, `mgr`, `recv`, `svr`, `tbl`, `hdlr`, `dlg`, `desc`, `proc`, `coll`.
  - When in doubt, spell out the full word.
- **Markdown**: no emojis in documentation files.
- **Git**:
  - Follow the commit message rules in [commit-message-instructions.md](commit-message-instructions.md).
  - All commit messages in English.
  - Each commit must represent exactly one logical change.
  - Run formatter and tests locally before committing.
- **Tests**: co-located with source (`_test.go`). Cover happy path and at least one error path. Required for new public functions. Tests may import `github.com/wspulse/server` to create echo servers — this is a test-only dependency.

## Critical Rules

1. **Read before write** — always read the target file fully before editing.
2. **Minimal changes** — one concern per edit; no drive-by refactors.
3. **No hardcoded secrets** — all configuration via environment variables.
4. **Thread safety** — `Send` and `Close` must be safe for concurrent use. All shared state must be protected by mutexes or channels.
5. **Accuracy** — if you have questions or need clarification, ask the user. Do not make assumptions without confirming.
6. **Language consistency** — when the user writes in Traditional Chinese, respond in Traditional Chinese; otherwise respond in English.
