# Contributing to wspulse/client-go

Thank you for your interest in contributing. This document describes the process and conventions expected for all contributions.

## Before You Start

- Open an issue to discuss significant changes before starting work.
- For bug fixes, write a failing test that reproduces the issue before modifying production code. The PR must include this test.
- For new features, confirm scope and API design in an issue first.

## Development Setup

```bash
git clone https://github.com/wspulse/client-go
cd client-go
# Clone dependencies alongside client-go (required for local replace directives)
git clone https://github.com/wspulse/server ../server
git clone https://github.com/wspulse/core ../core
go mod tidy
```

Requires: Go 1.26+, [golangci-lint](https://golangci-lint.run/), [goimports](https://pkg.go.dev/golang.org/x/tools/cmd/goimports).

## Pre-Commit Checklist

Run `make check` before every commit. It runs in order:

1. `make fmt` — formats all source files
2. `make lint` — runs `go vet` and `golangci-lint`; must pass with zero warnings
3. `make test` — runs tests with `-race`; must pass

If any step fails, do not commit.

## Commit Messages

Follow the format in [`.github/instructions/commit-message-instructions.md`](.github/instructions/commit-message-instructions.md):

```
<type>: <subject>

1.<reason> → <change>
```

All commit messages must be in English.

## Naming Conventions

- **Interface names** must use full words — no abbreviations. Write `Connection`, not `Conn`.
- **Variable and parameter names** follow standard Go style: single-letter or short receivers, idiomatic short names for local scope (`conn`, `fn`, `err`, `ok`, `n`, `i`, `v`).
- Banned in interface/type names: `Conn`, `Cfg`, `Mgr`, `Svc`, `Svr`, `Hdlr`, `Sess`, `Recv`, `Tbl`, `Dlg`, `Proc`, `Coll`.

## Thread Safety

`Send` and `Close` must remain safe for concurrent use from any number of goroutines. All shared state must be protected by mutexes or channels. Every goroutine launched must have an explicit, documented exit condition. `Close()` must guarantee all internal goroutines have exited. Use `go.uber.org/goleak` to verify — it is integrated in `TestMain`.

## API Compatibility

wspulse/client-go follows semantic versioning. Any change that removes, renames, or alters the signature of an exported symbol is a **breaking change** and requires a major version bump.

- Before removing a symbol, mark it `// Deprecated: use Xxx instead.` in a minor release.
- Adding a method to an exported interface is also a breaking change.
- When in doubt, add a new symbol alongside the old one.

## Performance-Sensitive Changes

Changes to reconnect backoff, message throughput, or codec must include a benchmark. Run `make bench` and include before/after numbers in the PR description.

## Pull Request Guidelines

- One PR per logical change.
- Do not reformat code unrelated to your change — it creates noise in the diff.
- All CI checks must pass before review.
- Describe what changed and why, not just what the diff shows.
