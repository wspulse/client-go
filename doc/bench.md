# Benchmarks

The table below is a checked-in baseline for `wspulse/client-go`, last
refreshed locally by a maintainer running `make bench-sync` on the hardware
noted in the table. The CI workflow runs the same benchmarks on every PR
and uploads the raw `bench.txt` as an artefact; download it from the run
page if you need to compare specific numbers between branches. CI does not
regenerate or commit this file.

Variance between machines is expected — these baselines are a regression
sanity check, not a portability claim. Single runs at `-benchtime=3s -count=1`
are noisy at the few-percent level; a one-off `bench-sync` is enough for
order-of-magnitude regression detection but not micro-optimisation.

The matrix covers:

- `Send` — per-call cost across `shape ∈ {single, loop_10, loop_100}` and
  `messageSize ∈ {64 B, 1 KiB, 16 KiB}`. `single` measures one Send per timed
  iteration; `loop_K` measures K back-to-back Sends per iteration. Divide
  ns/op by K to get amortised per-message cost.
- `Reconnect backoff` — cost of one `backoff()` call. The formula must match
  all other client-* SDKs; this is regression-bait, not a hot path.

<!-- benchsync:client-go:start -->
Measured on `darwin/arm64` (`Apple M1 Max`).

| Operation | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Send (single, 64 B)` | 577.3 | 354 | 4 |
| `Send (single, 1 KiB)` | 4,868 | 4,975 | 39 |
| `Send (single, 16 KiB)` | 69,404 | 61,566 | 92 |
| `Send (loop_10, 64 B)` | 5,402 | 3,300 | 45 |
| `Send (loop_10, 1 KiB)` | 48,818 | 50,027 | 394 |
| `Send (loop_10, 16 KiB)` | 696,082 | 615,715 | 921 |
| `Send (loop_100, 64 B)` | 54,600 | 33,420 | 462 |
| `Send (loop_100, 1 KiB)` | 494,087 | 507,640 | 4,013 |
| `Send (loop_100, 16 KiB)` | 6,918,477 | 6,157,130 | 9,211 |
| `Reconnect backoff` | 7.824 | 0 | 0 |
<!-- benchsync:client-go:end -->
