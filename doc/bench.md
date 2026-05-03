# Benchmarks

Numbers below are the latest CI-published baseline for `wspulse/client-go`.
Regenerate locally with `make bench-sync`. The CI workflow uploads the raw
`bench.txt` output as an artefact on every PR; download it from the run page
if you need to compare specific numbers between branches.

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
| `Send (single, 64 B)` | 525.1 | 340 | 4 |
| `Send (single, 1 KiB)` | 4,868 | 5,113 | 40 |
| `Send (single, 16 KiB)` | 68,742 | 61,580 | 92 |
| `Send (loop_10, 64 B)` | 5,251 | 3,480 | 48 |
| `Send (loop_10, 1 KiB)` | 48,826 | 51,488 | 408 |
| `Send (loop_10, 16 KiB)` | 686,637 | 615,660 | 921 |
| `Send (loop_100, 64 B)` | 52,651 | 34,722 | 480 |
| `Send (loop_100, 1 KiB)` | 488,082 | 513,267 | 4,068 |
| `Send (loop_100, 16 KiB)` | 6,893,634 | 6,157,370 | 9,211 |
| `Reconnect backoff` | 7.207 | 0 | 0 |
<!-- benchsync:client-go:end -->
