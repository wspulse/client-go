package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBenchResults_ClientGoNames(t *testing.T) {
	t.Parallel()

	input := []byte(`
goos: darwin
goarch: arm64
cpu: Apple M1 Max
BenchmarkSend/shape=single/messageSize=64B-10           1000000      3104 ns/op    1736 B/op       5 allocs/op
BenchmarkSend/shape=loop_100/messageSize=16KiB-10           500     20000 ns/op   12000 B/op      40 allocs/op
BenchmarkReconnectBackoff-10                          50000000       104.0 ns/op       0 B/op       0 allocs/op
PASS
`)

	got, err := parseBenchResults(input)
	require.NoError(t, err)

	// Sub-benchmark names with slashes, equals, and underscores parse intact.
	assert.Equal(t, "3104", got.Results["BenchmarkSend/shape=single/messageSize=64B"].NSPerOp)
	assert.Equal(t, "20000", got.Results["BenchmarkSend/shape=loop_100/messageSize=16KiB"].NSPerOp)
	assert.Equal(t, "104.0", got.Results["BenchmarkReconnectBackoff"].NSPerOp)
}

func TestRun(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "doc"), 0o755))

	benchFile := filepath.Join(root, "bench.txt")
	require.NoError(t, os.WriteFile(benchFile, []byte(`
goos: darwin
goarch: arm64
cpu: Apple M1 Max
BenchmarkSend/shape=single/messageSize=64B-10           1000000      3104   ns/op     1736 B/op       5 allocs/op
BenchmarkSend/shape=single/messageSize=1KiB-10           800000      4500   ns/op     2700 B/op       7 allocs/op
BenchmarkSend/shape=single/messageSize=16KiB-10          200000     22000   ns/op    24000 B/op      11 allocs/op
BenchmarkSend/shape=loop_10/messageSize=64B-10           300000      8500   ns/op     5400 B/op      35 allocs/op
BenchmarkSend/shape=loop_10/messageSize=1KiB-10          250000     14000   ns/op    14000 B/op      45 allocs/op
BenchmarkSend/shape=loop_10/messageSize=16KiB-10         100000     65000   ns/op   123000 B/op      75 allocs/op
BenchmarkSend/shape=loop_100/messageSize=64B-10           50000     45000   ns/op    32000 B/op     310 allocs/op
BenchmarkSend/shape=loop_100/messageSize=1KiB-10          40000     90000   ns/op    78000 B/op     410 allocs/op
BenchmarkSend/shape=loop_100/messageSize=16KiB-10          5000    250000   ns/op   600000 B/op     710 allocs/op
BenchmarkReconnectBackoff-10                            50000000      104.0 ns/op        0 B/op       0 allocs/op
`), 0o644))

	docPath := filepath.Join(root, "doc", "bench.md")
	require.NoError(t, os.WriteFile(docPath, []byte(strings.TrimSpace(`
# Benchmarks

<!-- benchsync:client-go:start -->
stale
<!-- benchsync:client-go:end -->
`)+"\n"), 0o644))

	require.NoError(t, run([]string{"-root", root, "-input", benchFile}))

	got, err := os.ReadFile(docPath)
	require.NoError(t, err)
	doc := string(got)

	assert.Contains(t, doc, "Measured on `darwin/arm64` (`Apple M1 Max`).")
	assert.Contains(t, doc, "| `Send (single, 64 B)` | 3,104 | 1,736 | 5 |")
	assert.Contains(t, doc, "| `Send (loop_100, 16 KiB)` | 250,000 | 600,000 | 710 |")
	assert.Contains(t, doc, "| `Reconnect backoff` | 104 | 0 | 0 |")
}

func TestRun_MissingResult(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "doc"), 0o755))

	// Bench output is missing rows the target expects.
	benchFile := filepath.Join(root, "bench.txt")
	require.NoError(t, os.WriteFile(benchFile, []byte(`
BenchmarkReconnectBackoff-10  100 100 ns/op 0 B/op 0 allocs/op
`), 0o644))

	docPath := filepath.Join(root, "doc", "bench.md")
	require.NoError(t, os.WriteFile(docPath, []byte("<!-- benchsync:client-go:start -->\n<!-- benchsync:client-go:end -->\n"), 0o644))

	err := run([]string{"-root", root, "-input", benchFile})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing benchmark result")
}

func TestFormatMetric(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "107", formatMetric("107.0"))
	assert.Equal(t, "5.59", formatMetric("5.590"))
	assert.Equal(t, "1,626", formatMetric("1626"))
	assert.Equal(t, "3.007", formatMetric("3.007"))
}
