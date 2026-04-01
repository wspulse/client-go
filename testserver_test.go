//go:build integration

package client_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.uber.org/goleak"
)

var (
	tsWSPort  string
	tsCtlPort string
	tsCmd     *exec.Cmd
	tsBinDir  string // temp dir for installed binary; cleaned up on exit
)

func TestMain(m *testing.M) {
	// Install testserver binary from module proxy.
	tmpDir, err := os.MkdirTemp("", "testserver-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	tsBinDir = tmpDir
	installCmd := exec.Command("go", "install", "github.com/wspulse/testserver@v0.2.0")
	installCmd.Env = append(os.Environ(), "GOBIN="+tmpDir)
	installCmd.Stdout = os.Stdout
	installCmd.Stderr = os.Stderr
	if err := installCmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to install testserver: %v\n", err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	// Start testserver binary.
	tsCmd = exec.Command(filepath.Join(tmpDir, "testserver"))
	stderrPipe, err := tsCmd.StderrPipe()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create stderr pipe: %v\n", err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	if err := tsCmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start testserver: %v\n", err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	// Parse READY:<ws_port>:<control_port> from stderr.
	scanner := bufio.NewScanner(stderrPipe)
	ready := make(chan struct{})
	drainDone := make(chan struct{})
	go func() {
		defer close(drainDone)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "READY:") {
				parts := strings.Split(line, ":")
				if len(parts) == 3 {
					tsWSPort = parts[1]
					tsCtlPort = parts[2]
					close(ready)
					break
				}
			}
		}
		// Drain remaining stderr to prevent blocking.
		_, _ = io.Copy(io.Discard, stderrPipe)
	}()

	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		_ = tsCmd.Process.Kill()
		_, _ = tsCmd.Process.Wait()
		_ = os.RemoveAll(tsBinDir)
		fmt.Fprintf(os.Stderr, "testserver did not become ready within 10s\n")
		os.Exit(1)
	}

	// Run tests, then shut down testserver before leak check.
	// Kill order: m.Run → kill process (closes stderr pipe → drain goroutine
	// exits) → wait for drain goroutine → goleak.Find.
	exitCode := m.Run()

	_ = tsCmd.Process.Kill()
	_, _ = tsCmd.Process.Wait()
	<-drainDone
	_ = os.RemoveAll(tsBinDir)

	if err := goleak.Find(); err != nil {
		fmt.Fprintf(os.Stderr, "goroutine leak detected:\n%v\n", err)
		os.Exit(1)
	}
	os.Exit(exitCode)
}

// wsURL returns a WebSocket URL for the shared testserver.
// The query string should not include the leading "?".
func wsURL(query string) string {
	if query == "" {
		return fmt.Sprintf("ws://127.0.0.1:%s", tsWSPort)
	}
	return fmt.Sprintf("ws://127.0.0.1:%s?%s", tsWSPort, query)
}

// controlURL returns the full URL for a control API endpoint.
func controlURL(path string) string {
	return fmt.Sprintf("http://127.0.0.1:%s%s", tsCtlPort, path)
}

// controlResponse is the JSON response from the control API.
type controlResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// controlPost sends a POST to the control API and returns the response.
func controlPost(t *testing.T, path string) controlResponse {
	t.Helper()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Post(controlURL(path), "", nil) //nolint:noctx
	require.NoError(t, err, "control POST %s failed", path)
	defer resp.Body.Close()
	var cr controlResponse
	err = json.NewDecoder(resp.Body).Decode(&cr)
	require.NoError(t, err, "control POST %s: decode response", path)
	return cr
}

// controlGet sends a GET to the control API and returns the HTTP status code.
func controlGet(t *testing.T, path string) int {
	t.Helper()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Get(controlURL(path)) //nolint:noctx
	require.NoError(t, err, "control GET %s failed", path)
	defer resp.Body.Close()
	return resp.StatusCode
}

// kickConnection kicks a connection by ID via the control API.
// It retries for up to 2 seconds to handle the case where the server has not
// yet registered the connection.
func kickConnection(t *testing.T, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		cr := controlPost(t, "/kick?id="+id)
		if cr.OK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("kick %q: server did not register connection within 2s (last error: %s)", id, cr.Error)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// shutdownServer shuts down the shared testserver's WebSocket listener.
func shutdownServer(t *testing.T) {
	t.Helper()
	cr := controlPost(t, "/shutdown")
	require.True(t, cr.OK || cr.Error == "already shut down",
		"shutdown failed: %s", cr.Error)
}

// restartServer restarts the shared testserver's WebSocket listener and waits
// for the health endpoint to respond.
func restartServer(t *testing.T) {
	t.Helper()
	cr := controlPost(t, "/restart")
	require.True(t, cr.OK, "restart failed: %s", cr.Error)
	// Wait for health endpoint.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if code := controlGet(t, "/health"); code == 200 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("testserver did not become healthy after restart within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
