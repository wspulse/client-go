# TODOs

## Defensive: read writeErrCh before wsConnection.Close() in readPump defer

**Context**: readPump's defer calls `wsConnection.Close()` before checking `writeErrCh`. If writePump is mid-WriteMessage when Close() fires, the close-induced write error can land on `writeErrCh` and override the original read error in `onTransportDrop`.

**Fix**: Move the `writeErrCh` non-blocking receive before `wsConnection.Close()` in readPump's defer. If writePump already failed, its error is on the channel; if not, the channel is empty. Close() after reading prevents spurious override.

**Impact**: Low — race window is extremely small (1/1000 under race detector). The overridden error is still non-nil (net.ErrClosed), so reconnect logic is unaffected. Only diagnostics differ.

**Blocked by**: No reliable deterministic test. The race triggers ~0.1% of iterations under `-race -count=1000`. A fix requires either test infrastructure changes (Close hook to force synchronization) or accepting a non-deterministic test.

**PR comment**: https://github.com/wspulse/client-go/pull/32#discussion_r3045942733
