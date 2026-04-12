# TODOS

Items deferred from code review. Each entry links to the original comment.

---

- **Propagate pong timeout root cause to onTransportDrop** — currently onTransportDrop receives `net.ErrClosed` (from readPump) instead of the original `context.DeadlineExceeded` (from pingPump). Requires a cross-pump error channel. Low priority: behaviour contract does not mandate root-cause fidelity in onTransportDrop.
  - Source: [PR #43 comment](https://github.com/wspulse/client-go/pull/43#discussion_r3069131109)
