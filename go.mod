module github.com/wspulse/client-go

go 1.26.0

require (
	github.com/gorilla/websocket v1.5.3
	github.com/wspulse/core v0.2.1-0.20260402095235-9a8819142bfd // TODO: replace with tagged release after core v0.3.0
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.1
)

require go.uber.org/multierr v1.10.0 // indirect
