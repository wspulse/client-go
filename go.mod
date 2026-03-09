module github.com/wspulse/client-go

go 1.26.0

replace github.com/wspulse/server => ../server

replace github.com/wspulse/core => ../core

require (
	github.com/gorilla/websocket v1.5.3
	github.com/wspulse/server v0.0.0-00010101000000-000000000000
	go.uber.org/goleak v1.3.0
	go.uber.org/zap v1.27.1
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/wspulse/core v0.0.0-00010101000000-000000000000 // indirect
	go.uber.org/multierr v1.10.0 // indirect
)
