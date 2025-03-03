# lock-free-machine
A lock free replicated state machine

For local testing, use the following commands

## Broker
```bash
go run services/broker/main.go -directoryIP=localhost -directoryPort 8080 -brokerIP=localhost -brokerPort 8081
```

## Server
```bash
go run services/server/main.go -directoryIP=localhost -directoryPort 8081 -serverIP=localhost -serverPort 8082
```

## Directory
```bash
go run services/directory/main.go
```