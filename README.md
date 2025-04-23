# lock-free-machine
A lock free replicated state machine

For local testing, use the following commands

## Broker
```bash
go run services/broker/main.go -directoryIP=localhost -directoryPort 8080 -brokerIP=localhost -brokerPort 8081
```

## Server
```bash
go run services/server/main.go -directoryIP=localhost -directoryPort 8080 -serverIP=localhost -serverPort 8082
```

## Directory
```bash
go run services/directory/main.go
```


## Test case commands

```bash
sudo ufw deny from <IP>
```

## Commands to block incoming packets via ufw

```bash
sudo ufw deny from <IP>
sudo ufw allow from any
sudo ufw enable
```

