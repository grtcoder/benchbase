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
sudo ufw reset             # (if you want a clean slate)
sudo ufw default allow incoming
sudo ufw default allow outgoing
sudo ufw deny from 155.98.38.121
sudo ufw enable
```



## How to run scripts

First run setup_monitoring.sh
Then run_experiment.sh


loki -config.file=loki-config.yaml

