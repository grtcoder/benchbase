#!/bin/bash
###############
# Send the binaries to all the servers and brokers
###############

source ./env-vars.sh
REMOTE_HOME="/users/${cloudLabUserName}"

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="${cloudLabUserName}@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        wget https://github.com/grafana/loki/releases/latest/download/promtail-linux-amd64.zip
        unzip promtail-linux-amd64.zip
        chmod +x promtail-linux-amd64
        sudo mv promtail-linux-amd64 /usr/local/bin/promtail
        rm promtail-linux-amd64*
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="${cloudLabUserName}@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        wget https://github.com/grafana/loki/releases/latest/download/promtail-linux-amd64.zip
        unzip promtail-linux-amd64.zip
        chmod +x promtail-linux-amd64
        sudo mv promtail-linux-amd64 /usr/local/bin/promtail
        rm promtail-linux-amd64*
        exit
EOF
done


###############
# Generate promtail-config.yml locally
###############
cat > configs/promtail-config.yml <<EOF
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}:3100/loki/api/v1/push

scrape_configs:
  - job_name: l-free-machine
    static_configs:
      - targets:
          - localhost
        labels:
          job: l-free-machine
          app: l-free-machine
          __path__: ./logs/*.log
EOF

echo "Generated promtail-config.yml"

###############
# Generate prometheus.yml locally
###############
cat > configs/prometheus.yml <<EOF
global:
  scrape_interval: 15s
  scrape_timeout: 10s

scrape_configs:
  - job_name: "l-free-machine"
    metrics_path: /metrics
    static_configs:
      - targets:
EOF

# brokers on port 8083
METRICS_PORT=8083
for ((i=1; i<=numBroker; i++)); do
  host="broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}:$METRICS_PORT"
  printf '          - "%s"\n' "$host" >> configs/prometheus.yml
done

cat >> configs/prometheus.yml <<EOF
        labels:
          role: broker

      - targets:
EOF

# servers on port 8083
SERVER_METRICS_PORT=8083
for ((i=1; i<=numServer; i++)); do
  host="server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}:$SERVER_METRICS_PORT"
  printf '          - "%s"\n' "$host" >> configs/prometheus.yml
done

cat >> configs/prometheus.yml <<EOF
        labels:
          role: server
EOF
echo "Generated prometheus.yml"

###############
# Push prometheus.yml up and start Prometheus
###############


PROM_VER="3.5.0"
server_url="${cloudLabUserName}@monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
 # Commands to execute on the broker
cd ${REMOTE_HOME}
curl -O -L "https://github.com/grafana/loki/releases/latest/download/loki-linux-amd64.zip"
unzip loki-linux-amd64.zip
chmod +x loki-linux-amd64
sudo mv loki-linux-amd64 /usr/local/bin/loki
rm loki-linux-amd64*
curl -O -L "https://github.com/prometheus/prometheus/releases/download/v${PROM_VER}/prometheus-${PROM_VER}.linux-amd64.tar.gz"
tar xzf prometheus-${PROM_VER}.linux-amd64.tar.gz
chmod +x prometheus-${PROM_VER}.linux-amd64/prometheus
chmod +x prometheus-${PROM_VER}.linux-amd64/promtool
sudo mv prometheus-${PROM_VER}.linux-amd64/prometheus /usr/local/bin/prometheus
sudo mv prometheus-${PROM_VER}.linux-amd64/promtool  /usr/local/bin/promtool
rm -rf prometheus-${PROM_VER}.linux-amd64.tar.gz prometheus-${PROM_VER}.linux-amd64 
exit
EOF