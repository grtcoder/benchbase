#!/bin/bash
###############
# Send the binaries to all the servers and brokers
###############

source ./env-vars.sh

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="at6404@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        wget https://github.com/grafana/loki/releases/latest/download/promtail-linux-amd64.zip
        unzip promtail-linux-amd64.zip
        chmod +x promtail-linux-amd64
        sudo mv promtail-linux-amd64 /usr/local/bin/promtail
        rm promtail-linux-amd64*
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="at6404@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        wget https://github.com/grafana/loki/releases/latest/download/promtail-linux-amd64.zip
        unzip promtail-linux-amd64.zip
        chmod +x promtail-linux-amd64
        sudo mv promtail-linux-amd64 /usr/local/bin/promtail
        rm promtail-linux-amd64*
        exit
EOF
done

server_url="at6404@monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
 # Commands to execute on the broker
cd /users/at6404
curl -O -L "https://github.com/grafana/loki/releases/latest/download/loki-linux-amd64.zip"
unzip loki-linux-amd64.zip
chmod +x loki-linux-amd64
sudo mv loki-linux-amd64 /usr/local/bin/loki
rm loki-linux-amd64*
exit
EOF