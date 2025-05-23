#!/bin/bash
###############
# Send the binaries to all the servers and brokers
###############

read -p "Enter number of servers: " numServer
read -p "Enter number of brokers: " numBroker
read -p "Enter experiment name: " experimentName

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="dmm6096@broker${i}.${experimentName}.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        wget https://github.com/grafana/loki/releases/latest/download/promtail-linux-amd64.zip
        unzip promtail-linux-amd64.zip
        chmod +x promtail-linux-amd64
        sudo mv promtail-linux-amd64 /usr/local/bin/promtail
        rm promtail-linux-amd64*
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="dmm6096@server${i}.${experimentName}.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        wget https://github.com/grafana/loki/releases/latest/download/promtail-linux-amd64.zip
        unzip promtail-linux-amd64.zip
        chmod +x promtail-linux-amd64
        sudo mv promtail-linux-amd64 /usr/local/bin/promtail
        rm promtail-linux-amd64*
        exit
EOF
done

server_url="dmm6096@monitoring.${experimentName}.l-free-machine.emulab.net"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
 # Commands to execute on the broker
cd /users/dmm6096
curl -O -L "https://github.com/grafana/loki/releases/latest/download/loki-linux-amd64.zip"
unzip loki-linux-amd64.zip
chmod +x loki-linux-amd64
sudo mv loki-linux-amd64 /usr/local/bin/loki
rm loki-linux-amd64*
exit
EOF