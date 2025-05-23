#!/bin/bash
###############
# Send the binaries to all the servers and brokers
###############

GOOS=linux GOARCH=amd64 go build -o broker.new ../services/broker/
mv broker.new broker

GOOS=linux GOARCH=amd64 go build -o directory.new ../services/directory/
mv directory.new directory

GOOS=linux GOARCH=amd64 go build -o server.new ../services/server/
mv server.new server

read -p "Enter number of servers: " numServer
read -p "Enter number of brokers: " numBroker
read -p "Enter experiment name: " experimentName

# Setup monitoring


for ((i=1; i<=numBroker; i++)); do
    echo "Sending broker to broker${i}"
    url="dmm6096@broker${i}.${experimentName}.l-free-machine.emulab.net:/users/dmm6096"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null broker ./configs/promtail-config.yml ${url} &
done

for ((i=1; i<=numServer; i++)); do
    echo "Sending server to server${i}"
    url="dmm6096@server${i}.${experimentName}.l-free-machine.emulab.net:/users/dmm6096"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null server ./configs/promtail-config.yml ${url} &
done

url="dmm6096@directory.${experimentName}.l-free-machine.emulab.net:/users/dmm6096"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null directory ./configs/promtail-config.yml ${url} &

url="dmm6096@monitoring.${experimentName}.l-free-machine.emulab.net:/users/dmm6096"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ./configs/loki-config.yml ${url} &

wait
echo "All binary transfers are complete."

directory_url="dmm6096@directory.${experimentName}.l-free-machine.emulab.net"

# Start the directory first
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        ./directory > output.log 2>&1 &
        disown
        exit
EOF

monitoring_url="dmm6096@monitoring.${experimentName}.l-free-machine.emulab.net"
# Start the directory first
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        nohup loki -config.file=loki-config.yml > /dev/null 2>&1 &
        disown
        exit
EOF

read -p "After how long do you want to start ( in seconds )?: " startTime
# Calculate 1 minute later

currTimestamp=$(python3 -c "import time; print(time.time_ns())")
scheduleTimestamp=$((currTimestamp + ${startTime}*1000000000))
echo "Schedule Timestamp: $scheduleTimestamp, current Timestamp: $currTimestamp"

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="dmm6096@broker${i}.${experimentName}.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        nohup promtail --config.file=promtail-config.yml > /dev/null 2>&1 &
        nohup ./broker -directoryIP=${directory_url:8} -directoryPort 8080 -brokerIP=${broker_url:8} -brokerPort 8083 -startTimestamp ${scheduleTimestamp} -logFile ./logs/broker${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="dmm6096@server${i}.${experimentName}.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        nohup promtail --config.file=promtail-config.yml > /dev/null 2>&1 &
        nohup ./server -directoryIP=${directory_url:8} -directoryPort 8080 -serverIP=${server_url:8} -serverPort 8083 -startTimestamp ${scheduleTimestamp} -logFile ./logs/server${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done