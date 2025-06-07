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

source ./env-vars.sh

# Setup monitoring
for ((i=1; i<=numBroker; i++)); do
    echo "Sending broker to broker${i}"
    url="at6404@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}:/users/at6404"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null broker ./configs/promtail-config.yml ${url} &
done

# Setup servers
for ((i=1; i<=numServer; i++)); do
    echo "Sending server to server${i}"
    url="at6404@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}:/users/at6404"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null server ./configs/promtail-config.yml ${url} &
done

url="at6404@directory.${experimentName}.${projectName}.${clusterType}.${suffix}:/users/at6404"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null directory ./configs/promtail-config.yml ${url} &

url="at6404@monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}:/users/at6404"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ./configs/loki-config.yml ${url} &

wait
echo "All binary transfers are complete."


sleep 1m


directory_url="at6404@directory.${experimentName}.${projectName}.${clusterType}.${suffix}"

# Start the directory first
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        chmod +x directory
        ./directory > output.log 2>&1 &
        disown
        exit
EOF

monitoring_url="at6404@monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}"
# Start the directory first
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${monitoring_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        nohup /usr/local/bin/loki -config.file=loki-config.yml > /dev/null 2>&1 &
        disown
        exit
EOF

read -p "After how long do you want to start ( in seconds )?: " startTime

# Calculate the schedule timestamp based on the current time and user input
currTimestamp=$(python3 -c "import time; print(time.time_ns())")
scheduleTimestamp=$((currTimestamp + ${startTime}*1000000000))
echo "Schedule Timestamp: $scheduleTimestamp, current Timestamp: $currTimestamp"

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="at6404@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        chmod +x broker
        nohup promtail --config.file=promtail-config.yml > /dev/null 2>&1 &
        nohup ./broker -directoryIP=${directory_url:7} -directoryPort 8080 -brokerIP=${broker_url:7} -brokerPort 8083 -startTimestamp ${scheduleTimestamp} -logFile ./logs/broker${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="at6404@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        chmod +x server
        nohup promtail --config.file=promtail-config.yml > /dev/null 2>&1 &
        nohup ./server -directoryIP=${directory_url:7} -directoryPort 8080 -serverIP=${server_url:7} -serverPort 8083 -startTimestamp ${scheduleTimestamp} -logFile ./logs/server${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done