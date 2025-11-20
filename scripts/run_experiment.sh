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

GOOS=linux GOARCH=amd64 go build -o storage_reader.new ../services/storage_reader/
mv storage_reader.new storage_reader

source ./env-vars.sh
REMOTE_HOME="/users/${cloudLabUserName}"

# Setup monitoring
for ((i=1; i<=numBroker; i++)); do
    echo "Sending broker to broker${i}"
    url="${cloudLabUserName}@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}:${REMOTE_HOME}"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null broker ./configs/promtail-config.yml ${url} &
done

# Setup servers
for ((i=1; i<=numServer; i++)); do
    echo "Sending server to server${i}"
    url="${cloudLabUserName}@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}:${REMOTE_HOME}"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null server storage_reader ./configs/promtail-config.yml ${url} &
done

url="${cloudLabUserName}@directory.${experimentName}.${projectName}.${clusterType}.${suffix}:${REMOTE_HOME}"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null directory ./configs/promtail-config.yml ${url} &

url="${cloudLabUserName}@monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}:${REMOTE_HOME}"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ./configs/loki-config.yml ./configs/prometheus.yml ${url} &

wait
echo "All binary transfers are complete."


sleep 1m


read -p "After how long do you want to start ( in seconds )?: " startTime

# Calculate the schedule timestamp based on the current time and user input
currTimestamp=$(python3 -c "import time; print(time.time_ns())")
scheduleTimestamp=$((currTimestamp + ${startTime}*1000000000))
echo "Schedule Timestamp: $scheduleTimestamp, current Timestamp: $currTimestamp"

directory_url="${cloudLabUserName}@directory.${experimentName}.${projectName}.${clusterType}.${suffix}"

# Start the directory first
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        chmod +x directory
        ./directory -startTimestamp ${scheduleTimestamp} > output.log 2>&1 &
        disown
        exit
EOF

monitoring_url="${cloudLabUserName}@monitoring.${experimentName}.${projectName}.${clusterType}.${suffix}"
# Start the directory first
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${monitoring_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        mkdir -p prometheus-data
        nohup /usr/local/bin/loki -config.file=loki-config.yml > /dev/null 2>&1 &
        nohup /usr/local/bin/prometheus --config.file=prometheus.yml --storage.tsdb.path=./prometheus-data > prometheus.log 2>&1 &
        disown -a
        exit
EOF

sleep 10

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="${cloudLabUserName}@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        chmod +x broker
        nohup promtail --config.file=promtail-config.yml > /dev/null 2>&1 &
        nohup ./broker -directoryIP=${directory_url#*@} -directoryPort 8080 -brokerIP=${broker_url#*@} -brokerPort 8083 -dropRate ${dropRate} -logFile ./logs/broker${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done


for ((i=1; i<=numServer; i++)); do
    server_url="${cloudLabUserName}@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        chmod +x storage_reader
        nohup ./storage_reader -directoryIP=${directory_url#*@} -directoryPort 8080 -serverIP=${server_url#*@} -readerPort 9085 -startTimestamp ${scheduleTimestamp} -logFile ./logs/storage_reader${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="${cloudLabUserName}@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        trap '' HUP
        cd ${REMOTE_HOME}
        ulimit -n 65535
        chmod +x server
        nohup promtail --config.file=promtail-config.yml > /dev/null 2>&1 &
        setsid nohup ./server -directoryIP=${directory_url#*@} -directoryPort 8080 -serverIP=${server_url#*@} -serverPort 8083 -readerPort 9085 -dropRate ${dropRate} -logFile ./logs/server${i}.log > /dev/null 2>&1 &
        disown
        exit
EOF
done