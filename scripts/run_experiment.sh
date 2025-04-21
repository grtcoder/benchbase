#!/bin/bash
###############
# Send the binaries to all the servers and brokers
###############

GOOS=linux GOARCH=amd64 go build -o broker ../services/broker/
GOOS=linux GOARCH=amd64 go build -o directory ../services/directory/
GOOS=linux GOARCH=amd64 go build -o server ../services/server/

read -p "Enter number of servers: " numServer
read -p "Enter number of brokers: " numBroker

for ((i=1; i<=numBroker; i++)); do
    url="dmm6096@broker${i}.alpha-test.l-free-machine.emulab.net:/users/dmm6096"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null broker ${url} &
done

for ((i=1; i<=numServer; i++)); do
    url="dmm6096@server${i}.alpha-test.l-free-machine.emulab.net:/users/dmm6096"
    scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null server ${url} &
done

url="dmm6096@directory.alpha-test.l-free-machine.emulab.net:/users/dmm6096"
scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null directory ${url} &

wait
echo "All binary transfers are complete."



read -p "After how long do you want to start ( in seconds )?: " startTime
# Calculate 1 minute later

currTimestamp=$(python3 -c "import time; print(time.time_ns())")
scheduleTimestamp=$((currTimestamp + ${startTime}*1000000000))
echo "Schedule Timestamp: $scheduleTimestamp, current Timestamp: $currTimestamp"
directory_url="dmm6096@directory.alpha-test.l-free-machine.emulab.net"

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
    broker_url="dmm6096@broker${i}.alpha-test.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        ./broker -directoryIP=${directory_url} -directoryPort 8080 -brokerIP=${broker_url} -brokerPort 8083 -startTimestamp ${scheduleTimestamp} > output.log 2>&1 &
        disown
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="dmm6096@server${i}.alpha-test.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        ./server -directoryIP=${directory_url} -directoryPort 8080 -serverIP=${server_url} -serverPort 8083 -startTimestamp ${scheduleTimestamp} > output.log 2>&1 &
        disown
        exit
EOF
done

ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        ./directory > output.log 2>&1 &
        disown
        exit
EOF
