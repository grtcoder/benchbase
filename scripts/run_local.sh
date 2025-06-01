#!/bin/bash

killall broker
killall server
killall directory

rm broker
rm directory
rm server

go build -o broker.new ../services/broker/
mv broker.new broker

go build -o directory.new ../services/directory/
mv directory.new directory

go build -o server.new ../services/server/
mv server.new server

source ./env-vars.sh
read -p "After how long do you want to start ( in seconds )?: " startTime
# Calculate 1 minute later

currTimestamp=$(python3 -c "import time; print(time.time_ns())")
scheduleTimestamp=$((currTimestamp + ${startTime}*1000000000))
echo "Schedule Timestamp: $scheduleTimestamp, current Timestamp: $currTimestamp"

chmod +x directory
./directory > output.log 2>&1 &

sleep 2

for ((i=1; i<=numServer; i++)); do
        # Commands to execute on the broker
        chmod +x server
        ./server -directoryIP=localhost -directoryPort 8080 -serverIP=localhost -serverPort $((9080+${i})) -startTimestamp ${scheduleTimestamp} -logFile ./logs/server${i}.log &
done

# Loop over brokers and send multi-line SSH commands
for ((i=1; i<=numBroker; i++)); do
        # Commands to execute on the broker
        chmod +x broker
        ./broker -test -directoryIP=localhost -directoryPort 8080 -brokerIP=localhost -brokerPort $((8080+${i})) -startTimestamp ${scheduleTimestamp} -logFile ./logs/broker${i}.log &
done

disown

