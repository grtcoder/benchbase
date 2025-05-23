#!/bin/bash
# cleanup the processes.

rm broker
rm directory
rm server

read -p "Enter number of servers: " numServer
read -p "Enter number of brokers: " numBroker
read -p "Enter experiment name: " experimentName

for ((i=1; i<=numBroker; i++)); do
    echo "Sending broker to broker${i}"
    broker_url="dmm6096@broker${i}.${experimentName}.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        rm -rf logs
        sudo pkill -f broker
        sudo pkill -f promtail
        rm -f broker
        exit
EOF
    if [ $? -eq 0 ]; then
        echo "SSH command succeeded"
    else
        echo "SSH command failed"
    fi
done

for ((i=1; i<=numServer; i++)); do
    echo "Sending server to server${i}"
    server_url="dmm6096@server${i}.${experimentName}.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        sudo pkill -f server
        rm -f server output.log *.json
        rm -rf logs
        rm -rf packages*
        sudo pkill -f promtail
        exit
EOF
    if [ $? -eq 0 ]; then
        echo "SSH command succeeded"
    else
        echo "SSH command failed"
    fi
done

directory_url="dmm6096@directory.${experimentName}.l-free-machine.emulab.net"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        sudo pkill -f directory
        rm -rf logs
        sudo pkill -f promtail
        sudo rm -f directory output.log
        exit
EOF
    if [ $? -eq 0 ]; then
        echo "SSH command succeeded"
    else
        echo "SSH command failed"
    fi