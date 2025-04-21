#!/bin/bash
# cleanup the processes.

rm broker
rm directory
rm server

read -p "Enter number of servers: " numServer
read -p "Enter number of brokers: " numBroker

for ((i=1; i<=numBroker; i++)); do
    broker_url="dmm6096@broker${i}.alpha-test.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        sudo pkill -f broker
        exit
EOF
done

for ((i=1; i<=numServer; i++)); do
    server_url="dmm6096@server${i}.alpha-test.l-free-machine.emulab.net"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        sudo pkill -f server
        exit
EOF
done

directory_url="dmm6096@directory.alpha-test.l-free-machine.emulab.net"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/dmm6096
        sudo pkill -f directory
        exit
EOF