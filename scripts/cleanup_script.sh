#!/bin/bash
# cleanup the processes.

rm broker
rm directory
rm server

source ./env-vars.sh

for ((i=1; i<=numBroker; i++)); do
    broker_url="at6404@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    echo "Cleaning up broker${i} at ${broker_url}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        rm -rf logs
        sudo pkill -f broker
        sudo pkill -f promtail
        rm -f broker *.yml
        exit
EOF
    if [ $? -eq 0 ]; then
        echo "SSH command succeeded"
    else
        echo "SSH command failed"
    fi
done

for ((i=1; i<=numServer; i++)); do
    server_url="at6404@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    echo "Cleaning up server${i} at ${server_url}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        sudo pkill -f server
        rm -f server output.log *.json *.yml
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

directory_url="at6404@directory.${experimentName}.${projectName}.${clusterType}.${suffix}"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd /users/at6404
        sudo pkill -f directory
        rm -rf logs
        sudo pkill -f promtail
        sudo rm -f directory output.log *.yml
        exit
EOF
    if [ $? -eq 0 ]; then
        echo "SSH command succeeded"
    else
        echo "SSH command failed"
    fi