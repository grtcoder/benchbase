#!/bin/bash
# cleanup the processes.

rm broker
rm directory
rm server

source ./env-vars.sh
REMOTE_HOME="/users/${cloudLabUserName}"

for ((i=1; i<=numBroker; i++)); do
    broker_url="${cloudLabUserName}@broker${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    echo "Cleaning up broker${i} at ${broker_url}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${broker_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
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
    server_url="${cloudLabUserName}@server${i}.${experimentName}.${projectName}.${clusterType}.${suffix}"
    echo "Cleaning up server${i} at ${server_url}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${server_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
        sudo pkill -f server
        sudo pkill -f storage_reader
        rm -f server output.log *.json *.yml
        rm -f storage_reader output.log *.json *.yml
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

directory_url="${cloudLabUserName}@directory.${experimentName}.${projectName}.${clusterType}.${suffix}"
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ${directory_url} << EOF
        # Commands to execute on the broker
        cd ${REMOTE_HOME}
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