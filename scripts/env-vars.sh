#!/bin/bash

numServer=2
numBroker=2
experimentName="alpha-test"
clusterType="emulab"
projectName="l-free-machine"

if [ "$clusterType" == "emulab" ]; then
    export suffix="net"
else
    export suffix="us"
fi
