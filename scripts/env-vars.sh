#!/bin/bash

numServer=2
numBroker=2
experimentName="anand-test"
clusterType="emulab"
projectName="l-free-machine"
dropRate=0

if [ "$clusterType" == "emulab" ]; then
    export suffix="net"
else
    export suffix="us"
fi
