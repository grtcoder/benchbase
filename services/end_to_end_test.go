package main

import (
	"flag"
	"lockfreemachine/services/broker"
	"lockfreemachine/services/directory"
	"lockfreemachine/services/server"
	"lockfreemachine/services/storage_reader"

	"os"
	"testing"
	"fmt"
	"time"
)

func TestServer(t *testing.T) {
	startTimestamp := time.Now().Add(5*time.Second).UnixNano()
	t.Log("Start Timestamp in int64:", startTimestamp)
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{
		"cmd",
		// "-directoryIP=127.0.0.1",
		// "-directoryPort=8080",
		fmt.Sprintf("-startTimestamp=%d",startTimestamp),
		"-logFile=./logs/test_directory.log",
	}
	go directory.Main()
	time.Sleep(1 * time.Second) // Give directory time to start

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{
		"cmd",
		"-directoryIP=127.0.0.1",
		"-directoryPort=8080",
		"-brokerIP=127.0.0.1",
		"-brokerPort=9080",
		"-logFile=./logs/test_broker_1.log",
	}
	go broker.Main()
	time.Sleep(1 * time.Second) // Give directory time to start

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{
		"cmd",
		"-directoryIP=127.0.0.1",
		"-directoryPort=8080",
		"-brokerIP=127.0.0.1",
		"-brokerPort=9090",
		"-logFile=./logs/test_broker_2.log",
	}
	go broker.Main()
	time.Sleep(1 * time.Second) // Give directory time to start

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{
		"cmd",
		"-directoryIP=127.0.0.1",
		"-directoryPort=8080",
		"-serverIP=127.0.0.1",
		"-readerPort=9091",
		fmt.Sprintf("-startTimestamp=%d",startTimestamp),
		"-logFile=./logs/test_storage_reader.log",
	}
	go storage_reader.Main()
	time.Sleep(1 * time.Second) // Let the services run for a while

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = []string{
		"cmd",
		"-directoryIP=127.0.0.1",
		"-directoryPort=8080",
		"-serverIP=127.0.0.1",
		"-serverPort=9092",
		"-readerPort=9091",
		"-logFile=./logs/test_server.log",
	}
	go server.Main()
	time.Sleep(20 * time.Second) // Let the services run for a while
}