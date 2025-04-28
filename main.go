package main

import (
	"io"
	"lockfreemachine/pkg/commons"
	"log"
	"net/http"
	"time"
	"encoding/json"
)

func main() {
	url := "http://server1.alpha-test.l-free-machine.emulab.net:8083/requestPackage?brokerID=1"

	client := &http.Client{
		Timeout: time.Second,
	}
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("error sending request to get package: %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		// Read the response body
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("error reading response body: %s", err)
			return
		}

		// Create a new package instance
		pkg := &commons.Package{}
		// Decode the JSON response
		if err := json.Unmarshal(body, pkg); err != nil {
			log.Printf("error decoding JSON response: %s", err)
			return
		}

		// Process the package
		log.Printf("Received package: %#v\n", pkg)
		return
	} else {
		log.Printf("unexpected status code: %d", resp.StatusCode)
		return
	}
}
