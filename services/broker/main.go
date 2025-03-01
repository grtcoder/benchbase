package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"lockfreemachine/pkg/commons"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/gorilla/mux"
)

const (
	DISPATCH_PERIOD = 1000
	NUM_SERVERS     = 5
	BUFFER_SIZE = 10000
)


func CreatePackage(brokerID,packageCounter int,lastTransaction *commons.Transaction,transactionArray *queue.RingBuffer) *commons.Package {
	var pkg []*commons.Transaction

	for transactionArray.Len()>0 {
		transaction,err := transactionArray.Poll(2*time.Second)
		if err!=nil {
			log.Printf("Error getting transaction from queue: %s",err)
			continue
		}
		transaction,ok := transaction.(*commons.Transaction)
		if !ok {
			log.Printf("Error type assertion for transaction failed %T failed",transaction)
			continue
		}
		pkg=append(pkg,transaction.(*commons.Transaction))
	}
	return &commons.Package{
		BrokerID:brokerID,
		PackageCounter:packageCounter,
		Transactions:pkg,
	}
}

func handleTransactionRequest(q *queue.RingBuffer) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		// Read data from request
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// Process the data
		transaction := &commons.Transaction{}
		json.Unmarshal(body, &transaction)
		// ...

		// Example: Print the data
		log.Println("Received data:", transaction)
		q.Put(transaction)
		w.WriteHeader(http.StatusOK)
	}	
}

func handleDirectoryMessage(w http.ResponseWriter, r *http.Request) {
	// Read data from request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Process the data
	transaction := &commons.InfoMap{}
	if err:=json.Unmarshal(body, &transaction); err!=nil {
		log.Printf("Error unmarshalling JSON: %s",err)
		http.Error(w, "Error unmarshalling JSON", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func setupBroker(directoryAddr,brokerIP string,brokerPort int,lfreeQueue *queue.RingBuffer) (*http.Server,error) {
	r := mux.NewRouter()
	r.HandleFunc("/handleDirectoryPackage", handleDirectoryMessage).Methods("POST")
	r.HandleFunc("/handleTransaction", handleTransactionRequest(lfreeQueue)).Methods("POST")

	// Create JSON payload
	data:=map[string]interface{}{
		"ip":brokerIP,
		"port":brokerPort,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil,fmt.Errorf("error encoding JSON: %v", err)
	}

	resp,err := http.Post(fmt.Sprintf("%s/registerbroker",directoryAddr), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil,fmt.Errorf("error sending request to directory: %v, could not start broker", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil,fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	// Read response
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))

	directoryInfo := &commons.DirectoryMessage{}
	if err:=json.Unmarshal(body, &directoryInfo); err!=nil {
		return nil,fmt.Errorf("error unmarshalling JSON: %v", err)
	}
	

	// Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", brokerPort),
		Handler: r,
	},nil
}

func protocolDaemon(brokerID int,transactionArray *queue.RingBuffer) {
	packageCounter := 0
	lastTransaction := &commons.Transaction{}

	for {
		time.Sleep(DISPATCH_PERIOD * time.Millisecond)
		packageCounter++
		pkg := CreatePackage(brokerID, packageCounter, lastTransaction, transactionArray)
		var wg sync.WaitGroup
		for i := 0; i < NUM_SERVERS; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				http.Post()
			}(i)
		}
		wg.Wait()
	}
}


func main() {
	// Parse command line arguments
	var directoryIP string
	var directoryPort int

	flag.StringVar(&directoryIP, "directoryIP", "", "IP of directory")
	flag.IntVar(&directoryPort, "directoryPort", 0, "Port of directory")

	var brokerIP string
	var brokerPort int

	flag.StringVar(&brokerIP, "brokerIP", "", "IP of current broker")
	flag.IntVar(&brokerPort, "brokerPort", 0, "Port of broker")

	flag.Parse()

	if directoryIP == "" {
		log.Fatalf("directoryIP is required")
	}
	if directoryPort == 0 {
		log.Fatalf("directoryPort is required")
	}
	if brokerIP == "" {
		log.Fatalf("brokerIP is required")
	}
	if brokerPort == 0 {
		log.Fatalf("brokerPort is required")
	}

	// Create directory address to send initial setup request.
	directoryAddr := fmt.Sprintf("http://%s:%d", directoryIP, directoryPort)

	// Create a lock-free queue to store the incoming transactions
	transactionArray := queue.NewRingBuffer(BUFFER_SIZE)

	server,err := setupBroker(directoryAddr,brokerIP,brokerPort,transactionArray)
	if err != nil {
		log.Fatalf("Error setting up broker: %v", err)
	}


	// Graceful shutdown handling
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Println("broker Error:", err)
		}
	}()
	fmt.Println("broker running on http://localhost:8080")

	go func() {
		protocolDaemon(transactionArray)
		defer fmt.Println("Daemon routine stopped.")
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	go func() {
		// setup goroutine
		// Send HTTP request to directory to get
		req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:%d",directoryIP,directoryPort), nil)
		if err != nil {
			fmt.Println("Error creating request:", err)
			return
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("Error sending request:", err)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Error reading response body:", err)
			return
		}

		fmt.Println("Response:", string(body))
	}()
	fmt.Println("\nShutting down broker...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
	fmt.Println("broker stopped.")
}