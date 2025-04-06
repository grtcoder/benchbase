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
	"github.com/avast/retry-go"
)

const (
	DISPATCH_PERIOD = 5000
	NUM_SERVERS     = 5
	BUFFER_SIZE = 10000
	RETRY_TIME=100*time.Millisecond

)

type Broker struct {
	ID             int
	IP             string
	Port           int
	TransactionQueue   *queue.RingBuffer
	DirectoryAddr  string
	DirectoryInfo  *commons.NodesMap
	httpServer         *http.Server
}

func NewBroker(id int, ip string, port int, directoryIP string, directoryPort int) (*Broker, error) {
	transactionQueue := queue.NewRingBuffer(BUFFER_SIZE)
	directoryAddr := fmt.Sprintf("http://%s:%d", directoryIP, directoryPort)
	directoryInfo := &commons.NodesMap{
		ServerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
		BrokerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
	}

	broker := &Broker{
		ID:            id,
		IP:            ip,
		Port:          port,
		TransactionQueue:  transactionQueue,
		DirectoryAddr: directoryAddr,
		DirectoryInfo: directoryInfo,
	}

	if err := broker.register(); err != nil {
		return nil, fmt.Errorf("error registering broker: %v", err)
	}

	if err := commons.BroadcastNodesInfo(broker.ID,commons.BrokerType,broker.DirectoryInfo); err != nil {
		return nil, fmt.Errorf("error broadcasting nodes info: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc(commons.BROKER_UPDATE_DIRECTORY, commons.HandleUpdateDirectory(broker.DirectoryInfo)).Methods("POST")
	r.HandleFunc(commons.BROKER_TRANSACTION, broker.handleTransactionRequest).Methods("POST")

	broker.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", broker.Port),
		Handler: r,
	}

	return broker, nil
}

func (b *Broker) register() error {
	data := map[string]interface{}{
		"ip":   b.IP,
		"port": b.Port,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s%s", b.DirectoryAddr,commons.DIRECTORY_REGISTER_BROKER), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error sending request to directory: %v, could not start broker", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))

	if err := json.Unmarshal(body, &b.DirectoryInfo); err != nil {
		return fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	log.Printf("directoryInfo: %#v\n", b.DirectoryInfo.BrokerMap)
	b.ID = b.DirectoryInfo.BrokerMap.Version

	log.Printf("Setup complete. Broker registered with ID: %d", b.ID)
	return nil
}

func (b *Broker) handleTransactionRequest(w http.ResponseWriter, r *http.Request) {
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

	// Example: Print the data
	log.Println("Received data:", transaction)
	b.TransactionQueue.Put(transaction)
	w.WriteHeader(http.StatusOK)
}

func (b *Broker) sendPackage(serverURL string,jsonData []byte) func() error {
	return func() error {
		resp, err := http.Post(serverURL, "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("error sending request to %s: %s", serverURL, err)
		}

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d for server %s", resp.StatusCode, serverURL)
		}
		return nil
	}
}

func (b *Broker) protocolDaemon() {
	packageCounter := 0

	for {
		log.Printf("Daemon routine is running...")
		time.Sleep(DISPATCH_PERIOD * time.Millisecond)
		packageCounter++
		pkg := b.CreatePackage(packageCounter)

		log.Printf("package of size %d created\n", len(pkg.Transactions))

		jsonData, err := json.Marshal(pkg)
		if err != nil {
			log.Printf("Error marshalling package: %s", err)
			continue
		}

		var wg sync.WaitGroup
		for serverID, node := range b.DirectoryInfo.ServerMap.Data {
			wg.Add(1)
			go func(serverID int, node *commons.NodeInfo) {
				defer wg.Done()

				if err != nil {
					log.Printf("Server: %d, Error marshalling node info: %s", serverID, err)
					return
				}
				retry.Do(
					b.sendPackage(fmt.Sprintf("http://%s:%d%s", node.IP, node.Port,commons.SERVER_ADD_PACKAGE),jsonData),
					retry.Attempts(5),               // Number of retry attempts
					retry.Delay(RETRY_TIME),      // Delay between retries
					retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
					retry.OnRetry(func(n uint, err error) {
						log.Printf("Retry attempt %d: %v\n", n+1, err)
					}),
				)
			}(serverID, node)
		}

		wg.Wait()
	}
}

func (b *Broker) CreatePackage(packageCounter int) *commons.Package {
	var transactions []*commons.Transaction
	packageSize := b.TransactionQueue.Len()

	for packageSize > 0 {
		if b.TransactionQueue.Len() == 0 {
			log.Printf("Transaction array is empty")
			break
		}

		packageSize--
		transaction, err := b.TransactionQueue.Poll(2 * time.Second)
		if err != nil {
			log.Printf("Error getting transaction from queue: %s", err)
			continue
		}

		transaction, ok := transaction.(*commons.Transaction)
		if !ok {
			log.Printf("Error type assertion for transaction failed %T failed", transaction)
			continue
		}

		transactions = append(transactions, transaction.(*commons.Transaction))
	}

	pkg := &commons.Package{
		BrokerID:       b.ID,
		Transactions:   transactions,
	}

	packageCounter++
	return pkg
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

	broker, err := NewBroker(0, brokerIP, brokerPort, directoryIP, directoryPort)
	if err != nil {
		log.Fatalf("Error setting up broker: %v", err)
	}

	// Graceful shutdown handling
	go func() {
		if err := broker.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Println("broker Error:", err)
		}
	}()
	fmt.Println("broker running on http://localhost:8080")

	go func() {
		broker.protocolDaemon()
		defer fmt.Println("Daemon routine stopped.")
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	fmt.Println("\nShutting down broker...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	broker.httpServer.Shutdown(ctx)
	fmt.Println("broker stopped.")
}