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
	DISPATCH_PERIOD = 5000
	NUM_SERVERS     = 5
	BUFFER_SIZE = 10000
)


func CreatePackage(brokerID,packageCounter int,transactionArray *queue.RingBuffer) *commons.Package {
	var transactions []*commons.Transaction

	// Whatever is the value of the transactionArray at this instant is the current package size. All packages added after this will be added to the next package 
	packageSize := transactionArray.Len()
	for packageSize>0 {
		if(transactionArray.Len()==0) {
			log.Printf("Transaction array is empty")
			break
		}
		packageSize--
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
		transactions=append(transactions,transaction.(*commons.Transaction))
	}
	
	pkg := &commons.Package{
		BrokerID:brokerID,
		PackageCounter:packageCounter,
		Transactions:transactions,
	}

	// Increment the package counter.
	packageCounter++
	return pkg
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

		// Example: Print the data
		log.Println("Received data:", transaction)
		q.Put(transaction)
		w.WriteHeader(http.StatusOK)
	}	
}

func handleDevicesRequest(w http.ResponseWriter, r *http.Request) {
	// Read data from request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Process the data
	transaction := &commons.DeviceMap{}
	if err:=json.Unmarshal(body, &transaction); err!=nil {
		log.Printf("Error unmarshalling JSON: %s",err)
		http.Error(w, "Error unmarshalling JSON", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

	// Create JSON payload
	func registerBroker(brokerID *int,directoryAddr, brokerIP string, brokerPort int,directoryInfo *commons.DeviceMap) error {
		data := map[string]interface{}{
			"ip":   brokerIP,
			"port": brokerPort,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("error encoding JSON: %v", err)
		}

		resp, err := http.Post(fmt.Sprintf("%s/registerBroker", directoryAddr), "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("error sending request to directory: %v, could not start broker", err)
		}

		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		// Read response
		body, _ := io.ReadAll(resp.Body)
		fmt.Println("Response:", string(body))

		if err := json.Unmarshal(body, &directoryInfo); err != nil {
			return fmt.Errorf("error unmarshalling JSON: %v", err)
		}

		log.Printf("directoryInfo: %#v\n",directoryInfo.BrokerMap)
		*brokerID = directoryInfo.BrokerMap.Version

		log.Printf("Setup complete. Broker registered with ID: %d", *brokerID)
		return nil
	}

func broadcastDirectory(brokerID int,deviceInfo *commons.DeviceMap) error {
	var wg sync.WaitGroup

	jsonData, err := json.Marshal(deviceInfo)
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}
	for id, node := range deviceInfo.BrokerMap.Data {

		// Don't send to self.
		if id==brokerID {
			continue
		}
		wg.Add(1)
		go func(id int, node *commons.NodeInfo) {
			defer wg.Done()

			resp, err := http.Post(fmt.Sprintf("http://%s:%d/handleDirectoryPackage", node.IP, node.Port), "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Error sending request to server %d: %s", id, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf("Unexpected status code: %d for server %d", resp.StatusCode, id)
			}
		}(id, node)
	}
	for id, node := range deviceInfo.ServerMap.Data {
		wg.Add(1)
		go func(id int, node *commons.NodeInfo) {
			defer wg.Done()

			resp, err := http.Post(fmt.Sprintf("http://%s:%d/handleDirectoryPackage", node.IP, node.Port), "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Error sending request to server %d: %s", id, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf("Unexpected status code: %d for server %d", resp.StatusCode, id)
			}
		}(id, node)
	}
	wg.Wait()
	log.Printf("Broadcast method complete.")
	return nil
}

func setupBroker(directoryAddr,brokerIP string,brokerPort int,lfreeQueue *queue.RingBuffer,deviceInfo *commons.DeviceMap,brokerID *int) (*http.Server,error) {
	r := mux.NewRouter()
	r.HandleFunc("/handleDirectoryPackage", handleDevicesRequest).Methods("POST")
	r.HandleFunc("/handleTransaction", handleTransactionRequest(lfreeQueue)).Methods("POST")

	if err := registerBroker(brokerID,directoryAddr,brokerIP,brokerPort,deviceInfo); err!=nil {
		return nil,fmt.Errorf("error registering broker: %v",err)
	}

	if err := broadcastDirectory(*brokerID,deviceInfo); err!=nil {
		return nil,fmt.Errorf("error registering broker: %v",err)
	}

	// Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", brokerPort),
		Handler: r,
	},nil
}

func protocolDaemon(brokerID int,transactionArray *queue.RingBuffer,directoryInfo *commons.DeviceMap) {
	packageCounter := 0

	for {
		log.Printf("Daemon routine is running...")
		time.Sleep(DISPATCH_PERIOD * time.Millisecond)
		packageCounter++
		pkg := CreatePackage(brokerID, packageCounter, transactionArray)

		log.Printf("package of size %d created\n", len(pkg.Transactions))

		jsonData,err := json.Marshal(pkg)
		if err!=nil {
			log.Printf("Error marshalling package: %s",err)
			continue
		}
		var wg sync.WaitGroup
		for serverID, node := range directoryInfo.ServerMap.Data {
			wg.Add(1)
			go func(serverID int,node *commons.NodeInfo) {
				defer wg.Done()

				if err!=nil {
					log.Printf("Server: %d, Error marshalling node info: %s",serverID,err)
					return
				}
				resp,err := http.Post(fmt.Sprintf("http://%s:%d/brokerPackage",node.IP,node.Port),"application/json", bytes.NewBuffer(jsonData))
				if err!=nil {
					log.Printf("Error sending request to server %d: %s",serverID,err)
					return
				}
				if resp.StatusCode != http.StatusOK {
					log.Printf("Unexpected status code: %d for server %d", resp.StatusCode,serverID)
				}
			}(serverID,node)
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
	brokerID := 0
	// Create a lock-free queue to store the incoming transactions
	transactionArray := queue.NewRingBuffer(BUFFER_SIZE)
	directoryInfo := &commons.DeviceMap{
		ServerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
		BrokerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
	}
	server,err := setupBroker(directoryAddr,brokerIP,brokerPort,transactionArray,directoryInfo,&brokerID)
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
		protocolDaemon(brokerID,transactionArray,directoryInfo)
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