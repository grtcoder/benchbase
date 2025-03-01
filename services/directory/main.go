package main

import (
	 "net/http"
	 "log"
	 "encoding/json"
	 "os"
	 "os/signal"
	 "context"
	 "time"
	 "io"

	 "lockfreemachine/pkg/commons"

	 "github.com/gorilla/mux"
)



var brokerInfo *commons.InfoMap
var serverInfo *commons.InfoMap

// This will also serve as the version of the broker information.
var latestBrokerID int

// This will also serve as the version of the server information.
var latestServerID int

func registerBroker(w http.ResponseWriter, r *http.Request) {
	// Read the JSON body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close() // Close body after reading

	brokerInfo.Lock()
	brokerID := latestBrokerID

	currBroker:=&commons.NodeInfo{}
	if err=json.Unmarshal(body,&currBroker);err!=nil {
		log.Printf("error while unmarshalling broker info, error: %s",err)
		http.Error(w, "Invalid JSON sent by broker", http.StatusBadRequest)
		return
	}

	brokerInfo.Set(brokerID, currBroker)
	brokerInfo.SetVersion(latestBrokerID)
	latestBrokerID++

	brokerInfo.Unlock()

	log.Printf("Broker registered with ID: %d",brokerID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(brokerInfo)
}

func registerServer(w http.ResponseWriter, r *http.Request) {
	// Read the JSON body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close() // Close body after reading



	serverInfo.Lock()
	serverID := latestServerID

	currServer:=&commons.NodeInfo{}
	if err=json.Unmarshal(body,&currServer);err!=nil {
		log.Printf("error while unmarshalling server info, error: %s",err)
		http.Error(w, "Invalid JSON sent by server", http.StatusBadRequest)
		return
	}

	serverInfo.Set(serverID,currServer)
	serverInfo.SetVersion(latestServerID)
	latestServerID++

	serverInfo.Unlock()

	log.Printf("Server registered with ID: %d",serverID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(serverInfo)
}

func main(){
	latestBrokerID = 0
	brokerInfo = &commons.InfoMap{Data: make(map[int]*commons.NodeInfo)}
	serverInfo = &commons.InfoMap{Data: make(map[int]*commons.NodeInfo)}

	r := mux.NewRouter()
	r.HandleFunc("/registerBroker", registerBroker).Methods("POST")
	r.HandleFunc("/registerServer", registerServer).Methods("POST")

	// Graceful shutdown handling
	server := &http.Server{
		Addr:    ":8081",
		Handler: r,
	}

	// Graceful shutdown handling
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Println("Server Error:", err)
		}
	}()
	log.Println("Server running on http://localhost:8081")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
}