package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"lockfreemachine/pkg/commons"

	"github.com/gorilla/mux"
)



var nodesInfo *commons.NodesMap

// This will also serve as the version of the broker information.
var latestBrokerID int

// This will also serve as the version of the server information.
var latestServerID int

func registerBroker(w http.ResponseWriter, r *http.Request) {
	// Read the JSON body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("error while reading request body, error: %s",err)
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close() // Close body after reading

	brokerInfo := nodesInfo.BrokerMap
	brokerInfo.Lock()
	latestBrokerID++
	brokerID := latestBrokerID

	currBroker:=&commons.NodeInfo{}
	if err=json.Unmarshal(body,&currBroker);err!=nil {
		log.Printf("error while unmarshalling broker info, error: %s",err)
		http.Error(w, "Invalid JSON sent by broker", http.StatusBadRequest)
		return
	}

	brokerInfo.Set(brokerID, currBroker)
	brokerInfo.SetVersion(brokerID)

	brokerInfo.Unlock()

	log.Printf("Broker registered with ID: %d",brokerID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(nodesInfo)
}

func registerServer(w http.ResponseWriter, r *http.Request) {
	// Read the JSON body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close() // Close body after reading


	serverInfo := nodesInfo.ServerMap
	serverInfo.Lock()
	latestServerID++
	serverID := latestServerID

	currServer:=&commons.NodeInfo{}
	if err=json.Unmarshal(body,&currServer);err!=nil {
		log.Printf("error while unmarshalling server info, error: %s",err)
		http.Error(w, "Invalid JSON sent by server", http.StatusBadRequest)
		return
	}

	serverInfo.Set(serverID,currServer)
	serverInfo.SetVersion(serverID)

	serverInfo.Unlock()

	log.Printf("Server registered with ID: %d",serverID)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(nodesInfo)
}

func main(){
	// Initialize the broker and server counter to 0.
	latestBrokerID = 0
	latestServerID = 0

	nodesInfo = &commons.NodesMap{
		ServerMap: &commons.DirectoryMap{
			Data: make(map[int]*commons.NodeInfo),
			Version: 0,
		},
		BrokerMap: &commons.DirectoryMap{
			Data: make(map[int]*commons.NodeInfo),
			Version: 0,
		},
	}

	r := mux.NewRouter()
	r.HandleFunc(commons.DIRECTORY_REGISTER_BROKER, registerBroker)
	r.HandleFunc(commons.DIRECTORY_REGISTER_SERVER, registerServer)

	// Graceful shutdown handling
	server := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Graceful shutdown handling
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Println("Server Error:", err)
		}
	}()
	log.Println("Server running on http://localhost:8080")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	log.Println("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
}