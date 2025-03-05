package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"io"
	"lockfreemachine/pkg/commons"
	"os/signal"
	"time"
	"strconv"
	"bytes"

	"encoding/json"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/gorilla/mux"
)

const ( 
	TIMER=5*time.Second
	BUFFER_SIZE=10000
)

type Server struct {
	ip string
	port int
	directoryAddr string
	serverID int
	nodeInfo *commons.DeviceMap
	brokerStatus map[int]bool
	serverStatus map[int]bool
	answer map[int]map[int]*commons.Package
	lfreeQueue *queue.RingBuffer
}

func (s *Server) writeToFile(pkg *commons.Package) {
	// Write to file
	file, err := os.Create(fmt.Sprintf("pkg_%d_%d.json",s.serverID,pkg.PackageCounter))
	if err != nil {
		log.Printf("error creating file: %s", err)
		return
	}
	defer file.Close()

	// Encode package to JSON
	jsonData, err := json.Marshal(pkg)
	if err != nil {
		log.Printf("error encoding JSON: %s", err)
		return
	}

	// Write JSON data to file
	_, err = file.Write(jsonData)
	if err != nil {
		log.Printf("error writing to file: %s", err)
		return
	}

	log.Println("Package written to file")
}

func (s *Server) writePackage(pkg *commons.Package) error {
	// Write to file
	file, err := os.Create(fmt.Sprintf("pkg_%d_%d.json",pkg.BrokerID,pkg.PackageCounter))
	if err != nil {
		log.Printf("error creating file: %s", err)
		return fmt.Errorf("error creating file: %s", err)
	}
	defer file.Close()

	// Encode package to JSON
	jsonData, err := json.Marshal(pkg)
	if err != nil {
		log.Printf("error encoding JSON: %s", err)
		return fmt.Errorf("error encoding JSON: %s", err)
	}

	// Write JSON data to file
	_, err = file.Write(jsonData)
	if err != nil {
		log.Printf("error writing to file: %s", err)
		return fmt.Errorf("error writing to file: %s", err)
	}

	log.Println("Package written to file")
	return nil
}

func (s *Server) readPackage(brokerID, packageCounter int) (*commons.Package, error) {
	// Write to file
	jsonData, err := os.ReadFile(fmt.Sprintf("pkg_%d_%d.json",brokerID,packageCounter))
	if err != nil {
		log.Printf("error reading file: %s", err)
		return nil, fmt.Errorf("error reading file: %s", err)
	}

	// Encode package to JSON
	pkg := &commons.Package{}
	if err := json.Unmarshal(jsonData, pkg); err != nil {
		log.Printf("error decoding JSON: %s", err)
		return nil, fmt.Errorf("error decoding JSON: %s", err)
	}

	return pkg, nil
}


func (s *Server) handleAddPackage(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close() // Close body after reading
		pkg := &commons.Package{}
		if err := json.Unmarshal(body, pkg); err!=nil {
			log.Printf("error while unmarshalling broker info, error: %s",err)
			http.Error(w, "Invalid JSON sent by broker", http.StatusBadRequest)
			return
		}
		fmt.Printf("Package: %#v\n",pkg)

		s.lfreeQueue.Put(pkg)
		s.answer[pkg.BrokerID][pkg.PackageCounter] = pkg
		w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRequestPackage(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	packageID := params.Get("packageID")
	brokerID := params.Get("brokerID")

	packageIDInt, err := strconv.Atoi(packageID)
	if err != nil {
		log.Printf("error converting packageID to int: %s", err)
		http.Error(w, "Invalid packageID", http.StatusBadRequest)
		return
	}

	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		log.Printf("error converting brokerID to int: %s", err)
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}

	pkg,err :=s.readPackage(brokerIDInt, packageIDInt)
	if err != nil {
		log.Printf("error reading package: %s", err)
		http.Error(w, "Error reading package", http.StatusBadRequest)
		return
	}

	jsonData, err := json.Marshal(pkg)
	if err != nil {
		log.Printf("error encoding JSON: %s", err)
		http.Error(w, "Error encoding JSON", http.StatusBadRequest)
		return
	}

	// Perform necessary operations with packageID and brokerID

	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}

func (s *Server) setupServer() (*http.Server,error) {
	r := mux.NewRouter()
	r.HandleFunc("/nodesPackage", handleNodesMessage).Methods("POST")
	r.HandleFunc("/addPackage", s.handleAddPackage).Methods("POST")
	r.HandleFunc("/requestPackage", s.handleRequestPackage).Methods("GET")

	// Create JSON payload
	data:=map[string]interface{}{
		"ip":s.ip,
		"port":s.port,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil,fmt.Errorf("error encoding JSON: %v", err)
	}

	resp,err := http.Post(fmt.Sprintf("%s/registerServer",s.directoryAddr), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil,fmt.Errorf("error sending request to directory: %v, could not start server", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil,fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read response
	body, _ := io.ReadAll(resp.Body)
	fmt.Println("Response:", string(body))

	if err := json.Unmarshal(body, s.nodeInfo); err != nil {
		return nil,fmt.Errorf("error unmarshalling JSON: %v", err)
	}
	fmt.Printf("DevicesInfo: %#v\n",s.nodeInfo.ServerMap.Data)
	// Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: r,
	},nil
}


func (b *Server) protocolDaemon() {
	// Daemon routine
	for {
		time.Sleep(TIMER)
		fmt.Println("Daemon routine is running...")		
	}
}

func handleNodesMessage(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	var data map[string]interface{}
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Invalid JSON payload")
		return
	}

	val, ok := data["name"].(string)
	if !ok || val == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Name is required")
		return
	}

	fmt.Fprintf(w, "Hello, %s!", val)
}

func main() {
	// Parse command line arguments
	var directoryIP string
	var directoryPort int

	flag.StringVar(&directoryIP, "directoryIP", "", "IP of directory")
	flag.IntVar(&directoryPort, "directoryPort", 0, "Port of directory")

	var serverIP string
	var serverPort int

	flag.StringVar(&serverIP, "serverIP", "", "IP of current Server")
	flag.IntVar(&serverPort, "serverPort", 0, "Port of Server")

	flag.Parse()

	if directoryIP == "" {
		log.Fatalf("directoryIP is required")
	}
	if directoryPort == 0 {
		log.Fatalf("directoryPort is required")
	}
	if serverIP == "" {
		log.Fatalf("serverIP is required")
	}
	if serverPort == 0 {
		log.Fatalf("serverPort is required")
	}

	server := &Server{
		ip: serverIP,
		port: serverPort,
		directoryAddr: fmt.Sprintf("http://%s:%d", directoryIP, directoryPort),
		nodeInfo: &commons.DeviceMap{},
		brokerStatus: make(map[int]bool),
		serverStatus: make(map[int]bool),
		answer: make(map[int]map[int]*commons.Package),
		lfreeQueue: queue.NewRingBuffer(BUFFER_SIZE),
	}

	httpServer,err := server.setupServer()
	if err != nil {
		log.Fatalf("Error setting up server: %v", err)
	}

	// Graceful shutdown handling
	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Println("Server Error:", err)
		}
	}()
	fmt.Println("Server running on http://localhost:8080")

	go func() {
		server.protocolDaemon()
		defer fmt.Println("Daemon routine stopped.")
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	fmt.Println("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpServer.Shutdown(ctx)
	fmt.Println("Server stopped.")
}
