package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"io"
	"os/signal"
	"time"
	"bytes"

	"encoding/json"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/gorilla/mux"
)

const ( 
	TIMER=5*time.Second
	BUFFER_SIZE=10000
	isReady=false
 )

func handleBrokerPackage(*queue.RingBuffer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	val := r.FormValue("name")
	if val=="" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Name is required")
		return
	}
	fmt.Fprintf(w, "Hello, %s!", val)
}
}

func handleServerPackage(*queue.RingBuffer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	val := r.FormValue("name")
	if val=="" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Name is required")
		return
	}

	fmt.Fprintf(w, "Hello, %s!", val)
	}
}

func setupServer(directoryAddr,serverIP string,serverPort int,lfreeQueue *queue.RingBuffer) (*http.Server,error) {
	r := mux.NewRouter()
	r.HandleFunc("/", handleDirectoryMessage).Methods("POST")
	r.HandleFunc("/brokerPackage", handleBrokerPackage(lfreeQueue)).Methods("POST")

	// Create JSON payload
	data:=map[string]interface{}{
		"ip":serverIP,
		"port":serverPort,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil,fmt.Errorf("error encoding JSON: %v", err)
	}

	resp,err := http.Post(fmt.Sprintf("%s/registerServer",directoryAddr), "application/json", bytes.NewBuffer(jsonData))
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

	// Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", serverPort),
		Handler: r,
	},nil
}


func protocolDaemon(lfreeQueue *queue.RingBuffer) {
	// Daemon routine
	for {
		time.Sleep(TIMER)
		fmt.Println("Daemon routine is running...")		
	}
}

func handleDirectoryMessage(w http.ResponseWriter, r *http.Request) {
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

	// Create directory address to send initial setup request.
	directoryAddr := fmt.Sprintf("http://%s:%d", directoryIP, directoryPort)

	lfreeQueue := queue.NewRingBuffer(10)

	server,err := setupServer(directoryAddr,serverIP,serverPort,lfreeQueue)
	if err != nil {
		log.Fatalf("Error setting up server: %v", err)
	}


	// Graceful shutdown handling
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Println("Server Error:", err)
		}
	}()
	fmt.Println("Server running on http://localhost:8080")

	go func() {
		protocolDaemon(lfreeQueue)
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
	fmt.Println("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Shutdown(ctx)
	fmt.Println("Server stopped.")
}
