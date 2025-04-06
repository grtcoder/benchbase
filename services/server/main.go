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
	"sync"
	"strconv"
	"bytes"

	"encoding/json"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/gorilla/mux"
	"github.com/robfig/cron/v3"
	"github.com/avast/retry-go"
)

const ( 
	// Protocol time in seconds
	PROTOCOL_TIME_PERIOD=5
	BUFFER_SIZE=10000
	WAIT_FOR_BROKER_TIMER=2*time.Second
	RETRY_TIME=400*time.Millisecond
)

type Server struct {
	ip string
	port int
	directoryAddr string
	DirectoryInfo  *commons.NodesMap
	serverID int
	packageCounter map[int]int
	answer map[int]*commons.StateList
	packageArray *queue.RingBuffer
}


func (s *Server) requestPackage(serverURL string,brokerID, packageID int,pkg *commons.Package) func() error {
	return func() error {
		resp, err := http.Get(fmt.Sprintf("%s/package?brokerID=%d",serverURL, brokerID))
		if err != nil {
			log.Printf("error sending request to get package: %s", err)
			return fmt.Errorf("error sending request to get package: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Read the response body
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("error reading response body: %s", err)
				return fmt.Errorf("error reading response body: %s", err)
			}

			// Decode the JSON response
			if err := json.Unmarshal(body, pkg); err != nil {
				log.Printf("error decoding JSON response: %s", err)
				return fmt.Errorf("error decoding JSON response: %s", err)
			}

			// Process the package
			log.Printf("Received package: %#v\n", pkg)
			return nil
		} else {
			log.Printf("unexpected status code: %d", resp.StatusCode)
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
}

func (s *Server) requestPackageWithRetry(serverURL string,brokerID, packageID int, pkg *commons.Package) error {
	return retry.Do(
		s.requestPackage(serverURL,brokerID,packageID,pkg),
		retry.Attempts(5),               // Number of retry attempts
		retry.Delay(RETRY_TIME),      // Delay between retries
		retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Retry attempt %d: %v\n", n+1, err)
		}),
	)
}

func (s *Server) setupAnswerMap() {

	// reset map before beginning another iteration.
	s.answer=make(map[int]*commons.StateList)
	for k := range s.DirectoryInfo.ServerMap.Data {
		s.answer[k]=commons.NewStateList()
	}
}

func (s *Server) updateNotReceivedPackage() {
	for _,stateList := range s.answer {
		currHead := stateList.GetHead()
		if currHead.GetState()==commons.Undefined {
			stateList.UpdateState(currHead,
				commons.NewStateNode(
					&commons.StateValue{State: commons.NotReceived,
						Pkg: nil}))
		}
	}
}

func (s *Server) getBrokerList(state int32) []int {
	var brokerList []int
	for brokerID,stateList := range s.answer {
		currHead := stateList.GetHead()
		if currHead.GetState()==state {
			brokerList = append(brokerList, brokerID)
		}
	}
	return brokerList
}

func (s *Server) GetState(brokerID int) *commons.StateNode {
	stateList,ok := s.answer[brokerID]
	if !ok {
		return nil
	}
	currHead := stateList.GetHead()
	
	return currHead
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
		params := r.URL.Query()
		brokerID := params.Get("brokerID")
		brokerIDInt,err := strconv.Atoi(brokerID)
		if err!=nil {
			log.Printf("Invalid brokerID in request: %s: error: %s",brokerID,err)
			w.WriteHeader(http.StatusExpectationFailed)
			return
		}

		currHead:=s.GetState(brokerIDInt)
		if currHead==nil{
			log.Printf("Package is missing")
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if currHead.GetState()==commons.Undefined {
			log.Printf("Dropping package for brokerID: %d, since IgnoreBroker state is set",currHead.GetState())
			w.WriteHeader(http.StatusNotFound)
			return
		}

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

	
		s.answer[pkg.BrokerID].UpdateState(currHead,commons.NewStateNode(&commons.StateValue{State: commons.Received,Pkg: pkg}))
		s.packageArray.Put(pkg)
		w.WriteHeader(http.StatusOK)
}

func (s *Server) handleRequestPackage(w http.ResponseWriter, r *http.Request){
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

	currHead := s.answer[brokerIDInt].GetHead()
	if currHead.GetState() == commons.Received {
		pkg := currHead.GetPackage()
		if(pkg.PackageCounter!=packageIDInt){
			// Return package only if the requested packageID matches the packageCounter
			w.WriteHeader(http.StatusNotFound)
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
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

func (s *Server) handleIgnoreBroker(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	brokerID := params.Get("brokerID")
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		log.Printf("error converting brokerID to int: %s", err)
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}

	if ok:=s.answer[brokerIDInt].UpdateState(nil,commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker,Pkg: nil})); !ok {
		log.Printf("error updating state for brokerID: %d to ignoreBroker", brokerIDInt)
		http.Error(w, "Error updating state", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleBrokerOk(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	brokerID := params.Get("brokerID")
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		log.Printf("error converting brokerID to int: %s", err)
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}
	currHead:=s.GetState(brokerIDInt)
	if currHead==nil{
		log.Printf("Package is missing")
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if currHead.GetState()==commons.Undefined {
		log.Printf("Dropping package for brokerID: %d, since IgnoreBroker state is set",currHead.GetState())
		w.WriteHeader(http.StatusNotFound)
		return
	}

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


	if ok:=s.answer[pkg.BrokerID].UpdateState(currHead,commons.NewStateNode(&commons.StateValue{State: commons.Received,Pkg: pkg})); ok {
		s.packageArray.Put(pkg)
	}
	// Write to file
	if err := s.writePackage(pkg); err != nil {
		log.Printf("error writing package to file: %s", err)
		http.Error(w, "Error writing package to file", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleReadPackageStorage(w http.ResponseWriter, r *http.Request) {
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
	r.HandleFunc(commons.SERVER_UPDATE_DIRECTORY, commons.HandleUpdateDirectory(s.DirectoryInfo)).Methods("POST")
	r.HandleFunc(commons.SERVER_ADD_PACKAGE, s.handleAddPackage).Methods("POST")
	r.HandleFunc(commons.SERVER_REQUEST_PACKAGE, s.handleRequestPackage).Methods("GET")
	r.HandleFunc(commons.SERVER_READ_STORAGE, s.handleReadPackageStorage).Methods("GET")
	r.HandleFunc(commons.SERVER_IGNORE_BROKER,s.handleIgnoreBroker).Methods("POST")
	r.HandleFunc(commons.SERVER_BROKER_OK,s.handleBrokerOk).Methods("POST")

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

	if err := json.Unmarshal(body, s.DirectoryInfo); err != nil {
		return nil,fmt.Errorf("error unmarshalling JSON: %v", err)
	}
	fmt.Printf("DevicesInfo: %#v\n",s.DirectoryInfo.ServerMap.Data)


	// Since we locking the directory service, we can safely assume that the version is the same as the serverID
	s.serverID = s.DirectoryInfo.ServerMap.Version

	// Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: r,
	},nil
}

func (s *Server) requestPackages(endpoint string,brokerList []int) {
	var wg sync.WaitGroup
	for _,brokerID := range brokerList {
		var pkg *commons.Package
		for serverID := range s.DirectoryInfo.ServerMap.Data {
			serverID := serverID

			// Skip the current server
			if serverID == s.serverID {
				continue
			}

			wg.Add(1)
			go func(serverID int) {
				defer wg.Done()
				serverURL := fmt.Sprintf("http://%s:%d/%s",s.DirectoryInfo.ServerMap.Data[serverID].IP,s.DirectoryInfo.ServerMap.Data[serverID].Port,endpoint)
				err := s.requestPackageWithRetry(serverURL,brokerID,0,pkg)
				if err != nil {
					log.Printf("error while requesting package: %s", err)
					return;
				}

				// If package received, update it as such in the answer array and push it to the package array.
				if ok:=s.answer[brokerID].UpdateState(s.answer[brokerID].GetHead(),commons.NewStateNode(&commons.StateValue{State: commons.Received,Pkg: pkg})); ok {
					s.packageArray.Put(pkg)
				}
				log.Printf("Package: %d received from server %d for broker %d\n",pkg.PackageCounter, serverID, brokerID)
			}(serverID)
		}
	}
	wg.Wait()
}

func (s *Server) sendIgnoreBroker(serverURL string) func() error{
	return func() error {
			resp, err := http.Post(serverURL, "application/json", nil)
			if err != nil {
				return fmt.Errorf("error sending request to server %s: %s", serverURL, err)
			}
			defer resp.Body.Close()
		return nil
	}
}
func (s *Server) sendIgnoreBrokerWithRetry(serverURL string) error {
	return retry.Do(
		s.sendIgnoreBroker(serverURL),
		retry.Attempts(5),               // Number of retry attempts
		retry.Delay(RETRY_TIME),      // Delay between retries
		retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			log.Printf("Retry attempt %d: %v\n", n+1, err)
		}),
	)
}

func (s *Server) sendIgnoreBrokerRequests(brokerList []int) {
	var wg sync.WaitGroup
	for _,brokerID := range brokerList {
		brokerID := brokerID
		for serverID := range s.DirectoryInfo.ServerMap.Data {
			// Skip the current server
			if serverID == s.serverID {
				continue
			}
			serverURL := fmt.Sprintf("http://%s:%d/%s?brokerID=%d",s.DirectoryInfo.ServerMap.Data[serverID].IP,s.DirectoryInfo.ServerMap.Data[serverID].Port,commons.SERVER_IGNORE_BROKER,brokerID)
			wg.Add(1)
			go func(serverURL string,brokerID int) {
				defer wg.Done()
				err := s.sendIgnoreBrokerWithRetry(serverURL)
				if err != nil {
					log.Printf("error while sending ignore broker request: %s", err)
					return
				}
				log.Printf("Ignore broker request sent to server %d for broker %d\n", serverID, brokerID)
			}(serverURL, brokerID)
		}
	}
	wg.Wait()
}


func (s *Server) protocolDaemon() {
	// Daemon routine
	for {
		// Setup the answerMap for fresh epoch.
		s.setupAnswerMap()
		log.Println("Answer map is setup, waiting for packages...")
		// Wait for Brokers to send packages.
		time.Sleep(WAIT_FOR_BROKER_TIMER)
		log.Println("Time is up, checking for received packages...")

		s.updateNotReceivedPackage()

		brokerList:= s.getBrokerList(commons.NotReceived)

		s.requestPackages(commons.SERVER_REQUEST_PACKAGE,brokerList)

		// Request package from the memory of other servers.
		// Get the list of brokers for which we have still not received the package.
		brokerList= s.getBrokerList(commons.NotReceived)
		s.requestPackages(commons.SERVER_READ_STORAGE,brokerList)

		brokerList= s.getBrokerList(commons.NotReceived)
		s.sendIgnoreBrokerRequests(brokerList)
		fmt.Println("Protocol Completed....")		
	}
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
		DirectoryInfo: &commons.NodesMap{},
		answer: make(map[int]*commons.StateList),
		packageArray: queue.NewRingBuffer(BUFFER_SIZE),
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

	c := cron.New()
	c.AddFunc(fmt.Sprintf("*/%d * * * * *",PROTOCOL_TIME_PERIOD), server.protocolDaemon)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	fmt.Println("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpServer.Shutdown(ctx)
	fmt.Println("Server stopped.")
}
