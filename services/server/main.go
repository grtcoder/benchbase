package server

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"lockfreemachine/pkg/commons"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"encoding/json"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/avast/retry-go"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger

const ( 
	// Protocol time in seconds

	BUFFER_SIZE=10000
	WRITE_BUFFER_SIZE=100
)


func (s *Server) writeToFile() {
	err := os.MkdirAll(fmt.Sprintf("./packages_%d",s.serverID), os.ModePerm)
	if err != nil {
		logger.Error("Failed to create folder", zap.Error(err))
		return
	}
	for data := range s.writeChan {
		
		err := s.writePackage(data)
		if err != nil {
			logger.Error("Failed to write package to file", zap.Error(err))
			return
		}
	}

	logger.Info("Write channel closed, stopping file writing.")
}

type Server struct {
	ip string
	port int
	directoryAddr string
	DirectoryInfo  *commons.NodesMap
	serverID int
	writeChan chan *commons.Package
	answer map[int]*commons.StateList

	packageArray *queue.RingBuffer
}

func (s *Server) requestPackage(serverURL string,brokerID int,pkg *commons.Package) func() error {
	return func() error {
		client := &http.Client{
			Timeout: commons.SERVER_REQUEST_TIMEOUT,
		}
		req, err := http.NewRequest("GET", fmt.Sprintf("%s?brokerID=%d",serverURL,brokerID), nil)
		if err != nil {
			panic(err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(s.serverID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.ServerType))

		resp, err := client.Do(req)
		if err != nil {
			logger.Error("Error sending request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
			return fmt.Errorf("error sending request to get package: %s", err)
		}

		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Read the response body
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				logger.Error("Error reading response body", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
				return fmt.Errorf("error reading response body: %s", err)
			}

			// Decode the JSON response
			if err := json.Unmarshal(body, pkg); err != nil {
				logger.Error("Error decoding JSON response", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
				return fmt.Errorf("error decoding JSON response: %s", err)
			}

			// Process the package
			logger.Info("Received package from server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("package", pkg))
			return nil
		} else {
			logger.Error("Unexpected status code", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("statusCode", resp.StatusCode))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
}

func (s *Server) requestPackageWithRetry(serverURL string,brokerID int) (*commons.Package,error) {
	pkg := &commons.Package{}
	err := retry.Do(
		s.requestPackage(serverURL,brokerID,pkg),
		retry.Attempts(commons.SERVER_RETRY), // Number of retry attempts
		retry.Delay(0),      // No delay
		retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
	return pkg,err
}

func (s *Server) setupAnswerMap() {

	// reset map before beginning another iteration.
	for k := range s.DirectoryInfo.BrokerMap.Data {
		if _,ok := s.answer[k];ok {
			if s.answer[k].GetHead().GetState() != commons.IgnoreBroker {
				s.answer[k]=commons.NewStateList()

			}
		} else {
			s.answer[k]=commons.NewStateList()
		}
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

func (s *Server) getBrokerList(state int) []int {
	var brokerList []int
	for brokerID,stateList := range s.answer {
		currHead := stateList.GetHead()
		if currHead.GetState() == commons.IgnoreBroker {
			// Ignore the broker if it is in the ignore list
			continue
		}

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
	file, err := os.Create(fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,pkg.BrokerID,pkg.PackageID))

	if err != nil {
		logger.Error("Failed to create file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,pkg.BrokerID,pkg.PackageID)), zap.Error(err))
		return fmt.Errorf("error creating file: %s", err)
	}
	defer file.Close()

	// Encode package to JSON
	jsonData, err := json.Marshal(pkg)
	if err != nil {
		logger.Error("Failed to encode package to JSON", zap.Error(err))
		return fmt.Errorf("error encoding JSON: %s", err)
	}

	// Write JSON data to file
	_, err = file.Write(jsonData)
	if err != nil {
		logger.Error("Failed to write JSON data to file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,pkg.BrokerID,pkg.PackageID)), zap.Error(err))
		return fmt.Errorf("error writing to file: %s", err)
	}

	logger.Info("Package written to file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,pkg.BrokerID,pkg.PackageID)), zap.Any("package", pkg))
	return nil
}

func (s *Server) readPackage(brokerID,packageID int) (*commons.Package, error) {
	// Write to file
	jsonData, err := os.ReadFile(fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,brokerID,packageID))
	if err != nil {
		logger.Error("Failed to read file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,brokerID,packageID)), zap.Error(err))
		return nil, fmt.Errorf("error reading file: %s", err)
	}

	// Encode package to JSON
	pkg := &commons.Package{}
	if err := json.Unmarshal(jsonData, pkg); err != nil {
		logger.Error("Failed to decode JSON", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,brokerID,packageID)), zap.Error(err))
		return nil, fmt.Errorf("error decoding JSON: %s", err)
	}

	logger.Info("Package read from file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,brokerID,packageID)), zap.Any("package", pkg))

	return pkg, nil
}

func (s *Server) handleAddPackage(w http.ResponseWriter, r *http.Request) {

		pkg := &commons.Package{}
		err := json.NewDecoder(r.Body).Decode(pkg)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}

		logger.Info("Received package from broker", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID))

		currHead:=s.GetState(pkg.BrokerID)
		if currHead==nil{
			logger.Info("Package is missing")
			http.Error(w, "Package is missing", http.StatusBadRequest)
			return
		}

		if currHead.GetState() == commons.IgnoreBroker {
			
			logger.Info("Dropping package since ignoreBroker is set",zap.String("brokerID", fmt.Sprintf("%d",pkg.BrokerID)))
			http.Error(w, fmt.Sprintf("Dropping package for brokerID: %d, since IgnoreBroker state is set",pkg.BrokerID), http.StatusBadRequest)
			return
		}

		if currHead.GetState() == commons.Undefined {
			// Only allow saving the package if the state is undefined.
			if ok:=s.answer[pkg.BrokerID].UpdateState(currHead,commons.NewStateNode(&commons.StateValue{State: commons.Received,Pkg: pkg})); ok {
				s.packageArray.Put(pkg)
				s.writeChan <- pkg
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if currHead.GetState() == commons.NotReceived {
			http.Error(w, fmt.Sprintf("broker%d package state is set to NotReceived",pkg.BrokerID), http.StatusBadRequest)
		}
}

func (s *Server) handleRequestPackage(w http.ResponseWriter, r *http.Request){
	params := r.URL.Query()
	brokerID := params.Get("brokerID")

	logger.Info("Received request for package", zap.String("brokerID", brokerID))
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}

	currHead := s.answer[brokerIDInt].GetHead()
	if currHead.GetState() == commons.Received {
		// We assume that the package is already in the packageArray
		pkg := currHead.GetPackage()
		if err := json.NewEncoder(w).Encode(pkg); err != nil {
			logger.Error("error encoding JSON response", zap.Error(err))
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}
		// Perform necessary operations with packageID and brokerID
		return
	}

	logger.Info("Package is not in received state", zap.Int("brokerID", brokerIDInt))
	http.Error(w, "Package is not present", http.StatusBadRequest)
}

func (s *Server) handleIgnoreBroker(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	brokerID := params.Get("brokerID")

	logger.Info("Received ignore broker request", zap.String("brokerID", brokerID))

	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}

	currHead := s.answer[brokerIDInt].GetHead()
	if currHead.GetState() == commons.NotReceived {
		if ok:=s.answer[brokerIDInt].UpdateState(currHead,commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker,Pkg: nil})); !ok {
			logger.Error("error updating state for brokerID", zap.Int("brokerID", brokerIDInt))
			http.Error(w, "Error updating state", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
	http.Error(w, "Couldn't set broker state to ignoreBroker ", http.StatusBadRequest)
}

func (s *Server) handleBrokerOk(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	brokerID := params.Get("brokerID")
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}
	currHead:=s.GetState(brokerIDInt)
	if currHead==nil{
		logger.Info("Package is missing")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if currHead.GetState() == commons.IgnoreBroker {
		logger.Info("Dropping package since ignoreBroker is set",zap.String("brokerID", fmt.Sprintf("%d",brokerIDInt)),zap.String("state", fmt.Sprintf("%d",currHead.GetState())))
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
		logger.Error("error while unmarshalling broker info", zap.Error(err))
		http.Error(w, "Invalid JSON sent by broker", http.StatusBadRequest)
		return
	}

	logger.Info("Received package from broker", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID))

	if ok:=s.answer[pkg.BrokerID].UpdateState(currHead,commons.NewStateNode(&commons.StateValue{State: commons.Received,Pkg: pkg})); ok {
		s.packageArray.Put(pkg)
		s.writeChan <- pkg
	}
	// Write to file
	if err := s.writePackage(pkg); err != nil {
		logger.Error("error writing package to file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,pkg.BrokerID,pkg.PackageID)), zap.Error(err))
		http.Error(w, "Error writing package to file", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleReadPackageStorage(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	brokerID := params.Get("brokerID")

	logger.Info("Received read package (storage) request", zap.String("brokerID", brokerID))
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}

	currHead := s.answer[brokerIDInt].GetHead()
	if currHead.GetState() != commons.Received {
		logger.Info("Package is not in received state", zap.Int("brokerID", brokerIDInt))
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// This looks weird but we need to get the packageID from the head of the state list.
	packageIDInt := currHead.GetPackage().PackageID

	pkg,err :=s.readPackage(brokerIDInt,packageIDInt)
	if err != nil {
		logger.Error("error reading package", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json",s.serverID,brokerIDInt,packageIDInt)), zap.Error(err))
		http.Error(w, "Error reading package", http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(pkg); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}

	// Perform necessary operations with packageID and brokerID
	w.WriteHeader(http.StatusOK)
}

func (s *Server) setupServer() (*http.Server,error) {
	r := mux.NewRouter()
	r.HandleFunc(commons.SERVER_UPDATE_DIRECTORY, commons.HandleUpdateDirectory(logger,s.DirectoryInfo)).Methods("POST")
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

	resp,err := http.Post(fmt.Sprintf("%s%s",s.directoryAddr,commons.DIRECTORY_REGISTER_SERVER), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil,fmt.Errorf("error sending request to directory: %v, could not start server", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil,fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read response
	body, _ := io.ReadAll(resp.Body)

	if err := json.Unmarshal(body, s.DirectoryInfo); err != nil {
		return nil,fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	logger.Info("Directory info received", zap.String("directoryInfo", string(body)))

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
		serverMap := s.DirectoryInfo.ServerMap
		for serverID := range serverMap.Data {
			serverID := serverID

			// Skip the current server
			if serverID == s.serverID {
				continue
			}

			wg.Add(1)
			go func(serverID int) {
				defer wg.Done()
				serverURL := fmt.Sprintf("http://%s:%d%s",serverMap.Data[serverID].IP,serverMap.Data[serverID].Port,endpoint)
				pkg,err := s.requestPackageWithRetry(serverURL,brokerID)
				if err != nil {
					logger.Error("error while requesting package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
					return;
				}

				// If package received, update it as such in the answer array and push it to the package array.
				if ok:=s.answer[brokerID].UpdateState(s.answer[brokerID].GetHead(),commons.NewStateNode(&commons.StateValue{State: commons.Received,Pkg: pkg})); ok {
					s.packageArray.Put(pkg)
					s.writeChan <- pkg
				}

				logger.Info("Received package from server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("package", pkg))
			}(serverID)
		}
	}
	wg.Wait()
}

func (s *Server) sendIgnoreBroker(serverURL string) func() error{
	return func() error {
			client := &http.Client{
				Timeout: commons.SERVER_REQUEST_TIMEOUT,
			}
			req, err := http.NewRequest("POST", serverURL, nil)
			if err != nil {
				panic(err)
			}
	
			// Set headers
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-NodeID", fmt.Sprint(s.serverID))
			req.Header.Set("X-NodeType", commons.GetNodeType(commons.ServerType))
	
			resp, err := client.Do(req)
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
		retry.Attempts(commons.SERVER_RETRY),               // Number of retry attempts
		retry.Delay(0),      // No delay
		retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying ignore broker request", zap.String("serverURL", serverURL), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
}

func (s *Server) sendIgnoreBrokerRequests(brokerList []int) {
	var wg sync.WaitGroup
	for _,brokerID := range brokerList {
		brokerID := brokerID
		serverMap := s.DirectoryInfo.ServerMap
		for serverID := range serverMap.Data {
			// Skip the current server
			if serverID == s.serverID {
				continue
			}
			serverID := serverID
			serverURL := fmt.Sprintf("http://%s:%d%s?brokerID=%d",serverMap.Data[serverID].IP,serverMap.Data[serverID].Port,commons.SERVER_IGNORE_BROKER,brokerID)
			wg.Add(1)
			go func(serverURL string,brokerID int) {
				defer wg.Done()
				err := s.sendIgnoreBrokerWithRetry(serverURL)
				if err != nil {
					logger.Error("error while sending ignore broker request", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
					return
				}
				logger.Info("Ignore broker request sent to server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID))
			}(serverURL, brokerID)
		}
	}
	wg.Wait()
}


func (s *Server) protocolDaemon(nextRun time.Time) {
	// Daemon routine
		// Setup the answerMap for fresh epoch.
		// We want the ticker to have millisecond precision
		ticker := time.NewTicker(time.Millisecond)
		logger.Info("Protocol Daemon started")

		logger.Info("Next run will happen at: %s", zap.Time("nextRun", nextRun))

		for tc := range ticker.C {
			if tc.Before(nextRun) {
				continue
			}

			waitPackage := nextRun.Add(commons.WAIT_FOR_BROKER_PACKAGE)
			nextRun = nextRun.Add(commons.EPOCH_PERIOD)

			s.setupAnswerMap()
			err:=commons.BroadcastNodesInfo(logger,s.serverID,commons.ServerType,s.DirectoryInfo)
			if err!=nil {
				logger.Error("error while broadcasting directory info", zap.Error(err))
			}

			logger.Info("Answer map is setup, waiting for packages...")
			// Wait for Brokers to send packages.
		// We do this instead of time.After because we don't know how long the Broadcast Nodes Info will take.
			for innerTc := range ticker.C {
				if innerTc.Equal(waitPackage) || innerTc.After(waitPackage) {
					break
				}
			}
			logger.Info("Waiting time is up, checking for received packages...")

			s.updateNotReceivedPackage()

			brokerList:= s.getBrokerList(commons.NotReceived)
			logger.Info("Missing Packages",zap.Any("list",brokerList))
			s.requestPackages(commons.SERVER_REQUEST_PACKAGE,brokerList)

			// Request package from the memory of other servers.
			// Get the list of brokers for which we have still not received the package.
			brokerList= s.getBrokerList(commons.NotReceived)
			logger.Info("Missing Packages",zap.Any("list",brokerList))


			s.requestPackages(commons.SERVER_READ_STORAGE,brokerList)

			brokerList= s.getBrokerList(commons.NotReceived)

			for _,brokerID := range brokerList {
				if s.answer[brokerID].GetHead().GetState() == commons.NotReceived {
					// We want to ignore the broker if the package is not received.
					if ok:=s.answer[brokerID].UpdateState(s.answer[brokerID].GetHead(),commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker,Pkg: nil})); ok {
						logger.Info("Ignoring broker", zap.Int("brokerID", brokerID))
					}
				}
			}
		
			// Send ignore broker requests to other servers.
			logger.Info("Sending ignore broker requests to other servers for brokers",zap.Any("brokers",brokerList))
			s.sendIgnoreBrokerRequests(brokerList)

			logger.Info("Protocol Completed")

			logger.Info("Next run will happen at: %s", zap.Time("nextRun", nextRun))

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

	var startTimestamp int64
	flag.Int64Var(&startTimestamp, "startTimestamp", 0, "Start timestamp")

	var logFile string
	flag.StringVar(&logFile, "logFile", "./logs/broker.log", "Path to the log file")

	flag.Parse()


	if directoryIP == "" {
		fmt.Print("directoryIP is required")
		return
	}
	if directoryPort == 0 {
		fmt.Print("directoryPort is required")
		return
	}

	if serverIP == "" {
		fmt.Printf("serverIP is required")
		return
	}

	if serverPort == 0 {
		fmt.Printf("serverPort is required")
		return
	}

	if startTimestamp == 0 {
		fmt.Printf("startTimestamp is required")
		return
	}

	err := os.Mkdir("./logs", 0755)
	if err != nil {
		fmt.Printf("failed to create logs directory: %v", err)
		return
	}
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFile, // Log file path
		MaxSize:    10,    // Max megabytes before log is rotated
		MaxBackups: 5,     // Max old log files to keep
		MaxAge:     28,    // Max days to retain old log files
		Compress:   true,  // Compress old files (.gz)
	}
		
	// Create Zap core with Lumberjack
	writeSyncer := zapcore.AddSync(lumberjackLogger)
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		zapcore.InfoLevel,
	)
			
	logger = zap.New(core)	
	logger = logger.With(zap.String("service", fmt.Sprintf("server %s",serverIP)))

	defer logger.Sync()

	server := &Server{
		ip: serverIP,
		port: serverPort,
		directoryAddr: fmt.Sprintf("http://%s:%d", directoryIP, directoryPort),
		DirectoryInfo: &commons.NodesMap{},
		answer: make(map[int]*commons.StateList),
		packageArray: queue.NewRingBuffer(BUFFER_SIZE),
		writeChan: make(chan *commons.Package, WRITE_BUFFER_SIZE),
	}

	httpServer,err := server.setupServer()
	if err != nil {
		logger.Error("Error setting up server", zap.Error(err))
	}

	// Start the writer goroutine. It will end when the channel is closed.
	go server.writeToFile()

	// Graceful shutdown handling
	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("Server Error", zap.Error(err))
		}
	}()

	logger.Info("Server running on http://localhost:%d", zap.Int("port", server.port))

	go func() {
		server.protocolDaemon(time.Unix(0, startTimestamp))
		defer fmt.Println("Daemon routine stopped.")
	}()
	
	var wg sync.WaitGroup
	wg.Add(1)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	logger.Info("\nShutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpServer.Shutdown(ctx)
	logger.Info("Server stopped.")
	close(server.writeChan)
}
