package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"lockfreemachine/src/pkg/commons"
	"math/rand"
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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// zap logger instance to log messages
var logger *zap.Logger

const (
	// Maximum number of packages the package Array can hold.
	BUFFER_SIZE = 10000

	// Maximum number of packages the write channel can hold.
	WRITE_BUFFER_SIZE = 100
)

var (
	httpRequests     *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	apiCallDuration  *prometheus.HistogramVec
	epochNumber      *prometheus.GaugeVec
	protocolDuration *prometheus.HistogramVec
	epochDuration    *prometheus.GaugeVec
)

func initMetrics(serverID int) {

	// Create a wrapped Registerer that injects labels
	wrappedRegisterer := prometheus.WrapRegistererWith(
		prometheus.Labels{"serverID": fmt.Sprint(serverID)}, // <- static label here
		prometheus.DefaultRegisterer,
	)

	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Count of all HTTP requests grouped by endpoint",
		},
		[]string{"path", "method"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_request_duration_milliseconds",
			Help:    "Duration of API requests in milliseconds",
			Buckets: prometheus.DefBuckets, // You can customize this
		},
		[]string{"path", "method", "srcID", "srcType"}, // group by endpoint/method if you want
	)

	apiCallDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "outbound_api_call_duration_seconds",
			Help:    "Duration of outbound API calls in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "targetID"},
	)

	// setupTime = prometheus.NewGaugeVec(
	// 	prometheus.GaugeOpts{
	// 		Name: "setup_time",
	// 		Help: "Time taken to set up the broker",
	// 	},
	// 	[]string{},
	// )

	epochNumber = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "protocol_current_epoch",
			Help: "Which epoch the protocol daemon is on",
		},
		[]string{}, // no extra labels
	)

	// 2) Histogram for each run’s duration
	protocolDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "protocol_epoch_duration_seconds",
			Help:    "Duration of a single protocol epoch run",
			Buckets: prometheus.DefBuckets,
		},
		[]string{}, // no extra labels
	)

	epochDuration = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "protocol_epoch_duration_in_seconds",
			Help: "Wall clock seconds taken by each protocol epoch run",
		},
		[]string{"epoch"},
	)

	wrappedRegisterer.MustRegister(httpRequests)
	wrappedRegisterer.MustRegister(requestDuration)
	wrappedRegisterer.MustRegister(apiCallDuration)
	wrappedRegisterer.MustRegister(epochNumber)
	wrappedRegisterer.MustRegister(protocolDuration)
	wrappedRegisterer.MustRegister(epochDuration)

}

// Middleware to measure duration with dynamic labels
func (s *Server) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rand.Float64() < s.dropRate {
			// increment a “dropped” metric if you want:
			httpRequests.WithLabelValues(r.URL.Path, r.Method).Inc()
			http.Error(w, "simulated network failure", http.StatusServiceUnavailable)
			return
		}

		nodeID := r.Header.Get("X-NodeID")     // Grab dynamic tenant ID from headers
		nodeType := r.Header.Get("X-NodeType") // Grab dynamic tenant ID from headers
		if nodeID == "" {
			nodeID = "unknown"
			nodeType = "unknown"
		}

		// if r.URL.Path == "/metrics" {
		// 	// skip
		// 	promhttp.Handler().ServeHTTP(w, r)
		// 	return
		// }
		httpRequests.WithLabelValues(r.URL.Path, r.Method).Inc()
		start := time.Now()

		// Serve the real handler
		next.ServeHTTP(w, r)

		duration := time.Since(start).Seconds()

		// Record with dynamic label
		requestDuration.WithLabelValues(r.URL.Path, r.Method, nodeID, nodeType).Observe(duration)
	})
}

func makeInstrumentedClient(targetID string) *http.Client {
	// Curry the targetID label, leaving method to be filled per-request
	curried := apiCallDuration.MustCurryWith(prometheus.Labels{
		"targetID": targetID,
	})

	// Wrap the default transport
	instrumentedTransport := promhttp.InstrumentRoundTripperDuration(
		curried,               // ObserverVec for apiCallDuration
		http.DefaultTransport, // underlying RoundTripper
	)

	return &http.Client{
		Transport: instrumentedTransport,
		Timeout:   commons.SERVER_REQUEST_TIMEOUT,
	}
}

// writeToFile is a goroutine that writes packages to files.
func (s *Server) writeToFile() {
	err := os.MkdirAll(fmt.Sprintf("./packages_%d", s.serverID), os.ModePerm)
	if err != nil {
		logger.Error("Failed to create folder", zap.Error(err))
		return
	}
	for data := range s.writeChan {
		// Read from the write channel and write the package to a file.
		err := s.writePackage(data)
		if err != nil {
			logger.Error("Failed to write package to file", zap.Error(err))
			return
		}
	}

	logger.Info("Write channel closed, stopping file writing.")
}

// findSetDifference finds the set difference between two lists of integers.
func findSetDifference(list1, list2 []int) []int {
	set := make(map[int]bool)
	result := []int{}

	for _, num := range list1 {
		set[num] = true
	}

	for _, num := range list2 {
		if _, ok := set[num]; !ok {
			result = append(result, num)
		}
	}

	return result
}

// Server struct represents the server instance.
type Server struct {
	ip             string // IP address of the server
	port           int    // Port number of the server
	readerPort     int    //port number of the reader service, the ip remains the same.
	directoryAddr  string
	packageCounter int                        // Address of the directory service
	DirectoryInfo  *commons.NodesMap          // Directory information
	serverID       int                        // Server ID
	writeChan      chan *commons.Package      // Channel to write packages to files
	answer         map[int]*commons.StateList // Map of broker IDs to their states
	packageArray   *queue.RingBuffer          // Array to hold packages
	dropRate       float64                    // api requests
}

// requestPackage sends a request to the server to get a package.
func (s *Server) requestPackage(serverURL string, brokerID int, pkg *commons.Package) func() error {
	return func() error {
		// client := &http.Client{
		// 	Timeout: commons.SERVER_REQUEST_TIMEOUT,
		// }
		client := makeInstrumentedClient("request-package")

		// Create a new HTTP request to request package
		req, err := http.NewRequest("GET", fmt.Sprintf("%s?brokerID=%d", serverURL, brokerID), nil)
		if err != nil {
			panic(err)
		}

		// Set headers for the request
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

func (s *Server) requestPackageFromStorage(serverURL string, brokerID int, packageID int, pkg *commons.Package) func() error {
	return func() error {
		// client := &http.Client{
		// 	Timeout: commons.SERVER_REQUEST_TIMEOUT,
		// }
		client := makeInstrumentedClient("request-package-storage")

		// Create a new HTTP request to request package
		req, err := http.NewRequest("GET", fmt.Sprintf("%s?brokerID=%d&packageID=%d", serverURL, brokerID, packageID), nil)
		if err != nil {
			panic(err)
		}

		// Set headers for the request
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(s.serverID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.ServerType))

		resp, err := client.Do(req)
		if err != nil {
			logger.Error("Error sending request to get package from storage", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID), zap.Error(err))
			return fmt.Errorf("error sending request to get package: %s", err)
		}

		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Read the response body
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				logger.Error("Error reading response body", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID), zap.Error(err))
				return fmt.Errorf("error reading response body: %s", err)
			}

			// Decode the JSON response
			if err := json.Unmarshal(body, pkg); err != nil {
				logger.Error("Error decoding JSON response", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID), zap.Error(err))
				return fmt.Errorf("error decoding JSON response: %s", err)
			}

			// Process the package
			logger.Info("Received package from storage server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID), zap.Any("package", pkg))
			return nil
		} else {
			logger.Error("Unexpected status code", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID), zap.Int("statusCode", resp.StatusCode))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
}

func (s *Server) sendPackage(serverURL string, jsonData []byte) func() error {
	return func() error {
		// Create an HTTP client with a per-request timeout
		// client := &http.Client{

		// 	// TODO: Change it to another configurable value.
		// 	Timeout: commons.SERVER_REQUEST_TIMEOUT,
		// }
		client := makeInstrumentedClient("server-send-package")

		req, err := http.NewRequest("POST", serverURL, bytes.NewBuffer(jsonData))
		if err != nil {
			panic(err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(s.serverID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.ServerType))

		resp, err := client.Do(req)

		if err != nil {
			return fmt.Errorf("error sending request to %s: %s", serverURL, err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body: %v", err)
		}
		bodyString := string(body)

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d for server %s: error: %s", resp.StatusCode, serverURL, bodyString)
		}
		return nil
	}
}

func (s *Server) requestPackageWithRetry(serverURL string, brokerID int) (*commons.Package, error) {
	pkg := &commons.Package{}
	err := retry.Do(
		s.requestPackage(serverURL, brokerID, pkg),
		retry.Attempts(commons.SERVER_RETRY), // Number of retry attempts
		retry.Delay(0),                       // No delay
		retry.DelayType(retry.FixedDelay),    // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
	return pkg, err
}

func (s *Server) requestPackageFromStorageWithRetry(serverURL string, brokerID int, packageID int) (*commons.Package, error) {
	pkg := &commons.Package{}
	err := retry.Do(
		s.requestPackageFromStorage(serverURL, brokerID, packageID, pkg),
		retry.Attempts(commons.SERVER_RETRY), // Number of retry attempts
		retry.Delay(0),                       // No delay
		retry.DelayType(retry.FixedDelay),    // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
	return pkg, err
}

func (s *Server) setupAnswerMap() {

	// reset map before beginning another iteration.
	for k := range s.DirectoryInfo.BrokerMap.Data {
		if _, ok := s.answer[k]; ok {
			if s.answer[k].GetHead().GetState() != commons.IgnoreBroker {
				s.answer[k] = commons.NewStateList()
			}
		} else {
			s.answer[k] = commons.NewStateList()
		}
	}
}

func (s *Server) updateNotReceivedPackage() {
	for _, stateList := range s.answer {
		currHead := stateList.GetHead()
		if currHead.GetState() == commons.Undefined {
			stateList.UpdateState(currHead,
				commons.NewStateNode(
					&commons.StateValue{State: commons.NotReceived,
						Pkg: nil}))
		}
	}
}

func (s *Server) getBrokerList(state int) []int {
	var brokerList []int
	for brokerID, stateList := range s.answer {
		currHead := stateList.GetHead()
		if currHead.GetState() == commons.IgnoreBroker {
			// Ignore the broker if it is in the ignore list
			continue
		}

		if currHead.GetState() == state {
			brokerList = append(brokerList, brokerID)
		}
	}
	return brokerList
}

func (s *Server) GetState(brokerID int) *commons.StateNode {
	stateList, ok := s.answer[brokerID]
	if !ok {
		return nil
	}
	currHead := stateList.GetHead()

	return currHead
}

func (s *Server) writePackage(pkg *commons.Package) error {
	// Write to file
	file, err := os.Create(fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, pkg.BrokerID, pkg.PackageID))

	if err != nil {
		logger.Error("Failed to create file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, pkg.BrokerID, pkg.PackageID)), zap.Error(err))
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
		logger.Error("Failed to write JSON data to file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, pkg.BrokerID, pkg.PackageID)), zap.Error(err))
		return fmt.Errorf("error writing to file: %s", err)
	}

	logger.Info("Package written to file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, pkg.BrokerID, pkg.PackageID)), zap.Any("package", pkg))
	return nil
}

func (s *Server) handleAddPackage(w http.ResponseWriter, r *http.Request) {

	// if rand.Float64() < s.dropRate {
	// 	http.Error(w, "simulated network failure", http.StatusServiceUnavailable)
	// 	return
	// }
	pkg := &commons.Package{}
	err := json.NewDecoder(r.Body).Decode(pkg)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	logger.Info("Received package from broker", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID))

	currHead := s.GetState(pkg.BrokerID)
	if currHead == nil {
		logger.Info("Package is missing")
		http.Error(w, "Package is missing", http.StatusBadRequest)
		return
	}

	if currHead.GetState() == commons.IgnoreBroker {

		logger.Info("Dropping package since ignoreBroker is set", zap.String("brokerID", fmt.Sprintf("%d", pkg.BrokerID)))
		http.Error(w, fmt.Sprintf("Dropping package for brokerID: %d, since IgnoreBroker state is set", pkg.BrokerID), http.StatusBadRequest)
		return
	}

	if currHead.GetState() == commons.Undefined {
		// Only allow saving the package if the state is undefined.
		// if ok := s.answer[pkg.BrokerID].UpdateState(currHead, commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})); ok {
		// 	s.packageArray.Put(pkg)
		// 	s.writeChan <- pkg
		// }
		// snapshot the values for this iteration
		p := pkg
		h := currHead

		go func(pkg *commons.Package, head *commons.StateNode) {
			if ok := s.answer[pkg.BrokerID].UpdateState(head,
				commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg}),
			); ok {
				s.packageArray.Put(pkg)
				s.writeChan <- pkg
			}
		}(p, h)
		w.WriteHeader(http.StatusOK)
		return
	}
	if currHead.GetState() == commons.NotReceived {
		http.Error(w, fmt.Sprintf("broker%d package state is set to NotReceived", pkg.BrokerID), http.StatusBadRequest)
	}
}

func (s *Server) handleRequestPackage(w http.ResponseWriter, r *http.Request) {
	// if rand.Float64() < s.dropRate {
	// 	http.Error(w, "simulated network failure", http.StatusServiceUnavailable)
	// 	return
	// }
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
		if ok := s.answer[brokerIDInt].UpdateState(currHead, commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil})); !ok {
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
	currHead := s.GetState(brokerIDInt)
	if currHead == nil {
		logger.Info("Package is missing")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if currHead.GetState() == commons.IgnoreBroker {
		logger.Info("Dropping package since ignoreBroker is set", zap.String("brokerID", fmt.Sprintf("%d", brokerIDInt)), zap.String("state", fmt.Sprintf("%d", currHead.GetState())))
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
	if err := json.Unmarshal(body, pkg); err != nil {
		logger.Error("error while unmarshalling broker info", zap.Error(err))
		http.Error(w, "Invalid JSON sent by broker", http.StatusBadRequest)
		return
	}

	logger.Info("Received package from broker ok request", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID))

	// if ok := s.answer[pkg.BrokerID].UpdateState(currHead, commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})); ok {
	// 	// If the package is successfully updated, we put it in the packageArray and write it to the write channel.

	// 	s.packageArray.Put(pkg)
	// 	s.writeChan <- pkg
	// }
	// snapshot the values for this iteration
	p := pkg
	h := currHead
	go func(pkg *commons.Package, head *commons.StateNode) {
		if ok := s.answer[pkg.BrokerID].UpdateState(head,
			commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg}),
		); ok {
			s.packageArray.Put(pkg)
			s.writeChan <- pkg
		}
	}(p, h)

	// Write the package to file
	// TODO: Remove this after testing.
	// if err := s.writePackage(pkg); err != nil {
	// 	logger.Error("error writing package to file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, pkg.BrokerID, pkg.PackageID)), zap.Error(err))
	// 	http.Error(w, "Error writing package to file", http.StatusInternalServerError)
	// 	return
	// }
	w.WriteHeader(http.StatusOK)
}

func (s *Server) registerServer() error {
	// Create JSON payload
	data := map[string]interface{}{
		"ip":         s.ip,
		"port":       s.port,
		"readerport": s.readerPort,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s%s", s.directoryAddr, commons.DIRECTORY_REGISTER_SERVER), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("error sending request to directory: %v, could not start server", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read response
	body, _ := io.ReadAll(resp.Body)

	if err := json.Unmarshal(body, s.DirectoryInfo); err != nil {
		return fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	logger.Info("Directory info received", zap.String("directoryInfo", string(body)))

	// Since we locking the directory service, we can safely assume that the version is the same as the serverID
	s.serverID = s.DirectoryInfo.ServerMap.Version
	logger.Info("Server registered", zap.Int("serverID", s.serverID))
	return nil
}

func (s *Server) setupServer() (*http.Server, error) {
	if err := s.registerServer(); err != nil {
		return nil, fmt.Errorf("error registering broker: %v", err)
	}
	err := commons.BroadcastNodesInfo(logger, s.serverID, commons.ServerType, s.DirectoryInfo)
	if err != nil {
		logger.Error("error while broadcasting directory info", zap.Error(err))
	}
	r := mux.NewRouter()
	r.Use(s.MetricsMiddleware)
	r.HandleFunc(commons.SERVER_UPDATE_DIRECTORY, commons.HandleUpdateDirectory(logger, s.DirectoryInfo)).Methods("POST")
	r.HandleFunc(commons.SERVER_ADD_PACKAGE, s.handleAddPackage).Methods("POST")
	r.HandleFunc(commons.SERVER_REQUEST_PACKAGE, s.handleRequestPackage).Methods("GET")

	//r.HandleFunc(commons.SERVER_READ_STORAGE, s.handleReadPackageStorage).Methods("GET")

	r.HandleFunc(commons.SERVER_IGNORE_BROKER, s.handleIgnoreBroker).Methods("POST")
	r.HandleFunc(commons.SERVER_BROKER_OK, s.handleBrokerOk).Methods("POST")

	// Prometheus metrics
	initMetrics(s.serverID)
	r.Path("/metrics").Handler(promhttp.Handler())

	// // Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: r,
	}, nil
}

// have another func like this to call new service.
func (s *Server) requestPackages(endpoint string, brokerList []int) {
	var wg sync.WaitGroup
	for _, brokerID := range brokerList {
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
				serverURL := fmt.Sprintf("http://%s:%d%s", serverMap.Data[serverID].IP, serverMap.Data[serverID].Port, endpoint)
				pkg, err := s.requestPackageWithRetry(serverURL, brokerID)
				if err != nil {
					logger.Error("error while requesting package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
					return
				}

				// If package received, update it as such in the answer array and push it to the package array.
				if ok := s.answer[brokerID].UpdateState(s.answer[brokerID].GetHead(), commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})); ok {
					s.packageArray.Put(pkg)
					s.writeChan <- pkg
				}

				logger.Info("Received package from server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("package", pkg))
			}(serverID)
		}
	}
	wg.Wait()
}

func (s *Server) requestPackagesFromStorage(endpoint string, brokerList []int) {
	var wg sync.WaitGroup
	for _, brokerID := range brokerList {
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
				serverURL := fmt.Sprintf("http://%s:%d%s", serverMap.Data[serverID].IP, serverMap.Data[serverID].ReaderPort, endpoint)
				currHead := s.answer[brokerID].GetHead()
				logger.Info("State state state", zap.Any("ans", s.answer))
				logger.Info("State state state", zap.Any("currhead", currHead))
				packageID := s.packageCounter
				// if currHead != nil {
				// 	packageID = currHead.GetPackage().PackageID
				// }
				pkg, err := s.requestPackageFromStorageWithRetry(serverURL, brokerID, packageID)
				if err != nil {
					logger.Error("error while requesting package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
					return
				}

				// If package received, update it as such in the answer array and push it to the package array.
				if ok := s.answer[brokerID].UpdateState(s.answer[brokerID].GetHead(), commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})); ok {
					s.packageArray.Put(pkg)
					s.writeChan <- pkg
				}

				logger.Info("Received package from server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("package", pkg))
			}(serverID)
		}
	}
	wg.Wait()
}

func (s *Server) sendIgnoreBroker(serverURL string) func() error {
	return func() error {
		// client := &http.Client{
		// 	Timeout: commons.SERVER_REQUEST_TIMEOUT,
		// }
		client := makeInstrumentedClient("send-ignore-broker")
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

func (s *Server) sendBrokerOkWithRetry(serverURL string, pkg *commons.Package) error {
	// Encode package to JSON
	jsonData, err := json.Marshal(pkg)
	if err != nil {
		logger.Error("Failed to encode package to JSON", zap.Error(err))
		return fmt.Errorf("error encoding JSON: %s", err)
	}
	return retry.Do(
		s.sendPackage(serverURL, jsonData),
		retry.Attempts(2),                 // Number of retry attempts
		retry.Delay(0),                    // No delay
		retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying broker ok request", zap.String("serverURL", serverURL), zap.Int("brokerID", pkg.BrokerID), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
}

func (s *Server) sendIgnoreBrokerWithRetry(serverURL string) error {
	return retry.Do(
		s.sendIgnoreBroker(serverURL),
		retry.Attempts(commons.SERVER_RETRY), // Number of retry attempts
		retry.Delay(0),                       // No delay
		retry.DelayType(retry.FixedDelay),    // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying ignore broker request", zap.String("serverURL", serverURL), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
}

func (s *Server) sendBrokerOkRequests(brokerList []int) {
	var wg sync.WaitGroup
	for _, brokerID := range brokerList {
		brokerID := brokerID
		serverMap := s.DirectoryInfo.ServerMap
		for serverID := range serverMap.Data {
			// Skip the current server
			if serverID == s.serverID {
				continue
			}

			currHead := s.answer[brokerID].GetHead()
			pkg := currHead.GetPackage()

			serverID := serverID
			serverURL := fmt.Sprintf("http://%s:%d%s?brokerID=%d", serverMap.Data[serverID].IP, serverMap.Data[serverID].Port, commons.SERVER_BROKER_OK, brokerID)
			wg.Add(1)
			go func(serverURL string, brokerID int) {
				defer wg.Done()
				err := s.sendBrokerOkWithRetry(serverURL, pkg)
				if err != nil {
					logger.Error("error while sending ignore broker request", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
					return
				}
				logger.Info("Broker ok request sent to server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID))
			}(serverURL, brokerID)
		}
	}
	wg.Wait()
}

func (s *Server) sendIgnoreBrokerRequests(brokerList []int) {
	var wg sync.WaitGroup
	for _, brokerID := range brokerList {
		brokerID := brokerID
		serverMap := s.DirectoryInfo.ServerMap
		for serverID := range serverMap.Data {
			// Skip the current server
			if serverID == s.serverID {
				continue
			}
			serverID := serverID
			serverURL := fmt.Sprintf("http://%s:%d%s?brokerID=%d", serverMap.Data[serverID].IP, serverMap.Data[serverID].Port, commons.SERVER_IGNORE_BROKER, brokerID)
			wg.Add(1)
			go func(serverURL string, brokerID int) {
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

func (s *Server) sendCurrentServerIDWithRetry(serverURL string) error {
	return retry.Do(
		s.sendServerID(serverURL),
		retry.Attempts(commons.SERVER_RETRY), // Number of retry attempts
		retry.Delay(0),                       // No delay
		retry.DelayType(retry.FixedDelay),    // Use fixed delay strategy
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying broker ok request", zap.String("serverURL", serverURL), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
}

func (s *Server) sendServerID(serverURL string) func() error {
	return func() error {
		// client := &http.Client{
		// 	Timeout: commons.SERVER_REQUEST_TIMEOUT,
		// }
		client := makeInstrumentedClient("send-server-id")

		// Create a new HTTP request to request package
		req, err := http.NewRequest("GET", serverURL, nil)
		if err != nil {
			panic(err)
		}

		// Set headers for the request
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(s.serverID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.ServerType))

		resp, err := client.Do(req)
		if err != nil {
			logger.Error("Error sending request to send ID", zap.String("serverURL", serverURL), zap.Error(err))
			return fmt.Errorf("error sending request to send ID: %s", err)
		}

		if resp.StatusCode == http.StatusOK {
			// Process the package
			logger.Info("Received Ack from Read server", zap.String("serverURL", serverURL))
			return nil
		} else {
			logger.Error("Unexpected status code", zap.String("serverURL", serverURL), zap.Int("statusCode", resp.StatusCode))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
}

func (s *Server) sendCurrentServerID() {
	var wg sync.WaitGroup
	serverID := s.serverID
	serverURL := fmt.Sprintf("http://%s:%d%s?ServerID=%d", s.ip, s.readerPort, commons.READING_SERVER_REGISTER_ID, serverID)
	wg.Add(1)
	go func(serverURL string) {
		defer wg.Done()
		err := s.sendCurrentServerIDWithRetry(serverURL)
		if err != nil {
			logger.Error("error while sending id to read server", zap.String("serverURL", serverURL), zap.Error(err))
			return
		}
		logger.Info("Ignore broker request sent to server", zap.String("serverURL", serverURL))
	}(serverURL)
	wg.Wait()
}

func (s *Server) protocolDaemon(nextRun time.Time) {
	// Daemon routine to run the protocol every epoch period.
	s.packageCounter = 0
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

		if s.packageCounter == 0 {
			s.setupAnswerMap()
		}
		s.packageCounter++
		epochNumber.WithLabelValues().Set(float64(s.packageCounter))
		start_time := time.Now()
		timer := prometheus.NewTimer(protocolDuration.WithLabelValues())
		err := commons.BroadcastNodesInfo(logger, s.serverID, commons.ServerType, s.DirectoryInfo)
		if err != nil {
			logger.Error("error while broadcasting directory info", zap.Error(err))
		}
		// Setup the answer map for fresh epoch.
		// s.setupAnswerMap()
		// s.packageCounter++
		// err := commons.BroadcastNodesInfo(logger, s.serverID, commons.ServerType, s.DirectoryInfo)
		// if err != nil {
		// 	logger.Error("error while broadcasting directory info", zap.Error(err))
		// }

		logger.Info("Step 1: Answer map is setup, waiting for packages...")

		// Wait for Brokers to send packages.

		// We do this instead of time.After because we don't know how long the Broadcast Nodes Info will take.
		for innerTc := range ticker.C {
			if innerTc.Equal(waitPackage) || innerTc.After(waitPackage) {
				break
			}
		}
		logger.Info("Waiting time is up, checking for received packages...")

		// Update the state of packages that are not received.
		s.updateNotReceivedPackage()

		firstPendingBrokerList := s.getBrokerList(commons.NotReceived)
		logger.Info("Missing Packages", zap.Any("list", firstPendingBrokerList))

		logger.Info("Step 2: Requesting packages from other servers...")
		s.requestPackages(commons.SERVER_REQUEST_PACKAGE, firstPendingBrokerList)

		// Request package from the memory of other servers.
		// Get the list of brokers for which we have still not received the package.
		pendingBrokerList := s.getBrokerList(commons.NotReceived)
		logger.Info("Missing Packages", zap.Any("list", pendingBrokerList))

		logger.Info("Step 3: Requesting packages from storage of other servers...")
		logger.Info("Missing Packages", zap.Any("list", pendingBrokerList))
		// ANAND TODO: call new service here
		s.requestPackagesFromStorage(commons.SERVER_READ_STORAGE, pendingBrokerList)

		pendingBrokerList = s.getBrokerList(commons.NotReceived)
		completedBrokerList := s.getBrokerList(commons.Received)

		brokerPackagesFromServer := findSetDifference(firstPendingBrokerList, completedBrokerList)
		logger.Info("Step 4 : Sending broker ok request to other with servers ( with packages )")
		s.sendBrokerOkRequests(brokerPackagesFromServer)

		logger.Info("Complete Packages", zap.Any("list", completedBrokerList))
		for _, brokerID := range pendingBrokerList {
			if s.answer[brokerID].GetHead().GetState() == commons.NotReceived {
				// We want to ignore the broker if the package is not received.
				if ok := s.answer[brokerID].UpdateState(s.answer[brokerID].GetHead(), commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil})); ok {
					logger.Info("Ignoring broker", zap.Int("brokerID", brokerID))
				}
			}
		}

		// Send ignore broker requests to other servers.
		logger.Info("Sending ignore broker requests to other servers for brokers", zap.Any("brokers", pendingBrokerList))
		s.sendIgnoreBrokerRequests(pendingBrokerList)
		timer.ObserveDuration()
		duration := time.Since(start_time).Seconds()
		epochDuration.WithLabelValues(strconv.Itoa(s.packageCounter)).Set(duration)
		logger.Info("Protocol Completed")
		logger.Info("setting up answer map for next epoch")
		s.setupAnswerMap()
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
	var readerPort int

	flag.StringVar(&serverIP, "serverIP", "", "IP of current Server")
	flag.IntVar(&serverPort, "serverPort", 0, "Port of Server")
	flag.IntVar(&readerPort, "readerPort", 0, "Port of Reader Service")

	var startTimestamp int64
	flag.Int64Var(&startTimestamp, "startTimestamp", 0, "Start timestamp")

	var dropRate float64
	flag.Float64Var(&dropRate, "dropRate", 0, "drop rate")
	rand.Seed(time.Now().UnixNano())

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

	if readerPort == 0 {
		fmt.Printf("readerPort is required")
		return
	}

	if startTimestamp == 0 {
		fmt.Printf("startTimestamp is required")
		return
	}

	// err := os.Mkdir("./logs", 0755)
	// if err != nil {
	// 	fmt.Printf("failed to create logs directory: %v", err)
	// 	return
	// }
	err := os.Mkdir("./logs", 0755)
	if err != nil && !os.IsExist(err) {
		// Only log or handle real errors
		panic(err)
	}
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFile, // Log file path
		MaxSize:    10,      // Max megabytes before log is rotated
		MaxBackups: 5,       // Max old log files to keep
		MaxAge:     28,      // Max days to retain old log files
		Compress:   true,    // Compress old files (.gz)
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
	logger = logger.With(zap.String("service", fmt.Sprintf("server %s", serverIP)))

	defer logger.Sync()

	server := &Server{
		ip:             serverIP,
		port:           serverPort,
		readerPort:     readerPort,
		directoryAddr:  fmt.Sprintf("http://%s:%d", directoryIP, directoryPort),
		packageCounter: 0,
		DirectoryInfo:  &commons.NodesMap{},
		answer:         make(map[int]*commons.StateList),
		packageArray:   queue.NewRingBuffer(BUFFER_SIZE),
		writeChan:      make(chan *commons.Package, WRITE_BUFFER_SIZE),
		dropRate:       dropRate,
	}

	httpServer, err := server.setupServer()
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
	// start new service
	// send ID to our reader service
	server.sendCurrentServerID()
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
