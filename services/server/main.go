package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"lockfreemachine/src/pkg/commons"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	BUFFER_SIZE = 100000

	// Maximum number of packages the write channel can hold.
	WRITE_BUFFER_SIZE = 1000
)

var (
	httpRequests     *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	apiCallDuration  *prometheus.HistogramVec
	epochNumber      *prometheus.GaugeVec
	protocolDuration *prometheus.HistogramVec
	epochDuration    prometheus.Gauge
	epochDuration1   *prometheus.GaugeVec
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
			Name:    "api_request_duration_seconds",
			Help:    "Duration of API requests in seconds",
			Buckets: prometheus.DefBuckets, // You can customize this
		},
		[]string{"path", "method", "srcType"}, // group by endpoint/method if you want
	)

	apiCallDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "outbound_api_call_duration_seconds",
			Help:    "Duration of outbound API calls in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "targetID"},
	)

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

	epochDuration = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "protocol_last_epoch_duration_in_seconds",
			Help: "Wall clock seconds taken by most recent protocol epoch run",
		},
	)

	epochDuration1 = prometheus.NewGaugeVec(
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
	wrappedRegisterer.MustRegister(epochDuration1)

}

// Middleware to measure duration with dynamic labels
func (s *Server) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		if rand.Float64() < s.dropRate {
			httpRequests.WithLabelValues(r.URL.Path, r.Method).Inc()
			http.Error(w, "simulated network failure", http.StatusServiceUnavailable)
			return
		}

		nodeID := r.Header.Get("X-NodeID")
		nodeType := r.Header.Get("X-NodeType")
		if nodeID == "" {
			nodeID = "unknown"
			nodeType = "unknown"
		}
		if nodeType == "" {
			nodeType = "unknown"
		}

		httpRequests.WithLabelValues(r.URL.Path, r.Method).Inc()
		start := time.Now()

		// Serve the real handler
		next.ServeHTTP(w, r)

		duration := time.Since(start).Seconds()

		requestDuration.WithLabelValues(r.URL.Path, r.Method, nodeType).Observe(duration)
	})
}

var sharedTr = &http.Transport{
	MaxIdleConns:        512,
	MaxIdleConnsPerHost: 128,
	IdleConnTimeout:     90 * time.Second,
	MaxConnsPerHost:     256,
	ForceAttemptHTTP2:   true,
	// Keep per-hop timeouts tight; the per-attempt context is the main guard.
	DialContext: (&net.Dialer{
		Timeout:   commons.SERVER_PER_ATTEMPT_TIMEOUT, // connect must fit per attempt
		KeepAlive: 30 * time.Second,
	}).DialContext,

	TLSHandshakeTimeout:   commons.SERVER_PER_ATTEMPT_TIMEOUT,
	ResponseHeaderTimeout: commons.SERVER_PER_ATTEMPT_TIMEOUT, // time to first byte
	ExpectContinueTimeout: 50 * time.Millisecond,
}

func makeInstrumentedClient(targetID string) *http.Client {
	// Curry the targetID label, leaving method to be filled per-request
	curried := apiCallDuration.MustCurryWith(prometheus.Labels{
		"targetID": targetID,
	})

	// Wrap the default transport
	instrumentedTransport := promhttp.InstrumentRoundTripperDuration(
		curried,  // ObserverVec for apiCallDuration
		sharedTr, // underlying RoundTripper
	)

	return &http.Client{
		Transport: instrumentedTransport,
		Timeout:   commons.SERVER_OVERALL_TIMEOUT,
	}
}

func (s *Server) startWriters() {
	err := os.MkdirAll(fmt.Sprintf("./packages_%d", s.serverID), os.ModePerm)
	if err != nil {
		logger.Error("Failed to create folder", zap.Error(err))
		return
	}
	for i := 0; i < 4; i++ {
		go func(id int) {
			for pkg := range s.writeChan {
				if err := s.writePackage(pkg); err != nil {
					logger.Error("write failed", zap.Int("writer", id), zap.Error(err))
				}
			}
		}(i)
	}
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
	packageCounter int                   // Address of the directory service
	DirectoryInfo  *commons.NodesMap     // Directory information
	serverID       int                   // Server ID
	writeChan      chan *commons.Package // Channel to write packages to files
	//answer            map[int]*commons.StateList // Map of broker IDs to their states
	packageArray      *queue.RingBuffer // Array to hold packages
	dropRate          float64           // api requests
	packageCountStart int
	startTimestamp    time.Time
	//mu                sync.RWMutex
	canReceive bool
	answer0    map[int]*commons.StateList
	answer1    map[int]*commons.StateList
	active     atomic.Pointer[map[int]*commons.StateList]
}

// requestPackage sends a request to the server to get a package.
func (s *Server) requestPackage(serverURL string, brokerID int, pkg *commons.Package) func() error {
	return func() error {
		client := makeInstrumentedClient("request-package")
		ctx, cancel := context.WithTimeout(context.Background(), commons.SERVER_PER_ATTEMPT_TIMEOUT)
		defer cancel()
		// Create a new HTTP request to request package
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s?brokerID=%d", serverURL, brokerID), nil)
		if err != nil {
			// logger.Error("oi panic panic requestPackage")
			// panic(err)
			logger.Error("Error creating request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
			return fmt.Errorf("error creating request to get package: %s", err)
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
			logger.Warn("Received package from server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("packageID", pkg.PackageID))
			return nil
		}

		if resp.StatusCode == http.StatusNotFound {
			defer io.Copy(io.Discard, resp.Body)
			logger.Error("Package not found", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("statusCode", resp.StatusCode))
			return fmt.Errorf("notfound: package not found at %s", serverURL)
		} else {
			defer io.Copy(io.Discard, resp.Body)
			logger.Error("Unexpected status code", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("statusCode", resp.StatusCode))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
}

func (s *Server) requestPackageFromStorage(serverURL string, brokerID int, packageID int, pkg *commons.Package) func() error {
	return func() error {
		client := makeInstrumentedClient("request-package-storage")

		ctx, cancel := context.WithTimeout(context.Background(), commons.SERVER_PER_ATTEMPT_TIMEOUT)
		defer cancel()
		// Create a new HTTP request to request package
		req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s?brokerID=%d&packageID=%d", serverURL, brokerID, packageID), nil)
		if err != nil {
			logger.Error("oi panic panic requestpackage from storage")
			return fmt.Errorf("error creating request to get package from storage: %s", err)
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
			logger.Warn("Received package from storage server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID))
			return nil
		} else {
			logger.Error("Unexpected status code", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("packageID", packageID), zap.Int("statusCode", resp.StatusCode))
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}
}

func (s *Server) sendPackage(serverURL string, jsonData []byte) func() error {
	return func() error {
		client := makeInstrumentedClient("server-send-package")

		ctx, cancel := context.WithTimeout(context.Background(), commons.SERVER_PER_ATTEMPT_TIMEOUT)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "POST", serverURL, bytes.NewBuffer(jsonData))
		if err != nil {
			logger.Error("oi panic panic sendPackage")
			return fmt.Errorf("error creating request to send package: %s", err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(s.serverID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.ServerType))

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error sending request to %s: %s", serverURL, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body: %v", err)
		}
		bodyString := string(body)

		if (resp.StatusCode != http.StatusOK) && (resp.StatusCode != http.StatusAccepted) {
			return fmt.Errorf("unexpected status code: %d for server %s: error: %s", resp.StatusCode, serverURL, bodyString)
		}
		return nil
	}
}

func (s *Server) requestPackageWithRetry(serverURL string, brokerID int) (*commons.Package, error) {
	pkg := &commons.Package{}
	err := retry.Do(
		s.requestPackage(serverURL, brokerID, pkg),
		retry.Attempts(commons.SERVER_RETRY_ATTEMPTS),  // Number of retry attempts
		retry.Delay(2*time.Millisecond),                // 2ms delay
		retry.DelayType(retry.RandomDelay),             // Use random delay strategy
		retry.MaxDelay(commons.SERVER_RETRY_MAX_DELAY), // cap at 5ms
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
		retry.RetryIf(func(err error) bool {
			// Do not retry if it's a 404
			if strings.HasPrefix(err.Error(), "notfound:") {
				return false
			}
			return true
		}),
	)
	return pkg, err
}

func (s *Server) requestPackageFromStorageWithRetry(serverURL string, brokerID int, packageID int) (*commons.Package, error) {
	pkg := &commons.Package{}
	err := retry.Do(
		s.requestPackageFromStorage(serverURL, brokerID, packageID, pkg),
		retry.Attempts(commons.SERVER_RETRY_ATTEMPTS),  // Number of retry attempts
		retry.Delay(2*time.Millisecond),                // 2ms delay
		retry.DelayType(retry.RandomDelay),             // Use random delay strategy
		retry.MaxDelay(commons.SERVER_RETRY_MAX_DELAY), // cap at 5ms
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying request to get package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
	return pkg, err
}

// func (s *Server) setupAnswerMap() {

// 	for k := range s.DirectoryInfo.BrokerMap.Data {
// 		s.mu.RLock()
// 		sl := s.answer[k]
// 		s.mu.RUnlock()
// 		if sl == nil {
// 			s.mu.Lock()
// 			if s.answer[k] == nil { // double-check after acquiring the lock
// 				s.answer[k] = commons.NewStateList()
// 			}
// 			sl = s.answer[k]
// 			s.mu.Unlock()
// 		}
// 		head := sl.GetHead()
// 		if head == nil || head.GetState() == commons.IgnoreBroker {
// 			continue
// 		}
// 		sl.ReplaceState(&commons.StateValue{State: commons.Undefined, Pkg: nil})
// 		// newHead := commons.NewStateNode(&commons.StateValue{
// 		//     State: commons.Undefined,
// 		// })
// 		// sl.UpdateState(head, newHead)
// 	}

// }

func (s *Server) initAnswerBuffers() {
	s.answer0 = make(map[int]*commons.StateList, len(s.DirectoryInfo.BrokerMap.Data))
	s.answer1 = make(map[int]*commons.StateList, len(s.DirectoryInfo.BrokerMap.Data))

	// Pre-create keys in both maps so you never change map structure at runtime.
	for k := range s.DirectoryInfo.BrokerMap.Data {
		s.answer0[k] = commons.NewStateList()
		s.answer1[k] = commons.NewStateList()
	}

	// Pick one as the initial active buffer
	s.active.Store(&s.answer0)
}

// Reset/prepare a target buffer (the inactive one)
func (s *Server) setupAnswerMapOn(target map[int]*commons.StateList) {
	// Reset states without touching keys (so no map writes)
	for k := range s.DirectoryInfo.BrokerMap.Data {
		sl := target[k]
		if sl == nil {
			// Shouldn't happen if you prefilled both maps, but be defensive.
			sl = commons.NewStateList()
			target[k] = sl
		}

		head := sl.GetHead()
		if head == nil || head.GetState() == commons.IgnoreBroker {
			continue
		}
		sl.ReplaceState(&commons.StateValue{State: commons.Undefined, Pkg: nil})
	}
}

// Flip active buffer on epoch change.
// If epoch is even use answer0, if odd use answer1 (as you wanted).
func (s *Server) flipEpoch(epoch uint64) {
	var next *map[int]*commons.StateList
	if epoch%2 == 0 {
		next = &s.answer0
	} else {
		next = &s.answer1
	}

	// Prepare the buffer that will become active.
	s.setupAnswerMapOn(*next)

	// Atomic publish: readers will immediately see the new map pointer.
	s.active.Store(next)
}

func (s *Server) currentAnswer() map[int]*commons.StateList {
	p := s.active.Load()
	if p == nil {
		return nil
	}
	return *p
}

func (s *Server) updateNotReceivedPackage() {
	for _, stateList := range s.currentAnswer() {
		if stateList == nil {
			continue
		}
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
	for brokerID, stateList := range s.currentAnswer() {
		if stateList == nil {
			continue
		}
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

// getStateList safely fetches the StateList for a broker.
func (s *Server) getStateList(brokerID int) (*commons.StateList, bool) {
	sl, ok := s.currentAnswer()[brokerID]
	return sl, ok && sl != nil
}

func (s *Server) GetState(brokerID int) *commons.StateNode {
	stateList, ok := s.getStateList(brokerID)
	if !ok {
		return nil
	}
	currHead := stateList.GetHead()

	return currHead
}

// TODO: Use update state instead of replace state?
// func (s *Server) setIgnoreInBoth(brokerID int) {
// 	for _, m := range []map[int]*commons.StateList{s.answer0, s.answer1} {
// 		if sl := m[brokerID]; sl != nil {
// 			sl.ReplaceState(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil})
// 		}
// 	}
// }

func (s *Server) setIgnoreInBoth(brokerID int) {
	for _, m := range []map[int]*commons.StateList{s.answer0, s.answer1} {
		if sl := m[brokerID]; sl != nil {
			//sl.ReplaceState(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil})
			sl.UpdateState(sl.GetHead(),
				commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil}),
			)
		}
	}
}

func (s *Server) writePackage(pkg *commons.Package) error {
	fpath := fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, pkg.BrokerID, pkg.PackageID)

	file, err := os.OpenFile(fpath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			logger.Info("File exists, skipping", zap.String("filePath", fpath))
			return nil
		}
		return fmt.Errorf("create %s: %w", fpath, err)
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
		logger.Error("Failed to write JSON data to file", zap.String("filePath", fpath), zap.Error(err))
		return fmt.Errorf("error writing to file: %s", err)
	}

	logger.Info("Package written to file", zap.String("filePath", fpath), zap.Any("package", pkg))
	return nil
}

func (s *Server) handleAddPackage(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	pkg := &commons.Package{}
	err := json.NewDecoder(r.Body).Decode(pkg)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	logger.Warn("Received package from broker", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID))

	if s.packageCounter > pkg.PackageID {
		logger.Warn("Received package with packageID less than current packageCounter, ignoring", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID), zap.Int("currentPackageCounter", s.packageCounter))
		//http.Error(w, "Stale package, ignoring", http.StatusAccepted)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	currHead := s.GetState(pkg.BrokerID)
	if currHead == nil {
		//logger.Info("Package is missing")
		//http.Error(w, "Package is missing", http.StatusBadRequest)
		http.Error(w, "Package is missing", http.StatusAccepted)
		return
	}

	if currHead.GetState() == commons.IgnoreBroker {

		//logger.Info("Dropping package since ignoreBroker is set", zap.String("brokerID", fmt.Sprintf("%d", pkg.BrokerID)))
		//http.Error(w, fmt.Sprintf("Dropping package for brokerID: %d, since IgnoreBroker state is set", pkg.BrokerID), http.StatusBadRequest)
		http.Error(w, fmt.Sprintf("Dropping package for brokerID: %d, since IgnoreBroker state is set", pkg.BrokerID), http.StatusAccepted)
		return
	}

	if currHead.GetState() == commons.Received {
		logger.Warn("Package is already received", zap.String("brokerID", fmt.Sprintf("%d", pkg.BrokerID)), zap.String("state", fmt.Sprintf("%d", currHead.GetState())))
		w.WriteHeader(http.StatusOK)
		return
	}

	if currHead.GetState() == commons.NotReceived {
		logger.Warn("Package state is set to not received", zap.String("brokerID", fmt.Sprintf("%d", pkg.BrokerID)), zap.String("state", fmt.Sprintf("%d", currHead.GetState())))
		//http.Error(w, fmt.Sprintf("broker%d package state is set to NotReceived", pkg.BrokerID), http.StatusBadRequest)
		//http.Error(w, "Package state is set to not received", http.StatusAccepted)
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if currHead.GetState() == commons.Undefined {
		sl, ok := s.getStateList(pkg.BrokerID)
		if !ok {
			http.Error(w, "Unknown brokerID", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		p := pkg
		h := currHead

		go func(pkg *commons.Package, head *commons.StateNode, sl *commons.StateList) {
			if ok := sl.UpdateState(head,
				commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg}),
			); ok {
				s.packageArray.Put(pkg)
				s.writeChan <- pkg
			}
		}(p, h, sl)
		return
	}
}

func (s *Server) handleRequestPackage(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	params := r.URL.Query()
	brokerID := params.Get("brokerID")
	logger.Info("Received request for package", zap.String("brokerID", brokerID))
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}
	sl, ok := s.getStateList(brokerIDInt)
	if !ok {
		http.Error(w, "Unknown brokerID", http.StatusNotFound)
		return
	}
	currHead := sl.GetHead()
	if currHead.GetState() == commons.Received {
		// We assume that the package is already in the packageArray
		pkg := currHead.GetPackage()
		if err := json.NewEncoder(w).Encode(pkg); err != nil {
			logger.Error("error encoding JSON response", zap.Error(err))
			//http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}
		// Perform necessary operations with packageID and brokerID
		return
	}

	logger.Warn("Package is not in received state", zap.Int("brokerID", brokerIDInt))
	http.Error(w, "Package is not present", http.StatusNotFound)
}

func (s *Server) handleIgnoreBroker(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	params := r.URL.Query()
	brokerID := params.Get("brokerID")

	logger.Warn("Received ignore broker request", zap.String("brokerID", brokerID))

	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}
	sl, ok := s.getStateList(brokerIDInt)
	if !ok {
		http.Error(w, "Unknown brokerID", http.StatusNotFound)
		return
	}
	currHead := sl.GetHead()
	if currHead.GetState() == commons.NotReceived || currHead.GetState() == commons.Undefined {
		// if ok := sl.UpdateState(currHead, commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil})); !ok {
		// 	logger.Error("error updating state for brokerID", zap.Int("brokerID", brokerIDInt))
		// 	http.Error(w, "Error updating state", http.StatusBadRequest)
		// 	return
		// }
		w.WriteHeader(http.StatusOK)
		go s.setIgnoreInBoth(brokerIDInt)
		return
	}
	http.Error(w, "Couldn't set broker state to ignoreBroker ", http.StatusBadRequest)
}

func (s *Server) handleBrokerOk(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() // Close body after reading
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
		logger.Warn("Handle broker Ok: BrokerID is missing")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if currHead.GetState() == commons.IgnoreBroker {
		logger.Warn("Dropping package since ignoreBroker is set", zap.String("brokerID", fmt.Sprintf("%d", brokerIDInt)), zap.String("state", fmt.Sprintf("%d", currHead.GetState())))
		w.WriteHeader(http.StatusOK)
		return
	}

	if currHead.GetState() == commons.Received {
		logger.Warn("Package is already received", zap.String("brokerID", fmt.Sprintf("%d", brokerIDInt)), zap.String("state", fmt.Sprintf("%d", currHead.GetState())))
		w.WriteHeader(http.StatusOK)
		return
	}

	body, err := io.ReadAll(r.Body)
	// defer r.Body.Close() // Close body after reading
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}
	pkg := &commons.Package{}
	if err := json.Unmarshal(body, pkg); err != nil {
		logger.Error("error while unmarshalling broker info", zap.Error(err))
		http.Error(w, "Invalid JSON sent by broker", http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
	logger.Info("Received package from broker ok request", zap.Int("brokerID", pkg.BrokerID), zap.Int("packageID", pkg.PackageID))

	p := pkg
	h := currHead
	go func(pkg *commons.Package, head *commons.StateNode) {
		if sl, ok := s.getStateList(pkg.BrokerID); ok {
			head := sl.GetHead()
			if head != nil && head.GetState() == commons.NotReceived {
				if sl.UpdateState(sl.GetHead(), commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})) {
					s.packageArray.Put(pkg)
					s.writeChan <- pkg
				}
			}
		}
	}(p, h)
}

type registerResp struct {
	Timestamp   time.Time        `json:"timestamp"` // nanoseconds
	EpochNumber int64            `json:"epochNumber"`
	NodesInfo   commons.NodesMap `json:"nodesInfo"`
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
		defer io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read response
	body, _ := io.ReadAll(resp.Body)

	var respStruct registerResp

	if err := json.Unmarshal(body, &respStruct); err != nil {
		logger.Error("Error unmarshalling JSON", zap.Error(err))
		return fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	//ETime := time.Unix(0, respStruct.Timestamp)
	ETime := respStruct.Timestamp
	ENumber := respStruct.EpochNumber
	s.DirectoryInfo = &respStruct.NodesInfo

	logger.Info("Directory info received", zap.Any("directoryInfo", s.DirectoryInfo))
	s.serverID = s.DirectoryInfo.ServerMap.Version
	s.startTimestamp = ETime
	s.packageCountStart = int(ENumber)
	logger.Info("Server registered", zap.Int("serverID", s.serverID))
	if ENumber > 0 {
		logger.Info("Server is recovered", zap.Int("serverID", s.serverID))
	}
	return nil
}

func (s *Server) setupServer() (*http.Server, error) {
	if err := s.registerServer(); err != nil {
		return nil, fmt.Errorf("error registering broker: %v", err)
	}
	err := commons.BroadcastNodesInfo(logger, s.serverID, commons.ServerType, s.DirectoryInfo, sharedTr)
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

				if sl, ok := s.getStateList(brokerID); ok {
					head := sl.GetHead()
					if head != nil && head.GetState() == commons.NotReceived {
						if sl.UpdateState(sl.GetHead(), commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})) {
							s.packageArray.Put(pkg)
							s.writeChan <- pkg
						}
					}

				}

				logger.Warn("Received package from server", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("package", pkg))
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
				packageID := s.packageCounter
				pkg, err := s.requestPackageFromStorageWithRetry(serverURL, brokerID, packageID)
				if err != nil {
					logger.Error("error while requesting package", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Error(err))
					return
				}
				// If package received, update it as such in the answer array and push it to the package array.
				if sl, ok := s.getStateList(brokerID); ok {
					head := sl.GetHead()
					if head != nil && head.GetState() == commons.NotReceived {
						if sl.UpdateState(head, commons.NewStateNode(&commons.StateValue{State: commons.Received, Pkg: pkg})) {
							s.packageArray.Put(pkg)
							s.writeChan <- pkg
						}
					}

				}

				logger.Warn("Received package from server storage", zap.String("serverURL", serverURL), zap.Int("brokerID", brokerID), zap.Any("package", pkg))
			}(serverID)
		}
	}
	wg.Wait()
}

func (s *Server) sendIgnoreBroker(serverURL string) func() error {
	return func() error {
		client := makeInstrumentedClient("send-ignore-broker")
		ctx, cancel := context.WithTimeout(context.Background(), commons.SERVER_PER_ATTEMPT_TIMEOUT)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "POST", serverURL, nil)
		if err != nil {
			logger.Info("oi panic panic send ignore broker")
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
		defer func() {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
		return nil
	}
}

func (s *Server) sendBrokerOkWithRetry(serverURL string, jsonData []byte, brokerid int) error {
	return retry.Do(
		s.sendPackage(serverURL, jsonData),
		retry.Attempts(commons.SERVER_RETRY_ATTEMPTS),  // Number of retry attempts
		retry.Delay(2*time.Millisecond),                // 2ms delay
		retry.DelayType(retry.RandomDelay),             // Use random delay strategy
		retry.MaxDelay(commons.SERVER_RETRY_MAX_DELAY), // cap at 5ms
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying broker ok request",
				zap.String("serverURL", serverURL),
				zap.Int("brokerID", brokerid),
				zap.Int("attempt", int(n)+1),
				zap.Error(err),
			)
		}),
	)
}

func (s *Server) sendIgnoreBrokerWithRetry(serverURL string) error {
	return retry.Do(
		s.sendIgnoreBroker(serverURL),
		retry.Attempts(commons.SERVER_RETRY_ATTEMPTS),  // Number of retry attempts
		retry.Delay(2*time.Millisecond),                // 2ms delay
		retry.DelayType(retry.RandomDelay),             // Use random delay strategy
		retry.MaxDelay(commons.SERVER_RETRY_MAX_DELAY), // cap at 5ms
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying ignore broker request", zap.String("serverURL", serverURL), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
}

func (s *Server) sendBrokerOkRequests(brokerList []int) {
	var wg sync.WaitGroup
	serverMap := s.DirectoryInfo.ServerMap
	for _, brokerID := range brokerList {
		sl, ok := s.getStateList(brokerID)
		if !ok {
			continue
		}
		if sl == nil {
			continue
		}
		currHead := sl.GetHead()
		if currHead == nil || currHead.GetState() != commons.Received {
			continue
		}
		pkg := currHead.GetPackage()
		if pkg == nil {
			continue
		}

		jsonData, err := json.Marshal(pkg)
		if err != nil {
			logger.Error("Failed to encode package to JSON", zap.Error(err))
			continue
		}
		for serverID := range serverMap.Data {
			// Skip the current server
			if serverID == s.serverID {
				continue
			}
			serverURL := fmt.Sprintf("http://%s:%d%s?brokerID=%d", serverMap.Data[serverID].IP, serverMap.Data[serverID].Port, commons.SERVER_BROKER_OK, brokerID)
			wg.Add(1)
			go func(serverURL string, brokerID int) {
				defer wg.Done()
				err := s.sendBrokerOkWithRetry(serverURL, jsonData, brokerID)
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
		retry.Attempts(commons.SERVER_RETRY_ATTEMPTS),  // Number of retry attempts
		retry.Delay(2*time.Millisecond),                // 2ms delay
		retry.DelayType(retry.RandomDelay),             // Use random delay strategy
		retry.MaxDelay(commons.SERVER_RETRY_MAX_DELAY), // cap at 5ms
		retry.OnRetry(func(n uint, err error) {
			logger.Error("Retrying broker ok request", zap.String("serverURL", serverURL), zap.Int("attempt", int(n)+1), zap.Error(err))
		}),
	)
}

func (s *Server) sendServerID(serverURL string) func() error {
	return func() error {
		client := makeInstrumentedClient("send-server-id")

		ctx, cancel := context.WithTimeout(context.Background(), commons.SERVER_PER_ATTEMPT_TIMEOUT)
		defer cancel()

		// Create a new HTTP request to request package
		req, err := http.NewRequestWithContext(ctx, "GET", serverURL, nil)
		if err != nil {
			logger.Info("oi panic panic sendServerID")
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
		defer func() {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()

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
		logger.Info("Sent current server ID to read server", zap.String("serverURL", serverURL))
	}(serverURL)
	wg.Wait()
}

func (s *Server) protocolDaemon(ctx context.Context, nextRun time.Time) {
	// Daemon routine to run the protocol every epoch period.
	s.packageCounter = s.packageCountStart
	// We want the ticker to have millisecond precision
	//ticker := time.NewTicker(time.Millisecond)
	logger.Info("Protocol Daemon started")
	logger.Warn("Next run will happen at: ", zap.Time("nextRun", nextRun))

	for {
		// Sleep (efficiently) until the next deadline or until we're asked to stop
		if d := time.Until(nextRun); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-t.C:
				// time to run
			case <-ctx.Done():
				t.Stop()
				logger.Error("Protocol daemon: context cancelled before next run")
				return
			}
		}

		waitPackage := nextRun.Add(commons.WAIT_FOR_BROKER_PACKAGE)
		//resetMaps := nextRun.Add(commons.EPOCH_PERIOD - 5*time.Millisecond)
		nextRun = nextRun.Add(commons.EPOCH_PERIOD)
		s.canReceive = true

		if s.packageCounter == 0 {
			s.initAnswerBuffers()
		} else {
			s.flipEpoch(uint64(s.packageCounter))
		}
		s.packageCounter++
		epochNumber.WithLabelValues().Set(float64(s.packageCounter))
		start := time.Now()
		promTimer := prometheus.NewTimer(protocolDuration.WithLabelValues())
		err := commons.BroadcastNodesInfo(logger, s.serverID, commons.ServerType, s.DirectoryInfo, sharedTr)
		if err != nil {
			logger.Error("error while broadcasting directory info", zap.Error(err))
		}

		logger.Warn("Step 1: Answer map is setup, waiting for packages...")

		// Wait for brokers until 'waitPackage' without polling
		if d := time.Until(waitPackage); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-t.C:
				// done waiting
			case <-ctx.Done():
				t.Stop()
				logger.Info("Protocol daemon: context cancelled during wait")
				promTimer.ObserveDuration()
				return
			}
		}

		logger.Warn("Waiting time is up, checking for received packages...")

		// Update the state of packages that are not received.
		s.updateNotReceivedPackage()

		firstPendingBrokerList := s.getBrokerList(commons.NotReceived)
		logger.Warn("Missing Packages", zap.Any("list", firstPendingBrokerList))

		logger.Warn("Step 2: Requesting packages from other servers...")
		s.requestPackages(commons.SERVER_REQUEST_PACKAGE, firstPendingBrokerList)

		// Request package from the memory of other servers.
		// Get the list of brokers for which we have still not received the package.
		pendingBrokerList := s.getBrokerList(commons.NotReceived)
		logger.Warn("Missing Packages", zap.Any("list", pendingBrokerList))

		logger.Warn("Step 3: Requesting packages from storage of other servers...")
		s.requestPackagesFromStorage(commons.SERVER_READ_STORAGE, pendingBrokerList)

		logger.Warn("Step 4 : Sending broker ok request to other with servers ( with packages )")
		pendingBrokerList = s.getBrokerList(commons.NotReceived)
		completedBrokerList := s.getBrokerList(commons.Received)
		logger.Info("Missing Packages", zap.Any("list", pendingBrokerList))
		// below 2 lines are where the problem's at
		brokerPackagesFromServer := findSetDifference(firstPendingBrokerList, pendingBrokerList)
		logger.Warn("brokerOk list", zap.Any("list", brokerPackagesFromServer))
		s.sendBrokerOkRequests(brokerPackagesFromServer)

		// TODO : check this
		pendingBrokerList = s.getBrokerList(commons.NotReceived)
		logger.Warn("Complete Packages", zap.Any("list", completedBrokerList))
		for _, brokerID := range pendingBrokerList {
			if sl, ok := s.getStateList(brokerID); ok {
				head := sl.GetHead()
				if head != nil && head.GetState() == commons.NotReceived {
					// We want to ignore the broker if the package is not received.
					// if ok := sl.UpdateState(head, commons.NewStateNode(&commons.StateValue{State: commons.IgnoreBroker, Pkg: nil})); ok {
					// 	logger.Info("Ignoring broker", zap.Int("brokerID", brokerID))
					// }
					s.setIgnoreInBoth(brokerID)
					logger.Info("Ignoring broker", zap.Int("brokerID", brokerID))
				}
			}
		}

		// Send ignore broker requests to other servers.
		logger.Warn("Sending ignore broker requests to other servers for brokers", zap.Any("brokers", pendingBrokerList))
		s.sendIgnoreBrokerRequests(pendingBrokerList)

		promTimer.ObserveDuration()
		duration := time.Since(start).Seconds()
		epochDuration1.WithLabelValues(strconv.Itoa(s.packageCounter)).Set(duration)
		epochDuration.Set(duration)
		// if d := time.Until(resetMaps); d > 0 {
		// 	t := time.NewTimer(d)
		// 	select {
		// 	case <-t.C:
		// 		// time to set up for next epoch (exactly ~5ms before end)
		// 	case <-ctx.Done():
		// 		t.Stop()
		// 		logger.Info("Protocol daemon: context cancelled before setupAnswerMap barrier")
		// 		promTimer.ObserveDuration()
		// 		return
		// 	}
		// } // if we're already past scheduleAt, fall through and run immediately

		// Do the switchover for the next epoch here
		//logger.Warn("setting up answer map for next epoch")
		s.canReceive = false // if you want to close reception 5ms early
		//s.setupAnswerMap()   // set up the answer map for the next epoch
		curLen := s.packageArray.Len()
		logger.Info("Protocol Completed")
		logger.Warn("Cur package array length: ", zap.Uint64("curLen", curLen))
		logger.Warn("Time time time: ", zap.Float64("duration", duration))
		logger.Warn("Next run will happen at: ", zap.Time("nextRun", nextRun))
		if s.packageCounter == 500 {
			logger.Warn("5000 epochs done, ending experiment")
			return
		}
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

	// var startTimestamp int64
	// flag.Int64Var(&startTimestamp, "startTimestamp", 0, "Start timestamp")

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

	err := os.Mkdir("./logs", 0755)
	if err != nil && !os.IsExist(err) {
		// Only log or handle real errors
		logger.Info("oi panic panic mkdir")
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
		zapcore.WarnLevel,
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
		answer0:        make(map[int]*commons.StateList),
		answer1:        make(map[int]*commons.StateList),
		packageArray:   queue.NewRingBuffer(BUFFER_SIZE),
		writeChan:      make(chan *commons.Package, WRITE_BUFFER_SIZE),
		dropRate:       dropRate,
		canReceive:     true,
	}

	httpServer, err := server.setupServer()
	if err != nil {
		logger.Error("Error setting up server", zap.Error(err))
	}

	// Start the writer goroutine. It will end when the channel is closed.
	//go server.writeToFile()
	go server.startWriters()

	// Graceful shutdown handling
	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("Server Error", zap.Error(err))
		}
	}()

	logger.Warn("Server running on http://localhost:%d", zap.Int("port", server.port))
	// start new service
	// send ID to our reader service
	server.sendCurrentServerID()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		for s := range sigs {
			logger.Warn("received signal", zap.String("sig", s.String()))
			cancel()
			return
		}
	}()

	go server.protocolDaemon(ctx, server.startTimestamp)

	// Block until context is cancelled
	<-ctx.Done()

	// Graceful shutdown
	logger.Warn("Shutting down server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown error", zap.Error(err))
	}
	close(server.writeChan)
	logger.Warn("Server stopped.")

}
