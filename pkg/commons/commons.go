package commons

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// const (
// 	BROADCAST_TIMEOUT         = 15 * time.Millisecond
// 	BROADCAST_OVERALL_TIMEOUT = 30 * time.Millisecond
// 	EPOCH_PERIOD              = 200 * time.Millisecond

// 	// We wait for 60% of the time for transactions. The rest of the transactions will be in the next package.
// 	WAIT_FOR_BROKER_PACKAGE = (50 * EPOCH_PERIOD) / 100
// 	BROKER_REQUEST_TIMEOUT  = 10 * time.Millisecond
// 	SERVER_REQUEST_TIMEOUT  = 10 * time.Millisecond
// 	BROKER_RETRY            = 5
// 	SERVER_RETRY            = 3

// 	SERVER_PER_ATTEMPT_TIMEOUT = 30 * time.Millisecond // read/response + connect per attempt
// 	SERVER_OVERALL_TIMEOUT     = 70 * time.Millisecond // hard upper bound for a call with retries
// 	SERVER_RETRY_ATTEMPTS      = 2                     // initial + 1 retry
// 	SERVER_RETRY_MAX_DELAY     = 10 * time.Millisecond // cap jittered backoff

// 	BROKER_PER_ATTEMPT_TIMEOUT = 60 * time.Millisecond  // read/response + connect per attempt
// 	BROKER_OVERALL_TIMEOUT     = 100 * time.Millisecond // hard upper bound for a call with retries
// 	BROKER_RETRY_ATTEMPTS      = 2                      // initial + 1 retry
// 	BROKER_RETRY_MAX_DELAY     = 10 * time.Millisecond  // cap jittered backoff
// )

const (
	BROADCAST_TIMEOUT         = 15 * time.Millisecond
	BROADCAST_OVERALL_TIMEOUT = 30 * time.Millisecond
	EPOCH_PERIOD              = 100 * time.Millisecond

	// We wait for 60% of the time for transactions. The rest of the transactions will be in the next package.
	WAIT_FOR_BROKER_PACKAGE = (50 * EPOCH_PERIOD) / 100
	BROKER_REQUEST_TIMEOUT  = 5 * time.Millisecond
	SERVER_REQUEST_TIMEOUT  = 5 * time.Millisecond
	BROKER_RETRY            = 5
	SERVER_RETRY            = 3

	SERVER_PER_ATTEMPT_TIMEOUT = 15 * time.Millisecond // read/response + connect per attempt
	SERVER_OVERALL_TIMEOUT     = 50 * time.Millisecond // hard upper bound for a call with retries
	SERVER_RETRY_ATTEMPTS      = 2                     // initial + 1 retry
	SERVER_RETRY_MAX_DELAY     = 5 * time.Millisecond  // cap jittered backoff

	BROKER_PER_ATTEMPT_TIMEOUT = 30 * time.Millisecond // read/response + connect per attempt
	BROKER_OVERALL_TIMEOUT     = 50 * time.Millisecond // hard upper bound for a call with retries
	BROKER_RETRY_ATTEMPTS      = 2                     // initial + 1 retry
	BROKER_RETRY_MAX_DELAY     = 5 * time.Millisecond  // cap jittered backoff
)

type NodeInfo struct {
	IP         string `json:"ip"`
	Port       int64  `json:"port"`
	ReaderPort int64  `json:"readerport"` //same as port for broker -> just make it redundant
}

type DirectoryMap struct {
	Data    map[int]*NodeInfo `json:"data"`
	Version int               `json:"version"`
	sync.RWMutex
}

type NodesMap struct {
	ServerMap *DirectoryMap `json:"serverMap"`
	BrokerMap *DirectoryMap `json:"brokerMap"`
}

func (n *NodesMap) checkAndUpdateServerMap(newServerMap *DirectoryMap) {
	n.ServerMap.Lock()
	defer n.ServerMap.Unlock()
	if n.ServerMap.Version < newServerMap.Version {
		n.ServerMap = newServerMap
	}
}

func (n *NodesMap) checkAndUpdateBrokerMap(newBrokerMap *DirectoryMap) {
	n.BrokerMap.Lock()
	defer n.BrokerMap.Unlock()
	if n.BrokerMap.Version < newBrokerMap.Version {
		n.BrokerMap = newBrokerMap
	}
}

func (n *NodesMap) CheckAndUpdateMap(newDirectoryMap *NodesMap) {
	n.checkAndUpdateBrokerMap(newDirectoryMap.BrokerMap)
	n.checkAndUpdateServerMap(newDirectoryMap.ServerMap)
}

type Operation struct {
	Key string `json:"key"`	
	Value string `json:"value"`
	Op int64 `json:"op"` // Operation type: 1 (Write), 2 (Delete), 3 (Read)
}

type Transaction struct {
	Id int64 `json:"id"`
	Timestamp int64 `json:"timestamp"`
	Operations []*Operation `json:"operations"`
}

// Declare constants for state of the package for an epoch.
const (
	Undefined    int = 1
	NotReceived  int = 2
	Received     int = 3
	IgnoreBroker int = 4
)

// Declare constants for the type of the node.
const (
	ServerType int = 1
	BrokerType int = 2
)

func GetNodeType(nodeType int) string {
	switch nodeType {
	case ServerType:
		return "server"
	case BrokerType:
		return "broker"
	default:
		return "unknown"
	}
}

type Package struct {
	BrokerID     int            `json:"brokerID"`
	PackageID    int            `json:"packageID"`
	Transactions []*Transaction `json:"transactions"`
}

func DummyTransactions() []*Transaction {
	// Create a dummy transaction with some operations
	operations := []*Operation{
		{ Key: "key1", Value: "value1", Op: 1},
		{ Key: "key2", Value: "value2", Op: 2},
	}
	return []*Transaction{
		{Timestamp: 1,Operations: operations},
	}
}

func (m *DirectoryMap) Get(key int) (*NodeInfo, bool) {
	m.RLock()
	defer m.RUnlock()
	val, ok := m.Data[key]
	return val, ok
}

func (m *DirectoryMap) SetVersion(version int) {
	m.Version = version
}

func (m *DirectoryMap) Set(key int, value *NodeInfo) {
	m.Data[key] = value
}

func HandleUpdateDirectory(logger *zap.Logger, directoryInfo *NodesMap) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		// Read data from request
		nodeType := r.Header.Get("X-NodeType")
		nodeID := r.Header.Get("X-NodeID")
		if nodeType == "" || nodeID == "" {
			nodeType = "unknown"
			nodeID = "unknown"
		}
		logger.Info("Received update directory request", zap.String("nodeType", nodeType), zap.String("nodeID", nodeID))
		w.WriteHeader(http.StatusOK)
		// if f, ok := w.(http.Flusher); ok {
		// 	f.Flush()
		// }

		body, err := io.ReadAll(r.Body)
		if err != nil {
			//http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		//defer r.Body.Close()

		// Process the data
		newDirectoryMap := NodesMap{}
		if err := json.Unmarshal(body, &newDirectoryMap); err != nil {
			logger.Error("Error unmarshalling JSON", zap.Error(err))
			//http.Error(w, "Error unmarshalling JSON", http.StatusBadRequest)
			return
		}
		// dec := json.NewDecoder(r.Body)
		// dec.DisallowUnknownFields()
		// if err := dec.Decode(&newDirectoryMap); err != nil {
		// 	logger.Warn("update decode failed",
		// 		zap.String("nodeType", nodeType), zap.String("nodeID", nodeID), zap.Error(err))
		// 	http.Error(w, "Invalid JSON", http.StatusBadRequest)
		// 	return
		// }

		ndm := newDirectoryMap
		go func(m *NodesMap) {
			//logger.Info("checking and updating map")
			directoryInfo.CheckAndUpdateMap(m)
		}(&ndm)

	}
}

func BroadcastNodesInfo(logger *zap.Logger, currNodeID int, currNodeType int, directoryInfo *NodesMap, sharedTransport *http.Transport) error {
	var wg sync.WaitGroup
	jsonData, err := json.Marshal(directoryInfo)
	if err != nil {
		logger.Error("Error encoding JSON", zap.Error(err))
		return fmt.Errorf("error encoding JSON: %v", err)
	}
	brokersmap := directoryInfo.BrokerMap
	serversmap := directoryInfo.ServerMap
	for id, node := range brokersmap.Data {
		if id == currNodeID && currNodeType == BrokerType {
			continue
		}

		logger.Info("Sending broadcast request to broker", zap.Int("id", id), zap.String("ip", node.IP), zap.Int64("port", node.Port), zap.Int64("readerport", node.ReaderPort))

		wg.Add(1)
		go func(id, currNodeID int, node *NodeInfo) {
			defer wg.Done()

			client := &http.Client{
				Transport: sharedTransport,
				Timeout:   BROADCAST_OVERALL_TIMEOUT,
			}

			ctx, cancel := context.WithTimeout(context.Background(), BROADCAST_TIMEOUT)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, BROKER_UPDATE_DIRECTORY), bytes.NewBuffer(jsonData))
			if err != nil {
				logger.Warn("Error creating request", zap.Int("id", id), zap.Error(err))
				return
			}
			// req, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, BROKER_UPDATE_DIRECTORY), bytes.NewBuffer(jsonData))
			// if err != nil {
			// 	panic(err)
			// }

			// Set headers
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-NodeID", fmt.Sprint(currNodeID))
			req.Header.Set("X-NodeType", GetNodeType(currNodeType))

			resp, err := client.Do(req)
			if err != nil {
				logger.Error("Error sending request to broker", zap.Int("id", id), zap.Error(err))
				return
			}
			defer func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				logger.Error("Unexpected status code", zap.Int("status_code", resp.StatusCode), zap.Int("broker_id", id))
			}
		}(id, currNodeID, node)
	}

	for id, node := range serversmap.Data {
		if id == currNodeID && currNodeType == ServerType {
			continue
		}

		logger.Info("Sending broadcast request to server", zap.Int("id", id), zap.String("ip", node.IP), zap.Int64("port", node.Port), zap.Int64("readerport", node.ReaderPort))

		wg.Add(1)
		go func(id int, node *NodeInfo) {
			defer wg.Done()

			client := &http.Client{
				Transport: sharedTransport,
				Timeout:   BROADCAST_OVERALL_TIMEOUT,
			}

			ctx, cancel := context.WithTimeout(context.Background(), BROADCAST_TIMEOUT)
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, SERVER_UPDATE_DIRECTORY), bytes.NewBuffer(jsonData))
			if err != nil {
				// Handle error
				logger.Warn("Error creating request", zap.Int("id", id), zap.Error(err))
				return
			}

			// Set headers
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-NodeID", fmt.Sprint(currNodeID))
			req.Header.Set("X-NodeType", GetNodeType(currNodeType))

			resp, err := client.Do(req)
			if err != nil {
				logger.Warn("Error sending request to server", zap.Int("id", id), zap.Error(err))
				return
			}
			defer func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()
			if resp.StatusCode != http.StatusOK {
				logger.Warn("Unexpected status code", zap.Int("status_code", resp.StatusCode), zap.Int("server_id", id))
			}
		}(id, node)
	}

	wg.Wait()

	logger.Warn("Broadcasting nodes info complete", zap.Int("currNodeID", currNodeID), zap.Int("currNodeType", currNodeType))
	return nil
}
