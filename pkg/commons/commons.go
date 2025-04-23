package commons

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	BROADCAST_TIMEOUT=100*time.Millisecond

	EPOCH_PERIOD=5000 * time.Millisecond

	// We wait for 60% of the time for transactions. The rest of the transactions will be in the next package.
	WAIT_FOR_TRANSACTIONS=(60*EPOCH_PERIOD)/100
	WAIT_FOR_BROKER_PACKAGE=1000*time.Millisecond
	BROKER_REQUEST_TIMEOUT=100*time.Millisecond
	SERVER_REQUEST_TIMEOUT=100*time.Millisecond
	BROKER_RETRY=5
	SERVER_RETRY=5
)

type NodeInfo struct {
	IP string `json:"ip"`
	Port int64 `json:"port"`
}

type DirectoryMap struct {
	Data map[int]*NodeInfo `json:"data"`
	Version int `json:"version"`
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
	Timestamp int64 `json:"timestamp"`
	Key string `json:"key"`	
	Value string `json:"value"`
	Op int64 `json:"op"`
}

type Transaction struct {
	Operations []*Operation `json:"operations"`
}


// Declare constants for state of the package for an epoch.
const (
	Undefined  int32 = 1
	NotReceived  int32 = 2
	Received  int32 = 3
	IgnoreBroker int32  = 4
)

// Declare constants for the type of the node.
const (
	ServerType int32 = 1
	BrokerType int32 = 2
)

type Package struct {
	BrokerID int `json:"brokerID"`
	PackageID int `json:"packageID"`
	Transactions []*Transaction `json:"transactions"`
}



func (m *DirectoryMap) Get(key int) (*NodeInfo,bool) {
	m.RLock()
	defer m.RUnlock()
	val, ok := m.Data[key]
	return val,ok
}

func (m *DirectoryMap) SetVersion(version int) {
	m.Version=version
}

func (m *DirectoryMap) Set(key int, value *NodeInfo) {
	m.Data[key] = value
}

func HandleUpdateDirectory(directoryInfo *NodesMap) func(w http.ResponseWriter, r *http.Request) {
	return func (w http.ResponseWriter, r *http.Request) {
		// Read data from request
		log.Printf("Received update directory request\n")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		// Process the data
		newDirectoryMap := &NodesMap{}
		if err := json.Unmarshal(body, &newDirectoryMap); err != nil {
			log.Printf("Error unmarshalling JSON: %s", err)
			http.Error(w, "Error unmarshalling JSON", http.StatusBadRequest)
			return
		}

		directoryInfo.CheckAndUpdateMap(newDirectoryMap)

		w.WriteHeader(http.StatusOK)
	}
}

func BroadcastNodesInfo(currNodeID int, currNodeType int32,directoryInfo *NodesMap) error {
	var wg sync.WaitGroup

	jsonData, err := json.Marshal(directoryInfo)
	if err != nil {
		return fmt.Errorf("error encoding JSON: %v", err)
	}
	client := &http.Client{
        Timeout: BROADCAST_TIMEOUT,
    }

	for id, node := range directoryInfo.BrokerMap.Data {
		if id == currNodeID && currNodeType == BrokerType {
			continue
		}

		log.Printf("Sending broadcast request to broker %d: %s:%d", id, node.IP, node.Port)

		wg.Add(1)
		go func(id int, node *NodeInfo) {
			defer wg.Done()

			resp, err := client.Post(fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, BROKER_UPDATE_DIRECTORY), "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Error sending request to server %d: %s", id, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf("Unexpected status code: %d for server %d", resp.StatusCode, id)
			}
		}(id, node)
	}

	for id, node := range directoryInfo.ServerMap.Data {
		if id == currNodeID && currNodeType == ServerType {
			continue
		}

		log.Printf("Sending broadcast request to server %d: %s:%d", id, node.IP, node.Port)

		wg.Add(1)
		go func(id int, node *NodeInfo) {
			defer wg.Done()

			resp, err := client.Post(fmt.Sprintf("http://%s:%d%s", node.IP, node.Port,SERVER_UPDATE_DIRECTORY), "application/json", bytes.NewBuffer(jsonData))
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

