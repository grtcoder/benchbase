package commons

import (
	"net/http"
	"io"
	"encoding/json"
	"log"
	"sync"
	"fmt"
	"bytes"
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


type Transaction struct {
	Timestamp int64 `json:"timestamp"`
	Key string `json:"key"`
	Value string `json:"value"`
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
	State int32 `json:"state"`
	BrokerID int `json:"brokerID"`
	PackageCounter int `json:"packageCounter"`
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

	for id, node := range directoryInfo.BrokerMap.Data {
		if id == currNodeID && currNodeType == BrokerType {
			continue
		}

		wg.Add(1)
		go func(id int, node *NodeInfo) {
			defer wg.Done()

			resp, err := http.Post(fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, BROKER_UPDATE_DIRECTORY), "application/json", bytes.NewBuffer(jsonData))
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
		wg.Add(1)
		go func(id int, node *NodeInfo) {
			defer wg.Done()

			resp, err := http.Post(fmt.Sprintf("http://%s:%d%s", node.IP, node.Port,SERVER_UPDATE_DIRECTORY), "application/json", bytes.NewBuffer(jsonData))
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