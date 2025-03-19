package commons

import (
	"sync"
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

// Define a new type
type State int32

// Declare constants using iota
const (
	Null State = iota
	Undefined State = 1
	NotReceived State = 2
	Received State = 3
	IgnoreBroker State = 4
)

type Package struct {
	State State `json:"state"`
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
