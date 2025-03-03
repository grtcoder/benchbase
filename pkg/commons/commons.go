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

type DeviceMap struct {
	ServerMap *DirectoryMap `json:"serverMap"`
	BrokerMap *DirectoryMap `json:"brokerMap"`
}

type Transaction struct {
	Timestamp int64 `json:"timestamp"`
	Key string `json:"key"`
	Value string `json:"value"`
}


type Package struct {
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
