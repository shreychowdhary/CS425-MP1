package main

import (
	"net"
	"sync"
)

type Node struct {
	Id         string
	HostName   string
	Port       string
	Connection net.Conn
	IsHost     bool
}

type Messages struct {
	RWMutex  sync.RWMutex
	Messages map[string]string
}

func (m *Messages) Contains(id string) bool {
	m.RWMutex.RLock()
	_, ok := m.Messages[id]
	m.RWMutex.RUnlock()
	return ok
}

func (m *Messages) Set(id string, message string) {
	m.RWMutex.Lock()
	m.Messages[id] = message
	m.RWMutex.Unlock()
}

type NodeMap struct {
	RWMutex sync.RWMutex
	Nodes   map[string]*Node
}

func (m *NodeMap) Delete(id string) {
	m.RWMutex.Lock()
	delete(m.Nodes, id)
	m.RWMutex.Unlock()
}

func (m *NodeMap) Set(id string, node *Node) {
	m.RWMutex.Lock()
	m.Nodes[id] = node
	m.RWMutex.Unlock()
}

func (m *NodeMap) Get(id string) *Node {
	m.RWMutex.RLock()
	node := m.Nodes[id]
	m.RWMutex.RUnlock()
	return node
}

func (m *NodeMap) Length() int {
	m.RWMutex.RLock()
	length := len(m.Nodes)
	m.RWMutex.RUnlock()
	return length
}

func (m *NodeMap) Contains(id string) bool {
	m.RWMutex.RLock()
	_, ok := m.Nodes[id]
	m.RWMutex.RUnlock()
	return ok
}
