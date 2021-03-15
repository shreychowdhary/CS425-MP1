package main

import (
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type BufferedMessage struct {
	Id          string
	Message     string
	Priority    SequenceNumber
	Deliverable bool
}

type Node struct {
	Id         string
	Address    string
	Port       string
	Connection net.Conn
	Input      chan string
	Output     chan string
	IsHost     bool
}

type Messages struct {
	RWMutex  sync.RWMutex
	Messages map[string]bool
}

func (m *Messages) Init() {
	m.Messages = make(map[string]bool)
}

func (m *Messages) Add(message string) bool {
	m.RWMutex.Lock()
	defer m.RWMutex.Unlock()
	_, ok := m.Messages[message]
	if ok {
		return true
	}
	m.Messages[message] = true
	return false
}

type NodeMap struct {
	RWMutex sync.RWMutex
	Nodes   map[string]*Node
}

func (m *NodeMap) Init() {
	m.Nodes = make(map[string]*Node)
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

type SequenceNumber struct {
	Value  int
	NodeId string
}

func (sq *SequenceNumber) Less(other SequenceNumber) bool {
	if sq.Value == other.Value {
		return sq.NodeId < other.NodeId
	}
	return sq.Value < other.Value
}

func (sq *SequenceNumber) Equals(other SequenceNumber) bool {
	return sq.Value == other.Value && sq.NodeId == other.NodeId
}

func (sq *SequenceNumber) ToString() string {
	return fmt.Sprintf("%d&%s", sq.Value, sq.NodeId)
}

func SequenceNumberFromString(value string) *SequenceNumber {
	value = strings.TrimSuffix(value, "\n")
	sq := new(SequenceNumber)
	values := strings.Split(value, "&")
	if len(values) != 2 {
		log.Panic("Invalid string passed in")
	}
	temp, err := strconv.Atoi(values[0])
	if err != nil {
		log.Panic(err)
	}
	sq.Value = temp
	sq.NodeId = values[1]
	return sq
}

type MaxSequenceNumber struct {
	SeqNumber SequenceNumber
	RWMutex   sync.RWMutex
}

func (sq *MaxSequenceNumber) Set(seqNumber SequenceNumber) {
	sq.RWMutex.Lock()
	if sq.SeqNumber.Less(seqNumber) {
		sq.SeqNumber = seqNumber
	}
	sq.RWMutex.Unlock()
}

func (sq *MaxSequenceNumber) Get() SequenceNumber {
	sq.RWMutex.RLock()
	seqNumber := sq.SeqNumber
	sq.RWMutex.RUnlock()
	return seqNumber
}

func (sq *MaxSequenceNumber) Increment() SequenceNumber {
	sq.RWMutex.Lock()
	defer sq.RWMutex.Unlock()
	sq.SeqNumber.Value++
	seqNum := sq.SeqNumber
	return seqNum
}

type Accounts struct {
	Balances map[string]int
	RWMutex  sync.RWMutex
}

func (a *Accounts) Init() {
	a.Balances = make(map[string]int)
}

func (a *Accounts) SetBalance(id string, balance int) {
	a.RWMutex.Lock()
	a.Balances[id] = balance
	a.RWMutex.Unlock()
}

func (a *Accounts) GetBalance(id string) (int, bool) {
	a.RWMutex.RLock()
	balance, ok := a.Balances[id]
	a.RWMutex.RUnlock()
	return balance, ok
}

func (a *Accounts) GetBalanceString() string {
	a.RWMutex.RLock()
	keys := make([]string, len(a.Balances))
	i := 0
	for k := range a.Balances {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	balanceString := "BALANCES "
	for _, id := range keys {
		if a.Balances[id] > 0 {
			balanceString += fmt.Sprintf("%s:%d ", id, a.Balances[id])
		}
	}
	a.RWMutex.RUnlock()
	return balanceString
}

type ResponseManager struct {
	Responders map[string][]string
	MaxSeq     map[string]SequenceNumber
	RWMutex    sync.RWMutex
}

func (rm *ResponseManager) Init() {
	rm.Responders = make(map[string][]string)
	rm.MaxSeq = make(map[string]SequenceNumber)
}

func (rm *ResponseManager) AddResponder(messageId string, nodeId string) {
	rm.RWMutex.Lock()
	rm.Responders[messageId] = append(rm.Responders[messageId], nodeId)
	rm.RWMutex.Unlock()
}

func (rm *ResponseManager) GetResponders(messageId string) []string {
	rm.RWMutex.RLock()
	responders := rm.Responders[messageId]
	rm.RWMutex.RUnlock()
	return responders
}

func (rm *ResponseManager) NumResponders(messageId string) int {
	rm.RWMutex.RLock()
	num := len(rm.Responders[messageId])
	rm.RWMutex.RUnlock()
	return num
}

func (rm *ResponseManager) SetMaxSequenceNumber(messageId string, seqNumber SequenceNumber) {
	rm.RWMutex.Lock()
	maxSeq, ok := rm.MaxSeq[messageId]
	if !ok || maxSeq.Less(seqNumber) {
		rm.MaxSeq[messageId] = seqNumber
	}
	rm.RWMutex.Unlock()
}

func (rm *ResponseManager) GetMaxSequenceNumber(messageId string) SequenceNumber {
	rm.RWMutex.RLock()
	maxSeq := rm.MaxSeq[messageId]
	rm.RWMutex.RUnlock()
	return maxSeq
}
