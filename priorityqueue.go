//From https://www.programmersought.com/article/6920229397/
package main

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

type PriorityQueue struct {
	Items map[string]*BufferedMessage
	mutex sync.RWMutex
}

func (pq *PriorityQueue) Init() {
	pq.Items = make(map[string]*BufferedMessage)
}

func (pq *PriorityQueue) Push(bufferedMessage *BufferedMessage) {
	pq.mutex.Lock()
	pq.Items[bufferedMessage.Id] = bufferedMessage
	pq.mutex.Unlock()
}

func (pq *PriorityQueue) Pop() *BufferedMessage {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	values := pq.sort()
	if len(values) > 0 {
		bufferedMessage := values[0]
		delete(pq.Items, values[0].Id)
		return bufferedMessage
	}
	return nil
}

func (pq *PriorityQueue) sort() []*BufferedMessage {
	values := make([]*BufferedMessage, len(pq.Items))
	i := 0
	for _, v := range pq.Items {
		values[i] = v
		i++
	}
	sort.Slice(values, func(i int, j int) bool {
		return values[i].Priority.Less(values[j].Priority)
	})
	return values
}

func (pq *PriorityQueue) Deliver(fn func(string, string)) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	values := pq.sort()
	for len(pq.Items) > 0 && values[0].Deliverable {
		bufferedMessage := values[0]
		delete(pq.Items, bufferedMessage.Id)
		values = values[1:]
		fn(bufferedMessage.Message, fmt.Sprintf("%s-%s", bufferedMessage.Priority.ToString(), bufferedMessage.Id))
		statsChannel <- fmt.Sprintf("Deliver~%d~%s\n", time.Now().UnixNano(), bufferedMessage.Id)
	}
}

func (pq *PriorityQueue) Update(id string, priority SequenceNumber) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()
	bufferedMessage, ok := pq.Items[id]
	if ok {
		bufferedMessage.Priority = priority
	} else {
		log.Println("NIL " + id + "-" + hostNode.Id)
	}
}

func (pq *PriorityQueue) SetDeliverable(id string, deliverable bool) {
	pq.mutex.Lock()
	bufferedMessage, ok := pq.Items[id]
	if ok {
		bufferedMessage.Deliverable = deliverable
	}
	pq.mutex.Unlock()
}

func (pq *PriorityQueue) Length() int {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()
	return len(pq.Items)
}
