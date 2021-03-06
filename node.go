package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

var nodes NodeMap
var hostNode Node
var receivedMessages Messages

func Check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func SetupFromConfigFile(filename string, semaphore chan string) int {
	content, err := ioutil.ReadFile(filename)
	Check(err)

	lines := strings.Split(string(content), "\n")
	numNodes, err := strconv.Atoi(lines[0])
	Check(err)
	log.Println(numNodes, "Other Nodes")
	for i := 1; i <= numNodes; i++ {
		nodeInfo := strings.Fields(lines[i])
		if len(nodeInfo) != 3 {
			log.Fatal("Not enough arguments for line", i)
		}

		Check(err)
		node := Node{
			Id:       nodeInfo[0],
			HostName: nodeInfo[1],
			Port:     nodeInfo[2],
			IsHost:   false,
		}

		if ConnectToNode(&node) {
			nodes.Set(node.Id, &node)
			go HandleNode(&node, semaphore)
		}
	}
	return numNodes
}

func ConnectToNode(node *Node) bool {
	connection, err := net.Dial("tcp", node.HostName+":"+node.Port)
	if err != nil {
		log.Println("Unable to connect to", node.Id, node.HostName)
		return false
	}
	node.Connection = connection
	log.Println("Connected to", node.Id)
	return true
}

func BasicMulticast(message string) {
	nodes.RWMutex.Lock()
	for _, node := range nodes.Nodes {
		if node.IsHost {
			HandleMessage(message, node)
			continue
		}

		_, err := Write(node.Connection, message)

		if err != nil {
			log.Println(err)
			delete(nodes.Nodes, node.Id)
		}
	}
	nodes.RWMutex.Unlock()
}

func Read(connection net.Conn) (string, error) {
	reader := bufio.NewReader(connection)
	return reader.ReadString('\n')
}

func Write(connection net.Conn, message string) (int, error) {
	return fmt.Fprintf(connection, message)
}

func HandleNode(node *Node, semaphore chan string) {
	defer node.Connection.Close()
	Write(node.Connection, fmt.Sprintf("%s\n", hostNode.Id))
	nodeHeader, err := Read(node.Connection)
	if err != nil {
		log.Println(err)
		if node.Id != "" {
			nodes.Delete(node.Id)
		}
		return
	}
	log.Println(nodeHeader)
	nodeInfo := strings.Fields(nodeHeader)
	if len(nodeInfo) != 1 {
		log.Println("Incorrect node header")
		return
	}
	node.Id = nodeInfo[0]
	nodes.Set(node.Id, node)
	log.Println(node.Id, "Setup Complete")
	semaphore <- node.Id
	for {
		message, err := Read(node.Connection)
		if err != nil {
			log.Println(err)
			//mutex
			if node.Id != "" {
				nodes.Delete(node.Id)
			}
			return
		}
		HandleMessage(message, node)

	}
}

func SendTransactions(semaphore chan string, numNodes int) {
	for i := 0; i < numNodes; i++ {
		nodeId := <-semaphore
		log.Println("Connected to", nodeId)
	}
	log.Println("All nodes connected")
	stdinReader := bufio.NewReader(os.Stdin)
	for {
		text, err := stdinReader.ReadString('\n')
		log.Println("Send", text)
		BasicMulticast(uuid.New().String() + " " + text)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func HandleMessage(message string, node *Node) {
	messageInfo := strings.Fields(message)
	if receivedMessages.Contains(messageInfo[0]) || node.Id == hostNode.Id {
		return
	}
	receivedMessages.Set(messageInfo[0], messageInfo[1])
	BasicMulticast(message)
	log.Println("Receive:", message)
}

func logStats(file *os.File, statsChannel chan string) {
	for {
		statsStr := <-statsChannel
		file.WriteString(statsStr)
	}
}

func main() {
	if len(os.Args) != 4 {
		log.Fatal("Format should be ./node id port config")
	}
	nodes.Nodes = make(map[string]*Node)
	receivedMessages.Messages = make(map[string]string)

	hostNode = Node{
		Id:     os.Args[1],
		Port:   os.Args[2],
		IsHost: true,
	}
	nodes.Set(hostNode.Id, &hostNode)

	file, err := os.Create(hostNode.Id + "_data.csv")
	Check(err)
	defer file.Close()
	statsChannel := make(chan string)
	defer close(statsChannel)
	go logStats(file, statsChannel)

	listen, err := net.Listen("tcp", ":"+hostNode.Port)
	Check(err)
	defer listen.Close()

	semaphore := make(chan string)
	numNodes := SetupFromConfigFile(os.Args[3], semaphore)

	go SendTransactions(semaphore, numNodes)
	for {
		connection, err := listen.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		node := Node{
			Connection: connection,
			IsHost:     false,
		}
		go HandleNode(&node, semaphore)
	}
}
