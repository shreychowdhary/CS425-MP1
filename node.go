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
	"time"

	"github.com/google/uuid"
)

var nodes NodeMap
var hostNode Node
var receivedMessages Messages
var receiveBuffer PriorityQueue
var maxSequenceNumber MaxSequenceNumber
var accounts Accounts
var responseManager ResponseManager
var statsChannel chan string
var printChannel chan string

func SetupFromConfigFile(filename string, semaphore chan string) int {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		log.Fatal(err)
	}

	lines := strings.Split(string(content), "\n")
	numNodes, err := strconv.Atoi(lines[0])
	if err != nil {
		log.Fatal(err)
	}
	log.Println(numNodes, "Other Nodes")
	for i := 1; i <= numNodes; i++ {
		nodeInfo := strings.Fields(lines[i])
		if len(nodeInfo) != 3 {
			log.Fatal("Not enough arguments for line", i)
		}

		node := Node{
			Id:      nodeInfo[0],
			Address: nodeInfo[1],
			Port:    nodeInfo[2],
			IsHost:  false,
			Input:   make(chan string),
			Output:  make(chan string),
		}

		if ConnectToNode(&node) {
			nodes.Set(node.Id, &node)
			go HandleNode(&node, semaphore)
		}
	}
	return numNodes
}

func ConnectToNode(node *Node) bool {
	connection, err := net.Dial("tcp", node.Address+":"+node.Port)
	if err != nil {
		log.Println("Unable to connect to", node.Id, node.Address)
		return false
	}
	node.Connection = connection
	go Write(node)
	go Read(node)
	log.Println("Connected to", node.Id)
	return true
}

func BasicMulticast(message string) {
	nodes.RWMutex.RLock()
	for _, node := range nodes.Nodes {
		node.Input <- message
	}
	nodes.RWMutex.RUnlock()
}

func Read(node *Node) {
	reader := bufio.NewReader(node.Connection)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err)
			nodes.Delete((node.Id))
			return
		}
		node.Output <- message
	}
}

func Write(node *Node) {
	for {
		message := <-node.Input
		if node.IsHost {
			node.Output <- message
		} else {
			_, err := fmt.Fprintf(node.Connection, message)
			if err != nil {
				nodes.Delete(node.Id)
				return
			}
		}
	}
}

func HandleNode(node *Node, semaphore chan string) {
	defer node.Connection.Close()
	node.Input <- fmt.Sprintf("%s\n", hostNode.Id)
	nodeHeader := <-node.Output

	nodeInfo := strings.Fields(nodeHeader)
	if len(nodeInfo) != 1 {
		log.Println(nodeHeader)
		log.Println("Incorrect node header")
		return
	}
	node.Id = nodeInfo[0]
	nodes.Set(node.Id, node)
	semaphore <- node.Id
	ListenToMessages(node)
}

func ListenToMessages(node *Node) {
	for {
		message := <-node.Output
		HandleMessage(message, node)
	}
}

func HandleMessage(message string, node *Node) {
	messageInfo := strings.Split(message, "~")
	messageId := messageInfo[0]
	messageHeader := messageInfo[0] + string(messageInfo[1][0])
	messageBody := messageInfo[1][1:]

	if receivedMessages.Add(message) {
		return
	}
	switch messageHeader[len(messageHeader)-1] {
	case '1':
		seqNum := maxSequenceNumber.Increment()
		seqNum.NodeId = hostNode.Id
		item := BufferedMessage{
			Id:          messageId,
			Message:     messageBody,
			Priority:    seqNum,
			Deliverable: false,
		}
		receiveBuffer.Push(&item)
		originalSenderId := strings.TrimSuffix(messageInfo[2], "\n")
		originalSender := nodes.Get(originalSenderId)
		if originalSender.IsHost {
			originalSender.Input <- fmt.Sprintf("%s~2%d&%s\n", messageId, seqNum.Value, hostNode.Id)
		} else {
			originalSender.Input <- fmt.Sprintf("%s~2%d&%s\n", messageId, seqNum.Value, hostNode.Id)
			go BasicMulticast(message)
		}

	case '2':
		seqNum := SequenceNumberFromString(messageBody)
		responseManager.SetMaxSequenceNumber(messageId, *seqNum)
		responseManager.AddResponder(messageId, node.Id)
		if responseManager.NumResponders(messageId) == nodes.Length() {
			maxSeqNum := responseManager.GetMaxSequenceNumber(messageId)
			maxSeqString := maxSeqNum.ToString()
			message := fmt.Sprintf("%s~3%s\n", messageId, maxSeqString)
			go BasicMulticast(message)
		}

	case '3':
		seqNum := SequenceNumberFromString(messageBody)
		receiveBuffer.Update(messageId, *seqNum)
		receiveBuffer.SetDeliverable(messageId, true)

		maxSequenceNumber.Set(*seqNum)
		if !node.IsHost {
			go BasicMulticast(message)
		}
		go receiveBuffer.Deliver(HandleTranscation)
	default:
		log.Panic("Default:" + message)
	}
}

func HandleTranscation(transaction string, id string) {
	transactionInfo := strings.Fields(transaction)
	switch transactionInfo[0] {
	case "DEPOSIT":
		balance, ok := accounts.GetBalance(transactionInfo[1])
		deposit, err := strconv.Atoi(transactionInfo[2])
		if err != nil {
			log.Println(err)
		}
		if ok {
			balance += deposit
		} else {
			balance = deposit
		}
		accounts.SetBalance(transactionInfo[1], balance)
	case "TRANSFER":
		sourceBalance, ok := accounts.GetBalance(transactionInfo[1])
		deposit, err := strconv.Atoi(transactionInfo[4])
		if err != nil {
			log.Println(err)
		}
		if ok && deposit <= sourceBalance {
			destBalance, ok := accounts.GetBalance(transactionInfo[3])
			if ok {
				destBalance += deposit
			} else {
				destBalance = deposit
			}
			accounts.SetBalance(transactionInfo[1], sourceBalance-deposit)
			accounts.SetBalance(transactionInfo[3], destBalance)
		}
	}
	balances := accounts.GetBalanceString()
	printChannel <- fmt.Sprintf("%s\n%s", transaction, balances)
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
		id := uuid.New().String()
		message := id + "~1" + strings.TrimSuffix(text, "\n") + "~" + hostNode.Id + "\n"
		statsChannel <- fmt.Sprintf("CREATE~%d~%s\n", time.Now().UnixNano(), id)
		//Start Timeout Check here so that it automatically sends after 10 seconds
		//TODO
		BasicMulticast(message)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func LogStats(file *os.File) {
	for {
		statsStr := <-statsChannel
		file.WriteString(statsStr)
	}
}

func PrintInfo() {
	for {
		printStr := <-printChannel
		fmt.Println(printStr)
	}
}

func main() {
	if len(os.Args) != 4 {
		log.Fatal("Format should be ./node id port config")
	}
	responseManager.Init()
	accounts.Init()
	nodes.Init()
	receivedMessages.Init()
	receiveBuffer.Init()
	hostNode = Node{
		Id:     os.Args[1],
		Port:   os.Args[2],
		IsHost: true,
		Input:  make(chan string, 100),
		Output: make(chan string, 100),
	}
	go Write(&hostNode)
	go ListenToMessages(&hostNode)

	nodes.Set(hostNode.Id, &hostNode)
	maxSequenceNumber.Set(*SequenceNumberFromString(("0&" + hostNode.Id)))

	file, err := os.Create(hostNode.Id + "_data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	statsChannel = make(chan string)
	printChannel = make(chan string)
	defer close(statsChannel)
	defer close(printChannel)
	go LogStats(file)
	go PrintInfo()

	listen, err := net.Listen("tcp", ":"+hostNode.Port)
	if err != nil {
		log.Fatal(err)
	}
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
			Input:      make(chan string),
			Output:     make(chan string),
		}
		go Write(&node)
		go Read(&node)
		go HandleNode(&node, semaphore)
	}
}
