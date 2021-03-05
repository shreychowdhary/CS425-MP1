package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Format should be ./logger serverport")
	}

	printChannel := make(chan string)
	statsChannel := make(chan string)
	defer close(printChannel)
	defer close(statsChannel)

	file, err := os.Create("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	go printClientData(printChannel)
	go logStats(file, statsChannel)

	serverPort := ":" + os.Args[1]
	listen, err := net.Listen("tcp", serverPort)
	if err != nil {
		log.Fatal(err)
	}
	defer listen.Close()

	for {
		connection, err := listen.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go handleClient(connection, printChannel, statsChannel)
	}
}

func handleClient(connection net.Conn, printChannel chan string, statsChannel chan string) {
	defer connection.Close()

	clientReader := bufio.NewReader(connection)
	nodeName, _ := clientReader.ReadString('\n')
	nodeName = strings.TrimSuffix(nodeName, "\n")
	printChannel <- fmt.Sprintf("%f - %s connected\n", getCurrentTime(), nodeName)
	for {
		text, err := clientReader.ReadString('\n')
		if err != nil {
			log.Println(err)
			break
		}
		substrings := strings.Split(text, " ")
		timestamp, err := strconv.ParseFloat(substrings[0], 64)
		if err != nil {
			log.Println(err)
		}
		printChannel <- fmt.Sprintf("%s %s %s", substrings[0], nodeName, substrings[1])
		statsChannel <- fmt.Sprintf("%f,%f,%d\n", getCurrentTime(), getCurrentTime()-timestamp, len(text))
	}
	printChannel <- fmt.Sprintf("%f - %s disconnected\n", getCurrentTime(), nodeName)
}

func printClientData(printChannel chan string) {
	for {
		logStr := <-printChannel
		fmt.Print(logStr)
	}
}

func logStats(file *os.File, statsChannel chan string) {
	for {
		statsStr := <-statsChannel
		file.WriteString(statsStr)
	}
}

func getCurrentTime() float64 {
	return float64(time.Now().UnixNano()) / 1.0e9
}
