package main

import (
	"bufio"
	"fmt"
	"log"
	"messagebroker/communication"
	"messagebroker/config"
	"net"
	"os"
	"strings"
)

func CallForGetMessage() communication.GetMessageReply {
	args := communication.GetMessageArgs{}
	reply := communication.GetMessageReply{}
	if !communication.Call("Broker.GetMessage", &args, &reply) {
		os.Exit(0)
	}
	return reply
}

func CallForPutBackMessage(message string) communication.PutBackMessageReply {
	args := communication.PutBackMessageArgs{Message: message}
	reply := communication.PutBackMessageReply{}
	if !communication.Call("Broker.PutBackMessage", &args, &reply) {
		os.Exit(0)
	}
	return reply
}

func connectToSubscribeService() net.Conn {
	pubSubHost := config.GetPubSubHost()
	pubSubPort := config.GetPubSubPort()
	connectionAddress := fmt.Sprintf("%s:%d", pubSubHost, pubSubPort)
	con, err := net.Dial("tcp", connectionAddress)
	if err != nil {
		log.Fatalln(err)
	}
	return con
}

func subscribeToTopic(topicName string, con net.Conn) {
	con.Write([]byte("subscribe " + topicName + "\n"))
}

func unsubscribeFromTopic(topicName string, con net.Conn) {
	con.Write([]byte("unsubscribe " + topicName + "\n"))
}

func listenForMessages(con net.Conn) {
	reader := bufio.NewReader(con)
	for {
		message, _ := reader.ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		fmt.Println("-------------------------------------------------------")
		fmt.Println("Message received from subscribed topic:", message)
		fmt.Println("-------------------------------------------------------")
	}
}

func handleCommands(con net.Conn) {
	fmt.Println("-------------------------------------------------------")
	fmt.Println("Enter command (type 'exit' to quit):")
	fmt.Println("get_message|send_back_message <message>|subscribe <topic>|unsubscribe <topic>")
	var fullCommand string
	reader := bufio.NewReader(os.Stdin)
	fullCommand, _ = reader.ReadString('\n')
	fullCommand = strings.TrimSuffix(fullCommand, "\n")
	commandSplitted := strings.Split(fullCommand, " ")
	command := commandSplitted[0]
	switch command {
	case "exit":
		os.Exit(0)
	case "get_message":
		reply := CallForGetMessage()
		if reply.Message == "" {
			fmt.Println("No message available")
		} else {
			fmt.Println("Message:", reply.Message)
		}
	case "send_back_message":
		message := commandSplitted[1]
		reply := CallForPutBackMessage(message)
		if reply.IsBufferOverflow {
			fmt.Println("Buffer overflow")
		} else {
			fmt.Println("Message sent successfully")
		}
	case "subscribe":
		topicName := commandSplitted[1]
		subscribeToTopic(topicName, con)
		fmt.Println("Subscribed to topic:", topicName)
		go listenForMessages(con)
	case "unsubscribe":
		topicName := commandSplitted[1]
		unsubscribeFromTopic(topicName, con)
		fmt.Println("Unsubscribed from topic:", topicName)
	default:
		fmt.Println("Unknown command")
	}
}

func main() {
	con := connectToSubscribeService()
	defer con.Close()
	for {
		handleCommands(con)
	}
}
