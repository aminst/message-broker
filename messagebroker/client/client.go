package main

import (
	"bufio"
	"fmt"
	"messagebroker/communication"
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

func split(s string) []string {
	return strings.Split(s, " ")
}

func handleCommands() {
	fmt.Println("Enter command (type 'exit' to quit):")
	var fullCommand string
	reader := bufio.NewReader(os.Stdin)
	fullCommand, _ = reader.ReadString('\n')
	fullCommand = strings.TrimSuffix(fullCommand, "\n")
	commandSplitted := split(fullCommand)
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
	default:
		fmt.Println("Unknown command")
	}
}

func main() {
	for {
		handleCommands()
	}
}
