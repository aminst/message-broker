package main

import (
	"bufio"
	"fmt"
	"messagebroker/communication"
	"os"
	"strings"
)

func CallForPutMessage(message string, isAsync bool) communication.PutMessageReply {
	args := communication.PutMessageArgs{Message: message, IsAsync: isAsync}
	reply := communication.PutMessageReply{}
	if !communication.Call("Broker.PutMessage", &args, &reply) {
		os.Exit(0)
	}
	return reply
}

func CallForGetBackMessage() communication.GetBackMessageReply {
	args := communication.GetBackMessageArgs{}
	reply := communication.GetBackMessageReply{}
	if !communication.Call("Broker.GetBackMessage", &args, &reply) {
		os.Exit(0)
	}
	return reply
}

func CallForCreateTopic(topic string) communication.CreateTopicReply {
	args := communication.CreateTopicArgs{TopicName: topic}
	reply := communication.CreateTopicReply{}
	if !communication.Call("Broker.CreateTopic", &args, &reply) {
		os.Exit(0)
	}
	return reply
}

func CallForPublish(topic string, message string) communication.PublishReply {
	args := communication.PublishArgs{TopicName: topic, Message: message}
	reply := communication.PublishReply{}
	if !communication.Call("Broker.Publish", &args, &reply) {
		os.Exit(0)
	}
	return reply
}

func handleCommands() {
	fmt.Println("Enter command (type 'exit' to quit):")
	var fullCommand string
	reader := bufio.NewReader(os.Stdin)
	fullCommand, _ = reader.ReadString('\n')
	fullCommand = strings.TrimSuffix(fullCommand, "\n")
	commandSplitted := strings.Split(fullCommand, " ")
	command := commandSplitted[0]
	switch command {
	case "exit":
		os.Exit(0)
	case "send_message_async":
		message := commandSplitted[1]
		reply := CallForPutMessage(message, true)
		if reply.IsBufferOverflow {
			fmt.Println("Buffer overflow")
		} else {
			fmt.Println("Message sent successfully")
		}
	case "send_message_sync":
		message := commandSplitted[1]
		reply := CallForPutMessage(message, false)
		if reply.IsBufferOverflow {
			fmt.Println("Buffer overflow")
		} else {
			fmt.Println("Message sent successfully")
		}
	case "get_back_message":
		reply := CallForGetBackMessage()
		message := reply.Message
		if message == "" {
			fmt.Println("No message to get back")
		} else {
			fmt.Println("Message: " + message)
		}
	case "create_topic":
		topic := commandSplitted[1]
		CallForCreateTopic(topic)
		fmt.Println("Topic created successfully")
	case "publish":
		topic := commandSplitted[1]
		message := commandSplitted[2]
		CallForPublish(topic, message)
		fmt.Println("Message published successfully")
	default:
		fmt.Println("Unknown command")
	}
}

func main() {
	for {
		handleCommands()
	}
}
