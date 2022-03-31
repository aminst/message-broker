package main

import (
	"bufio"
	"fmt"
	"log"
	"messagebroker/broker/queue"
	"messagebroker/communication"
	"messagebroker/config"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type Topic struct {
	name        string
	subscribers []net.Conn
}

type Broker struct {
	sendQueue               queue.Queue
	recvQueue               queue.Queue
	isSyncMessageTransfered bool
	topics                  map[string]*Topic
}

func (b *Broker) removeConnFromTopicSubscribers(conn net.Conn, topicName string) {
	for i, c := range b.topics[topicName].subscribers {
		if c == conn {
			b.topics[topicName].subscribers = append(b.topics[topicName].subscribers[:i], b.topics[topicName].subscribers[i+1:]...)
			return
		}
	}
}

func (b *Broker) isConnSubscribedToTopic(conn net.Conn, topicName string) bool {
	for _, c := range b.topics[topicName].subscribers {
		if c == conn {
			return true
		}
	}
	return false
}

func (b *Broker) PutMessage(args *communication.PutMessageArgs, reply *communication.PutMessageReply) error {
	b.isSyncMessageTransfered = false
	err := b.sendQueue.Put(&queue.Message{Data: args.Message})
	if err != nil {
		if err.Error() == queue.BufferOverflowError {
			reply.IsBufferOverflow = true
			return nil
		}
		return err
	}
	if args.IsAsync {
		return nil
	}
	for !b.isSyncMessageTransfered {
		time.Sleep(time.Second)
	}
	reply.IsBufferOverflow = false
	return nil
}

func (b *Broker) PutBackMessage(args *communication.PutBackMessageArgs, reply *communication.PutBackMessageReply) error {
	err := b.recvQueue.Put(&queue.Message{Data: args.Message})
	if err != nil {
		if err.Error() == queue.BufferOverflowError {
			reply.IsBufferOverflow = true
			return nil
		}
		return err
	}
	reply.IsBufferOverflow = false
	return nil
}

func (b *Broker) GetMessage(args *communication.GetMessageArgs, reply *communication.GetMessageReply) error {
	msg, err := b.sendQueue.Pop()
	if err != nil {
		return err
	}
	if msg != nil {
		reply.Message = msg.Data
		b.isSyncMessageTransfered = true
	}
	return nil
}

func (b *Broker) GetBackMessage(args *communication.GetBackMessageArgs, reply *communication.GetBackMessageReply) error {
	msg, err := b.recvQueue.Pop()
	if err != nil {
		return err
	}
	if msg != nil {
		reply.Message = msg.Data
	}
	return nil
}

func (b *Broker) CreateTopic(args *communication.CreateTopicArgs, reply *communication.CreateTopicReply) error {
	if _, ok := b.topics[args.TopicName]; ok {
		return fmt.Errorf("topic already exists")
	}
	b.topics[args.TopicName] = &Topic{args.TopicName, []net.Conn{}}
	return nil
}

func (b *Broker) Publish(args *communication.PublishArgs, reply *communication.PublishReply) error {
	if _, ok := b.topics[args.TopicName]; !ok {
		return fmt.Errorf("topic doesn't exist")
	}
	for _, c := range b.topics[args.TopicName].subscribers {
		fmt.Println(args.Message)
		c.Write([]byte(args.Message + "\n"))
	}
	return nil
}

func (b *Broker) serve() {
	rpc.Register(b)
	rpc.HandleHTTP()
	rpcHost := config.GetRPCHost()
	rpcPort := config.GetRPCPort()
	connectionAddress := fmt.Sprintf("%s:%d", rpcHost, rpcPort)
	l, e := net.Listen("tcp", connectionAddress)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func printMessages(messages []queue.Message) {
	fmt.Print("[")
	for i := len(messages) - 1; i >= 0; i-- {
		fmt.Printf("%s", messages[i].Data)
		if i > 0 {
			fmt.Print(" ")
		}
	}
	fmt.Println("]")
}

func handleCommands(b *Broker) {
	fmt.Println("Enter command (type 'exit' to quit):")
	fmt.Println("print_send_queue|print_recv_queue|clear_send_queue|clear_recv_queue|print_topics")
	var fullCommand string
	reader := bufio.NewReader(os.Stdin)
	fullCommand, _ = reader.ReadString('\n')
	fullCommand = strings.TrimSuffix(fullCommand, "\n")
	switch fullCommand {
	case "exit":
		os.Exit(0)
	case "print_send_queue":
		printMessages(b.sendQueue.Messages)
	case "print_recv_queue":
		printMessages(b.recvQueue.Messages)
	case "clear_send_queue":
		b.sendQueue.Clear()
		fmt.Println("send queue cleared")
	case "clear_recv_queue":
		b.recvQueue.Clear()
		fmt.Println("recv queue cleared")
	case "print_topics":
		for _, topic := range b.topics {
			fmt.Printf("%s: %d subscribers\n", topic.name, len(topic.subscribers))
		}
	default:
		fmt.Println("Unknown command")
	}
}

func closeConn(conn net.Conn, b *Broker) {
	for topicName := range b.topics {
		if b.isConnSubscribedToTopic(conn, topicName) {
			b.removeConnFromTopicSubscribers(conn, topicName)
		}
	}
	err := conn.Close()
	if err != nil {
		log.Print("close error:", err)
	}
}

func handleClientRequest(con net.Conn, b *Broker) {
	defer closeConn(con, b)
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadString('\n')
		if err != nil {
			log.Print("read error:", err)
			break
		}
		clientRequest = strings.TrimSuffix(clientRequest, "\n")
		clientRequestSplitted := strings.Split(clientRequest, " ")
		switch clientRequestSplitted[0] {
		case "subscribe":
			topic := clientRequestSplitted[1]
			b.topics[topic].subscribers = append(b.topics[topic].subscribers, con)
		case "unsubscribe":
			topic := clientRequestSplitted[1]
			b.removeConnFromTopicSubscribers(con, topic)
		}
	}
}

func acceptClient(subListener net.Listener, b *Broker) {
	for {
		con, err := subListener.Accept()
		if err != nil {
			log.Print("accept error:", err)
			continue
		}
		go handleClientRequest(con, b)
	}
}

func main() {
	broker := &Broker{queue.Queue{}, queue.Queue{}, false, make(map[string]*Topic)}
	broker.serve()

	pubSubHost := config.GetPubSubHost()
	pubSubPort := config.GetPubSubPort()
	connectionAddress := fmt.Sprintf("%s:%d", pubSubHost, pubSubPort)
	subListener, err := net.Listen("tcp", connectionAddress)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	go acceptClient(subListener, broker)

	for {
		handleCommands(broker)
	}
}
