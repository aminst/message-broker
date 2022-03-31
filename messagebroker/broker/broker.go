package main

import (
	"bufio"
	"fmt"
	"log"
	"messagebroker/communication"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"
)

const MaxBufferSize = 10
const BufferOverflowError = "buffer is full"

type Message struct {
	Data string
}

type Queue struct {
	messages []Message
}

func (q *Queue) Put(msg *Message) error {
	if len(q.messages) == MaxBufferSize {
		return fmt.Errorf(BufferOverflowError)
	}
	q.messages = append(q.messages, *msg)
	return nil
}

func (q *Queue) Get() (*Message, error) {
	if len(q.messages) == 0 {
		return nil, nil
	}
	return &q.messages[0], nil
}

func (q *Queue) Pop() (*Message, error) {
	if len(q.messages) == 0 {
		return nil, nil
	}
	msg := q.messages[0]
	q.messages = q.messages[1:]
	return &msg, nil
}

func (q *Queue) Clear() error {
	q.messages = []Message{}
	return nil
}

type Topic struct {
	name        string
	subscribers []net.Conn
}

type Broker struct {
	sendQueue               Queue
	recvQueue               Queue
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
	err := b.sendQueue.Put(&Message{args.Message})
	if err != nil {
		if err.Error() == BufferOverflowError {
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
	err := b.recvQueue.Put(&Message{args.Message})
	if err != nil {
		if err.Error() == BufferOverflowError {
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
	l, e := net.Listen("tcp", "0.0.0.0:8989")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func printMessages(messages []Message) {
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
	var fullCommand string
	reader := bufio.NewReader(os.Stdin)
	fullCommand, _ = reader.ReadString('\n')
	fullCommand = strings.TrimSuffix(fullCommand, "\n")
	switch fullCommand {
	case "exit":
		os.Exit(0)
	case "print_send_queue":
		printMessages(b.sendQueue.messages)
	case "print_recv_queue":
		printMessages(b.recvQueue.messages)
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
	broker := &Broker{Queue{}, Queue{}, false, make(map[string]*Topic)}
	broker.serve()

	subListener, err := net.Listen("tcp", "0.0.0.0:8990")
	if err != nil {
		log.Fatal("listen error:", err)
	}

	go acceptClient(subListener, broker)

	for {
		handleCommands(broker)
	}
}
