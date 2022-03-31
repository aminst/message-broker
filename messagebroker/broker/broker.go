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

type Broker struct {
	sendQueue               Queue
	recvQueue               Queue
	isSyncMessageTransfered bool
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

func (b *Broker) serve() {
	rpc.Register(b)
	rpc.HandleHTTP()
	sockName := communication.BrokerSock()
	os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
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
	default:
		fmt.Println("Unknown command")
	}
}

func main() {
	broker := &Broker{Queue{}, Queue{}, false}
	broker.serve()
	for {
		handleCommands(broker)
	}
}
