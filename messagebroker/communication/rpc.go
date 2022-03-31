package communication

import (
	"fmt"
	"log"
	"net/rpc"
)

type PutMessageArgs struct {
	Message string
	IsAsync bool
}

type PutMessageReply struct {
	IsBufferOverflow bool
}

type PutBackMessageArgs struct {
	Message string
}

type PutBackMessageReply struct {
	IsBufferOverflow bool
}

type GetMessageArgs struct {
}

type GetMessageReply struct {
	Message string
}

type GetBackMessageArgs struct {
}

type GetBackMessageReply struct {
	Message string
}

type CreateTopicArgs struct {
	TopicName string
}

type CreateTopicReply struct {
}

type PublishArgs struct {
	TopicName string
	Message   string
}

type PublishReply struct {
}

func Call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "0.0.0.0:8989")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
