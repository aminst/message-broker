package communication

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
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

func BrokerSock() string {
	s := "/var/tmp/broker-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func Call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := BrokerSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
