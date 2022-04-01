# Message Broker
This project implements a message broker using the Go programming language. It allows both queue-based and topic-based message passing.
Remote Procedure Calls are used for server and client communications with the message broker process. Additionally, clients can connect to the broker for subscribing to a particular topic.
Three executable go programs are implemented:
- **Broker**: This service offers RPC services and pub/sub connections.
- **Server**: It communicates with the broker for creating topics and sending messages on either queue or a particular topic.
- **Client**: This process acts as the consumer and can collect queue messages or subscribe to a specific topic.

## Prerequisites
Make sure you have Go v1.17 installed on your system.  
Also, make sure other processes do not use the ports specified in the config.json file. You can also change the ports in the config file to other open ports on your system.

## How to Run
### Broker
```bash
go run ./broker.go
```
### Server
```bash
go run ./server.go
```
### Client
```bash
go run ./client.go
```
## Supported Commands
### Broker
| Template                     | Example                         | Description                                                                  |
| ---------------------------- | ------------------------------- | ---------------------------------------------------------------------------- |
| print_send_queue             | print_send_queue                | Prints the first queue (messages from server to clients).                    |
| print_recv_queue             | print_recv_queue                | Prints the second queue (messages from clients to server).                   |
| clear_send_queue             | clear_send_queue                | Clears the first queue.                                                      |
| clear_recv_queue             | clear_recv_queue                | Clears the second queue.                                                     |
| print_topics                 | print_topics                    | Prints all available topics and their subscriber counts.                     |
| exit                         | exit                            | EXIT!                                                                        |
### Server
| Template                     | Example                         | Description                                                                  |
| ---------------------------- | ------------------------------- | ---------------------------------------------------------------------------- |
| send_message_async <message> | send_message_async test_message | Sends a message to the broker's send queue without being blocked.            |
| send_message_sync <message>  | send_message_sync test_message  | Sends a message to the broker and waits for the clients.                     |
| get_back_message             | get_back_message                | Gets message from broker's second queue.                                     | 
| create_topic <topic>         | create_topic test_topic         | Creates a new topic on the broker.                                           |
| publish <topic> <message>    | publish test_topic test_message | Sends a message on the specified topic so subscribing clients can access it. |
| exit                         | exit                            | EXIT!                                                                        |
### Client
| Template                     | Example                         | Description                                                                  |
| ---------------------------- | ------------------------------- | ---------------------------------------------------------------------------- |
| get_message                  | get_message                     | Gets a message from broker's first queue.                                    |
| send_back_message <message>  | send_back_message test_message  | Sends a message back to the server.                                          |
| subscribe <topic>            | subscribe test_topic            | Subscribes to a certain topic.                                               |
| unsubscribe <topic>          | unsubscribe test_topic          | Unsubscribes from the provided topic.                                        |
| exit                         | exit                            | EXIT!                                                                        |
