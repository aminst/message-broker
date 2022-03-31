package queue

import "fmt"

const MaxBufferSize = 10
const BufferOverflowError = "buffer is full"

type Message struct {
	Data string
}

type Queue struct {
	Messages []Message
}

func (q *Queue) Put(msg *Message) error {
	if len(q.Messages) == MaxBufferSize {
		return fmt.Errorf(BufferOverflowError)
	}
	q.Messages = append(q.Messages, *msg)
	return nil
}

func (q *Queue) Get() (*Message, error) {
	if len(q.Messages) == 0 {
		return nil, nil
	}
	return &q.Messages[0], nil
}

func (q *Queue) Pop() (*Message, error) {
	if len(q.Messages) == 0 {
		return nil, nil
	}
	msg := q.Messages[0]
	q.Messages = q.Messages[1:]
	return &msg, nil
}

func (q *Queue) Clear() error {
	q.Messages = []Message{}
	return nil
}
