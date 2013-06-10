package main

import (
	"fmt"
	zmq "github.com/alecthomas/gozmq"
)

func main() {
	context, err := zmq.NewContext()
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	push, err := context.NewSocket(zmq.PUSH)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	err = push.Connect("tcp://127.0.0.1:4001")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	sub, err := context.NewSocket(zmq.SUB)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	sub.SetSockOptString(zmq.SUBSCRIBE, "")
	err = sub.Connect("tcp://127.0.0.1:5001")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	defer func() {
		sub.Close()
		push.Close()
		context.Close()
	}()

	go func() {
		for i := 0; i < 10; i++ {
			push.Send([]byte("ECHO"), 0)
		}
	}()

	for {
		sub.Recv(0) // Receive empty key
		d, _ := sub.Recv(0)

		// NOTE: only some will be received
		fmt.Println(string(d))
	}
}
