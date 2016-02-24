package main

import (
	"fmt"
	"github.com/liyue201/amqp-rpc/rpc"
	"log"
)

var wait chan bool

type Arg struct {
	A int
	B int
}

func main() {

	client, err := rpc.Dial("amqp://guest:guest@localhost:5672/", "rpc_queue")
	if err != nil {
		log.Println(err)
	}
	defer client.Close()

	arg := Arg{A: 3, B: 1}
	var result int
	err = client.Call("Calculator.Add", &arg, &result, 10000)
	if err != nil {
		log.Println(err)
	} else {
		log.Println(fmt.Sprintf("%d + %d = %d", arg.A, arg.B, result))
	}

	err = client.Call("Calculator.Sub", &arg, &result, 10000)
	if err != nil {
		log.Println(err)
	} else {
		log.Println(fmt.Sprintf("%d - %d = %d", arg.A, arg.B, result))
	}
}
