package main

import (
	"fmt"
	"log"
	"time"

	"github.com/liyue201/amqp-rpc/rpc"
)

type Arg struct {
	A int
	B int
}

func main() {

	p := &rpc.Pool{
		MaxActive:   10,
		MaxIdle:     2,
		IdleTimeout: time.Second * 1000,
		Wait:        true,

		Dial: func() (rpc.Conn, error) {
			client, err := rpc.Dial("amqp://guest:guest@localhost:5672/", "rpc_queue")
			return client, err
		},
	}
	defer p.Close()

	client := p.Get()
	defer client.Close()

	arg := Arg{A: 3, B: 1}
	var result int
	client.Call("Calculator.Add", &arg, &result, 1<<30)
	log.Println(fmt.Sprintf("%d + %d = %d", arg.A, arg.B, result))
}
