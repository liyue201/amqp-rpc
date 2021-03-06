package main

import (
	"fmt"
	"log"

	"github.com/liyue201/amqp-rpc/rpc"
)

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
	client.Call("Calculator.Add", &arg, &result, 1<<30)
	log.Println(fmt.Sprintf("%d + %d = %d", arg.A, arg.B, result))

	client.Call("Calculator.Sub", &arg, &result, 1<<30)
	log.Println(fmt.Sprintf("%d - %d = %d", arg.A, arg.B, result))

}
