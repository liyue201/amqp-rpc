package main

import (
	"log"

	"github.com/liyue201/amqp-rpc/rpc"
)

type Calculator struct {
}

type Arg struct {
	A int
	B int
}

func (this *Calculator) Add(arg *Arg, result *int) error {
	*result = arg.A + arg.B
	return nil
}

func (this *Calculator) Sub(arg *Arg, result *int) error {
	*result = arg.A - arg.B
	return nil
}

func main() {
	server := rpc.NewServer()
	server.Register(new(Calculator))

	err := server.Serve("amqp://guest:guest@localhost:5672/", "rpc_queue", 10)
	if err != nil {
		log.Println(err)
	}
}
