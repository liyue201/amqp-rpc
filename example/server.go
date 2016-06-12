package main

import (
	"log"
	//"time"

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

	//go func() {
	//	time.Sleep(time.Second * 10)
	//	server.StopService()
	//}()

	err := server.Serve("amqp://guest:guest@localhost:5672/", "rpc_queue", 10)
	if err != nil {
		log.Println(err)
	}
}
