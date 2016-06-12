# amqp-rpc 
A golang RPC framwork base on amqp (Advanced Message Queuing Protocol)


##run example##
go run server.go

go run client.go

# Example

## Server

```go

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
```

## Client

```go

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

```
