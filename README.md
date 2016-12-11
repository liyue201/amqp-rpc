# amqp-rpc 
A rpc package based on amqp (Advanced Message Queuing Protocol)


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

```

## Client connection pool

```go

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

```
