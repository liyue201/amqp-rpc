package rpc

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// ServerError represents an error that has been returned from
// the remote side of the RPC connection.
type ServerError string

func (e ServerError) Error() string {
	return string(e)
}

var ErrShutdown = errors.New("connection is shut down")
var ErrTimeout = errors.New("Timeout")

// Call represents an active RPC.
type Call struct {
	ServiceMethod string      // The name of the service and method to call.
	Args          interface{} // The argument to the function (*struct).
	Reply         interface{} // The reply from the function (*struct).
	Error         error       // After completion, the error status.
	Done          chan *Call  // Strobes when call is complete.
}

type Client struct {
	serverQueueName string
	conn            *amqp.Connection
	chanel          *amqp.Channel
	queue           *amqp.Queue

	reqMutex sync.Mutex // protects following
	request  Request

	mutex    sync.Mutex // protects following
	seq      string
	pending  map[string]*Call
	closing  bool // user has called Close
	shutdown bool // server has told us to stop
	lastErr  error
}

func GetMd5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

func GetGuid() string {
	b := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return ""
	}
	return GetMd5String(base64.URLEncoding.EncodeToString(b))
}

func (call *Call) done() {
	select {
	case call.Done <- call:
	default:
		log.Println("rpc: discarding Call reply due to insufficient Done chan capacity")
	}
}

func NewClient(queueName string, conn *amqp.Connection, ch *amqp.Channel, q *amqp.Queue) (*Client, error) {
	client := &Client{serverQueueName: queueName, conn: conn, chanel: ch, queue: q}
	client.pending = make(map[string]*Call)
	return client, nil
}

func Dial(url string, serverQueueName string) (*Client, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		log.Println("err:", err)
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Println("err:", err)
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when usused
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		log.Println("err:", err)
		return nil, err
	}
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Println("err:", err)
		return nil, err
	}

	client, _ := NewClient(serverQueueName, conn, ch, &q) // &Client{conn: conn, chanel: ch, queue: &q}

	go client.handleDeliveries(msgs)

	return client, nil
}

func (client *Client) handleDeliveries(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		response := Response{}
		decBuf := bytes.NewBuffer(d.Body)
		dec := gob.NewDecoder(decBuf)
		err := dec.Decode(&response)
		if err != nil {
			log.Println("handleDeliveries, decode reponse err:", err)
			continue
		}
		seq := response.Seq
		client.mutex.Lock()
		call := client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		switch {
		case call == nil:
			log.Println("handleDeliveries error: call is nil")
		case response.Error != "":
			call.Error = ServerError(response.Error)
			call.done()
		default:
			err = dec.Decode(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}

	client.mutex.Lock()
	client.shutdown = true
	client.mutex.Unlock()

	for _, call := range client.pending {
		call.Error = ErrShutdown
		call.done()
	}

	client.fatal(ErrShutdown)

	log.Println("handleDeliveries:", ErrShutdown)
}

func (client *Client) Close() error {
	client.mutex.Lock()
	if client.closing {
		client.mutex.Unlock()
		return ErrShutdown
	}
	client.closing = true
	client.mutex.Unlock()

	err := client.conn.Close()
	if err != nil {
		return err
	}
	err = client.chanel.Close()
	return err
}

//expiration field describes the TTL period in milliseconds
func (client *Client) call(serviceMethod string, args interface{}, reply interface{}, done chan *Call, expiration uint64) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply
	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done
	client.send(call, expiration)
	return call
}

func (client *Client) Go(serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	const forever = 1 << 60
	return client.call(serviceMethod, args, reply, done, forever)
}

//expiration field describes the TTL period in milliseconds
func (client *Client) Call(serviceMethod string, args interface{}, reply interface{}, expiration uint64) error {
	//fmt.Println("Call:", serviceMethod, args, reply)
	call := client.call(serviceMethod, args, reply, make(chan *Call, 1), expiration)

	select {
	case <-call.Done:
		return call.Error
	case <-time.After(time.Millisecond * time.Duration(expiration)):
		return ErrTimeout
	}
	return call.Error
}

func (client *Client) send(call *Call, expiration uint64) {
	client.reqMutex.Lock()
	defer client.reqMutex.Unlock()

	// Register this call.
	client.mutex.Lock()
	if client.shutdown || client.closing {
		call.Error = ErrShutdown
		client.mutex.Unlock()
		call.done()
		return
	}

	seq := GetGuid()
	client.seq = seq
	client.pending[seq] = call
	client.mutex.Unlock()

	// Encode and send the request.
	client.request.Seq = seq
	client.request.ServiceMethod = call.ServiceMethod

	reqData, err := client.encodeRequest(&client.request, call.Args)
	if err != nil {
		log.Println("send,encodeRequest error:", err)
		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
		return
	}

	//log.Println("send:, reqData[]:", reqData)

	strExpiration := fmt.Sprintf("%d", expiration)
	err = client.chanel.Publish(
		"", // exchange
		client.serverQueueName, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "application/octet-stream",
			CorrelationId: client.request.Seq,
			ReplyTo:       client.queue.Name,
			Body:          reqData,
			Expiration:    strExpiration,
		})

	if err != nil {
		log.Println("send err:", err)

		client.mutex.Lock()
		call = client.pending[seq]
		delete(client.pending, seq)
		client.mutex.Unlock()
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

func (client *Client) encodeRequest(r *Request, body interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(r)
	if err != nil {
		return nil, err
	}
	err = enc.Encode(body)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (client *Client) fatal(err error) error {
	client.mutex.Lock()
	if client.lastErr == nil {
		client.lastErr = err
		client.conn.Close()
	}

	client.mutex.Unlock()
	return err
}

func (client *Client) Err() error {

	client.mutex.Lock()
	err := client.lastErr
	client.mutex.Unlock()
	return err
}
