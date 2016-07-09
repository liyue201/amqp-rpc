package rpc

import (
	"container/list"
	"errors"
	"sync"
	"time"
)

var ErrPoolExhausted = errors.New("rpc: connection pool exhausted")

var (
	errPoolClosed = errors.New("rpc: connection pool closed")
	errConnClosed = errors.New("rpc: connection closed")
)

type Conn interface {
	Call(serviceMethod string, args interface{}, reply interface{}, expiration uint64) error
	Close() error
	Err() error
}

type Pool struct {
	Dial         func() (Conn, error)
	TestOnBorrow func(c Conn, t time.Time) error
	MaxIdle      int
	MaxActive    int
	IdleTimeout  time.Duration
	Wait         bool
	mu           sync.Mutex
	cond         *sync.Cond
	closed       bool
	active       int
	idle         list.List
}

type idleConn struct {
	c Conn
	t time.Time
}

func (p *Pool) Get() Conn {
	c, err := p.get()
	if err != nil {
		return errorConnection{err}
	}
	return &pooledConnection{p: p, c: c}
}

// Close releases the resources used by the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	idle := p.idle
	p.idle.Init()
	p.closed = true
	p.active -= idle.Len()
	if p.cond != nil {
		p.cond.Broadcast()
	}
	p.mu.Unlock()
	for e := idle.Front(); e != nil; e = e.Next() {
		e.Value.(idleConn).c.Close()
	}
	return nil
}

func (p *Pool) get() (Conn, error) {
	p.mu.Lock()

	if timeout := p.IdleTimeout; timeout > 0 {
		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Back()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			if ic.t.Add(timeout).After(time.Now()) {
				break
			}
			p.idle.Remove(e)
			p.release()
			p.mu.Unlock()
			ic.c.Close()
			p.mu.Lock()
		}
	}

	for {
		// Get idle connection.

		for i, n := 0, p.idle.Len(); i < n; i++ {
			e := p.idle.Front()
			if e == nil {
				break
			}
			ic := e.Value.(idleConn)
			p.idle.Remove(e)
			test := p.TestOnBorrow
			p.mu.Unlock()

			if ic.c.Err() == nil && (test == nil || test(ic.c, ic.t) == nil) {
				return ic.c, nil
			}

			ic.c.Close()
			p.mu.Lock()
			p.release()
		}

		// Check for pool closed before dialing a new connection.

		if p.closed {
			p.mu.Unlock()
			return nil, errConnClosed
		}

		// Dial new connection if under limit.

		if p.MaxActive == 0 || p.active < p.MaxActive {
			dial := p.Dial
			p.active += 1
			p.mu.Unlock()
			c, err := dial()
			if err != nil {
				p.mu.Lock()
				p.release()
				p.mu.Unlock()
				c = nil
			}
			return c, err
		}

		if !p.Wait {
			p.mu.Unlock()
			return nil, ErrPoolExhausted
		}

		if p.cond == nil {
			p.cond = sync.NewCond(&p.mu)
		}
		p.cond.Wait()
	}
}

func (p *Pool) put(c Conn) error {
	err := c.Err()
	p.mu.Lock()
	if !p.closed && err == nil {
		p.idle.PushFront(idleConn{t: time.Now(), c: c})
		if p.idle.Len() > p.MaxIdle {
			c = p.idle.Remove(p.idle.Back()).(idleConn).c
		} else {
			c = nil
		}
	}

	if c == nil {
		if p.cond != nil {
			p.cond.Signal()
		}
		p.mu.Unlock()
		return nil
	}

	p.release()
	p.mu.Unlock()

	return c.Close()
}

func (p *Pool) release() {
	p.active -= 1
	if p.cond != nil {
		p.cond.Signal()
	}
}

type pooledConnection struct {
	p *Pool
	c Conn
}

func (pc *pooledConnection) Call(serviceMethod string, args interface{}, reply interface{},
	expiration uint64) error {
	return pc.c.Call(serviceMethod, args, reply, expiration)
}

func (pc *pooledConnection) Close() error {

	c := pc.c
	if _, ok := c.(errorConnection); ok {
		return nil
	}
	pc.c = errorConnection{errConnClosed}
	pc.p.put(c)
	return nil
}

func (pc *pooledConnection) Err() error {
	return pc.c.Err()
}

type errorConnection struct {
	err error
}

func (ec errorConnection) Call(serviceMethod string, args interface{}, reply interface{},
	expiration uint64) error {
	return ec.err
}

func (ec errorConnection) Close() error {
	return ec.err
}

func (ec errorConnection) Err() error {
	return ec.err
}
