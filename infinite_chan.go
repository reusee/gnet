package gnet

import (
  "net"
)

// generic
// InfiniteChan
// interface{}
// NewInfiniteChan

type InfiniteChan struct {
  In chan interface{}
  Out chan interface{}
  buffer []interface{}
  stop chan struct{}
}

func NewInfiniteChan() *InfiniteChan {
  self := &InfiniteChan{
    In: make(chan interface{}),
    Out: make(chan interface{}),
    buffer: make([]interface{}, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// bool
type InfiniteBoolChan struct {
  In chan bool
  Out chan bool
  buffer []bool
  stop chan struct{}
}

func NewInfiniteBoolChan() *InfiniteBoolChan {
  self := &InfiniteBoolChan{
    In: make(chan bool),
    Out: make(chan bool),
    buffer: make([]bool, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteBoolChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteBoolChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// []byte
type InfiniteByteSliceChan struct {
  In chan []byte
  Out chan []byte
  buffer [][]byte
  stop chan struct{}
}

func NewInfiniteByteSliceChan() *InfiniteByteSliceChan {
  self := &InfiniteByteSliceChan{
    In: make(chan []byte),
    Out: make(chan []byte),
    buffer: make([][]byte, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteByteSliceChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteByteSliceChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// *net.TCPConn
type InfiniteTCPConnChan struct {
  In chan *net.TCPConn
  Out chan *net.TCPConn
  buffer []*net.TCPConn
  stop chan struct{}
}

func NewInfiniteTCPConnChan() *InfiniteTCPConnChan {
  self := &InfiniteTCPConnChan{
    In: make(chan *net.TCPConn),
    Out: make(chan *net.TCPConn),
    buffer: make([]*net.TCPConn, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteTCPConnChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteTCPConnChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// *Session
type InfiniteSessionChan struct {
  In chan *Session
  Out chan *Session
  buffer []*Session
  stop chan struct{}
}

func NewInfiniteSessionChan() *InfiniteSessionChan {
  self := &InfiniteSessionChan{
    In: make(chan *Session),
    Out: make(chan *Session),
    buffer: make([]*Session, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func NewInfiniteSessionChanWithOutChan(out chan *Session) *InfiniteSessionChan {
  self := &InfiniteSessionChan{
    In: make(chan *Session),
    Out: out,
    buffer: make([]*Session, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteSessionChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteSessionChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// *ConnPool
type InfiniteConnPoolChan struct {
  In chan *ConnPool
  Out chan *ConnPool
  buffer []*ConnPool
  stop chan struct{}
}

func NewInfiniteConnPoolChan() *InfiniteConnPoolChan {
  self := &InfiniteConnPoolChan{
    In: make(chan *ConnPool),
    Out: make(chan *ConnPool),
    buffer: make([]*ConnPool, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteConnPoolChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteConnPoolChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// *Conn
type InfiniteConnChan struct {
  In chan *Conn
  Out chan *Conn
  buffer []*Conn
  stop chan struct{}
}

func NewInfiniteConnChan() *InfiniteConnChan {
  self := &InfiniteConnChan{
    In: make(chan *Conn),
    Out: make(chan *Conn),
    buffer: make([]*Conn, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteConnChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteConnChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

// ToSend
type InfiniteToSendChan struct {
  In chan ToSend
  Out chan ToSend
  buffer []ToSend
  stop chan struct{}
}

func NewInfiniteToSendChan() *InfiniteToSendChan {
  self := &InfiniteToSendChan{
    In: make(chan ToSend),
    Out: make(chan ToSend),
    buffer: make([]ToSend, 0, INITIAL_BUF_CAPACITY),
    stop: make(chan struct{}),
  }
  go self.start()
  return self
}

func (self *InfiniteToSendChan) start() {
  for {
    if len(self.buffer) > 0 {
      select {
      case self.Out <- self.buffer[0]:
        self.buffer = self.buffer[1:]
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    } else {
      select {
      case value := <-self.In:
        self.buffer = append(self.buffer, value)
      case <-self.stop:
        return
      }
    }
  }
}

func (self *InfiniteToSendChan) Stop() {
  self.buffer = nil
  close(self.stop)
}

