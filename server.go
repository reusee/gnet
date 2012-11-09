package gnet

import (
  "net"
  "encoding/binary"
  "time"
)

type Server struct {
  ln *net.TCPListener
  closed bool
  connPools map[uint64]*ConnPool
  New chan *Session
  newConnChan chan *net.TCPConn
  stop chan struct{}
  connPoolStopNotify chan *ConnPool
}

func NewServer(addr string, key string) (*Server, error) {
  laddr, err := net.ResolveTCPAddr("tcp", addr)
  if err != nil {
    return nil, err
  }

  ln, err := net.ListenTCP("tcp", laddr)
  if err != nil {
    return nil, err
  }

  self := &Server{
    ln: ln,
    connPools: make(map[uint64]*ConnPool),
    New: make(chan *Session, CHAN_BUF_SIZE),
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),
    stop: make(chan struct{}),
    connPoolStopNotify: make(chan *ConnPool, CHAN_BUF_SIZE),
  }

  go self.startAcceptChan()
  go self.start(key)

  return self, nil
}

func (self *Server) startAcceptChan() {
  for {
    conn, err := self.ln.AcceptTCP()
    if err != nil {
      if self.closed {
        return
      }
      continue
    }
    self.newConnChan <- conn
  }
}

func (self *Server) start(key string) {
  heartBeat := time.Tick(time.Second * 2)
  tick := 0

  LOOP:
  for { // listen for incoming connection
    select {
    case conn := <-self.newConnChan:
      var clientId uint64
      err := binary.Read(conn, binary.BigEndian, &clientId)
      if err != nil {
        continue
      }
      if self.connPools[clientId] == nil {
        self.log("new conn pool from %d", clientId)
        connPool := newConnPool(key, &(self.New))
        connPool.clientId = clientId
        connPool.stopNotify = self.connPoolStopNotify
        self.connPools[clientId] = connPool
      }
      self.connPools[clientId].newConnChan <- conn

    case <-heartBeat:
      self.log("tick %d %d conn pools", tick, len(self.connPools))

    case connPool := <-self.connPoolStopNotify:
      self.log("conn pool for client %d stop", connPool.clientId)
      delete(self.connPools, connPool.clientId)

    case <-self.stop:
      break LOOP
    }
    tick++
  }

  // finalizer
  for _, connPool := range self.connPools {
    connPool.Stop()
  }
}

func (self *Server) Stop() {
  self.closed = true
  self.ln.Close()
  close(self.stop)
}

func (self *Server) log(f string, vars ...interface{}) {
  colorp("35", "SERVER: " + f, vars...)
}
