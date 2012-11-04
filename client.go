package gnet

import (
  "net"
  "sync"
  "math/rand"
  "time"
)

func init() {
  rand.Seed(time.Now().UnixNano())
}

type Client struct {
  connPool *ConnPool
}

func NewClient(addr string, key string, conns int) (*Client, error) {
  raddr, err := net.ResolveTCPAddr("tcp", addr)
  if err != nil {
    return nil, err
  }

  dumbChan := make(chan *Session, CHAN_BUF_SIZE)
  go func() {
    for {
      <-dumbChan
    }
  }()
  self := &Client{
    connPool: newConnPool(key, dumbChan),
  }

  go func() { // watch for bad conn
    c := make(chan bool, CHAN_BUF_SIZE)
    self.connPool.badConn = c
    for {
      <-c
      retry := 5
      for retry > 0 {
        conn, err := net.DialTCP("tcp", nil, raddr)
        if err != nil {
          retry--
          continue
        }
        self.connPool.newConn <- conn
        break
      }
    }
  }()

  wg := new(sync.WaitGroup)
  wg.Add(conns)
  hasError := false
  for i := 0; i < conns; i++ {
    go func() {
      var conn *net.TCPConn
      conn, err = net.DialTCP("tcp", nil, raddr)
      if err != nil {
        hasError = true
      }
      self.connPool.newConn <- conn
      wg.Done()
    }()
  }
  wg.Wait()
  if hasError {
    return nil, err
  }

  return self, nil
}

func (self *Client) NewSession() *Session {
  id := uint64(rand.Int63())
  session := newSession(id, self.connPool.sendChan)
  self.connPool.sessions[id] = session
  return session
}
