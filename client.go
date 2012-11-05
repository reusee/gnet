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

  Close func()
}

func NewClient(addr string, key string, conns int) (*Client, error) {
  raddr, err := net.ResolveTCPAddr("tcp", addr)
  if err != nil {
    return nil, err
  }

  dumbChan := make(chan *Session, CHAN_BUF_SIZE)
  endDumbChan := make(chan bool)
  go func() { // newSessionChan
    for {
      select {
      case <-dumbChan:
      case <-endDumbChan:
        break
      }
    }
  }()

  connPool := newConnPool(key, dumbChan)

  endBadConnWatcher := make(chan bool)
  go func() { // watch for bad conn
    c := make(chan bool, CHAN_BUF_SIZE)
    connPool.badConnChan = c
    for {
      select {
      case <-c:
        retry := 5
        for retry > 0 {
          conn, err := net.DialTCP("tcp", nil, raddr)
          if err != nil {
            retry--
            continue
          }
          connPool.newConnChan <- conn
          break
        }
      case <-endBadConnWatcher:
        break
      }
    }
  }()

  self := &Client{
    connPool: connPool,
    Close: func() {
      endDumbChan <- true
      endBadConnWatcher <- true
    },
  }

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
      self.connPool.newConnChan <- conn
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
  session := newSession(id, self.connPool.sendDataChan, self.connPool.sendStateChan)
  self.connPool.sessions[id] = session
  return session
}
