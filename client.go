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
  livingConns int
  raddr *net.TCPAddr
}

func NewClient(addr string, key string, conns int) (*Client, error) {
  raddr, err := net.ResolveTCPAddr("tcp", addr)
  if err != nil {
    return nil, err
  }

  dumbChan := make(chan *Session, CHAN_BUF_SIZE)
  stopDumbChan := make(chan bool, 8)
  go func() { // newSessionChan
    for {
      select {
      case <-dumbChan:
      case <-stopDumbChan:
        break
      }
    }
  }()

  connPool := newConnPool(key, dumbChan)
  stopBadConnWatcher := make(chan bool, 8)
  stopHeartBeat := make(chan bool, 8)
  self := &Client{
    connPool: connPool,
    raddr: raddr,
  }
  self.Close = func() {
    stopDumbChan <- true
    stopBadConnWatcher <- true
    self.connPool.Close()
  }

  go func() { // watch for bad conn
    c := make(chan bool, CHAN_BUF_SIZE)
    connPool.deadConnNotify = c
    for {
      select {
      case <-c:
        if self.livingConns > 0 {
          self.livingConns--
        }
      case <-stopBadConnWatcher:
        break
      }
    }
  }()

  err = self.connect(conns)
  if err != nil {
    return nil, err
  }

  go func() {
    heartBeat := time.NewTicker(time.Second * 5)
    for {
      select {
      case <-heartBeat.C:

        self.log("living connections %d\n", self.livingConns)
        if self.livingConns == 0 && !self.connPool.closed {
          self.log("lost all connection to server\n")
          time.Sleep(time.Second * 30)
          self.connect(conns)
        } else if self.livingConns < conns && !self.connPool.closed {
          self.connect(conns - self.livingConns)
        }

      case <-stopHeartBeat:
        p("stop client heartbeat\n")
        return
      }
    }
  }()

  return self, nil
}

func (self *Client) connect(conns int) (err error) {
  wg := new(sync.WaitGroup)
  wg.Add(conns)
  for i := 0; i < conns; i++ {
    go func() {
      defer wg.Done()
      var conn *net.TCPConn
      conn, err = net.DialTCP("tcp", nil, self.raddr)
      if err != nil {
        return
      }
      self.connPool.newConnChan <- conn
      self.livingConns++
    }()
  }
  wg.Wait()
  return
}

func (self *Client) NewSession() *Session {
  id := uint64(rand.Int63())
  session := newSession(id, self.connPool)
  self.connPool.sessions[id] = session
  return session
}

func (self *Client) log(s string, vars ...interface{}) {
  p("CLIENT " + s, vars...)
}
