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
  conns int

  livingConns int
  raddr *net.TCPAddr

  deadConnNotify chan bool

  heartBeat *Ticker
}

func NewClient(addr string, key string, conns int) (*Client, error) {
  raddr, err := net.ResolveTCPAddr("tcp", addr)
  if err != nil {
    return nil, err
  }

  self := &Client{
    connPool: newConnPool(key, nil),
    conns: conns,
    raddr: raddr,

    deadConnNotify: make(chan bool, CHAN_BUF_SIZE),

    heartBeat: NewTicker(time.Second * 5),
  }

  go self.startDeadConnWatcher()
  go self.startHeartBeat()

  err = self.connect(conns)
  if err != nil {
    return nil, err
  }

  return self, nil
}

func (self *Client) Close() {
  self.connPool.Close()
  self.heartBeat.Stop()
  close(self.deadConnNotify)
}

func (self *Client) startDeadConnWatcher() {
  self.connPool.deadConnNotify = self.deadConnNotify
  for {
    _, ok := <-self.deadConnNotify
    if !ok {
      return
    }
    if self.livingConns > 0 {
      self.livingConns--
    }
  }
}

func (self *Client) startHeartBeat() {
  for {
    _, ok := <-self.heartBeat.C
    if !ok {
      return
    }

    self.log("living connections %d\n", self.livingConns)
    if self.livingConns == 0 && !self.connPool.closed {
      self.log("lost all connection to server\n")
      time.Sleep(time.Second * 30)
      self.connect(self.conns)
    } else if self.livingConns < self.conns && !self.connPool.closed {
      self.connect(self.conns - self.livingConns)
    }
  }
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
  return self.connPool.newSession(uint64(rand.Int63()))
}

func (self *Client) log(s string, vars ...interface{}) {
  p("CLIENT " + s, vars...)
}
