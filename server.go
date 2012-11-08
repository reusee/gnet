package gnet

import (
  "net"
)

type Server struct {
  ln *net.TCPListener
  closed bool

  connPools map[string]*ConnPool

  New chan *Session
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
    connPools: make(map[string]*ConnPool),
    New: make(chan *Session, CHAN_BUF_SIZE),
  }

  go self.start(key)

  return self, nil
}

func (self *Server) start(key string) {
  for { // listen for incoming connection
    conn, err := self.ln.AcceptTCP()
    if err != nil {
      if self.closed {
        return
      }
      continue
    }
    raddr := conn.RemoteAddr().String()
    host, _, _ := net.SplitHostPort(raddr)
    self.log("new conn from %s\n", host)
    if self.connPools[host] == nil { // a new remote host
      self.connPools[host] = newConnPool(key, &self.New)
    }
    self.connPools[host].newConnChan <- conn
  }
}

func (self *Server) Close() {
  self.closed = true
  self.ln.Close()
  for _, connPool := range self.connPools {
    self.log("start close conn pool\n")
    connPool.Close()
  }
}

func (self *Server) log(f string, vars ...interface{}) {
  p("SERVER: " + f, vars...)
}
