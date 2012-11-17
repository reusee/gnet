package gnet

import (
  "net"
  "sync"
)

func (self *Session) ProxyTCP(conn *net.TCPConn, bufferSize int) {
  writeClosed := make(chan struct{})
  readClosed := make(chan struct{})
  go func() {
    <-writeClosed
    <-readClosed
    conn.Close()
    self.log("proxy closed")
  }()
  // to conn
  go func() {
    var once sync.Once
    for {
      select {
      case msg := <-self.Message:
        switch msg.Tag {
        case DATA:
          conn.Write(msg.Data)
        case STATE:
          switch msg.State {
          case STATE_FINISH_SEND, STATE_ABORT_SEND:
            conn.CloseWrite()
            once.Do(func() {
              close(writeClosed)
            })
          case STATE_ABORT_READ:
          case STATE_FINISH_READ:
          }
        }
      case <-self.Stopped:
        return
      }
    }
  }()
  // from conn
  for {
    buf := make([]byte, bufferSize)
    n, err := conn.Read(buf)
    if err != nil {
      conn.CloseRead()
      close(readClosed)
      self.FinishSend()
      return
    }
    self.Send(buf[:n])
  }
}
