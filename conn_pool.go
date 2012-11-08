package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "time"
)

type ConnPool struct {
  newConnChan chan *net.TCPConn
  deadConnNotify chan bool

  byteKeys []byte
  uint64Keys []uint64

  sendQueue chan ToSend
  infoChan chan ToSend
  rawSendChan chan []byte

  sessions map[uint64]*Session
  newSessionChan *chan *Session

  bytesRead uint64
  bytesWrite uint64

  conns []*Conn

  closed bool
  heartBeat *Ticker
}

func newConnPool(key string, newSessionChan *chan *Session) *ConnPool {
  self := &ConnPool{
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),

    sendQueue: make(chan ToSend, CHAN_BUF_SIZE),
    infoChan: make(chan ToSend, CHAN_BUF_SIZE),
    rawSendChan: make(chan []byte, CHAN_BUF_SIZE),

    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,

    conns: make([]*Conn, 0, 64),

    heartBeat: NewTicker(time.Second * 3),
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)

  go self.start()

  return self
}

func (self *ConnPool) start() {
  var bytesWrite, bytesRead uint64
  infoBuf := new(bytes.Buffer)

  for {
    select {
    case conn, ok := <-self.newConnChan:
      if !ok {
        return
      }
      self.conns = append(self.conns, newConn(conn, self))

    case info, ok := <-self.infoChan:
      if !ok {
        return
      }
      binary.Write(infoBuf, binary.BigEndian, info.session.id)
      infoBuf.Write(info.frame)

    case _, ok := <-self.heartBeat.C:
      if !ok {
        return
      }

      curBytesRead, curBytesWrite := self.bytesRead, self.bytesWrite
      self.log("read %d / %d write %d / %d\n",
      curBytesRead - bytesRead, curBytesRead,
      curBytesWrite - bytesWrite, curBytesWrite)
      bytesWrite, bytesRead = curBytesWrite, curBytesRead

      info := infoBuf.Bytes()
      frame := new(bytes.Buffer)
      frame.Write([]byte{PACKET_TYPE_INFO})
      binary.Write(frame, binary.BigEndian, uint32(len(info)))
      frame.Write(info)
      self.rawSendChan <- frame.Bytes()
      infoBuf = new(bytes.Buffer)
    }
  }
}

func (self *ConnPool) newSession(sessionId uint64) *Session {
  session := newSession(sessionId, self)
  self.sessions[sessionId] = session
  if self.newSessionChan != nil {
    *(self.newSessionChan) <- session
  }
  return session
}

func (self *ConnPool) Close() {
  self.closed = true
  for _, conn := range self.conns {
    //TODO 有些conn是失效了的，或者已经close了
    conn.Close()
  }
  self.log("connections stopped\n")
  for serial, session := range self.sessions {
    session.stop()
    delete(self.sessions, serial)
  }
  self.log("sessions stopped\n")
  close(self.infoChan)
  close(self.newConnChan)
  self.heartBeat.Stop()
}

func (self *ConnPool) log(f string, vars ...interface{}) {
  p("CONNPOOL " + f, vars...)
}
