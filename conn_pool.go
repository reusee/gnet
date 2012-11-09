package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "time"
)

type ConnPool struct {
  clientId uint64
  stopNotify chan *ConnPool

  stop chan struct{}
  newConnChan chan *net.TCPConn
  deadConnNotify chan bool
  deadConnChan chan *Conn

  byteKeys []byte
  uint64Keys []uint64

  sendQueue chan ToSend
  infoChan chan ToSend
  rawSendQueue chan []byte

  sessions map[uint64]*Session
  newSessionChan *chan *Session

  conns map[uint64]*Conn
  maxConnNum int

  closed bool
}

func newConnPool(key string, newSessionChan *chan *Session) *ConnPool {
  self := &ConnPool{
    stop: make(chan struct{}),
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),
    deadConnChan: make(chan *Conn, CHAN_BUF_SIZE),

    sendQueue: make(chan ToSend, CHAN_BUF_SIZE),
    infoChan: make(chan ToSend, CHAN_BUF_SIZE),
    rawSendQueue: make(chan []byte, CHAN_BUF_SIZE),

    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,

    conns: make(map[uint64]*Conn),
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)

  go self.start()

  return self
}

func (self *ConnPool) start() {
  self.log("start")
  infoBuf := new(bytes.Buffer)
  heartBeat := time.Tick(time.Second * 2)
  tick := 0

  LOOP:
  for {
    select {
    case tcpConn := <-self.newConnChan:
      conn := newConn(tcpConn, self)
      self.conns[conn.id] = conn
      if len(self.conns) > self.maxConnNum {
        self.maxConnNum = len(self.conns)
      }

    case info := <-self.infoChan:
      binary.Write(infoBuf, binary.BigEndian, info.session.id)
      infoBuf.Write(info.data)

    case <-heartBeat:
      // send session infos
      info := infoBuf.Bytes()
      frame := new(bytes.Buffer)
      frame.Write([]byte{PACKET_TYPE_INFO})
      binary.Write(frame, binary.BigEndian, uint32(len(info)))
      frame.Write(info)
      self.rawSendQueue <- frame.Bytes()
      infoBuf = new(bytes.Buffer)

      self.log("tick %d conns %d", tick, len(self.conns))

      if self.maxConnNum > 0 && len(self.conns) == 0 { // client disconnected
        self.log("client disconnected")
        break LOOP
      }

    case conn := <-self.deadConnChan:
      self.log("delete conn %d", conn.id)
      delete(self.conns, conn.id)

    case <-self.stop:
      break LOOP
    }
    tick++
  }

  // finalizer
  self.log("stop")
  self.closed = true
  // stop conns
  for _, conn := range self.conns {
    conn.Stop()
  }
  // stop sessions
  for serial, session := range self.sessions {
    session.Stop()
    delete(self.sessions, serial)
  }
  // notify 
  if self.stopNotify != nil {
    self.stopNotify <- self
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

func (self *ConnPool) Stop() {
  close(self.stop)
}

func (self *ConnPool) log(f string, vars ...interface{}) {
  colorp("34", "CONNPOOL " + f, vars...)
}
