package gnet

import (
  "net"
  "encoding/binary"
  "io"
  "bytes"
  "sync/atomic"
  "time"
)

type Conn struct {
  conn *net.TCPConn
  pool *ConnPool

  heartBeat *Ticker

  readerStopped chan bool
  writerStopped chan bool

  err bool
}

func newConn(conn *net.TCPConn, connPool *ConnPool) *Conn {
  self := &Conn{
    conn: conn,
    pool: connPool,

    heartBeat: NewTicker(time.Second * 10),

    readerStopped: make(chan bool, 1),
    writerStopped: make(chan bool, 1),
  }

  go self.startReader()
  go self.startWriter()

  return self
}

func (self *Conn) startReader() {
  defer func() {
    self.readerStopped <- true
  }()
  for {
    var packetType byte
    p(">\n")
    err := binary.Read(self.conn, binary.BigEndian, &packetType)
    p(">>\n")
    if err != nil {
      self.err = true
      return
    }
    p(">>>\n")

    switch packetType {

    case PACKET_TYPE_SESSION:
      payload, sessionId, err := self.readSessionFrame()
      if err != nil {
        self.err = true
        return
      }
      session := self.pool.sessions[sessionId]
      if session == nil { // create new session
        session = self.pool.newSession(sessionId)
      }
      if session.closed {
        continue
      }
      session.incomingChan <- payload

    case PACKET_TYPE_INFO:
      var frameLen uint32
      err = binary.Read(self.conn, binary.BigEndian, &frameLen)
      if err != nil {
        self.err = true
        return
      }
      frame := make([]byte, frameLen)
      _, err := io.ReadFull(self.conn, frame)
      if err != nil {
        self.err = true
        return
      }
      var sessionId uint64
      frames := int(frameLen / 21)
      for i := 0; i < frames; i++ {
        binary.Read(bytes.NewReader(frame[i * 21: i * 21 + 8]), binary.BigEndian, &sessionId)
        session := self.pool.sessions[sessionId]
        if session == nil {
          continue
        }
        if session.closed {
          continue
        }
        sessionFrame := frame[i * 21 + 8: (i + 1) * 21]
        session.incomingChan <- sessionFrame
      }

    case PACKET_TYPE_PING:
    }
  }
}

func (self *Conn) readSessionFrame() ([]byte, uint64, error) {
  var sessionId uint64
  err := binary.Read(self.conn, binary.BigEndian, &sessionId)
  if err != nil {
    self.err = true
    return nil, 0, err
  }

  var frameLen uint32
  err = binary.Read(self.conn, binary.BigEndian, &frameLen)
  if err != nil {
    self.err = true
    return nil, 0, err
  }
  atomic.AddUint64(&self.pool.bytesRead, uint64(frameLen))

  encrypted := make([]byte, frameLen)
  _, err = io.ReadFull(self.conn, encrypted)
  if err != nil {
    self.err = true
    return nil, 0, err
  }

  frame := make([]byte, frameLen)
  xorSlice(encrypted, frame, int(frameLen), int(frameLen) % 8, self.pool.byteKeys, self.pool.uint64Keys)

  return frame, sessionId, nil
}

func (self *Conn) startWriter() {
  defer func() {
    self.writerStopped <- true
  }()
  var err error
  for {
    select {
    case toSend := <-self.pool.sendQueue:
      if toSend.session.remoteReadState == ABORT {
        continue
      }
      frame := self.packSessionFrame(toSend)
      _, err = self.conn.Write(frame)
      if err != nil {
        self.err = true
        self.pool.sendQueue <- toSend
        return
      }
      atomic.AddUint64(&self.pool.bytesWrite, uint64(len(frame)))

    case frame := <-self.pool.rawSendChan:
      _, err := self.conn.Write(frame)
      if err != nil {
        self.err = true
        return
      }

    case _, ok := <-self.heartBeat.C:
      if !ok {
        return
      }
      _, err := self.conn.Write([]byte{PACKET_TYPE_PING})
      if err != nil {
        self.err = true
        return
      }
      atomic.AddUint64(&self.pool.bytesWrite, uint64(1))
    }
  }
}

func (self *Conn) packSessionFrame(toSend ToSend) []byte {
  sessionId, frame := toSend.session.id, toSend.frame
  frameLen := len(frame)
  pack := make([]byte, 1 + 8 + 4 + frameLen)
  pack[0] = PACKET_TYPE_SESSION // packet type
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, sessionId) // session id
  binary.Write(buf, binary.BigEndian, uint32(frameLen)) // frame length
  copy(pack[1:13], buf.Bytes()[:12])
  xorSlice(frame, pack[13:], frameLen, frameLen % 8, self.pool.byteKeys, self.pool.uint64Keys)
  return pack
}

func (self *Conn) Close() {
  self.conn.Close()
  self.heartBeat.Stop()
  self.log("told reader to stop\n")
  <-self.readerStopped
  self.log("reader stopped\n")
  <-self.writerStopped
  self.log("writer stopped\n")
}

func (self *Conn) log(f string, vars ...interface{}) {
  p("CONN " + f, vars...)
}
