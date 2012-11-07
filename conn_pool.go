package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "io"
  "time"
  "sync/atomic"
  "sync"
)

type ConnPool struct {
  newConnChan chan *net.TCPConn
  deadConnChan chan bool

  byteKeys []byte
  uint64Keys []uint64

  sendChan chan ToSend
  infoChan chan ToSend
  rawSendChan chan []byte

  sessions map[uint64]*Session
  newSessionChan chan *Session

  stopListen chan bool
  stopHeartBeat chan bool

  bytesRead uint64
  bytesWrite uint64
}

func newConnPool(key string, newSessionChan chan *Session) *ConnPool {
  self := &ConnPool{
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),

    sendChan: make(chan ToSend, CHAN_BUF_SIZE),
    infoChan: make(chan ToSend, CHAN_BUF_SIZE),
    rawSendChan: make(chan []byte, CHAN_BUF_SIZE),

    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,

    stopListen: make(chan bool, 32),
    stopHeartBeat: make(chan bool, 32),
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)
  go self.start()
  return self
}

func (self *ConnPool) start() {
  var bytesWrite, bytesRead uint64
  infoBuf := new(bytes.Buffer)
  infoBufLock := new(sync.Mutex)

  heartBeat := time.NewTicker(time.Second * 3)
  go func() { // heartbeat
    for {
      select {
      case <-heartBeat.C:

        curBytesRead, curBytesWrite := self.bytesRead, self.bytesWrite
        self.log("read %d / %d write %d / %d\n",
          curBytesRead - bytesRead, curBytesRead,
          curBytesWrite - bytesWrite, curBytesWrite)
        bytesWrite, bytesRead = curBytesWrite, curBytesRead

        infoBufLock.Lock()
        info := infoBuf.Bytes()
        infoBuf = new(bytes.Buffer)
        infoBufLock.Unlock()
        frame := new(bytes.Buffer)
        frame.Write([]byte{PACKET_TYPE_INFO})
        binary.Write(frame, binary.BigEndian, uint32(len(info)))
        frame.Write(info)
        self.rawSendChan <- frame.Bytes()

      case <-self.stopHeartBeat:
        return
      }
    }
  }()

  for {
    select {
    case conn := <-self.newConnChan:
      //TODO way to end these thread
      self.log("new conn watchers started\n")
      go self.startConnWriter(conn)
      go self.startConnReader(conn)

    case info := <-self.infoChan:
      infoBufLock.Lock()
      binary.Write(infoBuf, binary.BigEndian, info.session.id)
      infoBuf.Write(info.frame)
      infoBufLock.Unlock()

    case <-self.stopListen:
      return
    }
  }
}

func (self *ConnPool) Close() {
  self.stopListen <- true
  self.stopHeartBeat <- true
}

func (self *ConnPool) dealWithDeadConn(conn *net.TCPConn) {
  if self.deadConnChan != nil {
    self.deadConnChan <- true
  }
  conn.Close()
}

func (self *ConnPool) startConnWriter(conn *net.TCPConn) {
  keepAliveTicker := time.NewTicker(time.Second * 10)
  var err error
  for {
    select {
    case toSend := <-self.sendChan:
      if toSend.session.remoteReadState == ABORT {
        continue
      }
      frame := self.packSessionPacket(toSend)
      _, err = conn.Write(frame)
      if err != nil {
        self.dealWithDeadConn(conn)
        self.sendChan <- toSend
        return
      }
      atomic.AddUint64(&self.bytesWrite, uint64(len(frame)))

    case frame := <-self.rawSendChan:
      _, err := conn.Write(frame)
      if err != nil {
        self.dealWithDeadConn(conn)
        return
      }

    case <-keepAliveTicker.C:
      _, err := conn.Write([]byte{PACKET_TYPE_PING})
      if err != nil {
        self.dealWithDeadConn(conn)
        return
      }
      atomic.AddUint64(&self.bytesWrite, uint64(1))
    }

  }
}

func (self *ConnPool) startConnReader(conn *net.TCPConn) {
  for {
    var packetType byte
    err := binary.Read(conn, binary.BigEndian, &packetType)
    if err != nil {
      self.dealWithDeadConn(conn)
      return
    }

    switch packetType {

    case PACKET_TYPE_SESSION:
      payload, sessionId, err := self.unpackSessionPacket(conn)
      if err != nil {
        self.dealWithDeadConn(conn)
        return
      }
      session := self.sessions[sessionId]
      if session == nil { // create new session
        session = newSession(sessionId, self)
        self.sessions[sessionId] = session
        self.newSessionChan <- session
      }
      session.incomingChan <- payload

    case PACKET_TYPE_INFO:
      var frameLen uint32
      binary.Read(conn, binary.BigEndian, &frameLen)
      if err != nil {
        self.dealWithDeadConn(conn)
        return
      }
      frame := make([]byte, frameLen)
      _, err := io.ReadFull(conn, frame)
      if err != nil {
        self.dealWithDeadConn(conn)
        return
      }
      var sessionId uint64
      frames := int(frameLen / 21)
      for i := 0; i < frames; i++ {
        binary.Read(bytes.NewReader(frame[i * 21: i * 21 + 8]), binary.BigEndian, &sessionId)
        session := self.sessions[sessionId]
        if session == nil {
          continue
        }
        sessionFrame := frame[i * 21 + 8: (i + 1) * 21]
        session.incomingChan <- sessionFrame
      }

    case PACKET_TYPE_PING:
    }
  }
}

func (self *ConnPool) packSessionPacket(toSend ToSend) []byte {
  sessionId, frame := toSend.session.id, toSend.frame
  frameLen := len(frame)
  pack := make([]byte, 1 + 8 + 4 + frameLen)
  pack[0] = PACKET_TYPE_SESSION // packet type
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, sessionId) // session id
  binary.Write(buf, binary.BigEndian, uint32(frameLen)) // frame length
  copy(pack[1:13], buf.Bytes()[:12])
  xorSlice(frame, pack[13:], frameLen, frameLen % 8, self.byteKeys, self.uint64Keys)
  return pack
}

func (self *ConnPool) unpackSessionPacket(conn *net.TCPConn) ([]byte, uint64, error) {
  var sessionId uint64
  err := binary.Read(conn, binary.BigEndian, &sessionId)
  if err != nil {
    self.dealWithDeadConn(conn)
    return nil, 0, err
  }

  var frameLen uint32
  err = binary.Read(conn, binary.BigEndian, &frameLen)
  if err != nil {
    self.dealWithDeadConn(conn)
    return nil, 0, err
  }
  atomic.AddUint64(&self.bytesRead, uint64(frameLen))

  encrypted := make([]byte, frameLen)
  _, err = io.ReadFull(conn, encrypted)
  if err != nil {
    self.dealWithDeadConn(conn)
    return nil, 0, err
  }

  frame := make([]byte, frameLen)
  xorSlice(encrypted, frame, int(frameLen), int(frameLen) % 8, self.byteKeys, self.uint64Keys)

  return frame, sessionId, nil
}

func (self *ConnPool) log(f string, vars ...interface{}) {
  p("CONNPOOL " + f, vars...)
}
