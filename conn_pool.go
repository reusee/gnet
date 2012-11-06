package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "io"
  "time"
  "sync/atomic"
)

type ConnPool struct {
  newConnChan chan *net.TCPConn
  deadConnChan chan bool

  byteKeys []byte
  uint64Keys []uint64

  sendChan chan ToSend

  sessions map[uint64]*Session
  newSessionChan chan *Session

  endChan chan bool

  bytesRead uint64
  bytesWrite uint64
}

func newConnPool(key string, newSessionChan chan *Session) *ConnPool {
  self := &ConnPool{
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),
    sendChan: make(chan ToSend, CHAN_BUF_SIZE),
    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,
    endChan: make(chan bool),
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)
  go self.start()
  return self
}

func (self *ConnPool) start() {
  ticker := time.NewTicker(time.Second * 3)
  var bytesWrite, bytesRead uint64
  for {
    select {
    case conn := <-self.newConnChan:
      //TODO way to end these thread
      self.log("new conn watchers started\n")
      go self.startConnWriter(conn)
      go self.startConnReader(conn)

    case <-self.endChan:
      break

    case <-ticker.C:
      curBytesRead, curBytesWrite := self.bytesRead, self.bytesWrite
      self.log("read %d / %d write %d / %d\n",
        curBytesRead - bytesRead, curBytesRead,
        curBytesWrite - bytesWrite, curBytesWrite)
      bytesWrite, bytesRead = curBytesWrite, curBytesRead
    }
  }
}

func (self *ConnPool) Close() {
  self.endChan <- true
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
    case data := <-self.sendChan:
      if data.session.remoteReadState == ABORT {
        continue
      }
      toSend := self.packSessionPacket(data.data)
      _, err = conn.Write(toSend)
      if err != nil {
        self.dealWithDeadConn(conn)
        self.sendChan <- data
        return
      }
      atomic.AddUint64(&self.bytesWrite, uint64(len(toSend)))

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
        session = newSession(sessionId, self.sendChan)
        self.sessions[sessionId] = session
        self.newSessionChan <- session
      }
      session.incomingChan <- payload

    case PACKET_TYPE_PING:
    }
  }
}

func (self *ConnPool) packSessionPacket(data []byte) []byte {
  dataLen := len(data)
  pack := make([]byte, 1 + 4 + dataLen)
  pack[0] = PACKET_TYPE_SESSION // packet type
  dataLenBuf := new(bytes.Buffer)
  binary.Write(dataLenBuf, binary.BigEndian, uint32(dataLen))
  copy(pack[1:5], dataLenBuf.Bytes()[:4]) // session payload len
  xorSlice(data, pack[5:], dataLen, dataLen % 8, self.byteKeys, self.uint64Keys)
  return pack
}

func (self *ConnPool) unpackSessionPacket(conn *net.TCPConn) ([]byte, uint64, error) {
  var payloadLen uint32
  var sessionId uint64
  err := binary.Read(conn, binary.BigEndian, &payloadLen)
  if err != nil {
    self.dealWithDeadConn(conn)
    return nil, 0, err
  }
  atomic.AddUint64(&self.bytesRead, uint64(payloadLen))

  encrypted := make([]byte, payloadLen)
  _, err = io.ReadFull(conn, encrypted)
  if err != nil {
    self.dealWithDeadConn(conn)
    return nil, 0, err
  }

  payload := make([]byte, payloadLen)
  xorSlice(encrypted, payload, int(payloadLen), int(payloadLen) % 8, self.byteKeys, self.uint64Keys)

  binary.Read(bytes.NewReader(payload[:8]), binary.BigEndian, &sessionId)
  payload = payload[8:]
  return payload, sessionId, nil
}

func (self *ConnPool) log(f string, vars ...interface{}) {
  p("CONNPOOL " + f, vars...)
}
