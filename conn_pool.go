package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "io"
)

type ConnPool struct {
  newConn chan *net.TCPConn

  byteKeys []byte
  uint64Keys []uint64

  sendChan chan []byte

  sessions map[uint64]*Session
  newSessionChan chan *Session
}

func newConnPool(key string, newSessionChan chan *Session) *ConnPool {
  self := &ConnPool{
    newConn: make(chan *net.TCPConn, CHAN_BUF_SIZE),
    sendChan: make(chan []byte, CHAN_BUF_SIZE),
    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)
  go self.start()
  return self
}

func (self *ConnPool) start() {
  for {
    select {
    case conn := <-self.newConn:
      go self.startConnWriter(conn)
      go self.startConnReader(conn)
    }
  }
}

//TODO handle read and write error

func (self *ConnPool) startConnWriter(conn *net.TCPConn) {
  for {
    packet := <-self.sendChan
    l := len(packet)
    toSend := make([]byte, 4 + l)
    packetLenBuf := new(bytes.Buffer)
    binary.Write(packetLenBuf, binary.BigEndian, uint32(l))
    copy(toSend[:4], packetLenBuf.Bytes()[:4])
    xorSlice(packet, toSend[4:], l, l % 8, self.byteKeys, self.uint64Keys)
    conn.Write(toSend)
  }
}

func (self *ConnPool) startConnReader(conn *net.TCPConn) {
  for {
    var packetLen uint32
    binary.Read(conn, binary.BigEndian, &packetLen)
    buf := make([]byte, packetLen)
    io.ReadFull(conn, buf)
    decrypted := make([]byte, packetLen)
    xorSlice(buf, decrypted, int(packetLen), int(packetLen) % 8, self.byteKeys, self.uint64Keys)
    var sessionId uint64
    binary.Read(bytes.NewReader(decrypted[:8]), binary.BigEndian, &sessionId)
    var serial uint32
    binary.Read(bytes.NewReader(decrypted[8:12]), binary.BigEndian, &serial)
    packet := decrypted[12:]

    session := self.sessions[sessionId]
    if session == nil { // create new session
      session = newSession(sessionId, self.sendChan)
      self.sessions[sessionId] = session
      self.newSessionChan <- session
    }
    session.packetChan <- Packet{serial: serial, packet: packet}
  }
}
