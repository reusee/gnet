package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "io"
  "time"
)

type ConnPool struct {
  newConn chan *net.TCPConn
  badConn chan bool

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
  keepAliveTicker := time.NewTicker(time.Second * 10)
  for {
    select {
    case packet := <-self.sendChan:
      l := len(packet)
      toSend := make([]byte, 1 + 4 + l)
      toSend[0] = TYPE_DATA
      packetLenBuf := new(bytes.Buffer)
      binary.Write(packetLenBuf, binary.BigEndian, uint32(l))
      copy(toSend[1:5], packetLenBuf.Bytes()[:4])
      xorSlice(packet, toSend[5:], l, l % 8, self.byteKeys, self.uint64Keys)
      _, err := conn.Write(toSend)
      if err != nil {
        if self.badConn != nil {
          self.badConn <- true
        }
        break
      }
    case <-keepAliveTicker.C:
      conn.Write([]byte{TYPE_PING})
    }
  }
}

func (self *ConnPool) startConnReader(conn *net.TCPConn) {
  for {
    var packetType byte
    err := binary.Read(conn, binary.BigEndian, &packetType)
    if err != nil {
      if self.badConn != nil {
        self.badConn <- true
      }
      break
    }

    switch packetType {

    case TYPE_DATA:
      var packetLen uint32
      err := binary.Read(conn, binary.BigEndian, &packetLen)
      if err != nil {
        if self.badConn != nil {
          self.badConn <- true
        }
        break
      }

      buf := make([]byte, packetLen)
      _, err = io.ReadFull(conn, buf)
      if err != nil {
        if self.badConn != nil {
          self.badConn <- true
        }
        break
      }

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

    case TYPE_PING: // just a ping
    }
  }
}
