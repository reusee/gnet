package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "io"
  "time"
)

type ConnPool struct {
  newConnChan chan *net.TCPConn
  badConnChan chan bool

  byteKeys []byte
  uint64Keys []uint64

  sendDataChan chan []byte

  sessions map[uint64]*Session
  newSessionChan chan *Session
}

func newConnPool(key string, newSessionChan chan *Session) *ConnPool {
  self := &ConnPool{
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),
    sendDataChan: make(chan []byte, CHAN_BUF_SIZE),
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
    case conn := <-self.newConnChan:
      go self.startConnWriter(conn)
      go self.startConnReader(conn)
    }
  }
}

func (self *ConnPool) startConnWriter(conn *net.TCPConn) {
  keepAliveTicker := time.NewTicker(time.Second * 10)
  for {
    select {
    case data := <-self.sendDataChan:
      //TODO dont send when session is close
      l := len(data)
      toSend := make([]byte, 1 + 4 + l)
      toSend[0] = TYPE_DATA
      packetLenBuf := new(bytes.Buffer)
      binary.Write(packetLenBuf, binary.BigEndian, uint32(l))
      copy(toSend[1:5], packetLenBuf.Bytes()[:4])
      xorSlice(data, toSend[5:], l, l % 8, self.byteKeys, self.uint64Keys)
      _, err := conn.Write(toSend)
      if err != nil {
        if self.badConnChan != nil {
          self.badConnChan <- true
        }
        self.sendDataChan <- data
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
      if self.badConnChan != nil {
        self.badConnChan <- true
      }
      break
    }

    switch packetType {

    case TYPE_DATA:
      var packetLen uint32
      err := binary.Read(conn, binary.BigEndian, &packetLen)
      if err != nil {
        if self.badConnChan != nil {
          self.badConnChan <- true
        }
        break
      }

      buf := make([]byte, packetLen)
      _, err = io.ReadFull(conn, buf)
      if err != nil {
        if self.badConnChan != nil {
          self.badConnChan <- true
        }
        break
      }

      decrypted := make([]byte, packetLen)
      xorSlice(buf, decrypted, int(packetLen), int(packetLen) % 8, self.byteKeys, self.uint64Keys)
      var sessionId uint64
      binary.Read(bytes.NewReader(decrypted[:8]), binary.BigEndian, &sessionId)
      var serial uint32
      binary.Read(bytes.NewReader(decrypted[8:12]), binary.BigEndian, &serial)
      data := decrypted[12:]

      session := self.sessions[sessionId]
      if session == nil { // create new session
        session = newSession(sessionId, self.sendDataChan)
        self.sessions[sessionId] = session
        self.newSessionChan <- session
      }
      //TODO don't send to closed session
      session.incomingPacketChan <- Packet{serial: serial, data: data}

    case TYPE_PING: // just a ping
    }
  }
}
