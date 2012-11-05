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

  sendDataChan chan DataToSend
  sendStateChan chan StateToSend

  sessions map[uint64]*Session
  newSessionChan chan *Session

  endChan chan bool
}

func newConnPool(key string, newSessionChan chan *Session) *ConnPool {
  self := &ConnPool{
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),
    sendDataChan: make(chan DataToSend, CHAN_BUF_SIZE),
    sendStateChan: make(chan StateToSend, CHAN_BUF_SIZE),
    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,
    endChan: make(chan bool),
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)
  go self.start()
  return self
}

func (self *ConnPool) start() {
  for {
    select {
    case conn := <-self.newConnChan:
      //TODO way to end these thread
      go self.startConnWriter(conn)
      go self.startConnReader(conn)

    case <-self.endChan:
      break
    }
  }
}

func (self *ConnPool) Close() {
  self.endChan <- true
}

func (self *ConnPool) startConnWriter(conn *net.TCPConn) {
  keepAliveTicker := time.NewTicker(time.Second * 10)
  var err error
  for {
    select {
    case data := <-self.sendDataChan:
      if data.session.remoteReadState == ABORT {
        continue
      }
      toSend := self.packData(data.data, TYPE_DATA)
      _, err = conn.Write(toSend)
      if err != nil {
        if self.badConnChan != nil {
          self.badConnChan <- true
        }
        self.sendDataChan <- data
        break
      }

    case state := <-self.sendStateChan:
      toSend := self.packData(state.data, TYPE_STATE)
      _, err = conn.Write(toSend)
      if err != nil {
        if self.badConnChan != nil {
          self.badConnChan <- true
        }
        self.sendStateChan <- state
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
      sessionId, serial, data, err := self.unpackData(conn)
      if err != nil {
        break
      }
      session := self.sessions[sessionId]
      if session == nil { // create new session
        session = newSession(sessionId, self.sendDataChan, self.sendStateChan)
        self.sessions[sessionId] = session
        self.newSessionChan <- session
      }
      if session.remoteSendState == NORMAL {
        session.incomingPacketChan <- Packet{serial: serial, data: data}
      }

    case TYPE_STATE: // state
      sessionId, serial, data, err := self.unpackData(conn)
      if err != nil {
        break
      }
      session := self.sessions[sessionId]
      if session == nil || session.closed { // ignore this packet
        continue
      }
      _ = serial
      state := data[0]
      switch state {
      case STATE_FINISH_SEND:
        //TODO use for session cleaning
      case STATE_FINISH_READ:
        //TODO use for session cleaning
      case STATE_ABORT_SEND: // drop all received packet
        session.remoteSendState = ABORT
      case STATE_ABORT_READ: // drop all outgoing packet
        session.remoteReadState = ABORT
      }

    case TYPE_PING: // just a ping
    }
  }
}

func (self *ConnPool) packData(data []byte, dataType byte) []byte {
  l := len(data)
  pack := make([]byte, 1 + 4 + l)
  pack[0] = dataType
  dataLenBuf := new(bytes.Buffer)
  binary.Write(dataLenBuf, binary.BigEndian, uint32(l))
  copy(pack[1:5], dataLenBuf.Bytes()[:4])
  xorSlice(data, pack[5:], l, l % 8, self.byteKeys, self.uint64Keys)
  return pack
}

func (self *ConnPool) unpackData(conn *net.TCPConn) (uint64, uint32, []byte, error) {
  var packetLen uint32
  err := binary.Read(conn, binary.BigEndian, &packetLen)
  if err != nil {
    if self.badConnChan != nil {
      self.badConnChan <- true
    }
    return 0, 0, nil, err
  }

  buf := make([]byte, packetLen)
  _, err = io.ReadFull(conn, buf)
  if err != nil {
    if self.badConnChan != nil {
      self.badConnChan <- true
    }
    return 0, 0, nil, err
  }

  decrypted := make([]byte, packetLen)
  xorSlice(buf, decrypted, int(packetLen), int(packetLen) % 8, self.byteKeys, self.uint64Keys)
  var sessionId uint64
  binary.Read(bytes.NewReader(decrypted[:8]), binary.BigEndian, &sessionId)
  var serial uint32
  binary.Read(bytes.NewReader(decrypted[8:12]), binary.BigEndian, &serial)
  data := decrypted[12:]

  return sessionId, serial, data, nil
}
