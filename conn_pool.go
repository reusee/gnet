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

  sendDataChan chan DataToSend
  sendStateChan chan StateToSend

  sessions map[uint64]*Session
  newSessionChan chan *Session

  endChan chan bool

  bytesRead uint64
  bytesWrite uint64
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
    case data := <-self.sendDataChan:
      if data.session.remoteReadState == ABORT {
        continue
      }
      toSend := self.packData(data.data, TYPE_DATA)
      _, err = conn.Write(toSend)
      if err != nil {
        self.dealWithDeadConn(conn)
        self.sendDataChan <- data
        return
      }
      atomic.AddUint64(&self.bytesWrite, uint64(len(toSend)))

    case state := <-self.sendStateChan:
      toSend := self.packData(state.data, TYPE_STATE)
      _, err = conn.Write(toSend)
      if err != nil {
        self.dealWithDeadConn(conn)
        self.sendStateChan <- state
        return
      }
      atomic.AddUint64(&self.bytesWrite, uint64(len(toSend)))

    case <-keepAliveTicker.C:
      _, err := conn.Write([]byte{TYPE_PING})
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

    case TYPE_DATA:
      sessionId, serial, data, err := self.unpackData(conn)
      if err != nil {
        return
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
        return
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
    self.dealWithDeadConn(conn)
    return 0, 0, nil, err
  }
  atomic.AddUint64(&self.bytesRead, uint64(packetLen))

  buf := make([]byte, packetLen)
  _, err = io.ReadFull(conn, buf)
  if err != nil {
    self.dealWithDeadConn(conn)
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

func (self *ConnPool) log(f string, vars ...interface{}) {
  p("CONNPOOL " + f, vars...)
}
