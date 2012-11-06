package gnet

import (
  "bytes"
  "encoding/binary"
  "sync/atomic"
  "container/heap"
  "time"
)

type Session struct {
  id uint64
  serial uint32
  closed bool

  stopReceive chan bool
  stopHeartBeat chan bool

  incomingSerial uint32
  maxIncomingSerial uint32
  incomingChan chan []byte
  sendChan chan ToSend

  readState int // for session cleaner
  sendState int // for session cleaner
  remoteReadState int // for Send() and conn writer
  remoteSendState int // 

  incomingDataCount uint32

  packetQueue PacketQueue

  Data chan []byte
}

type ToSend struct {
  session *Session
  data []byte
}

func newSession(id uint64, sendChan chan ToSend) *Session {
  self := &Session{
    id: id,

    stopReceive: make(chan bool, 32), // may be multiple push
    stopHeartBeat: make(chan bool, 32),

    incomingSerial: 1,
    incomingChan: make(chan []byte, CHAN_BUF_SIZE),
    sendChan: sendChan,

    readState: NORMAL,
    sendState: NORMAL,
    remoteReadState: NORMAL,
    remoteSendState: NORMAL,

    packetQueue: newPacketQueue(),

    Data: make(chan []byte, CHAN_BUF_SIZE),
  }
  go self.start()
  return self
}

type Packet struct {
  serial uint32
  data []byte
  index int
}

func (self *Session) start() {
  go func() { // heart beat
    heartBeat := time.NewTicker(time.Second * 3)
    for {
      select {
      case <-heartBeat.C:
        cur, max, count := self.incomingSerial, self.maxIncomingSerial, self.incomingDataCount
        if cur < max {
          self.log("packet gap %d %d %d\n", cur, max, count)
        }

      case <-self.stopHeartBeat:
        return
      }
    }
  }()

  for { // receive incoming packet
    select {
    case payload := <-self.incomingChan:
      packetType := payload[0]
      switch packetType {
      case SESSION_PACKET_TYPE_DATA:
        self.handleDataPacket(payload[1:])
      case SESSION_PACKET_TYPE_STATE:
        self.handleStatePacket(payload[1:])
      case SESSION_PACKET_TYPE_INFO:
      }

    case <-self.stopReceive:
      return

    case <-time.NewTimer(IDLE_TIME_BEFORE_SESSION_CLOSE).C:
      self.Close()
    }
  }
}

func (self *Session) handleDataPacket(data []byte) {
  atomic.AddUint32(&self.incomingDataCount, uint32(1))

  var serial uint32
  binary.Read(bytes.NewReader(data[:4]), binary.BigEndian, &serial)
  data = data[4:]

  if serial == self.incomingSerial {
    self.Data <- data
    self.incomingSerial++
  } else if serial > self.incomingSerial {
    packet := Packet{serial: serial, data: data}
    heap.Push(&self.packetQueue, &packet)
  }
  if serial > self.maxIncomingSerial {
    self.maxIncomingSerial = serial
  }
  for len(self.packetQueue) > 0 {
    next := heap.Pop(&self.packetQueue).(*Packet)
    if next.serial == self.incomingSerial {
      self.Data <- next.data
      self.incomingSerial++
    } else {
      heap.Push(&self.packetQueue, next)
      break
    }
  }
}

func (self *Session) handleStatePacket(payload []byte) {
  state := payload[0]
  switch state {
  case STATE_FINISH_SEND:
  case STATE_FINISH_READ:
  case STATE_ABORT_SEND: // drop all received packet
    self.remoteSendState = ABORT
  case STATE_ABORT_READ: // drop all outgoing packet
    self.remoteReadState = ABORT
  }
}

func (self *Session) packData(data []byte) []byte {
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, self.id) // session id

  buf.Write([]byte{SESSION_PACKET_TYPE_DATA}) // packet type

  serial := atomic.AddUint32(&self.serial, uint32(1))
  binary.Write(buf, binary.BigEndian, serial) // packet serial
  buf.Write(data) // data
  return buf.Bytes()
}

func (self *Session) packState(state byte) []byte {
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, self.id) // session id

  buf.Write([]byte{SESSION_PACKET_TYPE_STATE}) // packet type

  buf.Write([]byte{state}) // state
  return buf.Bytes()
}

func (self *Session) Send(data []byte) int {
  if self.remoteReadState == ABORT {
    return ABORT
  }
  self.sendChan <- ToSend{self, self.packData(data)}
  return NORMAL
}

func (self *Session) FinishSend() { // no more data will be send
  self.sendState = FINISH
  self.sendChan <- ToSend{self, self.packState(STATE_FINISH_SEND)}
}

func (self *Session) AbortSend() { // abort all pending data immediately
  self.sendState = ABORT
  self.sendChan <- ToSend{self, self.packState(STATE_ABORT_SEND)}
}

func (self *Session) FinishRead() { // no more data will be read
  self.readState = FINISH
  self.sendChan <- ToSend{self, self.packState(STATE_FINISH_READ)}
}

func (self *Session) AbortRead() { // stop reading immediately
  self.readState = ABORT
  self.sendChan <- ToSend{self, self.packState(STATE_ABORT_READ)}
}

func (self *Session) Close() {
  self.FinishRead()
  self.FinishSend()
  self.stopReceive <- true
  self.stopHeartBeat <- true
  self.closed = true
}

func (self *Session) Abort() {
  self.AbortSend()
  self.AbortRead()
  self.stopReceive <- true
  self.stopHeartBeat <- true
  self.closed = true
}

func (self *Session) log(f string, vars ...interface{}) {
  p(ps("SESSION %d %s", self.id, f), vars...)
}
