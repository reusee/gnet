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
  incomingPacketChan chan Packet
  incomingStateChan chan byte
  sendDataChan chan DataToSend
  sendStateChan chan StateToSend

  readState int // for session cleaner
  sendState int // for session cleaner
  remoteReadState int // for Send() and conn writer
  remoteSendState int // for incomingPacketChan and conn reader

  incomingPacketCount uint32

  Data chan []byte
}

type DataToSend struct {
  session *Session
  data []byte
}

type StateToSend struct {
  session *Session
  data []byte
}

func newSession(id uint64, sendDataChan chan DataToSend, sendStateChan chan StateToSend) *Session {
  self := &Session{
    id: id,

    stopReceive: make(chan bool, 32), // may be multiple push
    stopHeartBeat: make(chan bool, 32),

    incomingSerial: 1,
    incomingPacketChan: make(chan Packet, CHAN_BUF_SIZE),
    incomingStateChan: make(chan byte, CHAN_BUF_SIZE),
    sendDataChan: sendDataChan,
    sendStateChan: sendStateChan,

    readState: NORMAL,
    sendState: NORMAL,
    remoteReadState: NORMAL,
    remoteSendState: NORMAL,

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
        cur, max, count := self.incomingSerial, self.maxIncomingSerial, self.incomingPacketCount
        if cur < max {
          self.log("packet gap %d %d %d\n", cur, max, count)
        }

      case <-self.stopHeartBeat:
        return
      }
    }
  }()

  packetQueue := newPacketQueue()
  for { // receive incoming packet
    select {
    case packet := <-self.incomingPacketChan:
      self.incomingPacketCount++
      if packet.serial == self.incomingSerial {
        self.Data <- packet.data
        self.incomingSerial++
      } else if packet.serial > self.incomingSerial {
        heap.Push(&packetQueue, &packet)
      }
      if packet.serial > self.maxIncomingSerial {
        self.maxIncomingSerial = packet.serial
      }
      for len(packetQueue) > 0 {
        next := heap.Pop(&packetQueue).(*Packet)
        if next.serial == self.incomingSerial {
          self.Data <- next.data
          self.incomingSerial++
        } else {
          heap.Push(&packetQueue, next)
          break
        }
      }

    case state := <-self.incomingStateChan:
      switch state {
      case STATE_FINISH_SEND:
        //TODO use for session cleaning
      case STATE_FINISH_READ:
        //TODO use for session cleaning
      case STATE_ABORT_SEND: // drop all received packet
        self.remoteSendState = ABORT
      case STATE_ABORT_READ: // drop all outgoing packet
        self.remoteReadState = ABORT
      }

    case <-self.stopReceive:
      break

    case <-time.NewTimer(IDLE_TIME_BEFORE_SESSION_CLOSE).C:
      self.Close()
    }
  }
}

func (self *Session) packData(data []byte) []byte {
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, self.id) // session id
  serial := atomic.AddUint32(&self.serial, uint32(1))
  binary.Write(buf, binary.BigEndian, serial) // packet serial
  buf.Write(data) // data
  return buf.Bytes()
}

func sessionUnpackData(data []byte) (uint64, uint32, []byte) {
  var sessionId uint64
  binary.Read(bytes.NewReader(data[:8]), binary.BigEndian, &sessionId)
  var serial uint32
  binary.Read(bytes.NewReader(data[8:12]), binary.BigEndian, &serial)
  data = data[12:]
  return sessionId, serial, data
}

func (self *Session) packState(state byte) []byte {
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, self.id)
  buf.Write([]byte{state})
  return buf.Bytes()
}

func sessionUnpackState(data []byte) (uint64, byte) {
  var sessionId uint64
  binary.Read(bytes.NewReader(data[:8]), binary.BigEndian, &sessionId)
  state := data[8]
  return sessionId, state
}

func (self *Session) Send(data []byte) int {
  if self.remoteReadState == ABORT {
    return ABORT
  }
  self.sendDataChan <- DataToSend{self, self.packData(data)}
  return NORMAL
}

func (self *Session) FinishSend() { // no more data will be send
  self.sendState = FINISH
  self.sendStateChan <- StateToSend{self, self.packState(STATE_FINISH_SEND)}
}

func (self *Session) AbortSend() { // abort all pending data immediately
  self.sendState = ABORT
  self.sendStateChan <- StateToSend{self, self.packState(STATE_ABORT_SEND)}
}

func (self *Session) FinishRead() { // no more data will be read
  self.readState = FINISH
  self.sendStateChan <- StateToSend{self, self.packState(STATE_FINISH_READ)}
}

func (self *Session) AbortRead() { // stop reading immediately
  self.readState = ABORT
  self.sendStateChan <- StateToSend{self, self.packState(STATE_ABORT_READ)}
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
