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
  heartBeat *Ticker

  incomingSerial uint32
  maxIncomingSerial uint32
  incomingChan chan []byte
  sendQueue chan ToSend
  infoChan chan ToSend
  packets map[uint32][]byte

  readState int // for session cleaner
  sendState int // for session cleaner
  remoteReadState int // for Send() and conn writer
  remoteReadFinishAt uint32
  remoteSendState int // 
  remoteSendFinishAt uint32

  incomingDataCount uint32

  packetQueue PacketQueue

  dataBuffer chan Packet
  fetchedSerial uint32
  Data chan []byte
  State chan byte

  lastRemoteHeartbeatTime uint32
  lastRemoteCurSerial uint32
  lastRemoteMaxSerial uint32

  stopProvideData chan bool
  receiveStopped chan bool
  providerStopped chan bool
  heartBeatStopped chan bool
}

type ToSend struct {
  session *Session
  frame []byte
}

func newSession(id uint64, connPool *ConnPool) *Session {
  self := &Session{
    id: id,
    heartBeat: NewTicker(time.Second * 2),

    incomingSerial: 1,
    incomingChan: make(chan []byte, CHAN_BUF_SIZE),
    sendQueue: connPool.sendQueue,
    infoChan: connPool.infoChan,
    packets: make(map[uint32][]byte),

    readState: NORMAL,
    sendState: NORMAL,
    remoteReadState: NORMAL,
    remoteSendState: NORMAL,

    packetQueue: newPacketQueue(),

    dataBuffer: make(chan Packet, CHAN_BUF_SIZE),
    Data: make(chan []byte),
    State: make(chan byte, CHAN_BUF_SIZE),

    stopProvideData: make(chan bool, 1),
    receiveStopped: make(chan bool, 1),
    providerStopped: make(chan bool, 1),
    heartBeatStopped: make(chan bool, 1),
  }

  go self.startHeartBeat()
  go self.startDataProvider()
  go self.startReceive()

  return self
}

type Packet struct {
  serial uint32
  data []byte
  index int
}

func (self *Session) startHeartBeat() {
  defer func() {
    self.heartBeatStopped <- true
    self.log("heartbeat stopped\n")
  }()
  for {
    _, ok := <-self.heartBeat.C
    if !ok {
      break
    }
    cur, max, count := self.incomingSerial, self.maxIncomingSerial, self.incomingDataCount
    if cur < max {
      self.log("packet gap %d %d %d\n", cur, max, count)
    }

    self.sendInfo(cur, max)
  }
}

func (self *Session) startDataProvider() {
  defer func() {
    self.providerStopped <- true
    self.log("provider stopped\n")
  }()
  for {
    packet, ok := <-self.dataBuffer
    if !ok {
      return
    }
    serial, data := packet.serial, packet.data
    select {
    case self.Data <- data:
    case <-self.stopProvideData:
      return
    }
    self.fetchedSerial = serial
    if self.remoteReadState == FINISH && serial >= self.remoteReadFinishAt {
      self.State <- STATE_FINISH_READ
    }
    if self.remoteSendState == FINISH && serial >= self.remoteSendFinishAt {
      self.State <- STATE_FINISH_SEND
    }
  }
}

func (self *Session) startReceive() {
  defer func() {
    self.receiveStopped <- true
    self.log("receive stopped\n")
  }()
  for {
    frame, ok := <-self.incomingChan
    if !ok {
      return
    }
    packetType := frame[0]
    switch packetType {
    case SESSION_PACKET_TYPE_DATA:
      self.handleDataPacket(frame[1:])
    case SESSION_PACKET_TYPE_STATE:
      self.handleStatePacket(frame[1:])
    case SESSION_PACKET_TYPE_INFO:
      self.handleInfoPacket(frame[1:])
    }
  }
}

func (self *Session) handleDataPacket(data []byte) {
  atomic.AddUint32(&self.incomingDataCount, uint32(1))

  var serial uint32
  binary.Read(bytes.NewReader(data[:4]), binary.BigEndian, &serial)
  data = data[4:]

  packet := Packet{serial: serial, data: data}
  if serial == self.incomingSerial {
    self.dataBuffer <- packet
    self.incomingSerial++
  } else if serial > self.incomingSerial {
    heap.Push(&self.packetQueue, &packet)
  }
  if serial > self.maxIncomingSerial {
    self.maxIncomingSerial = serial
  }
  for len(self.packetQueue) > 0 {
    next := heap.Pop(&self.packetQueue).(*Packet)
    if next.serial == self.incomingSerial {
      self.dataBuffer <- *next
      self.incomingSerial++
    } else {
      heap.Push(&self.packetQueue, next)
      break
    }
  }
}

func (self *Session) handleStatePacket(frame []byte) {
  state := frame[0]
  switch state {
  case STATE_FINISH_SEND:
    self.remoteSendState = FINISH
    var serial uint32
    binary.Read(bytes.NewReader(frame[1:]), binary.BigEndian, &serial)
    self.remoteSendFinishAt = serial
    if self.remoteSendState == FINISH && self.fetchedSerial >= self.remoteSendFinishAt {
      self.State <- STATE_FINISH_SEND
    }
  case STATE_FINISH_READ:
    self.remoteReadState = FINISH
    var serial uint32
    binary.Read(bytes.NewReader(frame[1:]), binary.BigEndian, &serial)
    self.remoteReadFinishAt = serial
    if self.remoteReadState == FINISH && self.fetchedSerial >= self.remoteReadFinishAt {
      self.State <- STATE_FINISH_READ
    }
  case STATE_ABORT_SEND: // drop all received packet
    self.remoteSendState = ABORT
    self.State <- STATE_ABORT_SEND
  case STATE_ABORT_READ: // drop all outgoing packet
    self.remoteReadState = ABORT
    self.State <- STATE_ABORT_READ
  }
}

func (self *Session) handleInfoPacket(data []byte) {
  reader := bytes.NewReader(data)
  var timestamp, curSerial, maxSerial uint32
  binary.Read(reader, binary.BigEndian, &timestamp)
  if timestamp < self.lastRemoteHeartbeatTime {
    return
  }
  binary.Read(reader, binary.BigEndian, &curSerial)
  binary.Read(reader, binary.BigEndian, &maxSerial)

  for k, _ := range self.packets { // clear cached packet
    if k < curSerial {
      delete(self.packets, k)
    }
  }

  if curSerial <= self.serial && curSerial == self.lastRemoteCurSerial { // need to resend
    self.sendQueue <- ToSend{self, self.packets[curSerial]}
  }

  self.lastRemoteHeartbeatTime = timestamp
  self.lastRemoteCurSerial = curSerial
  self.lastRemoteMaxSerial = maxSerial
}

func (self *Session) packData(data []byte) []byte {
  buf := new(bytes.Buffer)
  buf.Write([]byte{SESSION_PACKET_TYPE_DATA}) // packet type

  serial := atomic.AddUint32(&self.serial, uint32(1))
  binary.Write(buf, binary.BigEndian, serial) // packet serial
  buf.Write(data) // data
  ret := buf.Bytes()
  self.packets[serial] = ret
  return ret
}

func (self *Session) packState(state byte, extra []byte) []byte {
  buf := new(bytes.Buffer)
  buf.Write([]byte{SESSION_PACKET_TYPE_STATE}) // packet type

  buf.Write([]byte{state}) // state
  buf.Write(extra) // extra information
  return buf.Bytes()
}

func (self *Session) sendInfo(curSerial uint32, maxSerial uint32) {
  buf := new(bytes.Buffer)
  buf.Write([]byte{SESSION_PACKET_TYPE_INFO}) // packet type

  binary.Write(buf, binary.BigEndian, uint32(time.Now().Unix())) // timestamp
  binary.Write(buf, binary.BigEndian, curSerial) // current waiting serial
  binary.Write(buf, binary.BigEndian, maxSerial) // max received serial

  self.infoChan <- ToSend{self, buf.Bytes()}
}

func (self *Session) Send(data []byte) int {
  if self.remoteReadState == ABORT {
    return ABORT
  }
  self.sendQueue <- ToSend{self, self.packData(data)}
  return NORMAL
}

func (self *Session) FinishSend() { // no more data will be send
  self.sendState = FINISH
  serialBuf := new(bytes.Buffer)
  binary.Write(serialBuf, binary.BigEndian, &self.serial)
  self.sendQueue <- ToSend{self, self.packState(STATE_FINISH_SEND, serialBuf.Bytes())}
}

func (self *Session) AbortSend() { // abort all pending data immediately
  self.sendState = ABORT
  self.sendQueue <- ToSend{self, self.packState(STATE_ABORT_SEND, []byte{})}
}

func (self *Session) FinishRead() { // no more data will be read
  self.readState = FINISH
  serialBuf := new(bytes.Buffer)
  binary.Write(serialBuf, binary.BigEndian, &self.serial)
  self.sendQueue <- ToSend{self, self.packState(STATE_FINISH_READ, serialBuf.Bytes())}
}

func (self *Session) AbortRead() { // stop reading immediately
  self.readState = ABORT
  self.sendQueue <- ToSend{self, self.packState(STATE_ABORT_READ, []byte{})}
}

func (self *Session) stop() {
  self.closed = true
  close(self.incomingChan)
  p("start>\n")
  <-self.receiveStopped
  self.heartBeat.Stop()
  <-self.heartBeatStopped
  close(self.dataBuffer)
  self.stopProvideData <- true
  <-self.providerStopped
}

func (self *Session) log(f string, vars ...interface{}) {
  p(ps("SESSION %d %s", self.id, f), vars...)
}
