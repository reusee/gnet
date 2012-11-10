package gnet

import (
  "bytes"
  "encoding/binary"
  "sync/atomic"
  "container/heap"
  "time"
)

type Session struct {
  stop chan struct{}
  id uint64
  serial uint32
  closed bool

  incomingSerial uint32
  maxIncomingSerial uint32
  incomingChan *InfiniteByteSliceChan
  sendQueue *InfiniteToSendChan
  infoChan *InfiniteToSendChan
  //packets map[uint32][]byte

  readState int // for session cleaner
  sendState int // for session cleaner
  remoteReadState int // for Send() and conn writer
  remoteSendState int // 
  remoteReadFinishAt uint32
  remoteSendFinishAt uint32

  incomingDataCount uint32

  packetQueue PacketQueue

  Message chan Message
  messageBuffer []Message
  message chan Message
  stopDeliver chan struct{}

  lastRemoteHeartbeatTime uint32
  lastRemoteCurSerial uint32
  lastRemoteMaxSerial uint32

  C *InfiniteByteSliceChan

  stopNotify *InfiniteSessionChan
}

type ToSend struct {
  tag int
  session *Session
  data []byte
}

func newSession(id uint64, connPool *ConnPool) *Session {
  self := &Session{
    stop: make(chan struct{}),
    id: id,

    incomingSerial: 1,
    incomingChan: NewInfiniteByteSliceChan(),
    sendQueue: connPool.sendQueue,
    infoChan: connPool.infoChan,
    //packets: make(map[uint32][]byte),

    readState: NORMAL,
    sendState: NORMAL,
    remoteReadState: NORMAL,
    remoteSendState: NORMAL,

    packetQueue: newPacketQueue(),

    Message: make(chan Message),
    message: make(chan Message),
    messageBuffer: make([]Message, 0, INITIAL_BUF_CAPACITY),
    stopDeliver: make(chan struct{}),

    C: NewInfiniteByteSliceChan(),
  }

  go self.startMessageDeliver()
  go self.start()

  return self
}

func (self *Session) start() {
  self.log("strat")
  heartBeat := time.Tick(time.Second * 2)
  tick := 0
  LOOP:
  for {
    select {
    case packet := <-self.incomingChan.Out:
      self.handleIncoming(packet)
    case data := <-self.C.Out:
      self.Send(data)
    case <-heartBeat:
      self.showInfo()
      self.sendInfo()
      self.checkState()
      //self.log("tick %d", tick)
    case <-self.stop:
      break LOOP
    }
    tick++
  }

  self.log("stop")
  self.pushState(STATE_STOP)
  self.incomingChan.Stop()
  self.C.Stop()
  if self.stopNotify != nil {
    self.stopNotify.In <- self
  }
}

func (self *Session) startMessageDeliver() {
  for {
    if len(self.messageBuffer) > 0 {
      select {
      case self.Message <- self.messageBuffer[0]:
        self.messageBuffer = self.messageBuffer[1:]
      case value := <-self.message:
        self.messageBuffer = append(self.messageBuffer, value)
      case <-self.stopDeliver:
        return
      }
    } else {
      select {
      case value := <-self.message:
        self.messageBuffer = append(self.messageBuffer, value)
      case <-self.stopDeliver:
        return
      }
    }
  }
}

func (self *Session) showInfo() {
  cur, max, count := self.incomingSerial, self.maxIncomingSerial, self.incomingDataCount
  if cur < max {
    self.log("packet gap %d %d %d", cur, max, count)
  }
}

func (self *Session) sendInfo() {
  cur, max := self.incomingSerial, self.maxIncomingSerial
  buf := new(bytes.Buffer)
  buf.Write([]byte{SESSION_PACKET_TYPE_INFO}) // packet type

  binary.Write(buf, binary.BigEndian, uint32(time.Now().Unix())) // timestamp
  binary.Write(buf, binary.BigEndian, cur) // current waiting serial
  binary.Write(buf, binary.BigEndian, max) // max received serial

  self.infoChan.In <- ToSend{INFO, self, buf.Bytes()}
}

func (self *Session) checkState() {
  if self.sendState == FINISH || self.sendState == ABORT {
    if self.readState == FINISH || self.readState == ABORT {
      self.log("finish/abort read/send, stop")
      self.Stop()
    }
  }
}

func (self *Session) handleIncoming(packet []byte) {
  packetType := packet[0]
  switch packetType {
  case SESSION_PACKET_TYPE_DATA:
    self.handleDataPacket(packet[1:])
  case SESSION_PACKET_TYPE_STATE:
    self.handleStatePacket(packet[1:])
  case SESSION_PACKET_TYPE_INFO:
    self.handleInfoPacket(packet[1:])
  }
}

type Packet struct {
  serial uint32
  data []byte
  index int
}

type Message struct {
  Tag int
  Data []byte
  State byte
  Time time.Time
}

func (self *Session) handleDataPacket(data []byte) {
  atomic.AddUint32(&self.incomingDataCount, uint32(1))

  var serial uint32
  binary.Read(bytes.NewReader(data[:4]), binary.BigEndian, &serial)
  data = data[4:]

  packet := Packet{serial: serial, data: data}
  if serial == self.incomingSerial {
    self.pushData(packet)
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
      self.pushData(*next)
      self.incomingSerial++
    } else {
      heap.Push(&self.packetQueue, next)
      break
    }
  }
}

func (self *Session) pushData(packet Packet) {
  message := Message{
    Tag: DATA,
    Data: packet.data,
    Time: time.Now(),
  }
  self.message <- message
  if self.remoteReadState == FINISH && packet.serial >= self.remoteReadFinishAt {
    self.sendState = FINISH
    self.pushState(STATE_FINISH_READ)
  }
  if self.remoteSendState == FINISH && packet.serial >= self.remoteSendFinishAt {
    self.readState = FINISH
    self.pushState(STATE_ABORT_SEND)
  }
}

func (self *Session) pushState(state byte) {
  self.message <- Message{
    Tag: STATE,
    State: state,
    Time: time.Now(),
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
    if self.remoteSendState == FINISH && self.incomingSerial >= self.remoteSendFinishAt {
      self.readState = FINISH
      self.pushState(STATE_FINISH_SEND)
    }
  case STATE_FINISH_READ:
    self.remoteReadState = FINISH
    var serial uint32
    binary.Read(bytes.NewReader(frame[1:]), binary.BigEndian, &serial)
    self.remoteReadFinishAt = serial
    if self.remoteReadState == FINISH && self.incomingSerial >= self.remoteReadFinishAt {
      self.sendState = FINISH
      self.pushState(STATE_FINISH_READ)
    }
  case STATE_ABORT_SEND: // drop all received packet
    self.remoteSendState = ABORT
    self.readState = ABORT
    self.pushState(STATE_ABORT_SEND)
  case STATE_ABORT_READ: // drop all outgoing packet
    self.remoteReadState = ABORT
    self.sendState = ABORT
    self.pushState(STATE_ABORT_READ)
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

  //for serial, _ := range self.packets { // clear cached packet
  //  if serial < curSerial {
  //    self.log("clear cached %d", serial)
  //    delete(self.packets, serial)
  //  }
  //}

  //if curSerial <= self.serial && curSerial == self.lastRemoteCurSerial { // need to resend
  //  self.sendQueue.In <- ToSend{INFO, self, self.packets[curSerial]}
  //}

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
  //self.packets[serial] = ret
  return ret
}

func (self *Session) packState(state byte, extra []byte) []byte {
  buf := new(bytes.Buffer)
  buf.Write([]byte{SESSION_PACKET_TYPE_STATE}) // packet type

  buf.Write([]byte{state}) // state
  buf.Write(extra) // extra information
  return buf.Bytes()
}

func (self *Session) Send(data []byte) int {
  if self.remoteReadState == ABORT {
    return ABORT
  }
  self.sendQueue.In <- ToSend{DATA, self, self.packData(data)}
  return NORMAL
}

func (self *Session) FinishSend() { // no more data will be send
  self.sendState = FINISH
  serialBuf := new(bytes.Buffer)
  binary.Write(serialBuf, binary.BigEndian, &self.serial)
  self.sendQueue.In <- ToSend{STATE, self, self.packState(STATE_FINISH_SEND, serialBuf.Bytes())}
}

func (self *Session) FinishRead() { // no more data will be read
  self.readState = FINISH
  serialBuf := new(bytes.Buffer)
  binary.Write(serialBuf, binary.BigEndian, &self.serial)
  self.sendQueue.In <- ToSend{STATE, self, self.packState(STATE_FINISH_READ, serialBuf.Bytes())}
}

func (self *Session) Finish() {
  self.FinishRead()
  self.FinishSend()
}

func (self *Session) AbortSend() { // abort all pending data immediately
  self.sendState = ABORT
  self.sendQueue.In <- ToSend{STATE, self, self.packState(STATE_ABORT_SEND, []byte{})}
}

func (self *Session) AbortRead() { // stop reading immediately
  self.readState = ABORT
  self.sendQueue.In <- ToSend{STATE, self, self.packState(STATE_ABORT_READ, []byte{})}
}

func (self *Session) Abort() {
  self.AbortSend()
  self.AbortRead()
}

func (self *Session) Stop() {
  self.closed = true
  close(self.stop)
}

func (self *Session) log(f string, vars ...interface{}) {
  colorp("32", ps("SESSION %d %s", self.id, f), vars...)
}
