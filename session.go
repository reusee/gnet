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
  endChan chan bool
  closed bool

  incomingSerial uint32
  incomingPacketChan chan Packet
  sendDataChan chan DataToSend
  sendStateChan chan StateToSend

  readState int // for session cleaner
  sendState int // for session cleaner
  remoteReadState int // for Send() and conn writer
  remoteSendState int // for incomingPacketChan and conn reader

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
    endChan: make(chan bool, 32), // may be multiple push

    incomingSerial: 1,
    incomingPacketChan: make(chan Packet, CHAN_BUF_SIZE),
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
  packetQueue := newPacketQueue()
  for {
    select {
    case packet := <-self.incomingPacketChan:
      if packet.serial == self.incomingSerial {
        self.Data <- packet.data
        self.incomingSerial++
      } else {
        heap.Push(&packetQueue, &packet)
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

    case <-self.endChan:
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

func (self *Session) Send(data []byte) int {
  if self.remoteReadState == ABORT {
    return ABORT
  }
  self.sendDataChan <- DataToSend{self, self.packData(data)}
  return NORMAL
}

func (self *Session) FinishSend() { // no more data will be send
  self.sendState = FINISH
  self.sendStateChan <- StateToSend{self, self.packData([]byte{STATE_FINISH_SEND})}
}

func (self *Session) AbortSend() { // abort all pending data immediately
  self.sendState = ABORT
  self.sendStateChan <- StateToSend{self, self.packData([]byte{STATE_ABORT_SEND})}
}

func (self *Session) FinishRead() { // no more data will be read
  self.readState = FINISH
  self.sendStateChan <- StateToSend{self, self.packData([]byte{STATE_FINISH_READ})}
}

func (self *Session) AbortRead() { // stop reading immediately
  self.readState = ABORT
  self.sendStateChan <- StateToSend{self, self.packData([]byte{STATE_ABORT_READ})}
}

func (self *Session) Close() {
  self.FinishRead()
  self.FinishSend()
  self.endChan <- true
  self.closed = true
}

func (self *Session) Abort() {
  self.AbortSend()
  self.AbortRead()
  self.endChan <- true
  self.closed = true
}
