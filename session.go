package gnet

import (
  "bytes"
  "encoding/binary"
  "sync/atomic"
  "container/heap"
)

type Session struct {
  id uint64
  serial uint32
  sendChan chan []byte
  packetChan chan Packet
  incomingSerial uint32
  Data chan []byte
}

func newSession(id uint64, sendChan chan []byte) *Session {
  self := &Session{
    id: id,
    sendChan: sendChan,
    packetChan: make(chan Packet, CHAN_BUF_SIZE),
    incomingSerial: 1,
    Data: make(chan []byte, CHAN_BUF_SIZE),
  }
  go self.start()
  return self
}

type Packet struct {
  serial uint32
  packet []byte
  index int
}

func (self *Session) start() {
  packetQueue := make(PacketQueue, 0, 102400)
  for {
    select {
    case packet := <-self.packetChan:
      if packet.serial == self.incomingSerial {
        self.Data <- packet.packet
        self.incomingSerial++
      } else {
        heap.Push(&packetQueue, &packet)
      }
      for len(packetQueue) > 0 {
        next := heap.Pop(&packetQueue).(*Packet)
        if next.serial == self.incomingSerial {
          self.Data <- next.packet
          self.incomingSerial++
        } else {
          heap.Push(&packetQueue, next)
          break
        }
      }

    }
  }
}

func (self *Session) Send(data []byte) {
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, self.id) // session id
  serial := atomic.AddUint32(&self.serial, uint32(1))
  binary.Write(buf, binary.BigEndian, serial) // packet serial
  buf.Write(data) // data
  self.sendChan <- buf.Bytes()
}
