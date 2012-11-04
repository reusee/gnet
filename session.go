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
  incomingSerial uint32
  sendDataChan chan []byte
  incomingPacketChan chan Packet

  Data chan []byte
}

func newSession(id uint64, sendDataChan chan []byte) *Session {
  self := &Session{
    id: id,
    incomingSerial: 1,
    sendDataChan: sendDataChan,
    incomingPacketChan: make(chan Packet, CHAN_BUF_SIZE),
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
  packetQueue := make(PacketQueue, 0, 102400)
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

    }
  }
}

func (self *Session) Send(data []byte) {
  buf := new(bytes.Buffer)
  binary.Write(buf, binary.BigEndian, self.id) // session id
  serial := atomic.AddUint32(&self.serial, uint32(1))
  binary.Write(buf, binary.BigEndian, serial) // packet serial
  buf.Write(data) // data
  self.sendDataChan <- buf.Bytes()
}
