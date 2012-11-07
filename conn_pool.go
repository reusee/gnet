package gnet

import (
  "net"
  "bytes"
  "encoding/binary"
  "time"
  "sync"
)

type ConnPool struct {
  newConnChan chan *net.TCPConn
  deadConnNotify chan bool

  byteKeys []byte
  uint64Keys []uint64

  sendChan chan ToSend
  infoChan chan ToSend
  rawSendChan chan []byte

  sessions map[uint64]*Session
  newSessionChan chan *Session

  stopListen chan bool
  stopHeartBeat chan bool

  bytesRead uint64
  bytesWrite uint64
}

func newConnPool(key string, newSessionChan chan *Session) *ConnPool {
  self := &ConnPool{
    newConnChan: make(chan *net.TCPConn, CHAN_BUF_SIZE),

    sendChan: make(chan ToSend, CHAN_BUF_SIZE),
    infoChan: make(chan ToSend, CHAN_BUF_SIZE),
    rawSendChan: make(chan []byte, CHAN_BUF_SIZE),

    sessions: make(map[uint64]*Session),
    newSessionChan: newSessionChan,

    stopListen: make(chan bool, 32),
    stopHeartBeat: make(chan bool, 32),
  }
  self.byteKeys, self.uint64Keys = calculateKeys(key)
  go self.start()
  return self
}

func (self *ConnPool) start() {
  var bytesWrite, bytesRead uint64
  infoBuf := new(bytes.Buffer)
  infoBufLock := new(sync.Mutex)

  heartBeat := time.NewTicker(time.Second * 3)
  go func() { // heartbeat
    for {
      select {
      case <-heartBeat.C:

        curBytesRead, curBytesWrite := self.bytesRead, self.bytesWrite
        self.log("read %d / %d write %d / %d\n",
          curBytesRead - bytesRead, curBytesRead,
          curBytesWrite - bytesWrite, curBytesWrite)
        bytesWrite, bytesRead = curBytesWrite, curBytesRead

        infoBufLock.Lock()
        info := infoBuf.Bytes()
        infoBuf = new(bytes.Buffer)
        infoBufLock.Unlock()
        frame := new(bytes.Buffer)
        frame.Write([]byte{PACKET_TYPE_INFO})
        binary.Write(frame, binary.BigEndian, uint32(len(info)))
        frame.Write(info)
        self.rawSendChan <- frame.Bytes()

      case <-self.stopHeartBeat:
        return
      }
    }
  }()

  for {
    select {
    case conn := <-self.newConnChan:
      newConn(conn, self)

    case info := <-self.infoChan:
      infoBufLock.Lock()
      binary.Write(infoBuf, binary.BigEndian, info.session.id)
      infoBuf.Write(info.frame)
      infoBufLock.Unlock()

    case <-self.stopListen:
      return
    }
  }
}

func (self *ConnPool) Close() {
  self.stopListen <- true
  self.stopHeartBeat <- true
}

func (self *ConnPool) log(f string, vars ...interface{}) {
  p("CONNPOOL " + f, vars...)
}
