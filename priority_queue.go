package gnet

import (
  "container/heap"
)

type PacketQueue []*Packet

func newPacketQueue() *PacketQueue {
  packetQueue := make(PacketQueue, 0, INITIAL_BUF_CAPACITY)
  heap.Init(&packetQueue)
  return &packetQueue
}

func (self PacketQueue) Len() int {
  return len(self)
}

func (self PacketQueue) Less(i, j int) bool {
  return self[i].serial < self[j].serial
}

func (self PacketQueue) Swap(i, j int) {
  self[i], self[j] = self[j], self[i]
  self[i].index = i
  self[j].index = j
}

func (self *PacketQueue) Push(x interface{}) {
  item := x.(*Packet)
  item.index = len(*self)
  newSlice := append(*self, item)
  *self = newSlice
}

func (self *PacketQueue) Pop() interface{} {
  item := (*self)[len(*self) - 1]
  item.index = -1
  newSlice := (*self)[:len(*self) - 1]
  *self = newSlice
  return item
}
