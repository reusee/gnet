package gnet

type PacketQueue []*Packet

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
  a := *self
  n := len(a)
  a = a[0 : n + 1]
  item := x.(*Packet)
  item.index = n
  a[n] = item
  *self = a
}

func (self *PacketQueue) Pop() interface{} {
  a := *self
  n := len(a)
  item := a[n - 1]
  item.index = -1
  *self = a[0 : n - 1]
  return item
}
