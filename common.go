package gnet

import (
  "fmt"
)

const (
  CHAN_BUF_SIZE = 2 ^ 19

  TYPE_PING = byte(0)
  TYPE_DATA = byte(1)
  TYPE_STATE = byte(2)

  STATE_FINISH_SEND = byte(0)
  STATE_ABORT_SEND = byte(1)
  STATE_FINISH_READ = byte(2)
  STATE_ABORT_READ = byte(3)

  NORMAL = iota
  FINISH
  ABORT
)

func p(f string, vars ...interface{}) {
  fmt.Printf(f, vars...)
}
