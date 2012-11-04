package gnet

import (
  "fmt"
)

const (
  CHAN_BUF_SIZE = 2 ^ 19

  TYPE_PING = byte(0)
  TYPE_DATA = byte(1)
)

func p(f string, vars ...interface{}) {
  fmt.Printf(f, vars...)
}
