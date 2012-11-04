package gnet

import (
  "testing"
  "fmt"
  "bytes"
)

func TestNew(t *testing.T) {
  server, err := NewServer(":8888", "abcd")
  if err != nil {
    t.Fatal(err)
  }

  client, err := NewClient("localhost:8888", "abcd", 4)
  if err != nil {
    t.Fatal(err)
  }

  n := 10000

  go func() {
    session := client.NewSession()
    for i := 0; i < n; i++ {
      session.Send([]byte(fmt.Sprintf("%d", i)))
    }
  }()

  session := <-server.New
  for i := 0; i < n; i++ {
    data := <-session.Data
    expected := []byte(fmt.Sprintf("%d", i))
    if bytes.Compare(data, expected) != 0 {
      t.Fatal("wrong seq")
    }
  }
}
