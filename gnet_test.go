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

  n := 2000

  go func() {
    session := client.NewSession()
    for i := 0; i < n; i++ {
      session.Send([]byte(fmt.Sprintf("%d", i)))
      pong := <-session.Data
      fmt.Printf("%s\n", pong)
    }
    session.FinishSend()
  }()

  session := <-server.New
  for i := 0; i < n; i++ {
    data := <-session.Data
    expected := []byte(fmt.Sprintf("%d", i))
    if bytes.Compare(data, expected) != 0 {
      t.Fatal("wrong seq")
    }
    session.Send(data)
  }
}

func TestAbort(t *testing.T) {
  server, err := NewServer(":8889", "abcd")
  if err != nil {
    t.Fatal(err)
  }

  client, err := NewClient("localhost:8889", "abcd", 4)
  if err != nil {
    t.Fatal(err)
  }

  end := make(chan bool)
  go func() {
    session := client.NewSession()
    for {
      if session.Send([]byte("hello")) == ABORT {
        break
      }
    }
    end <- true
  }()

  session := <-server.New
  n := 0
  for {
    data := <-session.Data
    fmt.Printf("%s\n", data)
    n++
    if n > 5 {
      session.AbortRead()
      break
    }
  }
  <-end
}
