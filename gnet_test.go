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
  defer server.Close()

  client, err := NewClient("localhost:8888", "abcd", 4)
  if err != nil {
    t.Fatal(err)
  }
  defer client.Close()

  n := 2000

  go func() {
    session := client.NewSession()
    for i := 0; i < n; i++ {
      session.Send([]byte(fmt.Sprintf("%d", i)))
      <-session.Data
    }
    session.Close()
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
  session.Close()
}

func TestSessionAbort(t *testing.T) {
  server, err := NewServer(":8700", "abc")
  if err != nil {
    t.Fatal(err)
  }
  client, err := NewClient("localhost:8700", "abc", 8)
  if err != nil {
    t.Fatal(err)
  }

  go func() {
    session := <-server.New
    for {
      select {
      case data := <-session.Data:
        p("%s ", data)
        session.Send(data)
      case state := <-session.State:
        if state == STATE_ABORT_SEND {
          fmt.Printf("client abort send. quit\n")
          session.AbortRead()
          return
        }
      }
    }
  }()

  session := client.NewSession()
  for i := 0; i < 200; i++ {
    session.Send([]byte(fmt.Sprintf("%d", i)))
  }
  session.AbortSend()
  var data []byte
  LOOP: for {
    select {
    case data = <-session.Data:
    case state := <-session.State:
      if state == STATE_ABORT_READ {
        fmt.Printf("server abort read. quit. last echo %s\n", data)
        break LOOP
      }
    }
  }
}

func TestSessionFinish(t *testing.T) {
  server, err := NewServer(":8710", "abc")
  if err != nil {
    t.Fatal(err)
  }
  client, err := NewClient("localhost:8710", "abc", 8)
  if err != nil {
    t.Fatal(err)
  }

  go func() {
    session := <-server.New
    for {
      select {
      case data := <-session.Data:
        p("%s ", data)
        session.Send(data)
      case state := <-session.State:
        if state == STATE_FINISH_SEND {
          fmt.Printf("client finish send. quit\n")
          session.FinishRead()
          return
        }
      }
    }
  }()

  session := client.NewSession()
  for i := 0; i < 200; i++ {
    data := []byte(fmt.Sprintf("%d", i))
    session.Send(data)
  }
  session.FinishSend()
  var data []byte
  LOOP: for {
    select {
    case data = <-session.Data:
    case state := <-session.State:
      if state == STATE_FINISH_READ {
        fmt.Printf("server finish read. quit. last echo %s\n", data)
        break LOOP
      }
    }
  }
}
