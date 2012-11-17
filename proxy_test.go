package gnet

import (
  "testing"
  "net"
  "fmt"
  "bytes"
  "math/rand"
  "time"
)

func TestProxyTCP(t *testing.T) {
  rand.Seed(time.Now().UnixNano())
  bufSize := 65536

  // tcp echo server
  echoServerAddr, _ := net.ResolveTCPAddr("tcp", ":7801")
  go func() {
    ln, err := net.ListenTCP("tcp", echoServerAddr)
    if err != nil {
      t.Fatalf("tcp echo server listen error %v", err)
    }
    for {
      conn, _ := ln.Accept()
      go func() {
        for {
          buf := make([]byte, bufSize)
          n, err := conn.Read(buf)
          if err != nil {
            conn.Close()
            break
          }
          conn.Write(buf[:n])
        }
      }()
    }
  }()

  // proxy server part
  server, err := NewServer(":7800", "abc")
  if err != nil {
    t.Fatalf("proxy server part start error %v", err)
  }
  go func() {
    for {
      session := <-server.New
      go func() {
        conn, err := net.DialTCP("tcp", nil, echoServerAddr)
        if err != nil {
          t.Fatalf("proxy server part dial to echo server error %v", err)
        }
        session.ProxyTCP(conn, bufSize)
      }()
    }
  }()

  // proxy client part
  proxyServerAddr, _ := net.ResolveTCPAddr("tcp", "localhost:7802")
  client, err := NewClient("localhost:7800", "abc", 1)
  if err != nil {
    t.Fatalf("proxy client part start error %v", err)
  }
  init := make(chan struct{})
  go func() {
    ln, err := net.ListenTCP("tcp", proxyServerAddr)
    if err != nil {
      t.Fatalf("proxy client part tcp listen error %v", err)
    }
    close(init)
    for {
      conn, _ := ln.AcceptTCP()
      go func() {
        session := client.NewSession()
        session.ProxyTCP(conn, bufSize)
      }()
    }
  }()
  <-init

  // echo client
  for i := 0; i < 1000; i++ {
    conn, err := net.DialTCP("tcp", nil, proxyServerAddr)
    if err != nil {
      t.Fatalf("echo client dial error %v", err)
    }
    data := bytes.Repeat([]byte(fmt.Sprintf("%d", rand.Int63())), rand.Intn(50) + 1)
    conn.Write(data)
    fmt.Printf("sent %s\n", data)
    buf := make([]byte, bufSize)
    n, err := conn.Read(buf)
    if err != nil {
      fmt.Printf("read error %v\n", err)
    }
    fmt.Printf("receive %s\n", buf[:n])
    if !bytes.Equal(buf[:n], data) {
      t.Fatal("received error")
    }
    conn.Close()
  }
}
