package zrpc

import (
	"fmt"
	"testing"
	"time"
)

const (
	HelloCmd Cmd = iota
)

func TestZrpc(t *testing.T) {
	address := "localhost:8002"
	s := startServer(address)
	defer func() {
		fmt.Println("Shutdown")
		s.Shutdown()
	}()

	c, err := DialTCP(address, ConnectionConfig{}, nil)
	if err != nil {
		panic(err)
	}

	payload := make([]byte, 100)
	payload[0] = 'a'

	gate := make(chan struct{}, 1)

	for i := 0; i < 7; i++ {
		start := time.Now()
		c.Request(HelloCmd, payload, func(f *Frame) {
			gate <- struct{}{}
		})
		<-gate
		fmt.Println("cost", time.Since(start))
	}

}

func BenchmarkLatency(b *testing.B) {
	address := "localhost:8002"
	s := startServer(address)
	defer func() {
		fmt.Println("Shutdown")
		s.Shutdown()
	}()

	c, err := DialTCP(address, ConnectionConfig{}, nil)
	if err != nil {
		panic(err)
	}

	payload := make([]byte, 100)
	payload[0] = 'a'

	gate := make(chan struct{}, 1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		c.Request(HelloCmd, payload, func(f *Frame) {
			gate <- struct{}{}
		})
		<-gate
	}
}

func startServer(address string) (s *Server) {
	s, err := ListenTCP(address, ServerConfig{})
	if err != nil {
		panic(err)
	}
	mux := NewServeMux()
	mux.HandleFunc(HelloCmd, func(w Responser, requestFrame *Frame) {
		err := w.Response(requestFrame, requestFrame.Payload)
		if err != nil {
			panic(err)
		}
	})
	go s.Serve(mux)
	return
}
