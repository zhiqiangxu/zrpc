package zrpc

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// A Handler responds to an zrpc request.
type Handler interface {
	ServeZrpc(w Responser, frame *Frame)
}

type HandlerFunc func(w Responser, frame *Frame)

func (f HandlerFunc) ServeZrpc(w Responser, frame *Frame) {
	f(w, frame)
}

// ServeMux is zrpc request multiplexer.
type ServeMux struct {
	mu  sync.RWMutex
	m   map[Cmd]Handler
	ctx context.Context
}

func NewServeMux(ctx context.Context) *ServeMux { return &ServeMux{ctx: ctx} }

func (mux *ServeMux) HandleFunc(cmd Cmd, handler func(w Responser, requestFrame *Frame)) {
	mux.Handle(cmd, HandlerFunc(handler))
}

func (mux *ServeMux) Handle(cmd Cmd, handler Handler) {
	if handler == nil {
		panic("zrpc: nil handler")
	}

	if cmd > MaxCmd {
		panic("zrpc: cmd too big")
	}

	mux.mu.Lock()
	defer mux.mu.Unlock()

	if mux.m == nil {
		mux.m = make(map[Cmd]Handler)
	}
	if _, exist := mux.m[cmd]; exist {
		panic(fmt.Sprintf("zrpc: multiple registrations for cmd %d", cmd))
	}

	mux.m[cmd.Routing()] = handler
}

func (mux *ServeMux) ServeZrpc(w Responser, f *Frame) {
	routingCmd := f.Cmd.Routing()

	mux.mu.RLock()
	h, ok := mux.m[routingCmd]
	mux.mu.RUnlock()

	if !ok {
		l.Error("cmd not registered", zap.Uint32("cmd", uint32(f.Cmd)))
		w.Close()
		return
	}

	h.ServeZrpc(w, f)
}
