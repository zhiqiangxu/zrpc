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

func (mux *ServeMux) HandleFunc(cmd Cmd, handler func(w Responser, requestFrame *Frame)) error {
	return mux.Handle(cmd, HandlerFunc(handler))
}

func (mux *ServeMux) Handle(cmd Cmd, handler Handler) (err error) {
	if handler == nil {
		err = fmt.Errorf("zrpc: nil handler")
		return
	}

	if cmd > MaxCmd {
		err = fmt.Errorf("zrpc: cmd too big")
		return
	}

	mux.mu.Lock()
	defer mux.mu.Unlock()

	if mux.m == nil {
		mux.m = make(map[Cmd]Handler)
	}
	if _, exist := mux.m[cmd]; exist {
		err = fmt.Errorf("zrpc: multiple registrations for cmd %d", cmd)
		return
	}

	mux.m[cmd.Routing()] = handler
	return
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
