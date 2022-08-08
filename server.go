package zrpc

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/zhiqiangxu/util"
	"go.uber.org/zap"
)

type Responser interface {
	Response(requestFrame *Frame, payload []byte) error
	Close(reason error) error
	SetID(interface{})
	GetID() interface{}
}

type Server struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup
	ln         net.Listener
	config     ServerConfig
}

type ServerConfig struct {
	Connection ConnectionConfig
}

func newServer(ln net.Listener, config ServerConfig) *Server {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &Server{ln: ln, ctx: ctx, cancelFunc: cancelFunc, config: config}
}

func ListenTCP(address string, config ServerConfig) (s *Server, err error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return
	}

	s = newServer(ln, config)
	return
}

func ListenUnix(address string, config ServerConfig) (s *Server, err error) {
	ln, err := net.Listen("unix", address)
	if err != nil {
		return
	}

	s = newServer(ln, config)
	return
}

func LisenAndServeTCP(address string, h Handler, config ServerConfig) (err error) {
	s, err := ListenTCP(address, config)
	if err != nil {
		return
	}

	s.Serve(h)
	return
}

func LisenAndServeUnix(address string, h Handler, config ServerConfig) (err error) {
	s, err := ListenUnix(address, config)
	if err != nil {
		return
	}

	s.Serve(h)
	return
}

func (s *Server) Serve(h Handler) {
	util.GoFunc(&s.wg, func() {
		var tempDelay time.Duration // how long to sleep on accept failure
		for {
			rw, err := s.ln.Accept()
			if err == nil {
				tempDelay = 0

				util.GoFunc(&s.wg, func() {
					c, err := NewConnection(rw, h, s.config.Connection, true)
					if err != nil {
						l.Error("zrpc: NewConnection error when Server.Serve", zap.Error(err))
						return
					}
					c.serve(s.ctx)
				})
				continue
			}

			select {
			case <-s.ctx.Done():
				return
			default:
			}

			// handle error
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				l.Error("zrpc: Accept", zap.Duration("retrying in", tempDelay), zap.Error(err))
				time.Sleep(tempDelay)
				continue
			}
			l.Error("zrpc: Accept fatal", zap.Error(err)) // accept4: too many open files in system
			time.Sleep(time.Second)                       // keep trying instead of quit
		}
	})
}

func (s *Server) Shutdown() (err error) {
	err = s.ln.Close()
	if err != nil {
		return
	}

	s.cancelFunc()

	// TODO finish Connection.readFrame
	// s.wg.Wait()
	return
}

func (s *Server) GetCtx() context.Context {
	return s.ctx
}
