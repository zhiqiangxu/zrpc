package zrpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zhiqiangxu/util"
	"go.uber.org/zap"
)

type Connection struct {
	sync.RWMutex
	closed    int32
	wg        sync.WaitGroup
	rw        net.Conn
	h         Handler
	config    ConnectionConfig
	nextBytes []byte
	ridGen    uint64
	respes    map[uint64]func(*Frame)
	id        interface{}
	server    uint64
}

type ConnectionConfig struct {
	Wbuf            int
	Rbuf            int
	RTO             time.Duration
	WTO             time.Duration
	DefaultReadSize int
	OnClose         func(*Connection, error)
}

var DefaultReadSize = 300

// TCPConn in zrpc's aspect
type TCPConn interface {
	net.Conn
	SetKeepAlive(keepalive bool) error
	SetKeepAlivePeriod(d time.Duration) error
	SetWriteBuffer(bytes int) error
	SetReadBuffer(bytes int) error
}

func DialTCP(address string, config ConnectionConfig, h Handler) (c *Connection, err error) {
	rw, err := net.Dial("tcp", address)
	if err != nil {
		return
	}

	c, err = NewConnection(rw, h, config, false)
	if err != nil {
		return
	}

	go c.serve(context.Background())
	return
}

func DialUnix(address string, config ConnectionConfig, h Handler) (c *Connection, err error) {
	rw, err := net.Dial("unix", address)
	if err != nil {
		return
	}

	c, err = NewConnection(rw, h, config, false)
	if err != nil {
		return
	}

	go c.serve(context.Background())
	return
}

func NewConnection(rw net.Conn, h Handler, config ConnectionConfig, server bool) (c *Connection, err error) {
	serverInt := uint64(0)
	if server {
		serverInt = uint64(1)
	}
	c = &Connection{rw: rw, h: h, config: config, server: serverInt}
	err = c.init()
	if err != nil {
		if ce := c.close(err); ce != nil {
			l.Error("zrpc: Connection.Close error when NewConnection", zap.Error(err))
		}
	}
	return
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.rw.RemoteAddr()
}

func (c *Connection) CloseNoCallback() (err error) {
	ok := atomic.CompareAndSwapInt32(&c.closed, 0, 1)
	if !ok {
		err = fmt.Errorf("Connection is already closed")
		return
	}
	err = c.rw.Close()
	if err != nil {
		l.Error("zrpc: Connection.Close error", zap.Error(err))
	}
	return
}

func (c *Connection) Close(reason error) (err error) {
	ok := atomic.CompareAndSwapInt32(&c.closed, 0, 1)
	if !ok {
		err = fmt.Errorf("Connection is already closed")
		return
	}
	err = c.rw.Close()
	if err != nil {
		l.Error("zrpc: Connection.Close error", zap.Error(err))
	}

	if c.config.OnClose != nil {
		c.config.OnClose(c, reason)
	}
	return
}

func (c *Connection) close(reason error) (err error) {

	c.Close(reason)

	c.wg.Wait()
	return
}

const headerSize = 16

func (c *Connection) init() (err error) {
	tc, ok := c.rw.(TCPConn)
	if c.config.Wbuf > 0 {
		if !ok {
			err = fmt.Errorf("zrpc: Wbuf should be zero for non-TCPConn")
			return
		}
		err = tc.SetWriteBuffer(c.config.Wbuf)
		if err != nil {
			return
		}
	}

	if c.config.Rbuf > 0 {
		if !ok {
			err = fmt.Errorf("zrpc: Rbuf should be zero for non-TCPConn")
			return
		}
		err = tc.SetReadBuffer(c.config.Rbuf)
		if err != nil {
			return
		}
	}

	if c.config.DefaultReadSize == 0 {
		c.config.DefaultReadSize = DefaultReadSize
	} else if c.config.DefaultReadSize < headerSize {
		c.config.DefaultReadSize = headerSize
	}
	return
}

func (c *Connection) GetID() (id interface{}) {
	c.RLock()

	id = c.id

	c.RUnlock()
	return
}

func (c *Connection) SetID(id interface{}) {
	c.Lock()
	c.id = id
	c.Unlock()
}

func (c *Connection) serve(ctx context.Context) (err error) {

	defer func() {
		c.close(err)
	}()

	for {
		var frame *Frame
		frame, err = c.readFrame(ctx)
		if err != nil {
			return
		}

		if frame == nil {
			l.Warn("zrpc: bug, empty frame")
			return
		}
		if c.isResp(frame) {
			c.Lock()
			f := c.respes[frame.RequestID]
			if f != nil {
				delete(c.respes, frame.RequestID)
			}
			c.Unlock()
			if f != nil {
				f(frame)
				continue
			}
			l.Warn("zrpc: dropped response",
				zap.Uint32("cmd", uint32(frame.Cmd)),
				zap.Uint64("requestID", frame.RequestID),
				zap.String("payload", string(frame.Payload)),
				zap.Int("#payload", len(frame.Payload)),
				zap.Uint64("ridGen", atomic.LoadUint64(&c.ridGen)))
			continue
		}

		if c.h == nil {
			l.Warn(
				"zrpc: dropped request",
				zap.Uint32("cmd", uint32(frame.Cmd)),
				zap.Uint64("requestID", frame.RequestID),
				zap.String("payload", string(frame.Payload)),
				zap.Int("#payload", len(frame.Payload)),
				zap.Uint64("ridGen", atomic.LoadUint64(&c.ridGen)))
			continue
		}
		util.GoFunc(&c.wg, func() {
			c.h.ServeZrpc(c, frame)
		})

	}
}

func (conn *Connection) nextRequestID() uint64 {
	ridGen := atomic.AddUint64(&conn.ridGen, 1)
	return 2*ridGen + 1 + conn.server
}

func (conn *Connection) isResp(frame *Frame) bool {
	return conn.server == (frame.RequestID+1)%2
}

func (c *Connection) Request(cmd Cmd, payload []byte, f func(*Frame)) {
	requestID := c.nextRequestID()

	if atomic.LoadInt32(&c.closed) != 0 {
		f(nil)
		return
	}

	var header [headerSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	binary.BigEndian.PutUint64(header[4:], requestID)
	binary.BigEndian.PutUint32(header[12:], uint32(cmd))

	err := c.writeFrame(header, payload, f)
	if err != nil {
		l.Error("zrpc: writeFrame error", zap.Error(err))
	}
}

func (c *Connection) writeFrame(header [headerSize]byte, payload []byte, f func(*Frame)) (err error) {
	size := int64(headerSize + len(payload))
	buffs := net.Buffers{header[:], payload}
	wto := c.config.WTO

	c.Lock()
	defer c.Unlock()

	if f != nil {
		if c.respes == nil {
			c.respes = make(map[uint64]func(*Frame))
		}
		c.respes[binary.BigEndian.Uint64(header[4:])] = f
	}

	deadline := time.Now().Add(wto)
	if wto > 0 {
		err = c.rw.SetWriteDeadline(deadline)
		if err != nil {
			c.Close(err)
			return
		}
	}

	var n, offset int64
	for {
		n, err = buffs.WriteTo(c.rw)
		offset += n

		if offset == size {
			return nil
		}

		if err != nil {
			if opError, ok := err.(*net.OpError); ok && opError.Timeout() {
				if wto > 0 && time.Now().After(deadline) {
					c.Close(err)
					return
				} else {
					continue
				}
			}
			c.Close(err)
			return
		}
	}
}

func (c *Connection) Response(requestFrame *Frame, payload []byte) (err error) {

	var header [headerSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	binary.BigEndian.PutUint64(header[4:], requestFrame.RequestID)
	binary.BigEndian.PutUint32(header[12:], uint32(requestFrame.Cmd))

	return c.writeFrame(header, payload, nil)
}

func (c *Connection) readFrame(ctx context.Context) (frame *Frame, err error) {

	frame, payloadLength, err := c.readHeader()
	if err != nil {
		return
	}

	frame.Payload, err = c.readPayload(payloadLength)

	return
}

func (c *Connection) readHeader() (emptyFrame *Frame, payloadLength uint32, err error) {

	if len(c.nextBytes) >= headerSize {
		payloadLength = binary.BigEndian.Uint32(c.nextBytes)
		requestID := binary.BigEndian.Uint64(c.nextBytes[4:])
		cmdAndFlags := binary.BigEndian.Uint32(c.nextBytes[12:])
		cmd := Cmd(cmdAndFlags & 0xffffff)
		flags := FrameFlag(cmdAndFlags >> 24)
		// TODO pool
		emptyFrame = &Frame{RequestID: requestID, Cmd: cmd, Flags: flags}
		c.nextBytes = c.nextBytes[headerSize:]
		return
	}

	// TODO pool
	buf := make([]byte, c.config.DefaultReadSize)
	var n, offset int
	for {
		c.rw.SetReadDeadline(time.Time{})
		n, err = c.rw.Read(buf[offset:])
		offset += n
		if len(c.nextBytes)+offset >= headerSize {
			emptyFrame, payloadLength = c.parseSegmentedHeader(buf[0:offset])
			err = nil
			return
		}
		if err != nil {
			return
		}
	}
}

func (c *Connection) readPayload(length uint32) (payload []byte, err error) {
	if len(c.nextBytes) >= int(length) {
		payload = c.nextBytes[0:length]
		c.nextBytes = c.nextBytes[length:]
		return
	}

	payload = make([]byte, length+uint32(c.config.DefaultReadSize))
	copy(payload, c.nextBytes)
	offset := len(c.nextBytes)
	c.nextBytes = nil

	var n int
	for {
		if c.config.RTO > 0 {
			c.rw.SetReadDeadline(time.Now().Add(c.config.RTO))
		}
		n, err = c.rw.Read(payload[offset:])
		if offset+n >= int(length) {
			if offset+n > int(length) {
				c.nextBytes = payload[length : offset+n]
			}
			payload = payload[0:length]
			err = nil
			return
		}
		if err != nil {
			return
		}
		offset += n
	}
}

// invariant : len(c.nextBytes)+len(buf) >= headerSize
func (c *Connection) parseSegmentedHeader(buf []byte) (emptyFrame *Frame, payloadLength uint32) {

	var header [headerSize]byte
	copy(header[:], c.nextBytes)
	copy(header[len(c.nextBytes):], buf[0:headerSize-len(c.nextBytes)])

	payloadLength = binary.BigEndian.Uint32(header[:])
	requestID := binary.BigEndian.Uint64(header[4:])
	cmdAndFlags := binary.BigEndian.Uint32(header[12:])
	cmd := Cmd(cmdAndFlags & 0xffffff)
	flags := FrameFlag(cmdAndFlags >> 24)

	// TODO pool
	emptyFrame = &Frame{RequestID: requestID, Cmd: cmd, Flags: flags}
	c.nextBytes = buf[headerSize-len(c.nextBytes):]
	return

}
