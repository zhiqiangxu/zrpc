package zrpc

type Frame struct {
	RequestID uint64
	Flags     FrameFlag
	Cmd       Cmd
	Payload   []byte
}

type FrameFlag uint8

// only the lower two bytes are used for routing
// the 3rd byte can be used to store opaque value
type Cmd uint32

const (
	// MaxCmd for qrpc
	MaxCmd = 0xffffff
)

//go:nosplit
func (c Cmd) Routing() Cmd {
	return Cmd(c & 0xffff)
}
