package zrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"reflect"
)

func (mux *ServeMux) RegisterName(name string, receiver interface{}) (err error) {
	rcvrVal := reflect.ValueOf(receiver)
	if name == "" {
		return fmt.Errorf("no service name for type %s", rcvrVal.Type().String())
	}

	callbacks := suitableCallbacks(rcvrVal)
	if len(callbacks) == 0 {
		return fmt.Errorf("service %T doesn't have any suitable methods/subscriptions to expose", rcvrVal)
	}

	for method := range callbacks {
		cb := callbacks[method]
		fqName := fmt.Sprintf("%s/%s", name, method)
		cmd := crc32.ChecksumIEEE([]byte(fqName)) & MaxCmd
		mux.HandleFunc(Cmd(cmd), func(w Responser, requestFrame *Frame) {
			args, err := parsePositionalArguments(requestFrame.Payload, cb.argTypes)
			if err != nil {
				w.Response(requestFrame, []byte(fqName+":"+err.Error()))
				w.Close()
				return
			}

			result, err := cb.call(mux.ctx, fqName, args)
			if err != nil {
				jsonBytes, _ := json.Marshal(JsonrpcMessage{Error: fqName + ":" + err.Error()})
				w.Response(requestFrame, jsonBytes)
				return
			}

			resultBytes, _ := json.Marshal(result)
			jsonBytes, _ := json.Marshal(JsonrpcMessage{Result: resultBytes})
			w.Response(requestFrame, jsonBytes)

		})
	}

	return
}

func parsePositionalArguments(payload []byte, types []reflect.Type) ([]reflect.Value, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	var args []reflect.Value
	tok, err := dec.Token()
	switch {
	case err == io.EOF || tok == nil && err == nil:
		// "params" is optional and may be empty. Also allow "params":null even though it's
		// not in the spec because our own client used to send it.
	case err != nil:
		return nil, err
	case tok == json.Delim('['):
		// Read argument array.
		if args, err = parseArgumentArray(dec, types); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("non-array args")
	}
	// Set any missing args to nil.
	for i := len(args); i < len(types); i++ {
		if types[i].Kind() != reflect.Ptr {
			return nil, fmt.Errorf("missing value for required argument %d", i)
		}
		args = append(args, reflect.Zero(types[i]))
	}
	return args, nil
}

func parseArgumentArray(dec *json.Decoder, types []reflect.Type) ([]reflect.Value, error) {
	args := make([]reflect.Value, 0, len(types))
	for i := 0; dec.More(); i++ {
		if i >= len(types) {
			return args, fmt.Errorf("too many arguments, want at most %d", len(types))
		}
		argval := reflect.New(types[i])
		if err := dec.Decode(argval.Interface()); err != nil {
			return args, fmt.Errorf("invalid argument %d: %v", i, err)
		}
		if argval.IsNil() && types[i].Kind() != reflect.Ptr {
			return args, fmt.Errorf("missing value for required argument %d", i)
		}
		args = append(args, argval.Elem())
	}
	// Read end of args array.
	_, err := dec.Token()
	return args, err
}

func suitableCallbacks(receiver reflect.Value) map[string]*callback {
	typ := receiver.Type()
	callbacks := make(map[string]*callback)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		if method.PkgPath != "" {
			continue // method not exported
		}
		cb := newCallback(receiver, method.Func)
		if cb == nil {
			continue // function invalid
		}
		callbacks[method.Name] = cb
	}
	return callbacks
}

type callback struct {
	rcvr, fn reflect.Value
	hasCtx   bool
	errPos   int // err return idx, of -1 when method cannot return error
	argTypes []reflect.Type
}

func newCallback(receiver, fn reflect.Value) *callback {
	fntype := fn.Type()
	c := &callback{fn: fn, rcvr: receiver, errPos: -1}
	// Determine parameter types. They must all be exported or builtin types.
	c.makeArgTypes()

	// Verify return types. The function must return at most one error
	// and/or one other non-error value.
	outs := make([]reflect.Type, fntype.NumOut())
	for i := 0; i < fntype.NumOut(); i++ {
		outs[i] = fntype.Out(i)
	}
	if len(outs) > 2 {
		return nil
	}
	// If an error is returned, it must be the last returned value.
	switch {
	case len(outs) == 1 && isErrorType(outs[0]):
		c.errPos = 0
	case len(outs) == 2:
		if isErrorType(outs[0]) || !isErrorType(outs[1]) {
			return nil
		}
		c.errPos = 1
	}
	return c
}

var (
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
)

func isErrorType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Implements(errorType)
}

func (c *callback) makeArgTypes() {
	fntype := c.fn.Type()
	// Skip receiver and context.Context parameter (if present).
	firstArg := 1

	if fntype.NumIn() > firstArg && fntype.In(firstArg) == contextType {
		firstArg++
		c.hasCtx = true
	}
	// Add all remaining parameters.
	c.argTypes = make([]reflect.Type, fntype.NumIn()-firstArg)
	for i := firstArg; i < fntype.NumIn(); i++ {
		c.argTypes[i-firstArg] = fntype.In(i)
	}
}

func (c *callback) call(ctx context.Context, method string, args []reflect.Value) (res interface{}, errRes error) {
	fullargs := make([]reflect.Value, 0, 2+len(args))
	fullargs = append(fullargs, c.rcvr)
	if c.hasCtx {
		fullargs = append(fullargs, reflect.ValueOf(ctx))
	}
	fullargs = append(fullargs, args...)

	// Run the callback.
	results := c.fn.Call(fullargs)
	if len(results) == 0 {
		return nil, nil
	}

	if c.errPos >= 0 && !results[c.errPos].IsNil() {
		// Method has returned non-nil error value.
		err := results[c.errPos].Interface().(error)
		return reflect.Value{}, err
	}
	return results[0].Interface(), nil
}

type JsonrpcMessage struct {
	Error  string          `json:"error,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}
