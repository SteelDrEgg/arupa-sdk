package arupa

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// User is the authenticated identity forwarded by the host with a request.
type User struct {
	Username string
	Groups   []string
}

// SocketEvent is the framework-neutral representation of a host-forwarded
// Socket.IO event. Payload is the JSON-encoded array of event arguments.
type SocketEvent struct {
	RouteID   string
	Namespace string
	Event     string
	SocketID  string
	User      *User
	Payload   []byte
}

// EmitInstruction asks the host to emit a Socket.IO event. Payload is the
// JSON-encoded array of event arguments.
type EmitInstruction struct {
	Namespace string
	Target    string
	Event     string
	Payload   []byte
}

// Emitter sends Socket.IO events during a handler invocation.
type Emitter interface {
	Emit(EmitInstruction) error
}

// EventHandler handles one named Socket.IO event.
type EventHandler func(context.Context, SocketEvent, Emitter) error

// SocketListener is a small, concurrency-safe event listener registry. It is not a
// Socket.IO router: the host has already selected the service and all namespace
// and event policy checks have already happened before Handle is called.
type SocketListener struct {
	mu       sync.RWMutex
	handlers map[string][]EventHandler
	any      []EventHandler
}

// NewSocketListener creates an empty event listener registry.
func NewSocketListener() *SocketListener {
	return &SocketListener{handlers: make(map[string][]EventHandler)}
}

// On registers handler for a Socket.IO event name.
func (b *SocketListener) On(event string, handler EventHandler) error {
	if b == nil {
		return fmt.Errorf("arupa: event bus is nil")
	}
	if event == "" {
		return fmt.Errorf("arupa: socket event name is required")
	}
	if handler == nil {
		return fmt.Errorf("arupa: socket event handler is nil")
	}
	b.mu.Lock()
	if b.handlers == nil {
		b.handlers = make(map[string][]EventHandler)
	}
	b.handlers[event] = append(b.handlers[event], handler)
	b.mu.Unlock()
	return nil
}

// OnAny registers handler for every event received by the service.
func (b *SocketListener) OnAny(handler EventHandler) error {
	if b == nil {
		return fmt.Errorf("arupa: event bus is nil")
	}
	if handler == nil {
		return fmt.Errorf("arupa: socket event handler is nil")
	}
	b.mu.Lock()
	b.any = append(b.any, handler)
	b.mu.Unlock()
	return nil
}

// Handle dispatches event to its listeners and returns all emits produced by
// those listeners. Handlers run in registration order.
func (b *SocketListener) Handle(ctx context.Context, event SocketEvent) ([]EmitInstruction, error) {
	if b == nil {
		return nil, fmt.Errorf("arupa: event bus is nil")
	}
	b.mu.RLock()
	handlers := append([]EventHandler(nil), b.any...)
	handlers = append(handlers, b.handlers[event.Event]...)
	b.mu.RUnlock()

	emitter := &replyEmitter{}
	for _, handler := range handlers {
		if err := handler(ctx, event, emitter); err != nil {
			return nil, err
		}
	}
	return emitter.instructions(), nil
}

// NewEmitJSON encodes args as a Socket.IO event argument array.
func NewEmitJSON(namespace, target, event string, args ...any) (EmitInstruction, error) {
	if namespace == "" {
		return EmitInstruction{}, fmt.Errorf("arupa: emit namespace is required")
	}
	if event == "" {
		return EmitInstruction{}, fmt.Errorf("arupa: emit event is required")
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return EmitInstruction{}, fmt.Errorf("arupa: encode socket event arguments: %w", err)
	}
	return EmitInstruction{Namespace: namespace, Target: target, Event: event, Payload: payload}, nil
}

// EmitJSON encodes args as a Socket.IO event argument array and sends it.
func EmitJSON(emitter Emitter, namespace, target, event string, args ...any) error {
	if emitter == nil {
		return fmt.Errorf("arupa: socket emitter is nil")
	}
	instruction, err := NewEmitJSON(namespace, target, event, args...)
	if err != nil {
		return err
	}
	return emitter.Emit(instruction)
}

type replyEmitter struct {
	mu    sync.Mutex
	emits []EmitInstruction
}

func (e *replyEmitter) Emit(instruction EmitInstruction) error {
	if instruction.Namespace == "" {
		return fmt.Errorf("arupa: emit namespace is required")
	}
	if instruction.Event == "" {
		return fmt.Errorf("arupa: emit event is required")
	}
	e.mu.Lock()
	e.emits = append(e.emits, EmitInstruction{
		Namespace: instruction.Namespace,
		Target:    instruction.Target,
		Event:     instruction.Event,
		Payload:   append([]byte(nil), instruction.Payload...),
	})
	e.mu.Unlock()
	return nil
}

func (e *replyEmitter) instructions() []EmitInstruction {
	e.mu.Lock()
	defer e.mu.Unlock()
	instructions := make([]EmitInstruction, len(e.emits))
	for i, instruction := range e.emits {
		instructions[i] = instruction
		instructions[i].Payload = append([]byte(nil), instruction.Payload...)
	}
	return instructions
}
