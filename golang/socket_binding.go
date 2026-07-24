package arupa

import (
	"context"
	"fmt"
)

// SocketBinding supplies protocol-specific conversions around the shared socket
// listener.
type SocketBinding[Request any, Reply any] struct {
	Event func(*Request) SocketEvent
	Reply func([]EmitInstruction) *Reply
}

// HandleSocketEvent converts a protocol event, dispatches it through events,
// and converts all listener emits into the protocol reply type.
func (b SocketBinding[Request, Reply]) HandleSocketEvent(ctx context.Context, request *Request, events *SocketListener) (*Reply, error) {
	if request == nil {
		return nil, fmt.Errorf("arupa: protocol socket event is nil")
	}
	if b.Event == nil {
		return nil, fmt.Errorf("arupa: protocol socket event converter is nil")
	}
	if b.Reply == nil {
		return nil, fmt.Errorf("arupa: protocol socket event reply converter is nil")
	}
	emits, err := events.Handle(ctx, b.Event(request))
	if err != nil {
		return nil, err
	}
	return b.Reply(emits), nil
}
