package arupa

import (
	"context"
	"fmt"
)

// MessageBinding supplies protocol-specific conversions around a shared
// MessageListener.
type MessageBinding[Request any, Reply any] struct {
	Message func(*Request) IncomingMessage
	Reply   func(string) *Reply
}

// HandlePluginMessage converts, dispatches, and returns a protocol reply.
func (b MessageBinding[Request, Reply]) HandlePluginMessage(ctx context.Context, request *Request, listener *MessageListener) (*Reply, error) {
	if request == nil {
		return nil, fmt.Errorf("arupa: protocol plugin message is nil")
	}
	reply, err := listener.Handle(ctx, b.Message(request))
	if err != nil {
		return nil, err
	}
	return b.Reply(reply), nil
}
