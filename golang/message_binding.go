package arupa

import (
	"context"
	"fmt"
)

// ServiceMessageBinding supplies protocol-specific conversions around a
// shared ServiceMessageListener.
type ServiceMessageBinding[Request any, Reply any] struct {
	Message func(*Request) IncomingServiceMessage
	Reply   func(string) *Reply
	Error   func(error) *Reply
}

// HandleServiceMessage converts, dispatches, and returns a protocol reply.
func (b ServiceMessageBinding[Request, Reply]) HandleServiceMessage(ctx context.Context, request *Request, listener *ServiceMessageListener) (*Reply, error) {
	if request == nil {
		return nil, fmt.Errorf("arupa: protocol service message is nil")
	}
	if b.Message == nil {
		return nil, fmt.Errorf("arupa: protocol service message converter is nil")
	}
	if b.Reply == nil {
		return nil, fmt.Errorf("arupa: protocol service message reply converter is nil")
	}
	reply, err := listener.Handle(ctx, b.Message(request))
	if err != nil {
		if b.Error != nil {
			return b.Error(err), nil
		}
		return nil, err
	}
	return b.Reply(reply), nil
}
