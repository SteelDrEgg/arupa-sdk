package arupa

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// IncomingServiceMessage is a service message trusted and forwarded by the host.
// Source is set by the host and is never caller-controlled.
type IncomingServiceMessage struct {
	Source  string
	Target  string
	Topic   string
	Payload []byte
}

// OutgoingServiceMessage is a message sent to another registered service.
type OutgoingServiceMessage struct {
	Target  string
	Topic   string
	Payload []byte
}

// Validate verifies that a message can be sent through the host.
func (m OutgoingServiceMessage) Validate() error {
	if m.Target == "" {
		return fmt.Errorf("arupa: service message target is required")
	}
	return nil
}

// ServiceMessageSender sends a message and returns the target service's reply.
type ServiceMessageSender interface {
	SendServiceMessage(context.Context, OutgoingServiceMessage) (string, error)
}

// SendServiceJSON encodes payload as JSON, then delegates to SendServiceMessage.
func SendServiceJSON(ctx context.Context, sender ServiceMessageSender, target, topic string, payload any) (string, error) {
	if sender == nil {
		return "", fmt.Errorf("arupa: service message sender is nil")
	}
	message := OutgoingServiceMessage{Target: target, Topic: topic}
	if err := message.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("arupa: encode service message: %w", err)
	}
	message.Payload = encoded
	return sender.SendServiceMessage(ctx, message)
}

// ServiceMessageHandler handles a message topic and returns the target-facing
// reply.
type ServiceMessageHandler func(context.Context, IncomingServiceMessage) (string, error)

// ServiceMessageListener dispatches incoming service messages to one handler per
// topic. A single handler is intentional: service messages are request/reply,
// unlike Socket.IO events which may have multiple listeners.
type ServiceMessageListener struct {
	mu       sync.RWMutex
	handlers map[string]ServiceMessageHandler
	any      ServiceMessageHandler
}

// NewServiceMessageListener creates an empty service-message listener.
func NewServiceMessageListener() *ServiceMessageListener {
	return &ServiceMessageListener{handlers: make(map[string]ServiceMessageHandler)}
}

// On registers the sole handler for topic.
func (l *ServiceMessageListener) On(topic string, handler ServiceMessageHandler) error {
	if l == nil {
		return fmt.Errorf("arupa: service message listener is nil")
	}
	if topic == "" {
		return fmt.Errorf("arupa: service message topic is required")
	}
	if handler == nil {
		return fmt.Errorf("arupa: service message handler is nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.handlers == nil {
		l.handlers = make(map[string]ServiceMessageHandler)
	}
	if _, exists := l.handlers[topic]; exists {
		return fmt.Errorf("arupa: service message handler for topic %q already exists", topic)
	}
	l.handlers[topic] = handler
	return nil
}

// OnAny registers a fallback for topics without a dedicated handler.
func (l *ServiceMessageListener) OnAny(handler ServiceMessageHandler) error {
	if l == nil {
		return fmt.Errorf("arupa: service message listener is nil")
	}
	if handler == nil {
		return fmt.Errorf("arupa: service message handler is nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.any != nil {
		return fmt.Errorf("arupa: fallback service message handler already exists")
	}
	l.any = handler
	return nil
}

// Handle invokes the dedicated topic handler or fallback handler.
func (l *ServiceMessageListener) Handle(ctx context.Context, message IncomingServiceMessage) (string, error) {
	if l == nil {
		return "", fmt.Errorf("arupa: service message listener is nil")
	}
	l.mu.RLock()
	handler := l.handlers[message.Topic]
	if handler == nil {
		handler = l.any
	}
	l.mu.RUnlock()
	if handler == nil {
		return "", fmt.Errorf("arupa: no service message handler for topic %q", message.Topic)
	}
	return handler(ctx, message)
}
