package arupa

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// IncomingMessage is a plugin message trusted and forwarded by the host.
// Source is set by the host and is never caller-controlled.
type IncomingMessage struct {
	Source  string
	Topic   string
	Payload []byte
}

// OutgoingMessage is a message sent to another registered plugin.
type OutgoingMessage struct {
	Target  string
	Topic   string
	Payload []byte
}

// Validate verifies that a message can be sent through the host.
func (m OutgoingMessage) Validate() error {
	if m.Target == "" {
		return fmt.Errorf("arupa: message target is required")
	}
	if m.Topic == "" {
		return fmt.Errorf("arupa: message topic is required")
	}
	return nil
}

// MessageSender sends a message and returns the target plugin's reply.
type MessageSender interface {
	SendMessage(context.Context, OutgoingMessage) (string, error)
}

// SendJSON encodes payload as JSON, then delegates to sender.SendMessage.
func SendJSON(ctx context.Context, sender MessageSender, target, topic string, payload any) (string, error) {
	if sender == nil {
		return "", fmt.Errorf("arupa: message sender is nil")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("arupa: encode plugin message: %w", err)
	}
	return sender.SendMessage(ctx, OutgoingMessage{Target: target, Topic: topic, Payload: encoded})
}

// MessageHandler handles a message topic and returns the target-facing reply.
type MessageHandler func(context.Context, IncomingMessage) (string, error)

// MessageListener dispatches incoming plugin messages to one handler per
// topic. A single handler is intentional: plugin messages are request/reply,
// unlike Socket.IO events which may have multiple listeners.
type MessageListener struct {
	mu       sync.RWMutex
	handlers map[string]MessageHandler
	any      MessageHandler
}

// NewMessageListener creates an empty plugin-message listener.
func NewMessageListener() *MessageListener {
	return &MessageListener{handlers: make(map[string]MessageHandler)}
}

// On registers the sole handler for topic.
func (l *MessageListener) On(topic string, handler MessageHandler) error {
	if topic == "" {
		return fmt.Errorf("arupa: message topic is required")
	}
	if handler == nil {
		return fmt.Errorf("arupa: message handler is nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.handlers[topic]; exists {
		return fmt.Errorf("arupa: message handler for topic %q already exists", topic)
	}
	l.handlers[topic] = handler
	return nil
}

// OnAny registers a fallback for topics without a dedicated handler.
func (l *MessageListener) OnAny(handler MessageHandler) error {
	if handler == nil {
		return fmt.Errorf("arupa: message handler is nil")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.any != nil {
		return fmt.Errorf("arupa: fallback message handler already exists")
	}
	l.any = handler
	return nil
}

// Handle invokes the dedicated topic handler or fallback handler.
func (l *MessageListener) Handle(ctx context.Context, message IncomingMessage) (string, error) {
	if l == nil {
		return "", fmt.Errorf("arupa: message listener is nil")
	}
	l.mu.RLock()
	handler := l.handlers[message.Topic]
	if handler == nil {
		handler = l.any
	}
	l.mu.RUnlock()
	if handler == nil {
		return "", fmt.Errorf("arupa: no message handler for topic %q", message.Topic)
	}
	return handler(ctx, message)
}
