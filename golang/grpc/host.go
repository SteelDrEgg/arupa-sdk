package grpc

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/grpc"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const hostCallbackTokenMetadata = "x-panel-token"

type emitClient interface {
	Emit(context.Context, *pluginv1.EmitInstruction, ...googlegrpc.CallOption) (*pluginv1.EmitReply, error)
	SendPluginMessage(context.Context, *pluginv1.PluginMessage, ...googlegrpc.CallOption) (*pluginv1.PluginMessageReply, error)
}

// host is the gRPC-only callback bridge for host operations that a plugin can
// invoke after registration, including background Socket.IO emits.
type host struct {
	client emitClient
	closer io.Closer
	token  string
}

func newHost(ctx context.Context, address, token string) (*host, error) {
	if address == "" {
		return nil, fmt.Errorf("arupa/grpc: host callback address is required")
	}
	if token == "" {
		return nil, fmt.Errorf("arupa/grpc: host callback token is required")
	}
	conn, err := googlegrpc.DialContext(ctx, address, googlegrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: dial host callback: %w", err)
	}
	return &host{client: pluginv1.NewHostClient(conn), closer: conn, token: token}, nil
}

func (h *host) emit(ctx context.Context, instruction arupa.EmitInstruction) error {
	if h == nil || h.client == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.Emit(ctx, &pluginv1.EmitInstruction{
		Namespace: instruction.Namespace,
		Target:    instruction.Target,
		Event:     instruction.Event,
		Payload:   append([]byte(nil), instruction.Payload...),
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: host emit: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/grpc: host emit: %s", message)
	}
	return nil
}

func (h *host) sendMessage(ctx context.Context, message arupa.OutgoingMessage) (string, error) {
	if h == nil || h.client == nil {
		return "", fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if err := message.Validate(); err != nil {
		return "", err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.SendPluginMessage(ctx, &pluginv1.PluginMessage{
		Target:  message.Target,
		Topic:   message.Topic,
		Payload: append([]byte(nil), message.Payload...),
	})
	if err != nil {
		return "", fmt.Errorf("arupa/grpc: send plugin message: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return "", fmt.Errorf("arupa/grpc: send plugin message: %s", message)
	}
	return reply.GetMessage(), nil
}

func (h *host) close() error {
	if h == nil || h.closer == nil {
		return nil
	}
	return h.closer.Close()
}

type hostState struct {
	mu   sync.RWMutex
	host *host
}

func (s *hostState) replace(next *host) {
	s.mu.Lock()
	previous := s.host
	s.host = next
	s.mu.Unlock()
	_ = previous.close()
}

func (s *hostState) current() *host {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.host
}

func (s *hostState) close() error {
	s.mu.Lock()
	current := s.host
	s.host = nil
	s.mu.Unlock()
	return current.close()
}

func (p *HTTPPlugin) configureHost(ctx context.Context, request *pluginv1.RegisterRequest) error {
	if request == nil {
		return fmt.Errorf("arupa/grpc: register request is nil")
	}
	address := request.GetHostCallbackAddr()
	token := request.GetHostCallbackToken()
	if address == "" && token == "" {
		p.host.replace(nil)
		return nil
	}
	next, err := newHost(ctx, address, token)
	if err != nil {
		return err
	}
	p.host.replace(next)
	return nil
}

// Emit sends an instruction through the host callback. It is available after
// Register has completed successfully and can be called from background work.
func (p *HTTPPlugin) Emit(ctx context.Context, instruction arupa.EmitInstruction) error {
	return p.host.current().emit(ctx, instruction)
}

// EmitJSON encodes args as Socket.IO event arguments and sends them through
// the host callback.
func (p *HTTPPlugin) EmitJSON(ctx context.Context, namespace, target, event string, args ...any) error {
	instruction, err := arupa.NewEmitJSON(namespace, target, event, args...)
	if err != nil {
		return err
	}
	return p.Emit(ctx, instruction)
}

// SendMessage sends a request/reply message to another registered plugin.
func (p *HTTPPlugin) SendMessage(ctx context.Context, message arupa.OutgoingMessage) (string, error) {
	return p.host.current().sendMessage(ctx, message)
}

// SendJSON encodes payload as JSON, then delegates to SendMessage.
func (p *HTTPPlugin) SendJSON(ctx context.Context, target, topic string, payload any) (string, error) {
	return arupa.SendJSON(ctx, p, target, topic, payload)
}

// Close releases the gRPC host callback connection.
func (p *HTTPPlugin) Close() error {
	return p.host.close()
}
