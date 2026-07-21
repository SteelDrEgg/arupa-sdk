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

type hostClient interface {
	KVGet(context.Context, *pluginv1.KVGetRequest, ...googlegrpc.CallOption) (*pluginv1.KVGetReply, error)
	KVSet(context.Context, *pluginv1.KVSetRequest, ...googlegrpc.CallOption) (*pluginv1.KVSetReply, error)
	KVDelete(context.Context, *pluginv1.KVDeleteRequest, ...googlegrpc.CallOption) (*pluginv1.KVDeleteReply, error)
	KVList(context.Context, *pluginv1.KVListRequest, ...googlegrpc.CallOption) (*pluginv1.KVListReply, error)
	GetParams(context.Context, *pluginv1.ParamsGetRequest, ...googlegrpc.CallOption) (*pluginv1.ParamsGetReply, error)
	PatchParams(context.Context, *pluginv1.ParamsPatchRequest, ...googlegrpc.CallOption) (*pluginv1.ParamsPatchReply, error)
	Emit(context.Context, *pluginv1.EmitInstruction, ...googlegrpc.CallOption) (*pluginv1.EmitReply, error)
	SendPluginMessage(context.Context, *pluginv1.PluginMessage, ...googlegrpc.CallOption) (*pluginv1.PluginMessageReply, error)
}

// host is the gRPC-only callback bridge for host operations that a plugin can
// invoke after registration, including background Socket.IO emits.
type host struct {
	client hostClient
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

func (h *host) kvGet(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	if err := validateKVRequest(namespace, key); err != nil {
		return nil, false, err
	}
	if h == nil || h.client == nil {
		return nil, false, fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.KVGet(ctx, &pluginv1.KVGetRequest{Namespace: namespace, Key: key})
	if err != nil {
		return nil, false, fmt.Errorf("arupa/grpc: host kv get: %w", err)
	}
	return append([]byte(nil), reply.GetValue()...), reply.GetFound(), nil
}

func (h *host) kvSet(ctx context.Context, namespace, key string, value []byte) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if h == nil || h.client == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.KVSet(ctx, &pluginv1.KVSetRequest{Namespace: namespace, Key: key, Value: append([]byte(nil), value...)})
	if err != nil {
		return fmt.Errorf("arupa/grpc: host kv set: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/grpc: host kv set: %s", message)
	}
	return nil
}

func (h *host) kvDelete(ctx context.Context, namespace, key string) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if h == nil || h.client == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.KVDelete(ctx, &pluginv1.KVDeleteRequest{Namespace: namespace, Key: key})
	if err != nil {
		return fmt.Errorf("arupa/grpc: host kv delete: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/grpc: host kv delete: %s", message)
	}
	return nil
}

func (h *host) kvList(ctx context.Context, namespace string) ([]string, error) {
	if namespace == "" {
		return nil, fmt.Errorf("arupa: kv namespace is required")
	}
	if h == nil || h.client == nil {
		return nil, fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.KVList(ctx, &pluginv1.KVListRequest{Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: host kv list: %w", err)
	}
	return append([]string(nil), reply.GetKeys()...), nil
}

func (h *host) getParams(ctx context.Context) (map[string]string, error) {
	if h == nil || h.client == nil {
		return nil, fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.GetParams(ctx, &pluginv1.ParamsGetRequest{})
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: host get params: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return nil, fmt.Errorf("arupa/grpc: host get params: %s", message)
	}
	return arupa.CloneParams(reply.GetParams()), nil
}

func (h *host) patchParams(ctx context.Context, patch arupa.ParamsPatch) error {
	if h == nil || h.client == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = metadata.AppendToOutgoingContext(ctx, hostCallbackTokenMetadata, h.token)
	reply, err := h.client.PatchParams(ctx, &pluginv1.ParamsPatchRequest{
		Set:    arupa.CloneParams(patch.Set),
		Delete: append([]string(nil), patch.Delete...),
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: host patch params: %w", err)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/grpc: host patch params: %s", message)
	}
	return nil
}

func validateKVRequest(namespace, key string) error {
	if namespace == "" {
		return fmt.Errorf("arupa: kv namespace is required")
	}
	if key == "" {
		return fmt.Errorf("arupa: kv key is required")
	}
	return nil
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

func (p *Plugin) configureHost(ctx context.Context, request *pluginv1.RegisterRequest) error {
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
func (p *Plugin) Emit(ctx context.Context, instruction arupa.EmitInstruction) error {
	return p.host.current().emit(ctx, instruction)
}

// EmitJSON encodes args as Socket.IO event arguments and sends them through
// the host callback.
func (p *Plugin) EmitJSON(ctx context.Context, namespace, target, event string, args ...any) error {
	instruction, err := arupa.NewEmitJSON(namespace, target, event, args...)
	if err != nil {
		return err
	}
	return p.Emit(ctx, instruction)
}

// SendMessage sends a request/reply message to another registered plugin.
func (p *Plugin) SendMessage(ctx context.Context, message arupa.OutgoingMessage) (string, error) {
	return p.host.current().sendMessage(ctx, message)
}

// SendJSON encodes payload as JSON, then delegates to SendMessage.
func (p *Plugin) SendJSON(ctx context.Context, target, topic string, payload any) (string, error) {
	return arupa.SendJSON(ctx, p, target, topic, payload)
}

// KV returns a KV store scoped to this plugin's registered name.
func (p *Plugin) KV() arupa.KVStore {
	if p == nil {
		return arupa.NewKVStore(nil, "")
	}
	return arupa.NewKVStore(p, p.Registration.Name)
}

// KVGet reads a value from a non-empty host KV namespace.
func (p *Plugin) KVGet(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	if p == nil {
		return nil, false, fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	return p.host.current().kvGet(ctx, namespace, key)
}

// KVSet writes a value to a non-empty host KV namespace.
func (p *Plugin) KVSet(ctx context.Context, namespace, key string, value []byte) error {
	if p == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	return p.host.current().kvSet(ctx, namespace, key, value)
}

// KVDelete removes a key from a non-empty host KV namespace.
func (p *Plugin) KVDelete(ctx context.Context, namespace, key string) error {
	if p == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	return p.host.current().kvDelete(ctx, namespace, key)
}

// KVList returns the keys in a non-empty host KV namespace.
func (p *Plugin) KVList(ctx context.Context, namespace string) ([]string, error) {
	if p == nil {
		return nil, fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	return p.host.current().kvList(ctx, namespace)
}

// InitialParams returns the Params received during the most recent Register.
func (p *Plugin) InitialParams() map[string]string {
	if p == nil {
		return map[string]string{}
	}
	return p.initialParams.Load()
}

// Params reads the current effective Params from the host.
func (p *Plugin) Params(ctx context.Context) (map[string]string, error) {
	if p == nil {
		return nil, fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	return p.host.current().getParams(ctx)
}

// PatchParams applies a partial update to this plugin's persisted Params.
func (p *Plugin) PatchParams(ctx context.Context, patch arupa.ParamsPatch) error {
	if p == nil {
		return fmt.Errorf("arupa/grpc: host callback is unavailable before successful registration")
	}
	return p.host.current().patchParams(ctx, patch)
}

// Close releases the gRPC host callback connection.
func (p *Plugin) Close() error {
	return p.host.close()
}
