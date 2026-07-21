package wasm

import (
	"context"
	"fmt"
	"net/http"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	pluginv1 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"
)

// RegistrationReply converts an SDK registration declaration to the generated
// WASM protocol response.
func RegistrationReply(registration arupa.Registration) (*pluginv1.RegisterReply, error) {
	return registrationBinding.RegistrationReply(registration)
}

// ServeHTTP converts generated WASM protocol values at the boundary and uses
// the shared framework-neutral HTTP adapter for all handler invocation.
func ServeHTTP(ctx context.Context, request *pluginv1.HTTPRequest, handler http.Handler) (*pluginv1.HTTPResponse, error) {
	return httpBinding.ServeHTTP(ctx, request, handler)
}

// Plugin is an optional WASM Plugin implementation around a normal
// http.Handler. It adds no framework routing.
type Plugin struct {
	Registration arupa.Registration
	Handler      http.Handler
	Events       *arupa.SocketListener
	Messages     *arupa.MessageListener
	// OnRegister runs after host callbacks and InitialParams are ready. An
	// error returned by the hook rejects registration.
	OnRegister    arupa.RegisterHook
	sender        arupa.MessageSender
	kvClient      arupa.KVClient
	paramsClient  paramsClient
	initialParams arupa.ParamsSnapshot
	logger        arupa.Logger
}

var _ pluginv1.Plugin = (*Plugin)(nil)
var _ arupa.ParamsClient = (*Plugin)(nil)
var _ arupa.Logger = (*Plugin)(nil)

type paramsClient interface {
	GetParams(context.Context) (map[string]string, error)
	PatchParams(context.Context, arupa.ParamsPatch) error
}

func (p *Plugin) Register(ctx context.Context, request *pluginv1.RegisterRequest) (*pluginv1.RegisterReply, error) {
	if request == nil {
		return nil, fmt.Errorf("arupa/wasm: register request is nil")
	}
	reply, err := RegistrationReply(p.Registration)
	if err != nil {
		return nil, err
	}
	p.sender = platformMessageSender()
	p.kvClient = platformKVClient()
	p.paramsClient = platformParamsClient()
	p.logger = platformLogger()
	p.initialParams.Store(request.GetParams())
	if p.OnRegister != nil {
		if err := p.OnRegister(ctx); err != nil {
			return nil, fmt.Errorf("arupa/wasm: on register: %w", err)
		}
	}
	return reply, nil
}

func (p *Plugin) HandleHTTP(ctx context.Context, request *pluginv1.HTTPRequest) (*pluginv1.HTTPResponse, error) {
	return ServeHTTP(ctx, request, p.Handler)
}

var httpBinding = arupa.HTTPBinding[pluginv1.HTTPRequest, pluginv1.HTTPResponse]{
	Request:  requestFromProto,
	Response: responseToProto,
}

var socketBinding = arupa.SocketBinding[pluginv1.SocketEvent, pluginv1.SocketEventReply]{
	Event: socketEventFromProto,
	Reply: socketReplyToProto,
}

// HandleSocketEvent converts generated WASM event values at the boundary and
// dispatches them through the shared event listener registry.
func HandleSocketEvent(ctx context.Context, event *pluginv1.SocketEvent, events *arupa.SocketListener) (*pluginv1.SocketEventReply, error) {
	return socketBinding.HandleSocketEvent(ctx, event, events)
}

var registrationBinding = arupa.RegistrationBinding[pluginv1.HTTPRoute, pluginv1.SocketNamespace, pluginv1.StaticMount, pluginv1.RegisterReply]{
	Route: func(route arupa.HTTPRoute) *pluginv1.HTTPRoute {
		return &pluginv1.HTTPRoute{Method: route.Method, Pattern: route.Pattern, Access: accessPolicy(route.Access)}
	},
	Namespace: func(namespace arupa.SocketNamespace) *pluginv1.SocketNamespace {
		eventAccess := make(map[string]*pluginv1.AccessPolicy, len(namespace.EventAccess))
		for event, policy := range namespace.EventAccess {
			eventAccess[event] = accessPolicy(policy)
		}
		return &pluginv1.SocketNamespace{Name: namespace.Name, Events: append([]string(nil), namespace.Events...), Access: accessPolicy(namespace.Access), EventAccess: eventAccess}
	},
	Mount: func(mount arupa.StaticMount) *pluginv1.StaticMount {
		return &pluginv1.StaticMount{Prefix: mount.Prefix, Directory: mount.Directory, Access: accessPolicy(mount.Access)}
	},
	Reply: func(name, version string, routes []*pluginv1.HTTPRoute, namespaces []*pluginv1.SocketNamespace, mounts []*pluginv1.StaticMount) *pluginv1.RegisterReply {
		return &pluginv1.RegisterReply{Name: name, Version: version, HttpRoutes: routes, SocketNamespaces: namespaces, StaticMounts: mounts}
	},
}

// HandleSocketEvent dispatches a host-forwarded event to registered listeners.
func (p *Plugin) HandleSocketEvent(ctx context.Context, event *pluginv1.SocketEvent) (*pluginv1.SocketEventReply, error) {
	return HandleSocketEvent(ctx, event, p.Events)
}

// HandlePluginMessage dispatches a host-forwarded plugin message to the
// registered message listener.
func (p *Plugin) HandlePluginMessage(ctx context.Context, message *pluginv1.PluginMessage) (*pluginv1.PluginMessageReply, error) {
	return HandlePluginMessage(ctx, message, p.Messages)
}

// SendMessage sends a request/reply message to another registered plugin.
func (p *Plugin) SendMessage(ctx context.Context, message arupa.OutgoingMessage) (string, error) {
	if p.sender == nil {
		return "", fmt.Errorf("arupa/wasm: host messaging is unavailable before registration")
	}
	return p.sender.SendMessage(ctx, message)
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
	if err := validateKVRequest(namespace, key); err != nil {
		return nil, false, err
	}
	if p == nil || p.kvClient == nil {
		return nil, false, fmt.Errorf("arupa/wasm: host KV is unavailable before registration")
	}
	return p.kvClient.KVGet(ctx, namespace, key)
}

// KVSet writes a value to a non-empty host KV namespace.
func (p *Plugin) KVSet(ctx context.Context, namespace, key string, value []byte) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if p == nil || p.kvClient == nil {
		return fmt.Errorf("arupa/wasm: host KV is unavailable before registration")
	}
	return p.kvClient.KVSet(ctx, namespace, key, value)
}

// KVDelete removes a key from a non-empty host KV namespace.
func (p *Plugin) KVDelete(ctx context.Context, namespace, key string) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if p == nil || p.kvClient == nil {
		return fmt.Errorf("arupa/wasm: host KV is unavailable before registration")
	}
	return p.kvClient.KVDelete(ctx, namespace, key)
}

// KVList returns the keys in a non-empty host KV namespace.
func (p *Plugin) KVList(ctx context.Context, namespace string) ([]string, error) {
	if namespace == "" {
		return nil, fmt.Errorf("arupa: kv namespace is required")
	}
	if p == nil || p.kvClient == nil {
		return nil, fmt.Errorf("arupa/wasm: host KV is unavailable before registration")
	}
	return p.kvClient.KVList(ctx, namespace)
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
	if p == nil || p.paramsClient == nil {
		return nil, fmt.Errorf("arupa/wasm: host Params are unavailable before registration")
	}
	return p.paramsClient.GetParams(ctx)
}

// PatchParams applies a partial update to this plugin's persisted Params.
func (p *Plugin) PatchParams(ctx context.Context, patch arupa.ParamsPatch) error {
	if p == nil || p.paramsClient == nil {
		return fmt.Errorf("arupa/wasm: host Params are unavailable before registration")
	}
	return p.paramsClient.PatchParams(ctx, patch)
}

// Log writes a host-owned plugin log record at level.
func (p *Plugin) Log(ctx context.Context, level arupa.LogLevel, message string) error {
	if p == nil || p.logger == nil {
		return fmt.Errorf("arupa/wasm: host logging is unavailable before registration")
	}
	level, err := arupa.NormalizeLogLevel(level)
	if err != nil {
		return err
	}
	return p.logger.Log(ctx, level, message)
}

// Debug writes a debug-level plugin log record.
func (p *Plugin) Debug(ctx context.Context, message string) error {
	return p.Log(ctx, arupa.LogDebug, message)
}

// Info writes an info-level plugin log record.
func (p *Plugin) Info(ctx context.Context, message string) error {
	return p.Log(ctx, arupa.LogInfo, message)
}

// Warn writes a warning-level plugin log record.
func (p *Plugin) Warn(ctx context.Context, message string) error {
	return p.Log(ctx, arupa.LogWarn, message)
}

// Error writes an error-level plugin log record.
func (p *Plugin) Error(ctx context.Context, message string) error {
	return p.Log(ctx, arupa.LogError, message)
}

func requestFromProto(request *pluginv1.HTTPRequest) arupa.HTTPRequest {
	if request == nil {
		return arupa.HTTPRequest{}
	}
	headers := make(http.Header, len(request.GetHeaders()))
	for key, value := range request.GetHeaders() {
		headers.Set(key, value)
	}
	return arupa.HTTPRequest{
		Method:     request.GetMethod(),
		Path:       request.GetPath(),
		Query:      request.GetQuery(),
		Headers:    headers,
		Body:       request.GetBody(),
		RemoteAddr: request.GetRemoteAddr(),
	}
}

func responseToProto(response arupa.HTTPResponse) *pluginv1.HTTPResponse {
	headers := make(map[string]string, len(response.Headers))
	for key, values := range response.Headers {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return &pluginv1.HTTPResponse{Status: int32(response.Status), Headers: headers, Body: response.Body}
}

func socketEventFromProto(event *pluginv1.SocketEvent) arupa.SocketEvent {
	var user *arupa.User
	if source := event.GetUser(); source != nil {
		user = &arupa.User{Username: source.GetUsername(), Groups: append([]string(nil), source.GetGroups()...)}
	}
	return arupa.SocketEvent{
		Namespace: event.GetNamespace(),
		Event:     event.GetEvent(),
		SocketID:  event.GetSocketId(),
		User:      user,
		Payload:   append([]byte(nil), event.GetPayload()...),
	}
}

func socketReplyToProto(emits []arupa.EmitInstruction) *pluginv1.SocketEventReply {
	reply := &pluginv1.SocketEventReply{Emits: make([]*pluginv1.EmitInstruction, 0, len(emits))}
	for _, emit := range emits {
		reply.Emits = append(reply.Emits, &pluginv1.EmitInstruction{
			Namespace: emit.Namespace,
			Target:    emit.Target,
			Event:     emit.Event,
			Payload:   append([]byte(nil), emit.Payload...),
		})
	}
	return reply
}

var messageBinding = arupa.MessageBinding[pluginv1.PluginMessage, pluginv1.PluginMessageReply]{
	Message: messageFromProto,
	Reply:   messageReplyToProto,
}

// HandlePluginMessage converts generated WASM values at the boundary and
// dispatches them through the shared message listener.
func HandlePluginMessage(ctx context.Context, message *pluginv1.PluginMessage, listener *arupa.MessageListener) (*pluginv1.PluginMessageReply, error) {
	return messageBinding.HandlePluginMessage(ctx, message, listener)
}

func messageFromProto(message *pluginv1.PluginMessage) arupa.IncomingMessage {
	return arupa.IncomingMessage{
		Source:  message.GetSource(),
		Topic:   message.GetTopic(),
		Payload: append([]byte(nil), message.GetPayload()...),
	}
}

func messageReplyToProto(reply string) *pluginv1.PluginMessageReply {
	return &pluginv1.PluginMessageReply{Message: reply}
}

func accessPolicy(policy arupa.AccessPolicy) *pluginv1.AccessPolicy {
	return &pluginv1.AccessPolicy{RequireAuth: policy.RequireAuth, Groups: append([]string(nil), policy.Groups...)}
}
