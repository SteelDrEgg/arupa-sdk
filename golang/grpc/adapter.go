// Package grpc adapts the framework-neutral Arupa SDK to the Service v2 gRPC
// protocol.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/grpc"
	hcplugin "github.com/hashicorp/go-plugin"
)

// RegisterReply validates an SDK service identity and converts it to the
// generated protocol reply.
func RegisterReply(info arupa.ServiceInfo) (*servicev2.RegisterReply, error) {
	return serviceInfoBinding.RegisterReply(info)
}

// ServeHTTP converts a generated request, invokes a standard Go HTTP handler,
// and preserves all response header values.
func ServeHTTP(ctx context.Context, request *servicev2.HTTPRequest, handler http.Handler) (*servicev2.HTTPResponse, error) {
	return httpBinding.ServeHTTP(ctx, request, handler)
}

// Service is the default Service v2 gRPC server backed by the shared SDK
// adapters.
type Service struct {
	servicev2.UnimplementedServiceServer

	Info       arupa.ServiceInfo
	Handler    http.Handler
	Events     *arupa.SocketListener
	Messages   *arupa.ServiceMessageListener
	OnRegister arupa.RegisterHook

	registerMu sync.Mutex
	registered bool
	closed     bool
	host       hostState
	listeners  listenerSet
	initial    arupa.RegisterSnapshot
}

var _ servicev2.ServiceServer = (*Service)(nil)
var _ BrokerReceiver = (*Service)(nil)
var _ arupa.HostClient = (*Service)(nil)

// SetGRPCBroker implements BrokerReceiver. ServeServer calls it before the
// generated gRPC server can receive Register.
func (s *Service) SetGRPCBroker(broker *hcplugin.GRPCBroker) {
	if s != nil {
		s.host.setBroker(broker)
	}
}

// Register establishes the caller-scoped Host broker connection, adopts
// inherited listeners, publishes the immutable startup snapshot, then invokes
// OnRegister.
func (s *Service) Register(ctx context.Context, request *servicev2.RegisterRequest) (*servicev2.RegisterReply, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/grpc: service is nil")
	}
	s.registerMu.Lock()
	defer s.registerMu.Unlock()
	if s.closed {
		return nil, fmt.Errorf("arupa/grpc: service is closed")
	}
	if s.registered {
		return nil, fmt.Errorf("arupa/grpc: service is already registered")
	}

	reply, err := RegisterReply(s.Info)
	if err != nil {
		return nil, err
	}
	registerContext, err := registerContextFromProto(request)
	if err != nil {
		return nil, err
	}
	nextListeners, err := buildListenerMap(registerContext.Listeners)
	if err != nil {
		return nil, err
	}

	if err := s.host.connect(ctx, request.GetHostBrokerId()); err != nil {
		_ = closeListenerMap(nextListeners)
		return nil, err
	}
	if err := s.listeners.replace(nextListeners); err != nil {
		_ = s.host.clear()
		return nil, err
	}
	s.initial.Store(registerContext)

	if s.OnRegister != nil {
		if err := s.OnRegister(nonNilContext(ctx)); err != nil {
			s.initial.Store(arupa.RegisterContext{})
			cleanupErr := errors.Join(s.listeners.clear(), s.host.clear())
			return nil, errors.Join(
				fmt.Errorf("arupa/grpc: on register: %w", err),
				cleanupErr,
			)
		}
	}
	s.registered = true
	return reply, nil
}

func (s *Service) HandleHTTP(ctx context.Context, request *servicev2.HTTPRequest) (*servicev2.HTTPResponse, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/grpc: service is nil")
	}
	return ServeHTTP(ctx, request, s.Handler)
}

func (s *Service) HandleSocketEvent(ctx context.Context, event *servicev2.SocketEvent) (*servicev2.SocketEventReply, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/grpc: service is nil")
	}
	return HandleSocketEvent(ctx, event, s.Events)
}

func (s *Service) HandleServiceMessage(ctx context.Context, message *servicev2.ServiceMessage) (*servicev2.ServiceMessageReply, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/grpc: service is nil")
	}
	return HandleServiceMessage(ctx, message, s.Messages)
}

// InitialRegisterContext returns an independent copy of the accepted startup
// context. The gRPC-only Host broker ID is intentionally not included.
func (s *Service) InitialRegisterContext() arupa.RegisterContext {
	if s == nil {
		return arupa.RegisterContext{Params: map[string]string{}}
	}
	return s.initial.Load()
}

// InitialParams returns the Params received by the accepted Register call.
func (s *Service) InitialParams() map[string]string {
	return s.InitialRegisterContext().Params
}

// InheritedListener returns the SDK-owned listener with id. Closing the
// returned value is safe and idempotent.
func (s *Service) InheritedListener(id string) (*InheritedListener, bool) {
	if s == nil {
		return nil, false
	}
	return s.listeners.get(id)
}

// InheritedListeners returns Host-provided listener metadata in ID order.
func (s *Service) InheritedListeners() []arupa.InheritedListener {
	if s == nil {
		return nil
	}
	return s.listeners.descriptors()
}

// Host returns the complete Host capability set after successful
// registration.
func (s *Service) Host() arupa.HostClient {
	current := s.currentHost()
	if current == nil {
		return nil
	}
	return current
}

func (s *Service) currentHost() *host {
	if s == nil {
		return nil
	}
	return s.host.current()
}

func (s *Service) Emit(ctx context.Context, instruction arupa.EmitInstruction) error {
	return s.currentHost().Emit(ctx, instruction)
}

func (s *Service) EmitJSON(ctx context.Context, namespace, target, event string, args ...any) error {
	instruction, err := arupa.NewEmitJSON(namespace, target, event, args...)
	if err != nil {
		return err
	}
	return s.Emit(ctx, instruction)
}

func (s *Service) SendServiceMessage(ctx context.Context, message arupa.OutgoingServiceMessage) (string, error) {
	return s.currentHost().SendServiceMessage(ctx, message)
}

func (s *Service) SendServiceJSON(ctx context.Context, target, topic string, payload any) (string, error) {
	return arupa.SendServiceJSON(ctx, s, target, topic, payload)
}

func (s *Service) KV() arupa.KVStore {
	if s == nil {
		return arupa.NewKVStore(nil, "")
	}
	return arupa.NewKVStore(s, s.Info.Name)
}

func (s *Service) KVGet(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	return s.currentHost().KVGet(ctx, namespace, key)
}

func (s *Service) KVSet(ctx context.Context, namespace, key string, value []byte) error {
	return s.currentHost().KVSet(ctx, namespace, key, value)
}

func (s *Service) KVDelete(ctx context.Context, namespace, key string) error {
	return s.currentHost().KVDelete(ctx, namespace, key)
}

func (s *Service) KVList(ctx context.Context, namespace string) ([]string, error) {
	return s.currentHost().KVList(ctx, namespace)
}

func (s *Service) Params(ctx context.Context) (map[string]string, error) {
	return s.currentHost().Params(ctx)
}

func (s *Service) PatchParams(ctx context.Context, patch arupa.ParamsPatch) error {
	return s.currentHost().PatchParams(ctx, patch)
}

func (s *Service) RegisterTransport(ctx context.Context, transport arupa.Transport) (arupa.RegistrationResult, error) {
	return s.currentHost().RegisterTransport(ctx, transport)
}

func (s *Service) UnregisterTransport(ctx context.Context, id string) (arupa.RegistrationResult, error) {
	return s.currentHost().UnregisterTransport(ctx, id)
}

func (s *Service) RegisterRoutes(ctx context.Context, routes []arupa.Route) (arupa.RegistrationResult, error) {
	return s.currentHost().RegisterRoutes(ctx, routes)
}

func (s *Service) UnregisterRoutes(ctx context.Context, ids []string) (arupa.RegistrationResult, error) {
	return s.currentHost().UnregisterRoutes(ctx, ids)
}

func (s *Service) Log(ctx context.Context, level arupa.LogLevel, message string) error {
	return s.currentHost().Log(ctx, level, message)
}

func (s *Service) LogDebug(ctx context.Context, message string) error {
	return s.Log(ctx, arupa.LogDebug, message)
}

func (s *Service) LogInfo(ctx context.Context, message string) error {
	return s.Log(ctx, arupa.LogInfo, message)
}

func (s *Service) LogWarn(ctx context.Context, message string) error {
	return s.Log(ctx, arupa.LogWarn, message)
}

func (s *Service) LogError(ctx context.Context, message string) error {
	return s.Log(ctx, arupa.LogError, message)
}

// Close releases the Host broker connection and all inherited listeners. It
// is safe to call concurrently and repeatedly.
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.registerMu.Lock()
	defer s.registerMu.Unlock()
	s.closed = true
	s.registered = false
	s.initial.Store(arupa.RegisterContext{})
	return errors.Join(s.listeners.close(), s.host.close())
}

var serviceInfoBinding = arupa.ServiceInfoBinding[servicev2.RegisterReply]{
	Reply: func(info arupa.ServiceInfo) *servicev2.RegisterReply {
		return &servicev2.RegisterReply{Name: info.Name, Version: info.Version}
	},
}

var httpBinding = arupa.HTTPBinding[servicev2.HTTPRequest, servicev2.HTTPResponse]{
	Request:  requestFromProto,
	Response: responseToProto,
}

var socketBinding = arupa.SocketBinding[servicev2.SocketEvent, servicev2.SocketEventReply]{
	Event: socketEventFromProto,
	Reply: socketReplyToProto,
}

var serviceMessageBinding = arupa.ServiceMessageBinding[servicev2.ServiceMessage, servicev2.ServiceMessageReply]{
	Message: serviceMessageFromProto,
	Reply: func(reply string) *servicev2.ServiceMessageReply {
		return &servicev2.ServiceMessageReply{Message: reply}
	},
	Error: func(err error) *servicev2.ServiceMessageReply {
		return &servicev2.ServiceMessageReply{Error: err.Error()}
	},
}

func HandleSocketEvent(ctx context.Context, event *servicev2.SocketEvent, events *arupa.SocketListener) (*servicev2.SocketEventReply, error) {
	return socketBinding.HandleSocketEvent(ctx, event, events)
}

func HandleServiceMessage(ctx context.Context, message *servicev2.ServiceMessage, listener *arupa.ServiceMessageListener) (*servicev2.ServiceMessageReply, error) {
	return serviceMessageBinding.HandleServiceMessage(ctx, message, listener)
}

func requestFromProto(request *servicev2.HTTPRequest) arupa.HTTPRequest {
	if request == nil {
		return arupa.HTTPRequest{}
	}
	return arupa.HTTPRequest{
		RouteID:      request.GetRouteId(),
		RoutePattern: request.GetRoutePattern(),
		Method:       request.GetMethod(),
		Path:         request.GetPath(),
		Query:        request.GetQuery(),
		Headers:      headersFromProto(request.GetHeaders()),
		Body:         append([]byte(nil), request.GetBody()...),
		RemoteAddr:   request.GetRemoteAddr(),
		User:         userFromProto(request.GetUser()),
	}
}

func responseToProto(response arupa.HTTPResponse) *servicev2.HTTPResponse {
	return &servicev2.HTTPResponse{
		Status:  int32(response.Status),
		Headers: headersToProto(response.Headers),
		Body:    append([]byte(nil), response.Body...),
	}
}

func headersFromProto(headers []*servicev2.Header) http.Header {
	out := make(http.Header, len(headers))
	for _, header := range headers {
		if header == nil || strings.TrimSpace(header.GetName()) == "" {
			continue
		}
		for _, value := range header.GetValues() {
			out.Add(header.GetName(), value)
		}
	}
	return out
}

func headersToProto(headers http.Header) []*servicev2.Header {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*servicev2.Header, 0, len(names))
	for _, name := range names {
		out = append(out, &servicev2.Header{
			Name:   name,
			Values: append([]string(nil), headers[name]...),
		})
	}
	return out
}

func userFromProto(source *servicev2.User) *arupa.User {
	if source == nil {
		return nil
	}
	return &arupa.User{
		Username: source.GetUsername(),
		Groups:   append([]string(nil), source.GetGroups()...),
	}
}

func socketEventFromProto(event *servicev2.SocketEvent) arupa.SocketEvent {
	if event == nil {
		return arupa.SocketEvent{}
	}
	return arupa.SocketEvent{
		RouteID:   event.GetRouteId(),
		Namespace: event.GetNamespace(),
		Event:     event.GetEvent(),
		SocketID:  event.GetSocketId(),
		User:      userFromProto(event.GetUser()),
		Payload:   append([]byte(nil), event.GetPayload()...),
	}
}

func socketReplyToProto(emits []arupa.EmitInstruction) *servicev2.SocketEventReply {
	reply := &servicev2.SocketEventReply{
		Emits: make([]*servicev2.EmitInstruction, 0, len(emits)),
	}
	for _, emit := range emits {
		reply.Emits = append(reply.Emits, &servicev2.EmitInstruction{
			Namespace: emit.Namespace,
			Target:    emit.Target,
			Event:     emit.Event,
			Payload:   append([]byte(nil), emit.Payload...),
		})
	}
	return reply
}

func serviceMessageFromProto(message *servicev2.ServiceMessage) arupa.IncomingServiceMessage {
	if message == nil {
		return arupa.IncomingServiceMessage{}
	}
	return arupa.IncomingServiceMessage{
		Source:  message.GetSource(),
		Target:  message.GetTarget(),
		Topic:   message.GetTopic(),
		Payload: append([]byte(nil), message.GetPayload()...),
	}
}

func registerContextFromProto(request *servicev2.RegisterRequest) (arupa.RegisterContext, error) {
	if request == nil {
		return arupa.RegisterContext{}, fmt.Errorf("arupa/grpc: register request is nil")
	}
	instanceID := strings.TrimSpace(request.GetInstanceId())
	if instanceID == "" {
		return arupa.RegisterContext{}, fmt.Errorf("arupa/grpc: register instance id is required")
	}
	registerContext := arupa.RegisterContext{
		InstanceID: instanceID,
		Params:     arupa.CloneParams(request.GetParams()),
		Listeners:  make([]arupa.InheritedListener, 0, len(request.GetListeners())),
	}
	for index, listener := range request.GetListeners() {
		if listener == nil {
			return arupa.RegisterContext{}, fmt.Errorf("arupa/grpc: inherited listener %d is nil", index)
		}
		converted := arupa.InheritedListener{
			ID:      strings.TrimSpace(listener.GetId()),
			FD:      listener.GetFd(),
			Network: strings.TrimSpace(listener.GetNetwork()),
			Address: listener.GetAddress(),
		}
		registerContext.Listeners = append(registerContext.Listeners, converted)
	}
	if err := registerContext.Validate(); err != nil {
		return arupa.RegisterContext{}, err
	}
	return registerContext, nil
}
