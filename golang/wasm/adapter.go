// Package wasm adapts the framework-neutral Arupa SDK to the generated WASM
// Service v2 protocol.
package wasm

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"
)

// Service implements the four Service v2 callbacks for a WASI module.
//
// Host capabilities become available before OnRegister runs. This lets the
// hook inspect current Params, register transports and routes, and perform any
// other host-backed startup work.
type Service struct {
	Info       arupa.ServiceInfo
	Handler    http.Handler
	Events     *arupa.SocketListener
	Messages   *arupa.ServiceMessageListener
	OnRegister arupa.RegisterHook

	registerMu  sync.Mutex
	registered  bool
	hostFactory func() servicev2.Host
	host        hostState
	initial     arupa.RegisterSnapshot
}

var _ servicev2.Service = (*Service)(nil)
var _ arupa.HostClient = (*Service)(nil)

// Register prepares the Host client and registration snapshot before invoking
// OnRegister, then returns this service's stable identity.
func (s *Service) Register(ctx context.Context, request *servicev2.RegisterRequest) (*servicev2.RegisterReply, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/wasm: service is nil")
	}
	s.registerMu.Lock()
	defer s.registerMu.Unlock()
	if s.registered {
		return nil, fmt.Errorf("arupa/wasm: service is already registered")
	}

	if request == nil {
		return nil, fmt.Errorf("arupa/wasm: register request is nil")
	}
	if err := s.Info.Validate(); err != nil {
		return nil, err
	}

	registerContext, err := registerContextFromProto(request)
	if err != nil {
		return nil, err
	}
	if err := registerContext.Validate(); err != nil {
		return nil, err
	}

	factory := s.hostFactory
	if factory == nil {
		factory = platformHostClient
	}
	nextHost := newHost(factory())
	if err := nextHost.available(); err != nil {
		return nil, err
	}
	s.host.replace(nextHost)

	s.initial.Store(registerContext)
	if s.OnRegister != nil {
		if err := s.OnRegister(callContext(ctx)); err != nil {
			s.host.clear()
			s.initial.Store(arupa.RegisterContext{})
			return nil, fmt.Errorf("arupa/wasm: on register: %w", err)
		}
	}

	s.registered = true
	return serviceInfoReply(s.Info), nil
}

// InitialRegisterContext returns an independent copy of the accepted
// registration request.
func (s *Service) InitialRegisterContext() arupa.RegisterContext {
	if s == nil {
		return arupa.RegisterContext{}
	}
	return s.initial.Load()
}

// InitialParams returns a copy of the Params received during registration.
func (s *Service) InitialParams() map[string]string {
	return arupa.CloneParams(s.InitialRegisterContext().Params)
}

// HandleHTTP adapts a host-forwarded HTTP request to Handler.
func (s *Service) HandleHTTP(ctx context.Context, request *servicev2.HTTPRequest) (*servicev2.HTTPResponse, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/wasm: service is nil")
	}
	return ServeHTTP(ctx, request, s.Handler)
}

// HandleSocketEvent dispatches a host-forwarded Socket.IO event.
func (s *Service) HandleSocketEvent(ctx context.Context, event *servicev2.SocketEvent) (*servicev2.SocketEventReply, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/wasm: service is nil")
	}
	return HandleSocketEvent(ctx, event, s.Events)
}

// HandleServiceMessage dispatches a host-forwarded service message.
func (s *Service) HandleServiceMessage(ctx context.Context, message *servicev2.ServiceMessage) (*servicev2.ServiceMessageReply, error) {
	if s == nil {
		return nil, fmt.Errorf("arupa/wasm: service is nil")
	}
	return HandleServiceMessage(ctx, message, s.Messages)
}

// ServeHTTP converts generated WASM protocol values at the boundary and uses
// the shared framework-neutral HTTP adapter for handler invocation.
func ServeHTTP(ctx context.Context, request *servicev2.HTTPRequest, handler http.Handler) (*servicev2.HTTPResponse, error) {
	if request == nil {
		return nil, fmt.Errorf("arupa/wasm: http request is nil")
	}
	response, err := arupa.ServeHTTP(ctx, requestFromProto(request), handler)
	if err != nil {
		return nil, err
	}
	return responseToProto(response), nil
}

// HandleSocketEvent converts a generated event and dispatches it through the
// shared listener.
func HandleSocketEvent(ctx context.Context, event *servicev2.SocketEvent, listener *arupa.SocketListener) (*servicev2.SocketEventReply, error) {
	if event == nil {
		return nil, fmt.Errorf("arupa/wasm: socket event is nil")
	}
	emits, err := listener.Handle(ctx, socketEventFromProto(event))
	if err != nil {
		return nil, err
	}
	return socketReplyToProto(emits), nil
}

// HandleServiceMessage converts a generated message and dispatches it through
// the shared listener.
func HandleServiceMessage(ctx context.Context, message *servicev2.ServiceMessage, listener *arupa.ServiceMessageListener) (*servicev2.ServiceMessageReply, error) {
	if message == nil {
		return nil, fmt.Errorf("arupa/wasm: service message is nil")
	}
	reply, err := listener.Handle(ctx, serviceMessageFromProto(message))
	if err != nil {
		return &servicev2.ServiceMessageReply{Error: err.Error()}, nil
	}
	return &servicev2.ServiceMessageReply{Message: reply}, nil
}

func serviceInfoReply(info arupa.ServiceInfo) *servicev2.RegisterReply {
	return &servicev2.RegisterReply{Name: info.Name, Version: info.Version}
}

func registerContextFromProto(request *servicev2.RegisterRequest) (arupa.RegisterContext, error) {
	if request == nil {
		return arupa.RegisterContext{}, fmt.Errorf("arupa/wasm: register request is nil")
	}
	listeners := make([]arupa.InheritedListener, 0, len(request.GetListeners()))
	for index, listener := range request.GetListeners() {
		if listener == nil {
			return arupa.RegisterContext{}, fmt.Errorf("arupa/wasm: inherited listener %d is nil", index)
		}
		listeners = append(listeners, arupa.InheritedListener{
			ID:      strings.TrimSpace(listener.GetId()),
			FD:      listener.GetFd(),
			Network: strings.TrimSpace(listener.GetNetwork()),
			Address: listener.GetAddress(),
		})
	}
	return arupa.RegisterContext{
		InstanceID: strings.TrimSpace(request.GetInstanceId()),
		Params:     arupa.CloneParams(request.GetParams()),
		Listeners:  listeners,
	}, nil
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
	out := make(http.Header)
	for _, header := range headers {
		if header == nil || header.GetName() == "" {
			continue
		}
		name := http.CanonicalHeaderKey(header.GetName())
		values := append([]string(nil), header.GetValues()...)
		out[name] = append(out[name], values...)
	}
	return out
}

func headersToProto(headers http.Header) []*servicev2.Header {
	if len(headers) == 0 {
		return nil
	}
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
		reply.Emits = append(reply.Emits, emitToProto(emit))
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

func emitToProto(emit arupa.EmitInstruction) *servicev2.EmitInstruction {
	return &servicev2.EmitInstruction{
		Namespace: emit.Namespace,
		Target:    emit.Target,
		Event:     emit.Event,
		Payload:   append([]byte(nil), emit.Payload...),
	}
}

func accessPolicyToProto(policy arupa.AccessPolicy) *servicev2.AccessPolicy {
	return &servicev2.AccessPolicy{
		RequireAuth: policy.RequireAuth,
		Groups:      append([]string(nil), policy.Groups...),
	}
}
