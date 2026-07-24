package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/grpc"
	hcplugin "github.com/hashicorp/go-plugin"
	googlegrpc "google.golang.org/grpc"
)

const hostUnavailable = "arupa/grpc: Host is unavailable before successful registration"

type hostRPCClient interface {
	KVGet(context.Context, *servicev2.KVGetRequest, ...googlegrpc.CallOption) (*servicev2.KVGetReply, error)
	KVSet(context.Context, *servicev2.KVSetRequest, ...googlegrpc.CallOption) (*servicev2.KVSetReply, error)
	KVDelete(context.Context, *servicev2.KVDeleteRequest, ...googlegrpc.CallOption) (*servicev2.KVDeleteReply, error)
	KVList(context.Context, *servicev2.KVListRequest, ...googlegrpc.CallOption) (*servicev2.KVListReply, error)
	GetParams(context.Context, *servicev2.ParamsGetRequest, ...googlegrpc.CallOption) (*servicev2.ParamsGetReply, error)
	PatchParams(context.Context, *servicev2.ParamsPatchRequest, ...googlegrpc.CallOption) (*servicev2.ParamsPatchReply, error)
	Emit(context.Context, *servicev2.EmitInstruction, ...googlegrpc.CallOption) (*servicev2.EmitReply, error)
	SendServiceMessage(context.Context, *servicev2.ServiceMessage, ...googlegrpc.CallOption) (*servicev2.ServiceMessageReply, error)
	RegisterTransport(context.Context, *servicev2.RegisterTransportRequest, ...googlegrpc.CallOption) (*servicev2.RegistrationReply, error)
	UnregisterTransport(context.Context, *servicev2.UnregisterTransportRequest, ...googlegrpc.CallOption) (*servicev2.RegistrationReply, error)
	RegisterRoutes(context.Context, *servicev2.RegisterRoutesRequest, ...googlegrpc.CallOption) (*servicev2.RegistrationReply, error)
	UnregisterRoutes(context.Context, *servicev2.UnregisterRoutesRequest, ...googlegrpc.CallOption) (*servicev2.RegistrationReply, error)
	Log(context.Context, *servicev2.LogRequest, ...googlegrpc.CallOption) (*servicev2.LogReply, error)
}

// host is the gRPC bridge for all Service v2 Host capabilities.
type host struct {
	client hostRPCClient
	closer io.Closer

	closeOnce sync.Once
	closeErr  error
}

var _ arupa.HostClient = (*host)(nil)

func newBrokerHost(ctx context.Context, broker *hcplugin.GRPCBroker, brokerID uint32) (*host, error) {
	if broker == nil {
		return nil, fmt.Errorf("arupa/grpc: gRPC broker is unavailable")
	}
	if brokerID == 0 {
		return nil, fmt.Errorf("arupa/grpc: Host broker id is required")
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}

	conn, err := dialBrokerWithContext(ctx, func() (*googlegrpc.ClientConn, error) {
		return broker.Dial(brokerID)
	})
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: dial Host broker %d: %w", brokerID, err)
	}
	if conn == nil {
		return nil, fmt.Errorf("arupa/grpc: dial Host broker %d returned a nil connection", brokerID)
	}
	return &host{client: servicev2.NewHostClient(conn), closer: conn}, nil
}

type brokerDialResult struct {
	connection *googlegrpc.ClientConn
	err        error
}

func dialBrokerWithContext(
	ctx context.Context,
	dial func() (*googlegrpc.ClientConn, error),
) (*googlegrpc.ClientConn, error) {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dial == nil {
		return nil, fmt.Errorf("gRPC broker dialer is nil")
	}

	result := make(chan brokerDialResult, 1)
	go func() {
		connection, err := dial()
		if ctx.Err() != nil {
			if connection != nil {
				_ = connection.Close()
			}
			return
		}
		select {
		case result <- brokerDialResult{connection: connection, err: err}:
		case <-ctx.Done():
			if connection != nil {
				_ = connection.Close()
			}
		}
	}()

	select {
	case result := <-result:
		if err := ctx.Err(); err != nil {
			if result.connection != nil {
				_ = result.connection.Close()
			}
			return nil, err
		}
		return result.connection, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (h *host) KVGet(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	if err := validateKVRequest(namespace, key); err != nil {
		return nil, false, err
	}
	if err := h.available(); err != nil {
		return nil, false, err
	}
	reply, err := h.client.KVGet(nonNilContext(ctx), &servicev2.KVGetRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return nil, false, fmt.Errorf("arupa/grpc: Host.KVGet: %w", err)
	}
	if reply == nil {
		return nil, false, fmt.Errorf("arupa/grpc: Host.KVGet returned a nil reply")
	}
	return append([]byte(nil), reply.GetValue()...), reply.GetFound(), nil
}

func (h *host) KVSet(ctx context.Context, namespace, key string, value []byte) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.KVSet(nonNilContext(ctx), &servicev2.KVSetRequest{
		Namespace: namespace,
		Key:       key,
		Value:     append([]byte(nil), value...),
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: Host.KVSet: %w", err)
	}
	if reply == nil {
		return fmt.Errorf("arupa/grpc: Host.KVSet returned a nil reply")
	}
	return replyError("Host.KVSet", reply.GetError())
}

func (h *host) KVDelete(ctx context.Context, namespace, key string) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.KVDelete(nonNilContext(ctx), &servicev2.KVDeleteRequest{
		Namespace: namespace,
		Key:       key,
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: Host.KVDelete: %w", err)
	}
	if reply == nil {
		return fmt.Errorf("arupa/grpc: Host.KVDelete returned a nil reply")
	}
	return replyError("Host.KVDelete", reply.GetError())
}

func (h *host) KVList(ctx context.Context, namespace string) ([]string, error) {
	if err := h.available(); err != nil {
		return nil, err
	}
	reply, err := h.client.KVList(nonNilContext(ctx), &servicev2.KVListRequest{Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: Host.KVList: %w", err)
	}
	if reply == nil {
		return nil, fmt.Errorf("arupa/grpc: Host.KVList returned a nil reply")
	}
	return append([]string(nil), reply.GetKeys()...), nil
}

func (h *host) Params(ctx context.Context) (map[string]string, error) {
	if err := h.available(); err != nil {
		return nil, err
	}
	reply, err := h.client.GetParams(nonNilContext(ctx), &servicev2.ParamsGetRequest{})
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: Host.GetParams: %w", err)
	}
	if reply == nil {
		return nil, fmt.Errorf("arupa/grpc: Host.GetParams returned a nil reply")
	}
	if message := reply.GetError(); message != "" {
		return nil, fmt.Errorf("arupa/grpc: Host.GetParams: %s", message)
	}
	return arupa.CloneParams(reply.GetParams()), nil
}

func (h *host) PatchParams(ctx context.Context, patch arupa.ParamsPatch) error {
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.PatchParams(nonNilContext(ctx), &servicev2.ParamsPatchRequest{
		Set:    arupa.CloneParams(patch.Set),
		Delete: append([]string(nil), patch.Delete...),
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: Host.PatchParams: %w", err)
	}
	if reply == nil {
		return fmt.Errorf("arupa/grpc: Host.PatchParams returned a nil reply")
	}
	return replyError("Host.PatchParams", reply.GetError())
}

func (h *host) Emit(ctx context.Context, instruction arupa.EmitInstruction) error {
	if instruction.Namespace == "" {
		return fmt.Errorf("arupa: emit namespace is required")
	}
	if instruction.Event == "" {
		return fmt.Errorf("arupa: emit event is required")
	}
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.Emit(nonNilContext(ctx), &servicev2.EmitInstruction{
		Namespace: instruction.Namespace,
		Target:    instruction.Target,
		Event:     instruction.Event,
		Payload:   append([]byte(nil), instruction.Payload...),
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: Host.Emit: %w", err)
	}
	if reply == nil {
		return fmt.Errorf("arupa/grpc: Host.Emit returned a nil reply")
	}
	return replyError("Host.Emit", reply.GetError())
}

func (h *host) SendServiceMessage(ctx context.Context, message arupa.OutgoingServiceMessage) (string, error) {
	if err := message.Validate(); err != nil {
		return "", err
	}
	if err := h.available(); err != nil {
		return "", err
	}
	reply, err := h.client.SendServiceMessage(nonNilContext(ctx), &servicev2.ServiceMessage{
		Target:  message.Target,
		Topic:   message.Topic,
		Payload: append([]byte(nil), message.Payload...),
	})
	if err != nil {
		return "", fmt.Errorf("arupa/grpc: Host.SendServiceMessage: %w", err)
	}
	if reply == nil {
		return "", fmt.Errorf("arupa/grpc: Host.SendServiceMessage returned a nil reply")
	}
	if message := reply.GetError(); message != "" {
		return "", fmt.Errorf("arupa/grpc: Host.SendServiceMessage: %s", message)
	}
	return reply.GetMessage(), nil
}

func (h *host) RegisterTransport(ctx context.Context, transport arupa.Transport) (arupa.RegistrationResult, error) {
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	wire, err := transportToProto(transport)
	if err != nil {
		return arupa.RegistrationResult{}, err
	}
	reply, err := h.client.RegisterTransport(nonNilContext(ctx), &servicev2.RegisterTransportRequest{Transport: wire})
	if err != nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/grpc: Host.RegisterTransport: %w", err)
	}
	return registrationResultFromProto("Host.RegisterTransport", reply)
}

func (h *host) UnregisterTransport(ctx context.Context, id string) (arupa.RegistrationResult, error) {
	id = strings.TrimSpace(id)
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	reply, err := h.client.UnregisterTransport(nonNilContext(ctx), &servicev2.UnregisterTransportRequest{Id: id})
	if err != nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/grpc: Host.UnregisterTransport: %w", err)
	}
	return registrationResultFromProto("Host.UnregisterTransport", reply)
}

func (h *host) RegisterRoutes(ctx context.Context, routes []arupa.Route) (arupa.RegistrationResult, error) {
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	wire := make([]*servicev2.Route, 0, len(routes))
	localFailures := make([]arupa.RegistrationFailure, 0)
	for _, route := range routes {
		converted, err := routeToProto(route)
		if err != nil {
			localFailures = append(localFailures, arupa.RegistrationFailure{
				ID:    strings.TrimSpace(route.ID),
				Error: err.Error(),
			})
			continue
		}
		wire = append(wire, converted)
	}
	reply, err := h.client.RegisterRoutes(nonNilContext(ctx), &servicev2.RegisterRoutesRequest{Routes: wire})
	if err != nil {
		return registrationResultWithLocalFailures(localFailures), fmt.Errorf("arupa/grpc: Host.RegisterRoutes: %w", err)
	}
	result, err := registrationResultFromProto("Host.RegisterRoutes", reply)
	if err != nil {
		return registrationResultWithLocalFailures(localFailures), err
	}
	return mergeRegistrationFailures(result, localFailures), nil
}

func (h *host) UnregisterRoutes(ctx context.Context, ids []string) (arupa.RegistrationResult, error) {
	normalized := make([]string, len(ids))
	for index, id := range ids {
		normalized[index] = strings.TrimSpace(id)
	}
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	reply, err := h.client.UnregisterRoutes(nonNilContext(ctx), &servicev2.UnregisterRoutesRequest{
		Ids: normalized,
	})
	if err != nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/grpc: Host.UnregisterRoutes: %w", err)
	}
	return registrationResultFromProto("Host.UnregisterRoutes", reply)
}

func (h *host) Log(ctx context.Context, level arupa.LogLevel, message string) error {
	if err := h.available(); err != nil {
		return err
	}
	level, err := arupa.NormalizeLogLevel(level)
	if err != nil {
		return err
	}
	reply, err := h.client.Log(nonNilContext(ctx), &servicev2.LogRequest{
		Level:   string(level),
		Message: message,
	})
	if err != nil {
		return fmt.Errorf("arupa/grpc: Host.Log: %w", err)
	}
	if reply == nil {
		return fmt.Errorf("arupa/grpc: Host.Log returned a nil reply")
	}
	return nil
}

func (h *host) available() error {
	if h == nil || h.client == nil {
		return errors.New(hostUnavailable)
	}
	return nil
}

func (h *host) close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		if h.closer != nil {
			h.closeErr = h.closer.Close()
		}
	})
	return h.closeErr
}

func replyError(operation, message string) error {
	if message != "" {
		return fmt.Errorf("arupa/grpc: %s: %s", operation, message)
	}
	return nil
}

func registrationResultFromProto(operation string, reply *servicev2.RegistrationReply) (arupa.RegistrationResult, error) {
	if reply == nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/grpc: %s returned a nil reply", operation)
	}
	result := arupa.RegistrationResult{
		Registered: append([]string(nil), reply.GetRegistered()...),
		Failures:   make([]arupa.RegistrationFailure, 0, len(reply.GetFailures())),
		Degraded:   reply.GetDegraded(),
		Message:    reply.GetError(),
	}
	for _, failure := range reply.GetFailures() {
		if failure == nil {
			continue
		}
		result.Failures = append(result.Failures, arupa.RegistrationFailure{
			ID:    failure.GetId(),
			Error: failure.GetError(),
		})
	}
	return result, nil
}

func registrationResultWithLocalFailures(failures []arupa.RegistrationFailure) arupa.RegistrationResult {
	return mergeRegistrationFailures(arupa.RegistrationResult{}, failures)
}

func mergeRegistrationFailures(result arupa.RegistrationResult, failures []arupa.RegistrationFailure) arupa.RegistrationResult {
	if len(failures) == 0 {
		return result
	}
	result.Failures = append(result.Failures, failures...)
	result.Degraded = true
	return result
}

func transportToProto(transport arupa.Transport) (*servicev2.Transport, error) {
	out := &servicev2.Transport{Id: strings.TrimSpace(transport.ID)}
	switch transport.Type {
	case "":
		out.Type = servicev2.TransportType_TRANSPORT_TYPE_UNSPECIFIED
	case arupa.TransportStatic:
		if transport.Proxy != nil {
			return nil, fmt.Errorf("static transport %q has proxy configuration", out.Id)
		}
		out.Type = servicev2.TransportType_TRANSPORT_TYPE_STATIC
		out.Config = &servicev2.Transport_Static{
			Static: &servicev2.StaticTransport{Source: transport.StaticSource},
		}
	case arupa.TransportHTTP:
		if transport.StaticSource != "" || transport.Proxy != nil {
			return nil, fmt.Errorf("http transport %q has incompatible configuration", out.Id)
		}
		out.Type = servicev2.TransportType_TRANSPORT_TYPE_HTTP
	case arupa.TransportSocketIO:
		if transport.StaticSource != "" || transport.Proxy != nil {
			return nil, fmt.Errorf("socket.io transport %q has incompatible configuration", out.Id)
		}
		out.Type = servicev2.TransportType_TRANSPORT_TYPE_SOCKET_IO
	case arupa.TransportProxy:
		if transport.StaticSource != "" {
			return nil, fmt.Errorf("proxy transport %q has static configuration", out.Id)
		}
		out.Type = servicev2.TransportType_TRANSPORT_TYPE_PROXY
		if transport.Proxy != nil {
			proxy, err := proxyToProto(transport.Proxy)
			if err != nil {
				return nil, err
			}
			out.Config = &servicev2.Transport_Proxy{Proxy: proxy}
		}
	default:
		return nil, fmt.Errorf("unsupported transport type %q", transport.Type)
	}
	return out, nil
}

func proxyToProto(proxy *arupa.ProxyTarget) (*servicev2.ProxyTransport, error) {
	out := &servicev2.ProxyTransport{Address: proxy.Address, Scheme: proxy.Scheme}
	switch proxy.Network {
	case "":
		out.Network = servicev2.ProxyNetwork_PROXY_NETWORK_UNSPECIFIED
	case arupa.ProxyInherited:
		out.Network = servicev2.ProxyNetwork_PROXY_NETWORK_INHERITED
	case arupa.ProxyUnix:
		out.Network = servicev2.ProxyNetwork_PROXY_NETWORK_UNIX
	case arupa.ProxyTCP:
		out.Network = servicev2.ProxyNetwork_PROXY_NETWORK_TCP
	default:
		return nil, fmt.Errorf("unsupported proxy network %q", proxy.Network)
	}
	return out, nil
}

func routeToProto(route arupa.Route) (*servicev2.Route, error) {
	id := strings.TrimSpace(route.ID)
	if (route.HTTP == nil) == (route.SocketIO == nil) {
		return nil, fmt.Errorf("arupa: route %q must declare exactly one route kind", id)
	}
	out := &servicev2.Route{Id: id, TransportId: strings.TrimSpace(route.TransportID)}
	switch {
	case route.HTTP != nil:
		out.Route = &servicev2.Route_Http{Http: &servicev2.HTTPRoute{
			Method:  route.HTTP.Method,
			Pattern: route.HTTP.Pattern,
			Access:  accessPolicyToProto(route.HTTP.Access),
		}}
	case route.SocketIO != nil:
		eventAccess := make(map[string]*servicev2.AccessPolicy, len(route.SocketIO.EventAccess))
		for event, policy := range route.SocketIO.EventAccess {
			eventAccess[event] = accessPolicyToProto(policy)
		}
		out.Route = &servicev2.Route_SocketIo{SocketIo: &servicev2.SocketIORoute{
			Namespace:   route.SocketIO.Namespace,
			Events:      append([]string(nil), route.SocketIO.Events...),
			Access:      accessPolicyToProto(route.SocketIO.Access),
			EventAccess: eventAccess,
		}}
	default:
		return nil, fmt.Errorf("route %q has no route configuration", route.ID)
	}
	return out, nil
}

func accessPolicyToProto(policy arupa.AccessPolicy) *servicev2.AccessPolicy {
	return &servicev2.AccessPolicy{
		RequireAuth: policy.RequireAuth,
		Groups:      append([]string(nil), policy.Groups...),
	}
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

func nonNilContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("arupa/grpc: dial Host broker: %w", ctx.Err())
	default:
		return nil
	}
}

type hostState struct {
	mu sync.RWMutex

	broker *hcplugin.GRPCBroker
	host   *host
	closed bool
	dial   func(context.Context, *hcplugin.GRPCBroker, uint32) (*host, error)

	closeOnce sync.Once
	closeErr  error
}

func (s *hostState) setBroker(broker *hcplugin.GRPCBroker) {
	s.mu.Lock()
	if !s.closed {
		s.broker = broker
	}
	s.mu.Unlock()
}

func (s *hostState) connect(ctx context.Context, brokerID uint32) error {
	if brokerID == 0 {
		return fmt.Errorf("arupa/grpc: Host broker id is required")
	}
	s.mu.RLock()
	broker := s.broker
	closed := s.closed
	dial := s.dial
	s.mu.RUnlock()
	if closed {
		return fmt.Errorf("arupa/grpc: service is closed")
	}

	if dial == nil {
		dial = newBrokerHost
	}
	next, err := dial(ctx, broker, brokerID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = next.close()
		return fmt.Errorf("arupa/grpc: service is closed")
	}
	previous := s.host
	s.host = next
	s.mu.Unlock()
	// The new Host connection is already live. Failure to close a superseded
	// connection must not turn a successful registration into a failure with
	// partially committed state.
	_ = previous.close()
	return nil
}

func (s *hostState) current() *host {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.host
}

// install is used by tests and by adapters that already own an established
// Host client connection.
func (s *hostState) install(next *host) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = next.close()
		return fmt.Errorf("arupa/grpc: service is closed")
	}
	previous := s.host
	s.host = next
	s.mu.Unlock()
	return previous.close()
}

func (s *hostState) clear() error {
	s.mu.Lock()
	current := s.host
	s.host = nil
	s.mu.Unlock()
	return current.close()
}

func (s *hostState) close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		current := s.host
		s.host = nil
		s.broker = nil
		s.mu.Unlock()
		s.closeErr = current.close()
	})
	return s.closeErr
}
