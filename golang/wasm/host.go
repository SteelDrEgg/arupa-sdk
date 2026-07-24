package wasm

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SteelDrEgg/arupa-sdk/golang"
	servicev2 "github.com/SteelDrEgg/arupa-sdk/golang/gen/wasm/proto"
)

// host adapts the generated WASM imports to the framework-neutral HostClient.
type host struct {
	client servicev2.Host
	active atomic.Bool
}

var _ arupa.HostClient = (*host)(nil)

func newHost(client servicev2.Host) *host {
	host := &host{client: client}
	host.active.Store(!isNilReply(client))
	return host
}

func (h *host) available() error {
	if h == nil || !h.active.Load() || isNilReply(h.client) {
		return fmt.Errorf("arupa/wasm: host imports are unavailable before registration")
	}
	return nil
}

func (h *host) invalidate() {
	if h != nil {
		h.active.Store(false)
	}
}

func callContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (h *host) KVGet(ctx context.Context, namespace, key string) ([]byte, bool, error) {
	if err := validateKVRequest(namespace, key); err != nil {
		return nil, false, err
	}
	if err := h.available(); err != nil {
		return nil, false, err
	}
	reply, err := h.client.KVGet(callContext(ctx), &servicev2.KVGetRequest{Namespace: namespace, Key: key})
	if err != nil {
		return nil, false, fmt.Errorf("arupa/wasm: host kv get: %w", err)
	}
	if reply == nil {
		return nil, false, fmt.Errorf("arupa/wasm: host kv get: empty response")
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
	reply, err := h.client.KVSet(callContext(ctx), &servicev2.KVSetRequest{
		Namespace: namespace,
		Key:       key,
		Value:     append([]byte(nil), value...),
	})
	if err != nil {
		return fmt.Errorf("arupa/wasm: host kv set: %w", err)
	}
	return replyError("kv set", reply)
}

func (h *host) KVDelete(ctx context.Context, namespace, key string) error {
	if err := validateKVRequest(namespace, key); err != nil {
		return err
	}
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.KVDelete(callContext(ctx), &servicev2.KVDeleteRequest{Namespace: namespace, Key: key})
	if err != nil {
		return fmt.Errorf("arupa/wasm: host kv delete: %w", err)
	}
	return replyError("kv delete", reply)
}

func (h *host) KVList(ctx context.Context, namespace string) ([]string, error) {
	if err := h.available(); err != nil {
		return nil, err
	}
	reply, err := h.client.KVList(callContext(ctx), &servicev2.KVListRequest{Namespace: namespace})
	if err != nil {
		return nil, fmt.Errorf("arupa/wasm: host kv list: %w", err)
	}
	if reply == nil {
		return nil, fmt.Errorf("arupa/wasm: host kv list: empty response")
	}
	return append([]string(nil), reply.GetKeys()...), nil
}

func (h *host) Params(ctx context.Context) (map[string]string, error) {
	if err := h.available(); err != nil {
		return nil, err
	}
	reply, err := h.client.GetParams(callContext(ctx), &servicev2.ParamsGetRequest{})
	if err != nil {
		return nil, fmt.Errorf("arupa/wasm: host get params: %w", err)
	}
	if reply == nil {
		return nil, fmt.Errorf("arupa/wasm: host get params: empty response")
	}
	if message := reply.GetError(); message != "" {
		return nil, fmt.Errorf("arupa/wasm: host get params: %s", message)
	}
	return arupa.CloneParams(reply.GetParams()), nil
}

func (h *host) PatchParams(ctx context.Context, patch arupa.ParamsPatch) error {
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.PatchParams(callContext(ctx), &servicev2.ParamsPatchRequest{
		Set:    arupa.CloneParams(patch.Set),
		Delete: append([]string(nil), patch.Delete...),
	})
	if err != nil {
		return fmt.Errorf("arupa/wasm: host patch params: %w", err)
	}
	return replyError("patch params", reply)
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
	reply, err := h.client.Emit(callContext(ctx), emitToProto(instruction))
	if err != nil {
		return fmt.Errorf("arupa/wasm: host emit: %w", err)
	}
	return replyError("emit", reply)
}

func (h *host) SendServiceMessage(ctx context.Context, message arupa.OutgoingServiceMessage) (string, error) {
	if err := message.Validate(); err != nil {
		return "", err
	}
	if err := h.available(); err != nil {
		return "", err
	}
	reply, err := h.client.SendServiceMessage(callContext(ctx), &servicev2.ServiceMessage{
		Target:  message.Target,
		Topic:   message.Topic,
		Payload: append([]byte(nil), message.Payload...),
	})
	if err != nil {
		return "", fmt.Errorf("arupa/wasm: send service message: %w", err)
	}
	if reply == nil {
		return "", fmt.Errorf("arupa/wasm: send service message: empty response")
	}
	if message := reply.GetError(); message != "" {
		return "", fmt.Errorf("arupa/wasm: send service message: %s", message)
	}
	return reply.GetMessage(), nil
}

func (h *host) RegisterTransport(ctx context.Context, transport arupa.Transport) (arupa.RegistrationResult, error) {
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	encoded, err := transportToProto(transport)
	if err != nil {
		return arupa.RegistrationResult{}, err
	}
	reply, err := h.client.RegisterTransport(callContext(ctx), &servicev2.RegisterTransportRequest{Transport: encoded})
	if err != nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/wasm: register transport: %w", err)
	}
	return registrationResultFromProto("register transport", reply)
}

func (h *host) UnregisterTransport(ctx context.Context, id string) (arupa.RegistrationResult, error) {
	id = strings.TrimSpace(id)
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	reply, err := h.client.UnregisterTransport(callContext(ctx), &servicev2.UnregisterTransportRequest{Id: id})
	if err != nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/wasm: unregister transport: %w", err)
	}
	return registrationResultFromProto("unregister transport", reply)
}

func (h *host) RegisterRoutes(ctx context.Context, routes []arupa.Route) (arupa.RegistrationResult, error) {
	if err := h.available(); err != nil {
		return arupa.RegistrationResult{}, err
	}
	encoded := make([]*servicev2.Route, 0, len(routes))
	localFailures := make([]arupa.RegistrationFailure, 0)
	for _, route := range routes {
		item, err := routeToProto(route)
		if err != nil {
			localFailures = append(localFailures, arupa.RegistrationFailure{
				ID:    strings.TrimSpace(route.ID),
				Error: err.Error(),
			})
			continue
		}
		encoded = append(encoded, item)
	}
	reply, err := h.client.RegisterRoutes(callContext(ctx), &servicev2.RegisterRoutesRequest{Routes: encoded})
	if err != nil {
		return registrationResultWithLocalFailures(localFailures), fmt.Errorf("arupa/wasm: register routes: %w", err)
	}
	result, err := registrationResultFromProto("register routes", reply)
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
	reply, err := h.client.UnregisterRoutes(callContext(ctx), &servicev2.UnregisterRoutesRequest{
		Ids: normalized,
	})
	if err != nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/wasm: unregister routes: %w", err)
	}
	return registrationResultFromProto("unregister routes", reply)
}

func (h *host) Log(ctx context.Context, level arupa.LogLevel, message string) error {
	normalized, err := arupa.NormalizeLogLevel(level)
	if err != nil {
		return err
	}
	if err := h.available(); err != nil {
		return err
	}
	reply, err := h.client.Log(callContext(ctx), &servicev2.LogRequest{
		Level:   string(normalized),
		Message: message,
	})
	if err != nil {
		return fmt.Errorf("arupa/wasm: host log: %w", err)
	}
	if reply == nil {
		return fmt.Errorf("arupa/wasm: host log: empty response")
	}
	return nil
}

// Host returns the complete Host capability set after successful
// registration.
func (s *Service) Host() arupa.HostClient {
	current := s.currentHost()
	if current == nil || current.available() != nil {
		return nil
	}
	return current
}

// KV returns a KV store scoped to this service's registered name.
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

func (s *Service) Emit(ctx context.Context, instruction arupa.EmitInstruction) error {
	return s.currentHost().Emit(ctx, instruction)
}

// EmitJSON encodes args as Socket.IO event arguments and emits them through
// the host.
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

// SendServiceJSON encodes payload as JSON before sending it to another
// registered service.
func (s *Service) SendServiceJSON(ctx context.Context, target, topic string, payload any) (string, error) {
	return arupa.SendServiceJSON(ctx, s, target, topic, payload)
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

func (s *Service) currentHost() *host {
	if s == nil {
		return nil
	}
	return s.host.current()
}

type hostState struct {
	mu          sync.RWMutex
	currentHost *host
}

func (s *hostState) replace(next *host) {
	s.mu.Lock()
	previous := s.currentHost
	s.currentHost = next
	s.mu.Unlock()
	previous.invalidate()
}

func (s *hostState) clear() {
	s.mu.Lock()
	current := s.currentHost
	s.currentHost = nil
	s.mu.Unlock()
	current.invalidate()
}

func (s *hostState) current() *host {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentHost
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

type errorReply interface {
	GetError() string
}

func replyError(operation string, reply errorReply) error {
	if isNilReply(reply) {
		return fmt.Errorf("arupa/wasm: host %s: empty response", operation)
	}
	if message := reply.GetError(); message != "" {
		return fmt.Errorf("arupa/wasm: host %s: %s", operation, message)
	}
	return nil
}

func isNilReply(reply any) bool {
	if reply == nil {
		return true
	}
	value := reflect.ValueOf(reply)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func registrationResultFromProto(operation string, reply *servicev2.RegistrationReply) (arupa.RegistrationResult, error) {
	if reply == nil {
		return arupa.RegistrationResult{}, fmt.Errorf("arupa/wasm: host %s: empty response", operation)
	}
	failures := make([]arupa.RegistrationFailure, 0, len(reply.GetFailures()))
	for _, failure := range reply.GetFailures() {
		if failure == nil {
			continue
		}
		failures = append(failures, arupa.RegistrationFailure{
			ID:    failure.GetId(),
			Error: failure.GetError(),
		})
	}
	return arupa.RegistrationResult{
		Registered: append([]string(nil), reply.GetRegistered()...),
		Failures:   failures,
		Degraded:   reply.GetDegraded(),
		Message:    reply.GetError(),
	}, nil
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
			network, err := proxyNetworkToProto(transport.Proxy.Network)
			if err != nil {
				return nil, err
			}
			out.Config = &servicev2.Transport_Proxy{
				Proxy: &servicev2.ProxyTransport{
					Network: network,
					Address: transport.Proxy.Address,
					Scheme:  transport.Proxy.Scheme,
				},
			}
		}
	default:
		return nil, fmt.Errorf("transport %q has unsupported type %q", transport.ID, transport.Type)
	}
	return out, nil
}

func proxyNetworkToProto(network arupa.ProxyNetwork) (servicev2.ProxyNetwork, error) {
	switch network {
	case "":
		return servicev2.ProxyNetwork_PROXY_NETWORK_UNSPECIFIED, nil
	case arupa.ProxyInherited:
		return servicev2.ProxyNetwork_PROXY_NETWORK_INHERITED, nil
	case arupa.ProxyUnix:
		return servicev2.ProxyNetwork_PROXY_NETWORK_UNIX, nil
	case arupa.ProxyTCP:
		return servicev2.ProxyNetwork_PROXY_NETWORK_TCP, nil
	default:
		return servicev2.ProxyNetwork_PROXY_NETWORK_UNSPECIFIED, fmt.Errorf("unsupported proxy network %q", network)
	}
}

func routeToProto(route arupa.Route) (*servicev2.Route, error) {
	id := strings.TrimSpace(route.ID)
	if (route.HTTP == nil) == (route.SocketIO == nil) {
		return nil, fmt.Errorf("arupa: route %q must declare exactly one route kind", id)
	}
	out := &servicev2.Route{Id: id, TransportId: strings.TrimSpace(route.TransportID)}
	switch {
	case route.HTTP != nil:
		out.Route = &servicev2.Route_Http{
			Http: &servicev2.HTTPRoute{
				Method:  route.HTTP.Method,
				Pattern: route.HTTP.Pattern,
				Access:  accessPolicyToProto(route.HTTP.Access),
			},
		}
	case route.SocketIO != nil:
		eventAccess := make(map[string]*servicev2.AccessPolicy, len(route.SocketIO.EventAccess))
		for event, policy := range route.SocketIO.EventAccess {
			eventAccess[event] = accessPolicyToProto(policy)
		}
		out.Route = &servicev2.Route_SocketIo{
			SocketIo: &servicev2.SocketIORoute{
				Namespace:   route.SocketIO.Namespace,
				Events:      append([]string(nil), route.SocketIO.Events...),
				Access:      accessPolicyToProto(route.SocketIO.Access),
				EventAccess: eventAccess,
			},
		}
	default:
		return nil, fmt.Errorf("route %q has no configuration", route.ID)
	}
	return out, nil
}
