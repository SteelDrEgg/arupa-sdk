package arupa

import (
	"context"
	"fmt"
	"net/http"
)

// HTTPBinding supplies the protocol-specific conversions around the shared
// HTTP adapter.
type HTTPBinding[Request any, Response any] struct {
	Request  func(*Request) HTTPRequest
	Response func(HTTPResponse) *Response
}

// ServeHTTP converts a protocol request, invokes the shared adapter, and
// converts the response back to the protocol type.
func (b HTTPBinding[Request, Response]) ServeHTTP(ctx context.Context, request *Request, handler http.Handler) (*Response, error) {
	if request == nil {
		return nil, fmt.Errorf("arupa: protocol http request is nil")
	}
	response, err := ServeHTTP(ctx, b.Request(request), handler)
	if err != nil {
		return nil, err
	}
	return b.Response(response), nil
}

// RegistrationBinding supplies the protocol-specific message constructors for
// a Registration. The declaration validation and list conversion are shared by
// every protocol binding.
type RegistrationBinding[Route any, Namespace any, Mount any, Reply any] struct {
	Route     func(HTTPRoute) *Route
	Namespace func(SocketNamespace) *Namespace
	Mount     func(StaticMount) *Mount
	Reply     func(name, version string, routes []*Route, namespaces []*Namespace, mounts []*Mount) *Reply
}

// RegistrationReply converts a Registration into a protocol reply.
func (b RegistrationBinding[Route, Namespace, Mount, Reply]) RegistrationReply(registration Registration) (*Reply, error) {
	if err := registration.Validate(); err != nil {
		return nil, err
	}

	routes := make([]*Route, 0, len(registration.HTTPRoutes))
	for _, route := range registration.HTTPRoutes {
		routes = append(routes, b.Route(route))
	}
	namespaces := make([]*Namespace, 0, len(registration.SocketNamespaces))
	for _, namespace := range registration.SocketNamespaces {
		namespaces = append(namespaces, b.Namespace(namespace))
	}
	mounts := make([]*Mount, 0, len(registration.StaticMounts))
	for _, mount := range registration.StaticMounts {
		mounts = append(mounts, b.Mount(mount))
	}

	return b.Reply(registration.Name, registration.Version, routes, namespaces, mounts), nil
}
