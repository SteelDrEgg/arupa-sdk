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
	if b.Request == nil {
		return nil, fmt.Errorf("arupa: protocol http request converter is nil")
	}
	if b.Response == nil {
		return nil, fmt.Errorf("arupa: protocol http response converter is nil")
	}
	response, err := ServeHTTP(ctx, b.Request(request), handler)
	if err != nil {
		return nil, err
	}
	return b.Response(response), nil
}

// ServiceInfoBinding supplies the protocol-specific Register reply constructor.
type ServiceInfoBinding[Reply any] struct {
	Reply func(ServiceInfo) *Reply
}

// RegisterReply validates and converts a service identity.
func (b ServiceInfoBinding[Reply]) RegisterReply(info ServiceInfo) (*Reply, error) {
	if err := info.Validate(); err != nil {
		return nil, err
	}
	if b.Reply == nil {
		return nil, fmt.Errorf("arupa: protocol register reply converter is nil")
	}
	return b.Reply(info), nil
}
