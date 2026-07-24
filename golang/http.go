package arupa

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
)

// HTTPRequest is the framework-neutral representation of an incoming request.
// Protocol bindings convert their generated request type into this structure.
type HTTPRequest struct {
	RouteID      string
	RoutePattern string
	Method       string
	Path         string
	Query        string
	Headers      http.Header
	Body         []byte
	RemoteAddr   string
	User         *User
}

type userContextKey struct{}
type httpRouteContextKey struct{}

// HTTPRouteMatch identifies the host route selected for an HTTP request.
type HTTPRouteMatch struct {
	ID      string
	Pattern string
}

// UserFromContext returns the authenticated user forwarded by the host.
// It reports false when the request is unauthenticated.
func UserFromContext(ctx context.Context) (*User, bool) {
	user, ok := ctx.Value(userContextKey{}).(*User)
	return user, ok && user != nil
}

// HTTPRouteFromContext returns the host route selected for the request.
func HTTPRouteFromContext(ctx context.Context) (HTTPRouteMatch, bool) {
	route, ok := ctx.Value(httpRouteContextKey{}).(HTTPRouteMatch)
	return route, ok
}

// HTTPResponse is the framework-neutral representation of a handler response.
// Protocol bindings convert this structure into their generated response type.
type HTTPResponse struct {
	Status  int
	Headers http.Header
	Body    []byte
}

// ServeHTTP adapts an HTTPRequest to a standard Go HTTP handler.
//
// It contains no knowledge of gRPC or WASM. The host has already authorized
// and forwarded the request; handler owns all application routing and
// middleware. The selected host route is exposed through
// HTTPRouteFromContext.
func ServeHTTP(ctx context.Context, request HTTPRequest, handler http.Handler) (HTTPResponse, error) {
	if handler == nil {
		return HTTPResponse{}, fmt.Errorf("arupa: http handler is nil")
	}

	path := request.Path
	if path == "" {
		path = "/"
	}
	target := &url.URL{
		Scheme:   "http",
		Host:     "arupa.local",
		Path:     path,
		RawQuery: request.Query,
	}

	requestContext := ctx
	if requestContext == nil {
		requestContext = context.Background()
	}
	if request.RouteID != "" || request.RoutePattern != "" {
		requestContext = context.WithValue(requestContext, httpRouteContextKey{}, HTTPRouteMatch{
			ID:      request.RouteID,
			Pattern: request.RoutePattern,
		})
	}
	if request.User != nil {
		requestContext = context.WithValue(requestContext, userContextKey{}, &User{
			Username: request.User.Username,
			Groups:   append([]string(nil), request.User.Groups...),
		})
	}

	httpRequest, err := http.NewRequestWithContext(requestContext, request.Method, target.String(), bytes.NewReader(request.Body))
	if err != nil {
		return HTTPResponse{}, fmt.Errorf("arupa: build http request: %w", err)
	}
	httpRequest.RequestURI = target.RequestURI()
	httpRequest.RemoteAddr = request.RemoteAddr
	httpRequest.Header = make(http.Header, len(request.Headers))
	for key, values := range request.Headers {
		for _, value := range values {
			httpRequest.Header.Add(key, value)
		}
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httpRequest)
	response := recorder.Result()
	defer response.Body.Close()

	return HTTPResponse{
		Status:  response.StatusCode,
		Headers: response.Header.Clone(),
		Body:    append([]byte(nil), recorder.Body.Bytes()...),
	}, nil
}
