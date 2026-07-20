package arupa

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
)

// HTTPRequest is the framework-neutral representation of an incoming request.
// Protocol bindings convert their generated request type into this structure.
type HTTPRequest struct {
	Method     string
	Path       string
	Query      string
	Headers    http.Header
	Body       []byte
	RemoteAddr string
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
// It contains no knowledge of gRPC, WASM, RoutePattern, or plugin routing.
// The host has already authorized and forwarded the request; handler owns all
// application routing and middleware.
func ServeHTTP(ctx context.Context, request HTTPRequest, handler http.Handler) (HTTPResponse, error) {
	if handler == nil {
		return HTTPResponse{}, fmt.Errorf("arupa: http handler is nil")
	}

	requestURI := request.Path
	if requestURI == "" {
		requestURI = "/"
	}
	if request.Query != "" {
		requestURI += "?" + request.Query
	}

	httpRequest, err := http.NewRequestWithContext(ctx, request.Method, "http://arupa.local"+requestURI, bytes.NewReader(request.Body))
	if err != nil {
		return HTTPResponse{}, fmt.Errorf("arupa: build http request: %w", err)
	}
	httpRequest.RequestURI = requestURI
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
