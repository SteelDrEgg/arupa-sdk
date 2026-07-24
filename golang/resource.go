package arupa

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

// TransportType identifies how the kernel forwards traffic for a route.
type TransportType string

const (
	TransportStatic   TransportType = "static"
	TransportHTTP     TransportType = "http"
	TransportSocketIO TransportType = "socket.io"
	TransportProxy    TransportType = "proxy"
)

// ProxyNetwork identifies how the kernel reaches a proxy service.
type ProxyNetwork string

const (
	ProxyInherited ProxyNetwork = "inherited"
	ProxyUnix      ProxyNetwork = "unix"
	ProxyTCP       ProxyNetwork = "tcp"
)

// ProxyTarget describes a native HTTP upstream. An inherited target uses
// Address as the inherited listener ID; an empty value selects "proxy".
type ProxyTarget struct {
	Network ProxyNetwork
	Address string
	Scheme  string
}

// Validate verifies the transport-independent proxy declaration.
func (p ProxyTarget) Validate() error {
	switch p.Network {
	case ProxyInherited:
		// Address may be empty to select the conventional "proxy" listener.
	case ProxyUnix:
		address := strings.TrimSpace(p.Address)
		if address == "" {
			return fmt.Errorf("arupa: unix proxy address is required")
		}
		if !filepath.IsAbs(address) {
			return fmt.Errorf("arupa: unix proxy address must be absolute")
		}
	case ProxyTCP:
		if strings.TrimSpace(p.Address) == "" {
			return fmt.Errorf("arupa: tcp proxy address is required")
		}
	default:
		return fmt.Errorf("arupa: unsupported proxy network %q", p.Network)
	}

	switch strings.ToLower(strings.TrimSpace(p.Scheme)) {
	case "", "http", "https":
		return nil
	default:
		return fmt.Errorf("arupa: proxy scheme must be http or https")
	}
}

// Transport declares one service-owned traffic backend.
type Transport struct {
	ID           string
	Type         TransportType
	StaticSource string
	Proxy        *ProxyTarget
}

// Validate verifies a transport declaration before it is sent to the host.
func (t Transport) Validate() error {
	if strings.TrimSpace(t.ID) == "" {
		return fmt.Errorf("arupa: transport id is required")
	}

	switch t.Type {
	case TransportStatic:
		source := strings.TrimSpace(t.StaticSource)
		if source == "" {
			return fmt.Errorf("arupa: static transport %q source is required", t.ID)
		}
		if filepath.IsAbs(source) {
			return fmt.Errorf("arupa: static transport %q source must be relative", t.ID)
		}
		clean := filepath.Clean(source)
		if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
			return fmt.Errorf("arupa: static transport %q source escapes the service root", t.ID)
		}
		if t.Proxy != nil {
			return fmt.Errorf("arupa: static transport %q has proxy configuration", t.ID)
		}
	case TransportHTTP, TransportSocketIO:
		if t.StaticSource != "" || t.Proxy != nil {
			return fmt.Errorf("arupa: %s transport %q has incompatible configuration", t.Type, t.ID)
		}
	case TransportProxy:
		if t.StaticSource != "" {
			return fmt.Errorf("arupa: proxy transport %q has static configuration", t.ID)
		}
		if t.Proxy == nil {
			return fmt.Errorf("arupa: proxy transport %q target is required", t.ID)
		}
		if err := t.Proxy.Validate(); err != nil {
			return fmt.Errorf("arupa: proxy transport %q: %w", t.ID, err)
		}
	default:
		return fmt.Errorf("arupa: unsupported transport type %q", t.Type)
	}
	return nil
}

// AccessPolicy is the host-side authorization policy for an ingress route.
type AccessPolicy struct {
	RequireAuth bool
	Groups      []string
}

// HTTPRoute declares an HTTP path forwarded through a transport.
type HTTPRoute struct {
	Method  string
	Pattern string
	Access  AccessPolicy
}

// SocketIORoute declares a Socket.IO namespace forwarded through a transport.
type SocketIORoute struct {
	Namespace   string
	Events      []string
	Access      AccessPolicy
	EventAccess map[string]AccessPolicy
}

// Route binds one HTTP or Socket.IO route to a transport.
type Route struct {
	ID          string
	TransportID string
	HTTP        *HTTPRoute
	SocketIO    *SocketIORoute
}

// Validate verifies the structural route declaration.
func (r Route) Validate() error {
	if strings.TrimSpace(r.ID) == "" {
		return fmt.Errorf("arupa: route id is required")
	}
	if strings.TrimSpace(r.TransportID) == "" {
		return fmt.Errorf("arupa: route %q transport id is required", r.ID)
	}
	if (r.HTTP == nil) == (r.SocketIO == nil) {
		return fmt.Errorf("arupa: route %q must declare exactly one route kind", r.ID)
	}
	if r.HTTP != nil && strings.TrimSpace(r.HTTP.Pattern) == "" {
		return fmt.Errorf("arupa: http route %q pattern is required", r.ID)
	}
	if r.SocketIO != nil {
		if strings.TrimSpace(r.SocketIO.Namespace) == "" {
			return fmt.Errorf("arupa: socket.io route %q namespace is required", r.ID)
		}
		for i, event := range r.SocketIO.Events {
			if strings.TrimSpace(event) == "" {
				return fmt.Errorf("arupa: socket.io route %q event %d is empty", r.ID, i)
			}
		}
		for event := range r.SocketIO.EventAccess {
			if strings.TrimSpace(event) == "" {
				return fmt.Errorf("arupa: socket.io route %q has an empty event access key", r.ID)
			}
		}
	}
	return nil
}

// RegistrationFailure describes one item rejected by a batch operation.
type RegistrationFailure struct {
	ID    string
	Error string
}

// RegistrationResult preserves both successful and failed items returned by
// the host. Message is the host's top-level error text; the separate Go error
// returned by ResourceRegistrar is reserved for transport and encoding errors.
type RegistrationResult struct {
	Registered []string
	Failures   []RegistrationFailure
	Degraded   bool
	Message    string
}

// Clone returns an independent copy of the result.
func (r RegistrationResult) Clone() RegistrationResult {
	return RegistrationResult{
		Registered: append([]string(nil), r.Registered...),
		Failures:   append([]RegistrationFailure(nil), r.Failures...),
		Degraded:   r.Degraded,
		Message:    r.Message,
	}
}

// Successful reports whether the host reported no failed or degraded work.
func (r RegistrationResult) Successful() bool {
	return !r.Degraded && r.Message == "" && len(r.Failures) == 0
}

// ResourceRegistrar manages service-owned transports and routes.
type ResourceRegistrar interface {
	RegisterTransport(context.Context, Transport) (RegistrationResult, error)
	UnregisterTransport(context.Context, string) (RegistrationResult, error)
	RegisterRoutes(context.Context, []Route) (RegistrationResult, error)
	UnregisterRoutes(context.Context, []string) (RegistrationResult, error)
}
