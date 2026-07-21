// Package arupa contains framework-neutral building blocks for Arupa plugins.
package arupa

import (
	"context"
	"fmt"
)

// RegisterHook runs while a plugin is registering, after its host capabilities
// and initial Params snapshot are available. Returning an error rejects the
// registration.
//
// The hook may use the Plugin's Params method to read the current effective
// host configuration, or InitialParams to read the received startup snapshot.
type RegisterHook func(context.Context) error

// AccessPolicy is the host-side authorization policy for an ingress route or
// Socket.IO namespace.
type AccessPolicy struct {
	RequireAuth bool
	Groups      []string
}

// HTTPRoute declares an ingress path that the host can authorize and forward
// to a plugin. It is not an application framework route.
type HTTPRoute struct {
	Method  string
	Pattern string
	Access  AccessPolicy
}

// StaticMount declares a static path served by the host.
type StaticMount struct {
	Prefix    string
	Directory string
	Access    AccessPolicy
}

// SocketNamespace declares a Socket.IO namespace handled by the plugin.
type SocketNamespace struct {
	Name        string
	Events      []string
	Access      AccessPolicy
	EventAccess map[string]AccessPolicy
}

// Registration declares the information returned from Plugin.Register.
type Registration struct {
	Name             string
	Version          string
	HTTPRoutes       []HTTPRoute
	SocketNamespaces []SocketNamespace
	StaticMounts     []StaticMount
}

// Validate verifies the minimum information required to register a plugin.
func (r Registration) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("arupa: plugin name is required")
	}
	if r.Version == "" {
		return fmt.Errorf("arupa: plugin version is required")
	}
	for i, route := range r.HTTPRoutes {
		if route.Pattern == "" {
			return fmt.Errorf("arupa: http route %d has an empty pattern", i)
		}
	}
	for i, namespace := range r.SocketNamespaces {
		if namespace.Name == "" {
			return fmt.Errorf("arupa: socket namespace %d has an empty name", i)
		}
	}
	return nil
}
