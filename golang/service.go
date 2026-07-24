// Package arupa contains backend-neutral building blocks for Arupa services.
package arupa

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// ContractVersion is the service contract implemented by this SDK.
const ContractVersion = 2

// ServiceInfo is the stable identity returned by Service.Register.
type ServiceInfo struct {
	Name    string
	Version string
}

// Validate verifies the identity fields required by the kernel.
func (i ServiceInfo) Validate() error {
	if strings.TrimSpace(i.Name) == "" {
		return fmt.Errorf("arupa: service name is required")
	}
	if strings.TrimSpace(i.Version) == "" {
		return fmt.Errorf("arupa: service version is required")
	}
	return nil
}

// RegisterHook runs while a service is registering, after its HostClient,
// initial Params snapshot, and RegisterContext are available. Returning an
// error rejects registration.
type RegisterHook func(context.Context) error

// InheritedListener describes a listener inherited by a native service.
// FD is meaningful in the child process that received the registration.
type InheritedListener struct {
	ID      string
	FD      uint32
	Network string
	Address string
}

// Validate verifies the metadata required to identify an inherited listener.
func (l InheritedListener) Validate() error {
	if strings.TrimSpace(l.ID) == "" {
		return fmt.Errorf("arupa: inherited listener id is required")
	}
	if l.FD < 3 {
		return fmt.Errorf("arupa: inherited listener %q fd must be at least 3", l.ID)
	}
	if strings.TrimSpace(l.Network) == "" {
		return fmt.Errorf("arupa: inherited listener %q network is required", l.ID)
	}
	if strings.TrimSpace(l.Address) == "" {
		return fmt.Errorf("arupa: inherited listener %q address is required", l.ID)
	}
	return nil
}

// RegisterContext is the backend-neutral startup state supplied by the
// kernel. Transport-specific connection details, such as a gRPC broker ID,
// are consumed by the corresponding backend before this value is stored.
type RegisterContext struct {
	InstanceID string
	Params     map[string]string
	Listeners  []InheritedListener
}

// Validate verifies the startup identity and listener metadata.
func (r RegisterContext) Validate() error {
	if strings.TrimSpace(r.InstanceID) == "" {
		return fmt.Errorf("arupa: service instance id is required")
	}
	seen := make(map[string]struct{}, len(r.Listeners))
	for i, listener := range r.Listeners {
		if err := listener.Validate(); err != nil {
			return fmt.Errorf("arupa: inherited listener %d: %w", i, err)
		}
		if _, exists := seen[listener.ID]; exists {
			return fmt.Errorf("arupa: inherited listener id %q is duplicated", listener.ID)
		}
		seen[listener.ID] = struct{}{}
	}
	return nil
}

// Clone returns a deep-enough copy for independent use by application code.
func (r RegisterContext) Clone() RegisterContext {
	return RegisterContext{
		InstanceID: r.InstanceID,
		Params:     CloneParams(r.Params),
		Listeners:  append([]InheritedListener(nil), r.Listeners...),
	}
}

// Listener returns the inherited listener with id.
func (r RegisterContext) Listener(id string) (InheritedListener, bool) {
	for _, listener := range r.Listeners {
		if listener.ID == id {
			return listener, true
		}
	}
	return InheritedListener{}, false
}

// RegisterSnapshot stores the most recently received RegisterContext.
type RegisterSnapshot struct {
	mu      sync.RWMutex
	context RegisterContext
}

// Store replaces the snapshot with an independent copy.
func (s *RegisterSnapshot) Store(registerContext RegisterContext) {
	s.mu.Lock()
	s.context = registerContext.Clone()
	s.mu.Unlock()
}

// Load returns an independent copy of the current snapshot.
func (s *RegisterSnapshot) Load() RegisterContext {
	if s == nil {
		return RegisterContext{Params: map[string]string{}}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.context.Clone()
}
