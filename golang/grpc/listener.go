package grpc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/SteelDrEgg/arupa-sdk/golang"
)

// InheritedListener owns one listener passed to a gRPC service by file
// descriptor. It implements net.Listener and makes Close idempotent.
//
// The original inherited descriptor is closed as soon as net.FileListener
// creates its managed copy. The returned listener is then the sole descriptor
// owned by the SDK.
type InheritedListener struct {
	descriptor arupa.InheritedListener
	listener   net.Listener

	closeOnce sync.Once
	closeErr  error
}

var _ net.Listener = (*InheritedListener)(nil)

func openInheritedListener(descriptor arupa.InheritedListener) (*InheritedListener, error) {
	return openInheritedListenerWith(descriptor, net.FileListener)
}

func openInheritedListenerWith(
	descriptor arupa.InheritedListener,
	fromFile func(*os.File) (net.Listener, error),
) (*InheritedListener, error) {
	descriptor.ID = strings.TrimSpace(descriptor.ID)
	descriptor.Network = strings.TrimSpace(descriptor.Network)
	if descriptor.ID == "" {
		return nil, fmt.Errorf("arupa/grpc: inherited listener id is required")
	}
	// Never allow a malformed RegisterRequest to claim stdin, stdout, or
	// stderr as a network listener.
	if descriptor.FD < 3 {
		return nil, fmt.Errorf("arupa/grpc: inherited listener %q has unsafe fd %d", descriptor.ID, descriptor.FD)
	}
	if fromFile == nil {
		return nil, fmt.Errorf("arupa/grpc: inherited listener factory is nil")
	}

	file := os.NewFile(uintptr(descriptor.FD), "arupa-listener-"+descriptor.ID)
	if file == nil {
		return nil, fmt.Errorf("arupa/grpc: inherited listener %q has invalid fd %d", descriptor.ID, descriptor.FD)
	}
	listener, err := fromFile(file)
	closeErr := file.Close()
	if err != nil {
		return nil, fmt.Errorf("arupa/grpc: open inherited listener %q: %w", descriptor.ID, err)
	}
	if closeErr != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("arupa/grpc: close inherited listener %q source fd: %w", descriptor.ID, closeErr)
	}
	if descriptor.Network != "" && listener.Addr() != nil &&
		descriptor.Network != listener.Addr().Network() {
		_ = listener.Close()
		return nil, fmt.Errorf(
			"arupa/grpc: inherited listener %q network is %q, fd is %q",
			descriptor.ID,
			descriptor.Network,
			listener.Addr().Network(),
		)
	}
	return &InheritedListener{descriptor: descriptor, listener: listener}, nil
}

// Descriptor returns an independent copy of the Host-provided listener
// metadata. FD is informational: ownership has moved to this listener.
func (l *InheritedListener) Descriptor() arupa.InheritedListener {
	if l == nil {
		return arupa.InheritedListener{}
	}
	return l.descriptor
}

func (l *InheritedListener) Accept() (net.Conn, error) {
	if l == nil || l.listener == nil {
		return nil, net.ErrClosed
	}
	return l.listener.Accept()
}

func (l *InheritedListener) Addr() net.Addr {
	if l == nil || l.listener == nil {
		return nil
	}
	return l.listener.Addr()
}

func (l *InheritedListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		if l.listener != nil {
			l.closeErr = l.listener.Close()
		}
	})
	return l.closeErr
}

type listenerSet struct {
	mu sync.RWMutex

	listeners map[string]*InheritedListener
	closed    bool

	closeOnce sync.Once
	closeErr  error
}

func buildListenerMap(descriptors []arupa.InheritedListener) (map[string]*InheritedListener, error) {
	listeners := make(map[string]*InheritedListener, len(descriptors))
	for index, descriptor := range descriptors {
		id := strings.TrimSpace(descriptor.ID)
		if _, exists := listeners[id]; exists {
			closeListenerMap(listeners)
			return nil, fmt.Errorf("arupa/grpc: inherited listener %d duplicates id %q", index, id)
		}
		listener, err := openInheritedListener(descriptor)
		if err != nil {
			closeListenerMap(listeners)
			return nil, err
		}
		listeners[id] = listener
	}
	return listeners, nil
}

func (s *listenerSet) replace(next map[string]*InheritedListener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errors.Join(
			fmt.Errorf("arupa/grpc: service is closed"),
			closeListenerMap(next),
		)
	}
	previous := s.listeners
	s.listeners = next
	s.mu.Unlock()
	// The replacement is already installed. A stale listener close error must
	// not make Register report failure with committed new state.
	_ = closeListenerMap(previous)
	return nil
}

func (s *listenerSet) get(id string) (*InheritedListener, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	listener, ok := s.listeners[strings.TrimSpace(id)]
	return listener, ok
}

func (s *listenerSet) descriptors() []arupa.InheritedListener {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.listeners))
	for id := range s.listeners {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]arupa.InheritedListener, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.listeners[id].Descriptor())
	}
	return out
}

func (s *listenerSet) clear() error {
	s.mu.Lock()
	current := s.listeners
	s.listeners = nil
	s.mu.Unlock()
	return closeListenerMap(current)
}

func (s *listenerSet) close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		current := s.listeners
		s.listeners = nil
		s.mu.Unlock()
		s.closeErr = closeListenerMap(current)
	})
	return s.closeErr
}

func closeListenerMap(listeners map[string]*InheritedListener) error {
	var joined error
	for _, listener := range listeners {
		joined = errors.Join(joined, listener.Close())
	}
	return joined
}
