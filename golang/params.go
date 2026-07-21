package arupa

import (
	"context"
	"sync"
)

// ParamsPatch applies independent additions, replacements, and deletions to a
// plugin's persisted Params override. A key present in both fields is set.
type ParamsPatch struct {
	Set    map[string]string
	Delete []string
}

// ParamsClient reads and updates the current Params for the calling plugin.
// The host determines the caller identity from the authenticated callback.
type ParamsClient interface {
	Params(context.Context) (map[string]string, error)
	PatchParams(context.Context, ParamsPatch) error
}

// CloneParams returns an independent copy of params.
func CloneParams(params map[string]string) map[string]string {
	if len(params) == 0 {
		return map[string]string{}
	}
	copy := make(map[string]string, len(params))
	for key, value := range params {
		copy[key] = value
	}
	return copy
}

// ParamsSnapshot stores a concurrency-safe copy of the Params received at
// registration. It is a startup snapshot; use ParamsClient.Params to read
// the current host value.
type ParamsSnapshot struct {
	mu     sync.RWMutex
	params map[string]string
}

// Store replaces the snapshot with a copy of params.
func (s *ParamsSnapshot) Store(params map[string]string) {
	s.mu.Lock()
	s.params = CloneParams(params)
	s.mu.Unlock()
}

// Load returns an independent copy of the snapshot.
func (s *ParamsSnapshot) Load() map[string]string {
	if s == nil {
		return map[string]string{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CloneParams(s.params)
}
