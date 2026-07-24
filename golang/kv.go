package arupa

import (
	"context"
	"fmt"
)

// KVClient invokes the host KV API. KVList with an empty namespace lists
// namespace names; other operations address keys within a namespace. Service
// code that only needs its own namespace should normally use KVStore.
type KVClient interface {
	KVGet(context.Context, string, string) ([]byte, bool, error)
	KVSet(context.Context, string, string, []byte) error
	KVDelete(context.Context, string, string) error
	KVList(context.Context, string) ([]string, error)
}

// KVStore provides access to one non-empty KV namespace.
type KVStore interface {
	Get(context.Context, string) ([]byte, bool, error)
	Set(context.Context, string, []byte) error
	Delete(context.Context, string) error
	List(context.Context) ([]string, error)
}

type scopedKV struct {
	client    KVClient
	namespace string
}

// NewKVStore scopes client to namespace. Operations reject an empty namespace
// so services cannot accidentally access the host-wide namespace listing.
func NewKVStore(client KVClient, namespace string) KVStore {
	return scopedKV{client: client, namespace: namespace}
}

func (s scopedKV) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if err := s.validate(key); err != nil {
		return nil, false, err
	}
	value, found, err := s.client.KVGet(ctx, s.namespace, key)
	if err != nil {
		return nil, false, err
	}
	return append([]byte(nil), value...), found, nil
}

func (s scopedKV) Set(ctx context.Context, key string, value []byte) error {
	if err := s.validate(key); err != nil {
		return err
	}
	return s.client.KVSet(ctx, s.namespace, key, append([]byte(nil), value...))
}

func (s scopedKV) Delete(ctx context.Context, key string) error {
	if err := s.validate(key); err != nil {
		return err
	}
	return s.client.KVDelete(ctx, s.namespace, key)
}

func (s scopedKV) List(ctx context.Context) ([]string, error) {
	if err := s.validateNamespace(); err != nil {
		return nil, err
	}
	keys, err := s.client.KVList(ctx, s.namespace)
	if err != nil {
		return nil, err
	}
	return append([]string(nil), keys...), nil
}

func (s scopedKV) validate(key string) error {
	if err := s.validateNamespace(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("arupa: kv key is required")
	}
	return nil
}

func (s scopedKV) validateNamespace() error {
	if s.client == nil {
		return fmt.Errorf("arupa: kv client is nil")
	}
	if s.namespace == "" {
		return fmt.Errorf("arupa: kv namespace is required")
	}
	return nil
}
