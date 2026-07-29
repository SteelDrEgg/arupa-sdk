package arupa

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	// SysNamespace contains read-only kernel projections available to services.
	SysNamespace = "sys"

	// SysServicePrefix identifies running-service records in SysNamespace.
	SysServicePrefix = "service/"

	// SysCatalogPrefix identifies discovered-package records in SysNamespace.
	SysCatalogPrefix = SysServicePrefix + "catalog/"
)

// ServiceRecord is the kernel's externally visible projection of a running
// service, published at SysServicePrefix + InstanceID.
type ServiceRecord struct {
	InstanceID string      `json:"instance_id"`
	Name       string      `json:"name"`
	Version    string      `json:"version"`
	Type       string      `json:"type"`
	Path       string      `json:"path"`
	Transports []Transport `json:"transports,omitempty"`
	Routes     []Route     `json:"routes,omitempty"`
}

// CatalogEntry is metadata discovered from a service package without running
// it. The kernel currently publishes this projection at SysCatalogPrefix +
// Name using its Go field names as JSON keys.
type CatalogEntry struct {
	Name            string         `json:"Name"`
	Version         string         `json:"Version"`
	Type            string         `json:"Type"`
	ContractVersion int            `json:"ContractVersion"`
	Command         string         `json:"Command"`
	Metadata        map[string]any `json:"Metadata"`
	PackagePath     string         `json:"PackagePath"`
}

// SystemStore reads the kernel-owned, read-only projections in SysNamespace.
// Construct one with NewSystemStore after the service has registered.
type SystemStore interface {
	GetServiceRecord(context.Context, string) (*ServiceRecord, bool, error)
	ListServiceRecords(context.Context) ([]ServiceRecord, error)
	GetCatalogEntry(context.Context, string) (*CatalogEntry, bool, error)
	ListCatalogEntries(context.Context) ([]CatalogEntry, error)
}

type systemStore struct {
	client KVClient
}

// NewSystemStore creates a read-only view of the kernel's sys KV namespace.
func NewSystemStore(client KVClient) SystemStore {
	return systemStore{client: client}
}

func (s systemStore) GetServiceRecord(ctx context.Context, instanceID string) (*ServiceRecord, bool, error) {
	key, err := systemKey(SysServicePrefix, instanceID)
	if err != nil {
		return nil, false, err
	}
	var record ServiceRecord
	found, err := s.getJSON(ctx, key, &record)
	if err != nil || !found {
		return nil, found, err
	}
	return &record, true, nil
}

func (s systemStore) ListServiceRecords(ctx context.Context) ([]ServiceRecord, error) {
	keys, err := s.listKeys(ctx, SysServicePrefix, SysCatalogPrefix)
	if err != nil {
		return nil, err
	}
	records := make([]ServiceRecord, 0, len(keys))
	for _, key := range keys {
		var record ServiceRecord
		found, err := s.getJSON(ctx, key, &record)
		if err != nil {
			return nil, fmt.Errorf("arupa: decode system service record %q: %w", key, err)
		}
		if found {
			records = append(records, record)
		}
	}
	return records, nil
}

func (s systemStore) GetCatalogEntry(ctx context.Context, name string) (*CatalogEntry, bool, error) {
	key, err := systemKey(SysCatalogPrefix, name)
	if err != nil {
		return nil, false, err
	}
	var entry CatalogEntry
	found, err := s.getJSON(ctx, key, &entry)
	if err != nil || !found {
		return nil, found, err
	}
	return &entry, true, nil
}

func (s systemStore) ListCatalogEntries(ctx context.Context) ([]CatalogEntry, error) {
	keys, err := s.listKeys(ctx, SysCatalogPrefix, "")
	if err != nil {
		return nil, err
	}
	entries := make([]CatalogEntry, 0, len(keys))
	for _, key := range keys {
		var entry CatalogEntry
		found, err := s.getJSON(ctx, key, &entry)
		if err != nil {
			return nil, fmt.Errorf("arupa: decode system catalog entry %q: %w", key, err)
		}
		if found {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (s systemStore) getJSON(ctx context.Context, key string, destination any) (bool, error) {
	if s.client == nil {
		return false, fmt.Errorf("arupa: system store client is nil")
	}
	value, found, err := s.client.KVGet(ctx, SysNamespace, key)
	if err != nil || !found {
		return found, err
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return true, err
	}
	return true, nil
}

func (s systemStore) listKeys(ctx context.Context, prefix, excludedPrefix string) ([]string, error) {
	if s.client == nil {
		return nil, fmt.Errorf("arupa: system store client is nil")
	}
	keys, err := s.client.KVList(ctx, SysNamespace)
	if err != nil {
		return nil, err
	}
	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		if strings.HasPrefix(key, prefix) && (excludedPrefix == "" || !strings.HasPrefix(key, excludedPrefix)) {
			filtered = append(filtered, key)
		}
	}
	sort.Strings(filtered)
	return filtered, nil
}

func systemKey(prefix, id string) (string, error) {
	if strings.TrimSpace(id) == "" {
		return "", fmt.Errorf("arupa: system record id is required")
	}
	return prefix + id, nil
}
