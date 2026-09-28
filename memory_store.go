package gotelemetryingest

import (
	"sort"
	"sync"
	"time"
)

// MemoryStore 是纯内存的 Store 实现。
// 它与 FileStore 保持相同的 CAS 语义，用于测试与无需持久化的部署。
type MemoryStore struct {
	mu      sync.Mutex
	devices map[string]*deviceState
}

// NewMemoryStore 创建内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{devices: make(map[string]*deviceState)}
}

func (m *MemoryStore) RegisterDevice(d Device, now time.Time) (*deviceState, error) {
	if d.ID == "" {
		return nil, ErrInvalidArgument
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.devices[d.ID]; ok {
		return nil, ErrDeviceExists
	}
	st := newDeviceState(d)
	if st.RegisteredAt.IsZero() {
		st.RegisteredAt = now
	}
	m.devices[d.ID] = st
	return st.clone(), nil
}

func (m *MemoryStore) Load(id string) (*deviceState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.devices[id]
	if !ok {
		return nil, ErrNotFound
	}
	return st.clone(), nil
}

func (m *MemoryStore) UpdateDevice(prev, next *deviceState) error {
	if prev == nil || next == nil || prev.ID != next.ID {
		return ErrInvalidArgument
	}
	if next.Version != prev.Version+1 {
		return ErrConflictVersion
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.devices[prev.ID]
	if !ok {
		return ErrNotFound
	}
	if cur.Version != prev.Version {
		return ErrConflictVersion
	}
	m.devices[prev.ID] = next.clone()
	return nil
}

func (m *MemoryStore) ListDevices() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.devices))
	for id := range m.devices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (m *MemoryStore) Close() error { return nil }
