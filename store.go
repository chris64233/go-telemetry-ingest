package gotelemetryingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
)

var (
	ErrDeviceNotFound = errors.New("device not found")
	ErrDeviceExists   = errors.New("device already exists")
)

// state 是全部可持久化状态。一次 mutate 内的所有变更（确认位置、汇总、
// 事件、拒绝记录、版本号）在同一次快照写盘中落盘，构成同一个持久化边界。
type state struct {
	Devices    map[string]*Device          `json:"devices"`
	Events     map[string]map[uint64]Event `json:"events"`
	Rejections map[string][]Rejection      `json:"rejections"`
}

func newState() *state {
	return &state{
		Devices:    make(map[string]*Device),
		Events:     make(map[string]map[uint64]Event),
		Rejections: make(map[string][]Rejection),
	}
}

func (s *state) ensure() {
	if s.Devices == nil {
		s.Devices = make(map[string]*Device)
	}
	if s.Events == nil {
		s.Events = make(map[string]map[uint64]Event)
	}
	if s.Rejections == nil {
		s.Rejections = make(map[string][]Rejection)
	}
}

// Store 是线程安全的持久化存储。path 为空时仅保存在内存。
// 所有写操作经 mutate 在互斥锁内完成并原子落盘（临时文件 + rename），
// 因此并发填补相邻缺口不会丢失推进结果，重启后也不会重复累加。
type Store struct {
	mu   sync.Mutex
	path string
	st   *state
}

// OpenStore 打开（或创建）位于 path 的存储；path 为空字符串时使用纯内存存储。
func OpenStore(path string) (*Store, error) {
	s := &Store{path: path, st: newState()}
	if path == "" {
		return s, nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read store: %w", err)
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, s.st); err != nil {
			return nil, fmt.Errorf("decode store: %w", err)
		}
	}
	s.st.ensure()
	return s, nil
}

// mutate 在写锁内执行 fn；fn 返回 nil 时把整体状态原子落盘。
// fn 返回错误时不产生任何持久化变更。
func (s *Store) mutate(fn func(st *state) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := fn(s.st); err != nil {
		return err
	}
	return s.persistLocked()
}

// view 在读锁内执行 fn。fn 内不得修改状态，返回值需自行拷贝。
func (s *Store) view(fn func(st *state)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(s.st)
}

func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	data, err := json.Marshal(s.st)
	if err != nil {
		return fmt.Errorf("encode store: %w", err)
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("commit store: %w", err)
	}
	return nil
}
