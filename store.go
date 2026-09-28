package gotelemetryingest

import (
	"errors"
	"time"
)

// ErrConflictVersion 并发修改冲突：提交所依据的设备版本已过期。
// 服务层捕获该错误后基于最新版本重试（CAS）。
var ErrConflictVersion = errors.New("device version conflict")

// deviceState 是一台设备的完整持久化状态。
//
// 汇总（Summary）与确认位置（AckedSeq）、关闭水位（ClosedSeq）、
// 已确认事件、被封缺口、拒绝记录全部包含在同一结构中，因此一次
// Commit 就能把它们放进同一个持久化事务边界（同一 WAL 帧）。
type deviceState struct {
	ID           string    `json:"id"`
	Window       int64     `json:"window"`
	RegisteredAt time.Time `json:"registeredAt"`
	Version      int64     `json:"version"`
	AckedSeq     int64     `json:"ackedSeq"`
	ClosedSeq    int64     `json:"closedSeq"`
	// Events 保存所有原始事件（含窗口内暂存的），键为序号。
	Events map[int64]*storedEvent `json:"events"`
	// SealedGaps 已被水位永久封闭的缺口（按 Start 升序）。
	SealedGaps []SealedGap `json:"sealedGaps"`
	// Rejections 被拒绝的提交及原因（按时间追加）。
	Rejections []Rejection `json:"rejections"`
	// Summary 恰好一次累计的汇总；其推进与 AckedSeq 在同一帧提交。
	Sum        float64   `json:"sum"`
	Min        float64   `json:"min"`
	Max        float64   `json:"max"`
	EventCount int64     `json:"eventCount"`
	FirstTime  time.Time `json:"firstTime"`
	LastTime   time.Time `json:"lastTime"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

func newDeviceState(d Device) *deviceState {
	return &deviceState{
		ID:           d.ID,
		Window:       d.Window,
		RegisteredAt: d.RegisteredAt,
		Version:      1,
		Events:       make(map[int64]*storedEvent),
	}
}

func (s *deviceState) clone() *deviceState {
	c := *s
	c.Events = make(map[int64]*storedEvent, len(s.Events))
	for k, v := range s.Events {
		e := *v
		c.Events[k] = &e
	}
	c.SealedGaps = append([]SealedGap(nil), s.SealedGaps...)
	c.Rejections = append([]Rejection(nil), s.Rejections...)
	return &c
}

// summary 从持久化状态构造对外的汇总视图。
func (s *deviceState) summary() Summary {
	return Summary{
		DeviceID:       s.ID,
		AckedSeq:       s.AckedSeq,
		ClosedSeq:      s.ClosedSeq,
		EventCount:     s.EventCount,
		Sum:            s.Sum,
		Min:            s.Min,
		Max:            s.Max,
		FirstEventTime: s.FirstTime,
		LastEventTime:  s.LastTime,
		UpdatedAt:      s.UpdatedAt,
	}
}

// Store 是设备状态的持久化抽象。
//
// 所有读-改-写都通过 Load/UpdateDevice 完成：UpdateDevice 以期望版本做
// CAS，保证「水位推进与迟到事件并发」等场景下双方依据同一设备版本得到
// 确定结果（线性化），落后的一方收到 ErrConflictVersion 后重试。
type Store interface {
	// RegisterDevice 登记新设备。已存在时返回 ErrDeviceExists。
	RegisterDevice(d Device, now time.Time) (*deviceState, error)
	// Load 返回设备状态的独立副本；不存在返回 ErrNotFound。
	Load(id string) (*deviceState, error)
	// UpdateDevice 在同一个持久化事务中替换设备状态。
	// 仅当 next.Version == prev+1 且存储中的当前版本仍为 prev 时提交成功，
	// 否则返回 ErrConflictVersion。
	UpdateDevice(prev, next *deviceState) error
	// ListDevices 列出所有已登记设备 ID。
	ListDevices() ([]string, error)
	// Close 释放底层资源。
	Close() error
}
