package gotelemetryingest

import (
	"errors"
	"math"
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
	// Calibrations 标定版本历史，按生效序号升序；首个元素恒为基线版本。
	Calibrations []Calibration `json:"calibrations"`
	// CalAggs 各标定版本已确认读数的聚合，键为标定号；与读数在同一帧累计。
	CalAggs map[string]*CalibrationAggregate `json:"calAggs"`
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
		Calibrations: []Calibration{{
			ID:           BaselineCalibrationID,
			EffectiveSeq: 1,
			Operator:     "system",
			AppliedAt:    d.RegisteredAt,
		}},
		CalAggs: map[string]*CalibrationAggregate{
			BaselineCalibrationID: {
				CalibrationID: BaselineCalibrationID,
				EffectiveSeq:  1,
			},
		},
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
	c.Calibrations = append([]Calibration(nil), s.Calibrations...)
	if s.CalAggs != nil {
		c.CalAggs = make(map[string]*CalibrationAggregate, len(s.CalAggs))
		for k, v := range s.CalAggs {
			a := *v
			c.CalAggs[k] = &a
		}
	}
	return &c
}

// summary 从持久化状态构造对外的汇总视图。
func (s *deviceState) summary() Summary {
	sum := Summary{
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
	cur := currentCalibration(s)
	sum.CurrentCalibrationID = cur.ID
	for i, c := range s.Calibrations {
		agg, ok := s.CalAggs[c.ID]
		if !ok {
			agg = &CalibrationAggregate{
				CalibrationID:   c.ID,
				EffectiveSeq:    c.EffectiveSeq,
				ParameterDigest: c.ParameterDigest,
			}
		}
		entry := *agg
		entry.CalibrationID = c.ID
		entry.EffectiveSeq = c.EffectiveSeq
		entry.ParameterDigest = c.ParameterDigest
		entry.Current = c.ID == cur.ID
		// 该版本的生效区间 [effectiveSeq, end] 完全落入关闭水位之下时，
		// 聚合永久冻结：后续迟到读数只能被记录为 late，不能回写该聚合。
		end := int64(math.MaxInt64)
		if i+1 < len(s.Calibrations) {
			end = s.Calibrations[i+1].EffectiveSeq - 1
		}
		entry.Frozen = end <= s.ClosedSeq
		sum.History = append(sum.History, entry)
		if entry.Current {
			sum.Current = entry
		}
	}
	return sum
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
