package gotelemetryingest

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	// ErrConflict 表示相同序号对应不同内容。
	ErrConflict = errors.New("conflicting event for sequence")
	// ErrVersionConflict 表示水位推进所基于的设备版本已过期。
	ErrVersionConflict = errors.New("device version conflict")
	// ErrWatermarkRegression 表示水位不允许回退。
	ErrWatermarkRegression = errors.New("watermark must not move backwards")
)

// Service 提供设备遥测接收与汇总的核心逻辑。
type Service struct {
	store *Store
}

func NewService(store *Store) *Service {
	return &Service{store: store}
}

// RegisterDevice 登记设备。windowSize 为允许的乱序窗口：
// 序号落在 (Ack, Ack+windowSize] 内的事件才会被接收。
func (s *Service) RegisterDevice(id string, windowSize uint64) (*Device, error) {
	if id == "" {
		return nil, errors.New("device id must not be empty")
	}
	if windowSize == 0 {
		return nil, errors.New("window size must be at least 1")
	}
	d := &Device{ID: id, WindowSize: windowSize}
	err := s.store.mutate(func(st *state) error {
		if _, ok := st.Devices[id]; ok {
			return ErrDeviceExists
		}
		st.Devices[id] = d
		return nil
	})
	if err != nil {
		return nil, err
	}
	cp := *d
	return &cp, nil
}

// Ingest 接收一条事件。幂等语义：
//   - 相同序号、相同内容：返回原结果（StatusDuplicate），不产生任何变更；
//   - 相同序号、不同内容：返回 ErrConflict；
//   - 事件时间不晚于关闭水位：记录拒绝（ReasonLateEvent）；
//   - 序号超出乱序窗口：记录拒绝（ReasonBeyondWindow）。
//
// 事件被接收后，若其填补了连续确认位置之后的缺口，则推进 Ack，
// 并把新确认的事件在同一持久化边界内恰好一次地计入汇总。
func (s *Service) Ingest(ev Event) (*IngestResult, error) {
	if ev.DeviceID == "" {
		return nil, errors.New("device id must not be empty")
	}
	var res *IngestResult
	err := s.store.mutate(func(st *state) error {
		d, ok := st.Devices[ev.DeviceID]
		if !ok {
			return ErrDeviceNotFound
		}
		// 按序号去重（含已确认、已汇总的序号，原始事件全部保留）。
		if prev, ok := st.Events[ev.DeviceID][ev.Seq]; ok {
			if prev.contentEqual(ev) {
				res = &IngestResult{Status: StatusDuplicate, Ack: d.Ack, Version: d.Version}
				return nil
			}
			return fmt.Errorf("%w: device %q seq %d", ErrConflict, ev.DeviceID, ev.Seq)
		}
		// 迟到判定与窗口判定都基于当前设备版本下的状态，
		// 与并发的水位推进互斥串行，结果确定。
		if !ev.EventTime.After(d.Watermark) {
			st.Rejections[ev.DeviceID] = append(st.Rejections[ev.DeviceID], Rejection{
				DeviceID:      ev.DeviceID,
				Seq:           ev.Seq,
				EventTime:     ev.EventTime,
				Reason:        ReasonLateEvent,
				DeviceVersion: d.Version,
				RecordedAt:    time.Now(),
			})
			d.Version++
			res = &IngestResult{Status: StatusRejected, Reason: ReasonLateEvent, Ack: d.Ack, Version: d.Version}
			return nil
		}
		if ev.Seq > d.Ack+d.WindowSize {
			st.Rejections[ev.DeviceID] = append(st.Rejections[ev.DeviceID], Rejection{
				DeviceID:      ev.DeviceID,
				Seq:           ev.Seq,
				EventTime:     ev.EventTime,
				Reason:        ReasonBeyondWindow,
				DeviceVersion: d.Version,
				RecordedAt:    time.Now(),
			})
			d.Version++
			res = &IngestResult{Status: StatusRejected, Reason: ReasonBeyondWindow, Ack: d.Ack, Version: d.Version}
			return nil
		}
		// 接收事件并尽可能推进连续确认位置；新确认的事件在此处、
		// 与 Ack 一起在同一事务内计入汇总，保证恰好一次。
		if st.Events[ev.DeviceID] == nil {
			st.Events[ev.DeviceID] = make(map[uint64]Event)
		}
		st.Events[ev.DeviceID][ev.Seq] = ev
		for {
			next, ok := st.Events[ev.DeviceID][d.Ack+1]
			if !ok {
				break
			}
			d.Summary.add(next)
			d.Ack++
		}
		d.Version++
		res = &IngestResult{Status: StatusAccepted, Ack: d.Ack, Version: d.Version}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// AdvanceWatermark 把设备的关闭水位推进到 watermark。
// expectedVersion 必须与当前设备版本一致（乐观并发控制），
// 否则返回 ErrVersionConflict；水位只允许单调前进。
func (s *Service) AdvanceWatermark(deviceID string, watermark time.Time, expectedVersion uint64) (*Device, error) {
	var out Device
	err := s.store.mutate(func(st *state) error {
		d, ok := st.Devices[deviceID]
		if !ok {
			return ErrDeviceNotFound
		}
		if d.Version != expectedVersion {
			return fmt.Errorf("%w: device %q at version %d, expected %d",
				ErrVersionConflict, deviceID, d.Version, expectedVersion)
		}
		if watermark.Before(d.Watermark) {
			return ErrWatermarkRegression
		}
		d.Watermark = watermark
		d.Version++
		out = *d
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Gaps 返回连续确认位置之后的缺口（缺失序号）与已缓冲的乱序事件序号。
func (s *Service) Gaps(deviceID string) (*GapReport, error) {
	rep := &GapReport{DeviceID: deviceID}
	var err error
	s.store.view(func(st *state) {
		d, ok := st.Devices[deviceID]
		if !ok {
			err = ErrDeviceNotFound
			return
		}
		rep.Ack = d.Ack
		events := st.Events[deviceID]
		for seq := range events {
			if seq > rep.MaxSeq {
				rep.MaxSeq = seq
			}
		}
		for seq := d.Ack + 1; seq <= rep.MaxSeq; seq++ {
			if _, ok := events[seq]; ok {
				rep.Buffered = append(rep.Buffered, seq)
			} else {
				rep.Missing = append(rep.Missing, seq)
			}
		}
	})
	if err != nil {
		return nil, err
	}
	return rep, nil
}

// Events 返回设备已接收的全部原始事件，按序号升序。
func (s *Service) Events(deviceID string) ([]Event, error) {
	var out []Event
	var err error
	s.store.view(func(st *state) {
		if _, ok := st.Devices[deviceID]; !ok {
			err = ErrDeviceNotFound
			return
		}
		for _, ev := range st.Events[deviceID] {
			out = append(out, ev)
		}
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// Event 按序号查询一条原始事件。
func (s *Service) Event(deviceID string, seq uint64) (Event, error) {
	var ev Event
	var err error
	s.store.view(func(st *state) {
		if _, ok := st.Devices[deviceID]; !ok {
			err = ErrDeviceNotFound
			return
		}
		e, ok := st.Events[deviceID][seq]
		if !ok {
			err = fmt.Errorf("event not found: device %q seq %d", deviceID, seq)
			return
		}
		ev = e
	})
	return ev, err
}

// Summary 返回设备汇总、连续确认位置与当前版本。
func (s *Service) Summary(deviceID string) (Summary, uint64, uint64, error) {
	var sum Summary
	var ack, version uint64
	var err error
	s.store.view(func(st *state) {
		d, ok := st.Devices[deviceID]
		if !ok {
			err = ErrDeviceNotFound
			return
		}
		sum = d.Summary
		ack = d.Ack
		version = d.Version
	})
	return sum, ack, version, err
}

// GetDevice 返回设备当前状态。
func (s *Service) GetDevice(deviceID string) (Device, error) {
	var d Device
	var err error
	s.store.view(func(st *state) {
		dev, ok := st.Devices[deviceID]
		if !ok {
			err = ErrDeviceNotFound
			return
		}
		d = *dev
	})
	return d, err
}

// Rejections 返回设备全部被拒绝事件及原因。
func (s *Service) Rejections(deviceID string) ([]Rejection, error) {
	var out []Rejection
	var err error
	s.store.view(func(st *state) {
		if _, ok := st.Devices[deviceID]; !ok {
			err = ErrDeviceNotFound
			return
		}
		out = append(out, st.Rejections[deviceID]...)
	})
	return out, err
}
