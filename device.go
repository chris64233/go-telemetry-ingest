package gotelemetryingest

import "time"

// Event 是设备上报的一条遥测事件。
// 同一设备内 Seq 严格递增；EventTime 为事件在设备端发生的时间。
type Event struct {
	DeviceID  string    `json:"device_id"`
	Seq       uint64    `json:"seq"`
	EventTime time.Time `json:"event_time"`
	Value     float64   `json:"value"`
	Payload   string    `json:"payload,omitempty"`
}

// contentEqual 判断两条同序号事件的内容是否一致（用于幂等去重）。
func (e Event) contentEqual(o Event) bool {
	return e.EventTime.Equal(o.EventTime) &&
		e.Value == o.Value &&
		e.Payload == o.Payload
}

// Summary 是设备的累计汇总，只包含连续确认位置之前的事件，恰好累加一次。
type Summary struct {
	Count          int64      `json:"count"`
	TotalValue     float64    `json:"total_value"`
	FirstEventTime *time.Time `json:"first_event_time,omitempty"`
	LastEventTime  *time.Time `json:"last_event_time,omitempty"`
}

func (s *Summary) add(ev Event) {
	s.Count++
	s.TotalValue += ev.Value
	if s.FirstEventTime == nil || ev.EventTime.Before(*s.FirstEventTime) {
		t := ev.EventTime
		s.FirstEventTime = &t
	}
	if s.LastEventTime == nil || ev.EventTime.After(*s.LastEventTime) {
		t := ev.EventTime
		s.LastEventTime = &t
	}
}

// Device 是设备登记及其流状态的持久化记录。
type Device struct {
	ID         string `json:"id"`
	WindowSize uint64 `json:"window_size"`
	// Ack 是连续确认位置：序号 <= Ack 的事件已全部收到并计入汇总。
	Ack uint64 `json:"ack"`
	// Watermark 是关闭水位：事件时间 <= Watermark 的事件视为迟到并被拒绝。
	Watermark time.Time `json:"watermark"`
	// Version 是设备版本号，每次状态变更递增，用于水位推进的乐观并发控制。
	Version uint64  `json:"version"`
	Summary Summary `json:"summary"`
}

// 拒绝原因。
const (
	ReasonLateEvent    = "late_event"    // 事件时间不晚于已关闭水位
	ReasonBeyondWindow = "beyond_window" // 序号超出乱序窗口
)

// Rejection 记录一条被拒绝的事件及其原因，拒绝不影响已完成的汇总。
type Rejection struct {
	DeviceID  string    `json:"device_id"`
	Seq       uint64    `json:"seq"`
	EventTime time.Time `json:"event_time"`
	Reason    string    `json:"reason"`
	// DeviceVersion 是做出拒绝判定时的设备版本，保证与水位推进基于同一版本得出确定结果。
	DeviceVersion uint64    `json:"device_version"`
	RecordedAt    time.Time `json:"recorded_at"`
}

// 事件接收结果状态。
const (
	StatusAccepted  = "accepted"
	StatusDuplicate = "duplicate"
	StatusRejected  = "rejected"
)

// IngestResult 是一次事件接收的结果。
type IngestResult struct {
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Ack     uint64 `json:"ack"`
	Version uint64 `json:"version"`
}

// GapReport 描述设备当前连续确认位置之后的缺口与已缓冲事件。
type GapReport struct {
	DeviceID string   `json:"device_id"`
	Ack      uint64   `json:"ack"`
	MaxSeq   uint64   `json:"max_seq"`
	Missing  []uint64 `json:"missing"`
	Buffered []uint64 `json:"buffered"`
}
