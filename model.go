package gotelemetryingest

import (
	"errors"
	"time"
)

// 拒绝原因。
const (
	// ReasonConflict 相同序号对应不同内容。
	ReasonConflict = "conflict"
	// ReasonOutOfWindow 序号超出乱序窗口（seq > acked+1+window）。
	ReasonOutOfWindow = "out_of_window"
	// ReasonLate 序号已被关闭水位永久关闭（seq <= closed）。
	ReasonLate = "late"
)

// 事件接收后的状态分类。
const (
	// ResultAcked 事件已经（在本次或此前）计入连续确认区间。
	ResultAcked = "acked"
	// ResultBuffered 事件落入窗口内但前方仍有缺口，暂存待确认。
	ResultBuffered = "buffered"
	// ResultRejected 事件被拒绝，详见 Rejection.Reason。
	ResultRejected = "rejected"
)

// ErrNotFound 设备不存在。
var ErrNotFound = errors.New("device not found")

// ErrDeviceExists 设备已登记。
var ErrDeviceExists = errors.New("device already registered")

// ErrInvalidArgument 入参非法。
var ErrInvalidArgument = errors.New("invalid argument")

// Event 是设备提交的原始遥测事件。
type Event struct {
	// Sequence 设备内严格递增的序号，从 1 开始。
	Sequence int64 `json:"sequence"`
	// EventTime 设备端产生事件的时间，必须随序号严格递增。
	EventTime time.Time `json:"eventTime"`
	// Metric 指标名，非空。
	Metric string `json:"metric"`
	// Value 指标值。
	Value float64 `json:"value"`
}

// contentHash 已移除：去重/冲突直接用 sameContent 比较事件时间、指标与值。

// sameContent 判断两个事件内容（事件时间、指标、值）是否一致。
func (e Event) sameContent(o Event) bool {
	return e.EventTime.Equal(o.EventTime) &&
		e.Metric == o.Metric &&
		e.Value == o.Value
}

// Device 是登记时的设备配置。
type Device struct {
	ID string `json:"id"`
	// Window 乱序窗口大小：下一个期望序号 acked+1 之外，还允许提前
	// window 个序号到达（接收上沿为 acked+1+window）。
	// 0 表示不允许任何乱序（只收下一个紧邻序号）。
	Window int64 `json:"window"`
	// RegisteredAt 登记时间。
	RegisteredAt time.Time `json:"registeredAt"`
}

// Summary 是设备的汇总，只统计已连续确认的事件（恰好一次）。
type Summary struct {
	DeviceID       string    `json:"deviceId"`
	AckedSeq       int64     `json:"ackedSeq"`
	ClosedSeq      int64     `json:"closedSeq"`
	EventCount     int64     `json:"eventCount"`
	Sum            float64   `json:"sum"`
	Min            float64   `json:"min"`
	Max            float64   `json:"max"`
	FirstEventTime time.Time `json:"firstEventTime"`
	LastEventTime  time.Time `json:"lastEventTime"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

// storedEvent 保存原始事件及其首次到达的处理结果。
type storedEvent struct {
	Event Event `json:"event"`
	// FirstResult 首次接收时的处理结果：acked / buffered。
	// 事件随后由 buffered 变为 acked 时，首次提交得到的仍是 buffered。
	FirstResult string    `json:"firstResult"`
	ReceivedAt  time.Time `json:"receivedAt"`
}

// SealedGap 记录被关闭水位永久封闭的缺口。
// 缺口 [Start, End] 内的序号永远不会再计入汇总。
type SealedGap struct {
	Start    int64     `json:"start"`
	End      int64     `json:"end"`
	SealedAt time.Time `json:"sealedAt"`
}

// Rejection 保留被拒绝事件的原因与提交内容，不影响汇总。
type Rejection struct {
	Sequence   int64     `json:"sequence"`
	EventTime  time.Time `json:"eventTime"`
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`
	Reason     string    `json:"reason"`
	Detail     string    `json:"detail"`
	RejectedAt time.Time `json:"rejectedAt"`
}

// Gap 描述一个缺口。Sealed=true 表示该缺口已被水位关闭、无法再填补。
type Gap struct {
	Start  int64 `json:"start"`
	End    int64 `json:"end"`
	Sealed bool  `json:"sealed"`
}

// GapInfo 是缺口查询结果。
type GapInfo struct {
	DeviceID  string `json:"deviceId"`
	AckedSeq  int64  `json:"ackedSeq"`
	ClosedSeq int64  `json:"closedSeq"`
	// Open 仍可由事件填补的缺口（按序号升序）。
	Open []Gap `json:"open"`
	// Sealed 已被关闭水位永久封闭的缺口（按序号升序）。
	Sealed []Gap `json:"sealed"`
}

// EventRecord 是原始事件查询返回的一条记录。
type EventRecord struct {
	Event       Event  `json:"event"`
	FirstResult string `json:"firstResult"`
	// Acknowledged 表示该事件当前是否已计入连续确认区间。
	Acknowledged bool      `json:"acknowledged"`
	ReceivedAt   time.Time `json:"receivedAt"`
}

// IngestOutcome 是事件接收的处理结果。
type IngestOutcome struct {
	DeviceID string `json:"deviceId"`
	// Status: acked / buffered / rejected。
	Status string `json:"status"`
	// AckedSeq 本次处理之后设备的连续确认位置。
	AckedSeq int64 `json:"ackedSeq"`
	// Duplicate 标识该响应来自一次重复提交（幂等返回原结果）。
	Duplicate bool `json:"duplicate"`
	// Reason 当 Status=rejected 时的拒绝原因。
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// AdvanceResult 是水位推进结果。
type AdvanceResult struct {
	DeviceID  string `json:"deviceId"`
	ClosedSeq int64  `json:"closedSeq"`
	AckedSeq  int64  `json:"ackedSeq"`
	// NewlySealed 本次推进新封闭的缺口。
	NewlySealed []Gap `json:"newlySealed"`
	// Advanced 为 false 表示目标水位不高于当前水位（幂等空操作）。
	Advanced bool `json:"advanced"`
}
