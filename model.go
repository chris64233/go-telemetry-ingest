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

// ErrCalibrationConflict 相同标定号重放但设备/参数摘要/生效序号/操作者
// 与首次记录不一致。
var ErrCalibrationConflict = errors.New("calibration conflict: same calibration id replayed with different content")

// ErrInvalidArgument 入参非法。
var ErrInvalidArgument = errors.New("invalid argument")

// BaselineCalibrationID 是设备登记时自动写入的初始标定版本。
// 在任何业务标定生效之前到达的读数都采用该版本。
const BaselineCalibrationID = "baseline"

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

// Calibration 是一条不可变的标定版本记录。
// 记录固定设备（归属由存储键保证）、参数摘要、生效序号与操作者，
// 一旦写入只能被重放校验，不能修改。
type Calibration struct {
	// ID 标定号，在所有设备之间全局唯一；重放以它为幂等键。
	ID string `json:"id"`
	// ParameterDigest 标定参数摘要（调用方对完整参数集求摘要）。
	ParameterDigest string `json:"parameterDigest"`
	// EffectiveSeq 生效序号：该版本从该序号（含）起适用于读数。
	EffectiveSeq int64 `json:"effectiveSeq"`
	// Operator 记录/切换标定版本的操作者。
	Operator string `json:"operator"`
	// AppliedAt 记录写入时间。
	AppliedAt time.Time `json:"appliedAt"`
}

// CalibrationRequest 是登记（或重放）一条标定版本的入参。
type CalibrationRequest struct {
	ID              string
	ParameterDigest string
	EffectiveSeq    int64
	Operator        string
}

// CalibrationResult 是标定登记/重放的结果。
type CalibrationResult struct {
	DeviceID string `json:"deviceId"`
	Calibration
	// CurrentCalibrationID 本次处理之后设备的当前有效标定版本。
	CurrentCalibrationID string `json:"currentCalibrationId"`
	// CurrentEffectiveSeq 当前有效版本的生效序号。
	CurrentEffectiveSeq int64 `json:"currentEffectiveSeq"`
	// Duplicate 为 true 表示这是相同标定号的重放，返回首次结果。
	Duplicate bool `json:"duplicate"`
}

// DeviceView 是设备查询的综合视图：设备配置、标定历史与当前有效版本。
type DeviceView struct {
	Device               Device        `json:"device"`
	Calibrations         []Calibration `json:"calibrations"`
	CurrentCalibrationID string        `json:"currentCalibrationId"`
	AckedSeq             int64         `json:"ackedSeq"`
	ClosedSeq            int64         `json:"closedSeq"`
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
	// CurrentCalibrationID 当前有效标定版本（生效序号 <= ackedSeq+1 的最新版本）。
	CurrentCalibrationID string `json:"currentCalibrationId"`
	// Current 当前有效标定版本区间内已确认读数的聚合（当前值）。
	Current CalibrationAggregate `json:"current"`
	// History 各标定版本（含当前版本）的已确认读数聚合，按生效序号升序。
	// 旧版本条目为历史诊断值，水位关闭后冻结，迟到读数不会回写。
	History []CalibrationAggregate `json:"history"`
}

// CalibrationAggregate 是某个标定版本下已确认读数的聚合。
type CalibrationAggregate struct {
	CalibrationID   string    `json:"calibrationId"`
	EffectiveSeq    int64     `json:"effectiveSeq"`
	ParameterDigest string    `json:"parameterDigest"`
	EventCount      int64     `json:"eventCount"`
	Sum             float64   `json:"sum"`
	Min             float64   `json:"min"`
	Max             float64   `json:"max"`
	FirstEventTime  time.Time `json:"firstEventTime"`
	LastEventTime   time.Time `json:"lastEventTime"`
	// Current 标识该条目是否对应当前有效版本；false 表示历史诊断值。
	Current bool `json:"current"`
	// Frozen 标识该版本区间已被关闭水位永久结算，后续迟到读数无法回写。
	Frozen bool `json:"frozen"`
}

// storedEvent 保存原始事件及其首次到达的处理结果。
type storedEvent struct {
	Event Event `json:"event"`
	// FirstResult 首次接收时的处理结果：acked / buffered。
	// 事件随后由 buffered 变为 acked 时，首次提交得到的仍是 buffered。
	FirstResult string    `json:"firstResult"`
	ReceivedAt  time.Time `json:"receivedAt"`
	// CalibrationID 该读数采用的标定版本（首次到达时绑定，之后不再改变）。
	CalibrationID string `json:"calibrationId"`
	// EffectiveSeq 被采用版本在绑定时刻的生效序号（采用依据）。
	CalibrationEffectiveSeq int64 `json:"calibrationEffectiveSeq"`
	// CalibrationDigest 被采用版本在绑定时刻的参数摘要（采用依据）。
	CalibrationDigest string `json:"calibrationDigest"`
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
	Sequence  int64     `json:"sequence"`
	EventTime time.Time `json:"eventTime"`
	Metric    string    `json:"metric"`
	Value     float64   `json:"value"`
	// CalibrationID 迟到/冲突读数到达时按序号应采用的标定版本（历史诊断用）。
	CalibrationID string    `json:"calibrationId,omitempty"`
	Reason        string    `json:"reason"`
	Detail        string    `json:"detail"`
	RejectedAt    time.Time `json:"rejectedAt"`
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
	// CalibrationID / CalibrationEffectiveSeq / CalibrationDigest 是
	// 该读数绑定采用的标定版本及采用依据。
	CalibrationID           string `json:"calibrationId"`
	CalibrationEffectiveSeq int64  `json:"calibrationEffectiveSeq"`
	CalibrationDigest       string `json:"calibrationDigest"`
}

// IngestOutcome 是事件接收的处理结果。
type IngestOutcome struct {
	DeviceID string `json:"deviceId"`
	// Status: acked / buffered / rejected。
	Status string `json:"status"`
	// AckedSeq 本次处理之后设备的连续确认位置。
	AckedSeq int64 `json:"ackedSeq"`
	// CalibrationID 本读数采用的标定版本（rejected 时为按序号应采用的版本）。
	CalibrationID string `json:"calibrationId,omitempty"`
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
