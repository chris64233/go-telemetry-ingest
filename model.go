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
	// Value 设备上报的原始读数（未经标定换算）。
	Value float64 `json:"value"`
}

// 标定依据（basis）：说明一条读数采用某个标定版本的判定时刻。
const (
	// CalBasisAcked 读数在连续确认（drain）时绑定当时生效的标定版本。
	CalBasisAcked = "at_ack"
	// CalBasisSettled 读数在水位结算（此前 buffered）时绑定标定版本。
	CalBasisSettled = "at_watermark_settle"
	// CalBasisLate 读数在关闭水位之后迟到，仅按旧标定做历史诊断换算。
	CalBasisLate = "late_diagnostic"
)

// CalibrationParams 是一个标定版本的标定参数。
// 标定值 = 原始读数 * Scale + Offset。
type CalibrationParams struct {
	// Scale 增益，必须 > 0。
	Scale float64 `json:"scale"`
	// Offset 偏置。
	Offset float64 `json:"offset"`
}

// Apply 按本版本参数把原始读数换算为标定值。
func (p CalibrationParams) Apply(raw float64) float64 {
	return raw*p.Scale + p.Offset
}

// BaselineCalibrationID 是设备登记时自动建立的初始标定版本号，
// 参数为恒等换算（scale=1, offset=0），自序号 1 起生效。
const BaselineCalibrationID = "baseline"

// Calibration 是一条标定记录：固定设备、参数摘要、生效序号与操作者。
type Calibration struct {
	// CalibrationID 标定版本号，设备内唯一、非空。
	CalibrationID string `json:"calibrationId"`
	// DeviceID 该标定所属设备（标定记录与设备固定绑定）。
	DeviceID string `json:"deviceId"`
	// EffectiveSeq 生效序号：该版本适用于序号 >= EffectiveSeq 的读数，
	// 直到下一个标定版本生效之前。必须 > 提交时的已确认序号。
	EffectiveSeq int64 `json:"effectiveSeq"`
	// Params 标定参数。
	Params CalibrationParams `json:"params"`
	// Digest 标定参数摘要（SHA256，十六进制），由服务端根据参数计算。
	Digest string `json:"digest"`
	// Operator 执行标定切换的操作者，非空。
	Operator string `json:"operator"`
	// CreatedAt 标定记录创建时间。
	CreatedAt time.Time `json:"createdAt"`
}

// CalibrationSummary 是单个标定版本下已确认读数的聚合段。
type CalibrationSummary struct {
	CalibrationID string            `json:"calibrationId"`
	EffectiveSeq  int64             `json:"effectiveSeq"`
	Digest        string            `json:"digest"`
	Params        CalibrationParams `json:"params"`
	EventCount    int64             `json:"eventCount"`
	Sum           float64           `json:"sum"`
	Min           float64           `json:"min"`
	Max           float64           `json:"max"`
	// Current 为 true 表示该段对应当前有效标定版本；
	// false 表示旧标定版本，仅作为历史诊断值展示。
	Current bool `json:"current"`
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
	// CurrentCalibration 当前有效标定版本。
	CurrentCalibration Calibration `json:"currentCalibration"`
	// Calibrations 按生效序号升序的标定历史（含 baseline）。
	Calibrations []Calibration `json:"calibrations"`
	// Segments 按标定版本分段的聚合（按生效序号升序）；
	// 最后一段 Current=true 为当前值，其余为历史诊断值。
	Segments []CalibrationSummary `json:"segments"`
	// DiagnosticLateCount 历史诊断区中的旧标定迟到读数条数。
	DiagnosticLateCount int64 `json:"diagnosticLateCount"`
}

// storedEvent 保存原始事件及其首次到达的处理结果。
type storedEvent struct {
	Event Event `json:"event"`
	// FirstResult 首次接收时的处理结果：acked / buffered。
	// 事件随后由 buffered 变为 acked 时，首次提交得到的仍是 buffered。
	FirstResult string    `json:"firstResult"`
	ReceivedAt  time.Time `json:"receivedAt"`
	// CalibrationID 该读数最终采用的标定版本号。
	// 读数首次到达时可能为空（buffered 且尚未确认）；在确认或水位结算
	// 的同一事务中绑定，此后永不改变。
	CalibrationID string `json:"calibrationId,omitempty"`
	// CalibratedValue 按所采用标定参数换算后的标定值。
	CalibratedValue float64 `json:"calibratedValue"`
	// Basis 标定绑定依据：at_ack / at_watermark_settle。
	Basis string `json:"calibrationBasis,omitempty"`
}

// LateReading 是历史诊断区中的一条旧标定迟到读数。
// 它可以被记录，但永不回写当前聚合。
type LateReading struct {
	Event Event `json:"event"`
	// CalibrationID 该读数按其序号本应采用的旧标定版本号。
	CalibrationID string `json:"calibrationId"`
	// CalibratedValue 旧标定参数下的换算值（仅历史诊断用途）。
	CalibratedValue float64   `json:"calibratedValue"`
	RecordedAt      time.Time `json:"recordedAt"`
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
	// CalibrationID 拒绝时（如迟到）该读数本应适用的标定版本（诊断用）。
	CalibrationID string `json:"calibrationId,omitempty"`
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
	// CalibrationID 该读数采用的标定版本号；未确认的 buffered 读数可能为空。
	CalibrationID string `json:"calibrationId,omitempty"`
	// CalibratedValue 标定换算后的值；未确认的 buffered 读数可能为 0。
	CalibratedValue float64 `json:"calibratedValue"`
	// Basis 标定绑定依据：at_ack / at_watermark_settle。
	Basis string `json:"calibrationBasis,omitempty"`
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
	// CalibrationID 本次读数采用（或在迟到诊断中本应采用）的标定版本号。
	CalibrationID string `json:"calibrationId,omitempty"`
	// CalibratedValue 标定换算后的值。
	CalibratedValue float64 `json:"calibratedValue"`
	// Basis 标定绑定依据：at_ack / at_watermark_settle / late_diagnostic；
	// 尚未确认的 buffered 读数没有绑定依据，留空。
	Basis string `json:"calibrationBasis,omitempty"`
}

// ApplyCalibrationResult 是标定切换（或重放）的处理结果。
type ApplyCalibrationResult struct {
	Calibration Calibration `json:"calibration"`
	// Replayed 标识该响应来自相同标定号的重放（幂等返回原结果）。
	Replayed bool `json:"replayed"`
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
