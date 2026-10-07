package gotelemetryingest

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrConflict 相同序号提交了不同内容。
var ErrConflict = errors.New("sequence conflict: same sequence submitted with different content")

// RejectedError 携带一次拒绝提交的结构化结果（乱序窗口外 / 迟到 / 冲突）。
// 拒绝记录已在同一事务中持久化，但不影响汇总。
type RejectedError struct {
	Outcome *IngestOutcome
}

func (e *RejectedError) Error() string {
	if e.Outcome == nil {
		return "event rejected"
	}
	return fmt.Sprintf("event rejected: %s (%s)", e.Outcome.Reason, e.Outcome.Detail)
}

// Is 让 errors.Is(err, ErrConflict) 在冲突时成立。
func (e *RejectedError) Is(target error) bool {
	return target == ErrConflict && e.Outcome != nil && e.Outcome.Reason == ReasonConflict
}

// Service 在 Store 之上实现接收、确认推进、水位与查询语义。
type Service struct {
	store Store
	now   func() time.Time

	// locks 为每台设备提供进程内串行化；跨进程/多实例的串行化由
	// Store 的版本 CAS 兜底。
	lockMu sync.Mutex
	locks  map[string]*sync.Mutex

	// calMu 串行化标定号的全局占用检查（标定号跨设备唯一）。
	// 标定号索引保存在各设备状态内，持久化边界仍在 Store 帧上。
	calMu sync.Mutex
}

// NewService 创建服务。
func NewService(store Store) *Service {
	return &Service{
		store: store,
		now:   time.Now,
		locks: make(map[string]*sync.Mutex),
	}
}

// SetClock 替换时钟（测试用）。
func (s *Service) SetClock(now func() time.Time) {
	if now != nil {
		s.now = now
	}
}

func (s *Service) deviceLock(id string) *sync.Mutex {
	s.lockMu.Lock()
	m, ok := s.locks[id]
	if !ok {
		m = &sync.Mutex{}
		s.locks[id] = m
	}
	s.lockMu.Unlock()
	return m
}

// RegisterDevice 登记设备。window 为允许的乱序序号窗口（seq <= acked+window）。
func (s *Service) RegisterDevice(id string, window int64) (*Device, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: device id is empty", ErrInvalidArgument)
	}
	if window < 0 {
		return nil, fmt.Errorf("%w: window must be >= 0", ErrInvalidArgument)
	}
	now := s.now().UTC()
	d := Device{ID: id, Window: window, RegisteredAt: now}
	st, err := s.store.RegisterDevice(d, now)
	if err != nil {
		return nil, err
	}
	return &Device{ID: st.ID, Window: st.Window, RegisteredAt: st.RegisteredAt}, nil
}

// ListDevices 返回所有已登记设备 ID。
func (s *Service) ListDevices() ([]string, error) {
	return s.store.ListDevices()
}

// ApplyCalibration 为设备登记（或重放）一条标定版本。
//
//   - 标定号全局唯一：相同标定号重放到同一设备且参数摘要、生效序号、
//     操作者完全一致时幂等返回原结果（Duplicate=true）；设备或任一内容
//     变化返回 ErrCalibrationConflict。
//   - 生效序号不能早于已经确认的读数：effectiveSeq <= ackedSeq 时以
//     ErrInvalidArgument 拒绝；也必须晚于已有标定的生效序号。拒绝路径
//     不写入任何状态，当前有效标定版本保持不变。
//   - 标定记录本身不可变；新版本从生效序号起适用于读数，当前有效版本
//     在确认位置抵达生效序号后自然切换。
func (s *Service) ApplyCalibration(deviceID string, req CalibrationRequest) (*CalibrationResult, error) {
	if deviceID == "" {
		return nil, fmt.Errorf("%w: device id is empty", ErrInvalidArgument)
	}
	if strings.TrimSpace(req.ID) == "" {
		return nil, fmt.Errorf("%w: calibration id is empty", ErrInvalidArgument)
	}
	if req.ID == BaselineCalibrationID {
		return nil, fmt.Errorf("%w: calibration id %q is reserved", ErrInvalidArgument, BaselineCalibrationID)
	}
	if strings.TrimSpace(req.ParameterDigest) == "" {
		return nil, fmt.Errorf("%w: parameterDigest is empty", ErrInvalidArgument)
	}
	if strings.TrimSpace(req.Operator) == "" {
		return nil, fmt.Errorf("%w: operator is empty", ErrInvalidArgument)
	}
	if req.EffectiveSeq <= 0 {
		return nil, fmt.Errorf("%w: effectiveSeq must be >= 1", ErrInvalidArgument)
	}

	// 标定号全局占用检查 + 设备内提交在 calMu 下串行，杜绝两个设备并发
	// 抢到同一个标定号。设备级锁仍保证与本设备读数/水位互斥。
	s.calMu.Lock()
	defer s.calMu.Unlock()
	mu := s.deviceLock(deviceID)
	mu.Lock()
	defer mu.Unlock()

	for {
		st, err := s.store.Load(deviceID)
		if err != nil {
			return nil, err
		}

		// 1) 同设备内已有该标定号：重放幂等或内容冲突。
		for _, c := range st.Calibrations {
			if c.ID != req.ID {
				continue
			}
			if c.ParameterDigest != req.ParameterDigest ||
				c.EffectiveSeq != req.EffectiveSeq || c.Operator != req.Operator {
				return nil, fmt.Errorf("%w: calibration %q already recorded with different content", ErrCalibrationConflict, req.ID)
			}
			cur := currentCalibration(st)
			return &CalibrationResult{
				DeviceID:             deviceID,
				Calibration:          c,
				CurrentCalibrationID: cur.ID,
				CurrentEffectiveSeq:  cur.EffectiveSeq,
				Duplicate:            true,
			}, nil
		}

		// 2) 标定号可能已被其他设备占用：扫描全部设备。
		ids, err := s.store.ListDevices()
		if err != nil {
			return nil, err
		}
		for _, otherID := range ids {
			if otherID == deviceID {
				continue
			}
			other, lerr := s.store.Load(otherID)
			if lerr != nil {
				return nil, lerr
			}
			for _, c := range other.Calibrations {
				if c.ID == req.ID {
					return nil, fmt.Errorf("%w: calibration id %q already belongs to device %q", ErrCalibrationConflict, req.ID, otherID)
				}
			}
		}

		// 3) 生效序号边界：不能早于已确认读数，也不能早于已有版本。
		if req.EffectiveSeq <= st.AckedSeq {
			return nil, fmt.Errorf("%w: effectiveSeq %d must be greater than ackedSeq %d", ErrInvalidArgument, req.EffectiveSeq, st.AckedSeq)
		}
		last := st.Calibrations[len(st.Calibrations)-1]
		if req.EffectiveSeq <= last.EffectiveSeq {
			return nil, fmt.Errorf("%w: effectiveSeq %d must be greater than previous calibration effectiveSeq %d", ErrInvalidArgument, req.EffectiveSeq, last.EffectiveSeq)
		}

		now := s.now().UTC()
		cal := Calibration{
			ID:              req.ID,
			ParameterDigest: req.ParameterDigest,
			EffectiveSeq:    req.EffectiveSeq,
			Operator:        req.Operator,
			AppliedAt:       now,
		}
		prev := st.clone()
		st.Calibrations = append(st.Calibrations, cal)
		st.CalAggs[cal.ID] = &CalibrationAggregate{
			CalibrationID:   cal.ID,
			EffectiveSeq:    cal.EffectiveSeq,
			ParameterDigest: cal.ParameterDigest,
		}
		st.Version++
		st.UpdatedAt = now
		if err := s.store.UpdateDevice(prev, st); err != nil {
			if errors.Is(err, ErrConflictVersion) {
				continue
			}
			return nil, err
		}
		cur := currentCalibration(st)
		return &CalibrationResult{
			DeviceID:             deviceID,
			Calibration:          cal,
			CurrentCalibrationID: cur.ID,
			CurrentEffectiveSeq:  cur.EffectiveSeq,
		}, nil
	}
}

// currentCalibration 返回设备当前有效标定版本：生效序号 <= ackedSeq+1
// 的最新一条（下一条待确认读数将采用它）。
func currentCalibration(st *deviceState) Calibration {
	return calibrationFor(st, st.AckedSeq+1)
}

// ListCalibrations 返回设备标定历史（按生效序号升序），并标注当前有效版本。
func (s *Service) ListCalibrations(deviceID string) ([]Calibration, string, error) {
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, "", err
	}
	out := append([]Calibration(nil), st.Calibrations...)
	return out, currentCalibration(st).ID, nil
}

// DeviceView 返回设备配置、标定历史与当前有效标定版本。
func (s *Service) GetDeviceView(deviceID string) (*DeviceView, error) {
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	cur := currentCalibration(st)
	return &DeviceView{
		Device: Device{
			ID:           st.ID,
			Window:       st.Window,
			RegisteredAt: st.RegisteredAt,
		},
		Calibrations:         append([]Calibration(nil), st.Calibrations...),
		CurrentCalibrationID: cur.ID,
		AckedSeq:             st.AckedSeq,
		ClosedSeq:            st.ClosedSeq,
	}, nil
}

func validateEvent(e Event) error {
	if e.Sequence <= 0 {
		return fmt.Errorf("%w: sequence must be >= 1", ErrInvalidArgument)
	}
	if e.Metric == "" {
		return fmt.Errorf("%w: metric is empty", ErrInvalidArgument)
	}
	if e.EventTime.IsZero() {
		return fmt.Errorf("%w: eventTime is zero", ErrInvalidArgument)
	}
	if math.IsNaN(e.Value) || math.IsInf(e.Value, 0) {
		return fmt.Errorf("%w: value must be finite", ErrInvalidArgument)
	}
	return nil
}

// Ingest 接收一个设备事件。
//
// 返回值约定：
//   - 正常接收：*IngestOutcome（Status 为 acked/buffered），error 为 nil；
//   - 重复提交：*IngestOutcome（Duplicate=true，Status 为首次处理结果），error 为 nil；
//   - 被拒绝：*IngestOutcome（Status=rejected）同时返回 *RejectedError，
//     拒绝原因已持久化；冲突时 errors.Is(err, ErrConflict) 成立。
//
// 读数在首次到达时按序号绑定唯一标定版本（保存版本生效序号与参数摘要作为
// 采用依据），此后标定切换不会改绑。汇总、按标定版本聚合与确认位置的推进
// 在同一个 Store 事务内完成。
func (s *Service) Ingest(deviceID string, e Event) (*IngestOutcome, error) {
	if err := validateEvent(e); err != nil {
		return nil, err
	}
	mu := s.deviceLock(deviceID)
	mu.Lock()
	defer mu.Unlock()

	for {
		st, err := s.store.Load(deviceID)
		if err != nil {
			return nil, err
		}
		boundCal := calibrationFor(st, e.Sequence)

		// 1) 相同序号：去重 / 冲突。
		if existing, ok := st.Events[e.Sequence]; ok {
			if e.sameContent(existing.Event) {
				return &IngestOutcome{
					DeviceID:      deviceID,
					Status:        existing.FirstResult,
					AckedSeq:      st.AckedSeq,
					CalibrationID: existing.CalibrationID,
					Duplicate:     true,
				}, nil
			}
			out, rerr := s.recordRejection(st, e, boundCal, ReasonConflict,
				fmt.Sprintf("sequence %d already exists with different content", e.Sequence))
			if errors.Is(rerr, ErrConflictVersion) {
				continue
			}
			return out, rerr
		}

		// 2) 已被关闭水位永久关闭（迟到）。
		if e.Sequence <= st.ClosedSeq {
			out, rerr := s.recordRejection(st, e, boundCal, ReasonLate,
				fmt.Sprintf("sequence %d is below closed watermark %d", e.Sequence, st.ClosedSeq))
			if errors.Is(rerr, ErrConflictVersion) {
				continue
			}
			return out, rerr
		}

		// 3) 超出乱序窗口：acked+1 是下一个期望序号，其之后还允许提前
		// window 个序号（上限 acked+1+window）。
		if e.Sequence > st.AckedSeq+1+st.Window {
			out, rerr := s.recordRejection(st, e, boundCal, ReasonOutOfWindow,
				fmt.Sprintf("sequence %d exceeds window (acked=%d, window=%d)", e.Sequence, st.AckedSeq, st.Window))
			if errors.Is(rerr, ErrConflictVersion) {
				continue
			}
			return out, rerr
		}

		// 4) 事件时间必须随序号严格递增：与窗口内已到达的相邻序号比较。
		if msg := checkTimeOrder(st, e); msg != "" {
			return nil, fmt.Errorf("%w: %s", ErrInvalidArgument, msg)
		}

		// 5) 落入窗口，暂存；若正好填补队首则连续推进并累计汇总。
		prev := st.clone()
		now := s.now().UTC()
		st.Events[e.Sequence] = &storedEvent{
			Event:                   e,
			FirstResult:             ResultBuffered,
			ReceivedAt:              now,
			CalibrationID:           boundCal.ID,
			CalibrationEffectiveSeq: boundCal.EffectiveSeq,
			CalibrationDigest:       boundCal.ParameterDigest,
		}
		drain(st)
		status := ResultBuffered
		if e.Sequence <= st.AckedSeq {
			status = ResultAcked
		}
		st.Events[e.Sequence].FirstResult = status
		st.Version++
		st.UpdatedAt = now

		if err := s.store.UpdateDevice(prev, st); err != nil {
			if errors.Is(err, ErrConflictVersion) {
				continue // 有并发提交，基于新版本重新判定
			}
			return nil, err
		}
		return &IngestOutcome{
			DeviceID:      deviceID,
			Status:        status,
			AckedSeq:      st.AckedSeq,
			CalibrationID: boundCal.ID,
		}, nil
	}
}

// calibrationFor 返回序号 seq 在当前标定历史下应采用的标定版本：
// 生效序号 <= seq 的最新一条。标定记录按生效序号升序保存。
func calibrationFor(st *deviceState, seq int64) Calibration {
	chosen := st.Calibrations[0]
	for _, c := range st.Calibrations {
		if c.EffectiveSeq <= seq {
			chosen = c
		} else {
			break
		}
	}
	return chosen
}

// checkTimeOrder 校验事件时间相对已存储的相邻序号严格递增，返回非空错误描述。
// 前驱取「不大于 e.Sequence 的最大已存序号」（含最后一个已确认事件），
// 后继取确认位置之后窗口内「大于 e.Sequence 的最小序号」。
func checkTimeOrder(st *deviceState, e Event) string {
	var pred, succ *storedEvent
	for _, se := range st.Events {
		if se.Event.Sequence < e.Sequence {
			if pred == nil || se.Event.Sequence > pred.Event.Sequence {
				pred = se
			}
		} else if se.Event.Sequence > e.Sequence && se.Event.Sequence > st.AckedSeq {
			if succ == nil || se.Event.Sequence < succ.Event.Sequence {
				succ = se
			}
		}
	}
	if pred != nil && !e.EventTime.After(pred.Event.EventTime) {
		return fmt.Sprintf("eventTime %s not after sequence %d time %s",
			e.EventTime.Format(time.RFC3339Nano), pred.Event.Sequence, pred.Event.EventTime.Format(time.RFC3339Nano))
	}
	if succ != nil && !e.EventTime.Before(succ.Event.EventTime) {
		return fmt.Sprintf("eventTime %s not before sequence %d time %s",
			e.EventTime.Format(time.RFC3339Nano), succ.Event.Sequence, succ.Event.EventTime.Format(time.RFC3339Nano))
	}
	return ""
}

// drain 在连续事件齐备时推进 AckedSeq，并把新确认的事件恰好一次累计进汇总。
// 调用方必须保证 st 是已克隆的待提交状态。
func drain(st *deviceState) {
	for {
		next, ok := st.Events[st.AckedSeq+1]
		if !ok {
			return
		}
		applyToSummary(st, next.Event)
		applyToCalibrationAgg(st, next)
		st.AckedSeq++
	}
}

// applyToSummary 把一个事件按序号顺序计入汇总。调用顺序严格递增，
// 因此 LastTime/Min/Max 不需要跨事件重排。
func applyToSummary(st *deviceState, e Event) {
	st.EventCount++
	st.Sum += e.Value
	if st.EventCount == 1 {
		st.Min, st.Max = e.Value, e.Value
		st.FirstTime = e.EventTime
	} else {
		if e.Value < st.Min {
			st.Min = e.Value
		}
		if e.Value > st.Max {
			st.Max = e.Value
		}
	}
	st.LastTime = e.EventTime
}

// applyToCalibrationAgg 把读数按其绑定的标定版本恰好一次计入该版本聚合。
func applyToCalibrationAgg(st *deviceState, se *storedEvent) {
	agg, ok := st.CalAggs[se.CalibrationID]
	if !ok {
		agg = &CalibrationAggregate{
			CalibrationID:   se.CalibrationID,
			EffectiveSeq:    se.CalibrationEffectiveSeq,
			ParameterDigest: se.CalibrationDigest,
		}
		st.CalAggs[se.CalibrationID] = agg
	}
	e := se.Event
	agg.EventCount++
	agg.Sum += e.Value
	if agg.EventCount == 1 {
		agg.Min, agg.Max = e.Value, e.Value
		agg.FirstEventTime = e.EventTime
	} else {
		if e.Value < agg.Min {
			agg.Min = e.Value
		}
		if e.Value > agg.Max {
			agg.Max = e.Value
		}
	}
	agg.LastEventTime = e.EventTime
}

// recordRejection 在同一事务中追加拒绝记录并提交；对完全相同的拒绝
// 提交（同序号、同内容、同原因）幂等返回既有结果。
// 成功时返回的 error 为 *RejectedError；CAS 冲突时原样返回 ErrConflictVersion。
func (s *Service) recordRejection(st *deviceState, e Event, bound Calibration, reason, detail string) (*IngestOutcome, error) {
	for _, r := range st.Rejections {
		if r.Sequence == e.Sequence && r.Reason == reason &&
			r.Metric == e.Metric && r.Value == e.Value && r.EventTime.Equal(e.EventTime) {
			out := &IngestOutcome{
				DeviceID:      st.ID,
				Status:        ResultRejected,
				AckedSeq:      st.AckedSeq,
				CalibrationID: r.CalibrationID,
				Reason:        reason,
				Detail:        detail,
				Duplicate:     true,
			}
			return out, &RejectedError{Outcome: out}
		}
	}
	now := s.now().UTC()
	rec := Rejection{
		Sequence:      e.Sequence,
		EventTime:     e.EventTime,
		Metric:        e.Metric,
		Value:         e.Value,
		CalibrationID: bound.ID,
		Reason:        reason,
		Detail:        detail,
		RejectedAt:    now,
	}
	prev := st.clone()
	st.Rejections = append(st.Rejections, rec)
	st.Version++
	st.UpdatedAt = now
	if err := s.store.UpdateDevice(prev, st); err != nil {
		return nil, err
	}
	out := &IngestOutcome{
		DeviceID:      st.ID,
		Status:        ResultRejected,
		AckedSeq:      st.AckedSeq,
		CalibrationID: bound.ID,
		Reason:        reason,
		Detail:        detail,
	}
	return out, &RejectedError{Outcome: out}
}

// AdvanceWatermark 把设备的关闭水位推进到 targetSeq（含）。
//
//   - targetSeq 不高于当前水位时为幂等空操作（Advanced=false）；
//   - 推进后，(旧水位, targetSeq] 内缺失的序号成为永久封闭缺口，
//     已到达的事件则按序号恰好一次计入汇总；AckedSeq 与 ClosedSeq 一同
//     推进到 targetSeq，并在同一个持久化事务中提交；
//   - 与迟到事件并发时由 Store 的版本 CAS 线性化：先提交者按它看到的
//     版本生效，后提交者基于新版本重新判定，结果确定且无重复累计。
func (s *Service) AdvanceWatermark(deviceID string, targetSeq int64) (*AdvanceResult, error) {
	if targetSeq <= 0 {
		return nil, fmt.Errorf("%w: watermark must be >= 1", ErrInvalidArgument)
	}
	mu := s.deviceLock(deviceID)
	mu.Lock()
	defer mu.Unlock()

	for {
		st, err := s.store.Load(deviceID)
		if err != nil {
			return nil, err
		}
		if targetSeq <= st.ClosedSeq {
			return &AdvanceResult{
				DeviceID:  deviceID,
				ClosedSeq: st.ClosedSeq,
				AckedSeq:  st.AckedSeq,
				Advanced:  false,
			}, nil
		}

		prev := st.clone()
		now := s.now().UTC()
		var newly []Gap
		// 只有 target 超过当前确认位置时才需要结算缺口：AckedSeq 连续推进，
		// 已经确认的区间不可能再有缺口。target<=acked 时仅抬水位，
		// 不动汇总、不回退确认位置。
		if targetSeq > st.AckedSeq {
			newly = sealAndSettle(st, targetSeq, now)
			st.AckedSeq = targetSeq
		}
		st.ClosedSeq = targetSeq
		// target 之上若已连续到达，继续向前推进（这些事件已在窗口内，
		// 不能被遗留在 buffered 状态）。
		drain(st)
		st.Version++
		st.UpdatedAt = now

		if err := s.store.UpdateDevice(prev, st); err != nil {
			if errors.Is(err, ErrConflictVersion) {
				continue
			}
			return nil, err
		}
		return &AdvanceResult{
			DeviceID:    deviceID,
			ClosedSeq:   st.ClosedSeq,
			AckedSeq:    st.AckedSeq,
			NewlySealed: newly,
			Advanced:    true,
		}, nil
	}
}

// sealAndSettle 把 (AckedSeq, target] 结算到 st：缺失序号合并为封闭缺口，
// 已到达事件按序计入汇总。必须在水位推进的同一事务中调用。
func sealAndSettle(st *deviceState, target int64, now time.Time) []Gap {
	var newly []Gap
	var gapStart, gapEnd int64
	flushGap := func() {
		if gapStart != 0 {
			st.SealedGaps = append(st.SealedGaps, SealedGap{
				Start: gapStart, End: gapEnd, SealedAt: now,
			})
			newly = append(newly, Gap{Start: gapStart, End: gapEnd, Sealed: true})
			gapStart, gapEnd = 0, 0
		}
	}
	for seq := st.AckedSeq + 1; seq <= target; seq++ {
		if ev, ok := st.Events[seq]; ok {
			flushGap()
			applyToSummary(st, ev.Event)
			applyToCalibrationAgg(st, ev)
		} else {
			if gapStart == 0 {
				gapStart, gapEnd = seq, seq
			} else {
				gapEnd = seq
			}
		}
	}
	flushGap()
	sort.Slice(st.SealedGaps, func(i, j int) bool { return st.SealedGaps[i].Start < st.SealedGaps[j].Start })
	return newly
}

// Gaps 查询设备缺口：Open 为窗口内仍可填补的缺口，Sealed 为水位封闭的缺口。
func (s *Service) Gaps(deviceID string) (*GapInfo, error) {
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	info := &GapInfo{
		DeviceID:  deviceID,
		AckedSeq:  st.AckedSeq,
		ClosedSeq: st.ClosedSeq,
		Open:      []Gap{},
		Sealed:    []Gap{},
	}
	for _, g := range st.SealedGaps {
		info.Sealed = append(info.Sealed, Gap{Start: g.Start, End: g.End, Sealed: true})
	}
	// 前沿：已到达的最大窗口内序号；缺口为 (acked, frontier] 中的缺失序号。
	var frontier int64
	for seq := range st.Events {
		if seq > st.AckedSeq && seq > frontier {
			frontier = seq
		}
	}
	var gs, ge int64
	for seq := st.AckedSeq + 1; seq <= frontier; seq++ {
		if _, ok := st.Events[seq]; !ok {
			if gs == 0 {
				gs, ge = seq, seq
			} else {
				ge = seq
			}
		} else if gs != 0 {
			info.Open = append(info.Open, Gap{Start: gs, End: ge})
			gs, ge = 0, 0
		}
	}
	if gs != 0 {
		info.Open = append(info.Open, Gap{Start: gs, End: ge})
	}
	return info, nil
}

// ListEvents 分页查询原始事件（按序号升序）。
// from 为起始序号（含，0/负数表示从最早开始），limit<=0 表示默认上限。
func (s *Service) ListEvents(deviceID string, from, limit int64) ([]EventRecord, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	seqs := make([]int64, 0, len(st.Events))
	for seq := range st.Events {
		if seq >= from {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	out := make([]EventRecord, 0, len(seqs))
	for _, seq := range seqs {
		if int64(len(out)) >= limit {
			break
		}
		se := st.Events[seq]
		out = append(out, EventRecord{
			Event:                   se.Event,
			FirstResult:             se.FirstResult,
			Acknowledged:            seq <= st.AckedSeq,
			ReceivedAt:              se.ReceivedAt,
			CalibrationID:           se.CalibrationID,
			CalibrationEffectiveSeq: se.CalibrationEffectiveSeq,
			CalibrationDigest:       se.CalibrationDigest,
		})
	}
	return out, nil
}

// Summary 返回设备汇总（只含已连续确认事件，恰好一次）。
func (s *Service) Summary(deviceID string) (*Summary, error) {
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	sum := st.summary()
	return &sum, nil
}

// ListRejections 返回被拒绝的提交记录（按拒绝时间倒序，最多 limit 条）。
// limit<=0 时使用默认上限。
func (s *Service) ListRejections(deviceID string, limit int64) ([]Rejection, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	n := int64(len(st.Rejections))
	start := n - limit
	if start < 0 {
		start = 0
	}
	out := make([]Rejection, 0, n-start)
	for i := n - 1; i >= start; i-- {
		out = append(out, st.Rejections[i])
	}
	return out, nil
}
