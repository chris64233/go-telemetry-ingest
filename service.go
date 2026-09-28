package gotelemetryingest

import (
	"errors"
	"fmt"
	"math"
	"sort"
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
// 汇总与确认位置的推进在同一个 Store 事务内完成。
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

		// 1) 相同序号：去重 / 冲突。
		if existing, ok := st.Events[e.Sequence]; ok {
			if e.sameContent(existing.Event) {
				return &IngestOutcome{
					DeviceID:  deviceID,
					Status:    existing.FirstResult,
					AckedSeq:  st.AckedSeq,
					Duplicate: true,
				}, nil
			}
			out, rerr := s.recordRejection(st, e, ReasonConflict,
				fmt.Sprintf("sequence %d already exists with different content", e.Sequence))
			if errors.Is(rerr, ErrConflictVersion) {
				continue
			}
			return out, rerr
		}

		// 2) 已被关闭水位永久关闭（迟到）。
		if e.Sequence <= st.ClosedSeq {
			out, rerr := s.recordRejection(st, e, ReasonLate,
				fmt.Sprintf("sequence %d is below closed watermark %d", e.Sequence, st.ClosedSeq))
			if errors.Is(rerr, ErrConflictVersion) {
				continue
			}
			return out, rerr
		}

		// 3) 超出乱序窗口：acked+1 是下一个期望序号，其之后还允许提前
		// window 个序号（上限 acked+1+window）。
		if e.Sequence > st.AckedSeq+1+st.Window {
			out, rerr := s.recordRejection(st, e, ReasonOutOfWindow,
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
			Event:       e,
			FirstResult: ResultBuffered,
			ReceivedAt:  now,
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
			DeviceID: deviceID,
			Status:   status,
			AckedSeq: st.AckedSeq,
		}, nil
	}
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

// recordRejection 在同一事务中追加拒绝记录并提交；对完全相同的拒绝
// 提交（同序号、同内容、同原因）幂等返回既有结果。
// 成功时返回的 error 为 *RejectedError；CAS 冲突时原样返回 ErrConflictVersion。
func (s *Service) recordRejection(st *deviceState, e Event, reason, detail string) (*IngestOutcome, error) {
	for _, r := range st.Rejections {
		if r.Sequence == e.Sequence && r.Reason == reason &&
			r.Metric == e.Metric && r.Value == e.Value && r.EventTime.Equal(e.EventTime) {
			out := &IngestOutcome{
				DeviceID:  st.ID,
				Status:    ResultRejected,
				AckedSeq:  st.AckedSeq,
				Reason:    reason,
				Detail:    detail,
				Duplicate: true,
			}
			return out, &RejectedError{Outcome: out}
		}
	}
	now := s.now().UTC()
	rec := Rejection{
		Sequence:   e.Sequence,
		EventTime:  e.EventTime,
		Metric:     e.Metric,
		Value:      e.Value,
		Reason:     reason,
		Detail:     detail,
		RejectedAt: now,
	}
	prev := st.clone()
	st.Rejections = append(st.Rejections, rec)
	st.Version++
	st.UpdatedAt = now
	if err := s.store.UpdateDevice(prev, st); err != nil {
		return nil, err
	}
	out := &IngestOutcome{
		DeviceID: st.ID,
		Status:   ResultRejected,
		AckedSeq: st.AckedSeq,
		Reason:   reason,
		Detail:   detail,
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
			Event:        se.Event,
			FirstResult:  se.FirstResult,
			Acknowledged: seq <= st.AckedSeq,
			ReceivedAt:   se.ReceivedAt,
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
