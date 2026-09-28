package gotelemetryingest

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func eventAt(seq int64, v float64) Event {
	return Event{
		Sequence:  seq,
		EventTime: time.Date(2026, 9, 28, 0, 0, int(seq), 0, time.UTC),
		Metric:    "temp",
		Value:     v,
	}
}

func mustIngest(t *testing.T, svc *Service, dev string, e Event) *IngestOutcome {
	t.Helper()
	out, err := svc.Ingest(dev, e)
	if err != nil {
		t.Fatalf("Ingest(%d): %v", e.Sequence, err)
	}
	return out
}

func TestOrderedIngestAdvancesAndSummarizes(t *testing.T) {
	svc := NewService(NewMemoryStore())
	if _, err := svc.RegisterDevice("d1", 5); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 3; seq++ {
		out := mustIngest(t, svc, "d1", eventAt(seq, float64(seq)*10))
		if out.Status != ResultAcked || out.AckedSeq != seq {
			t.Fatalf("seq %d: got status=%s acked=%d", seq, out.Status, out.AckedSeq)
		}
	}
	sum, err := svc.Summary("d1")
	if err != nil {
		t.Fatal(err)
	}
	if sum.EventCount != 3 || sum.AckedSeq != 3 || sum.Sum != 60 ||
		sum.Min != 10 || sum.Max != 30 {
		t.Fatalf("unexpected summary: %+v", sum)
	}
	if !sum.FirstEventTime.Equal(eventAt(1, 10).EventTime) ||
		!sum.LastEventTime.Equal(eventAt(3, 30).EventTime) {
		t.Fatalf("summary time range wrong: %+v", sum)
	}
}

func TestOutOfOrderWindowFillsGapAndDrains(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)

	out := mustIngest(t, svc, "d", eventAt(2, 20))
	if out.Status != ResultBuffered || out.AckedSeq != 0 {
		t.Fatalf("expected buffered, got %+v", out)
	}
	out = mustIngest(t, svc, "d", eventAt(4, 40))
	if out.Status != ResultBuffered {
		t.Fatalf("seq4 expected buffered, got %s", out.Status)
	}
	out = mustIngest(t, svc, "d", eventAt(3, 30))
	if out.Status != ResultBuffered || out.AckedSeq != 0 {
		t.Fatalf("seq3 should still wait for seq1, got %+v", out)
	}
	out = mustIngest(t, svc, "d", eventAt(1, 10))
	if out.Status != ResultAcked || out.AckedSeq != 4 {
		t.Fatalf("filling seq1 should drain through 4, got %+v", out)
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 4 || sum.Sum != 100 {
		t.Fatalf("summary: %+v", sum)
	}

	info, err := svc.Gaps("d")
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Open) != 0 || len(info.Sealed) != 0 {
		t.Fatalf("expected no gaps, got %+v", info)
	}
}

func TestGapsReportedUntilFilled(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(2, 2))
	mustIngest(t, svc, "d", eventAt(4, 4)) // gap at 3
	mustIngest(t, svc, "d", eventAt(6, 6)) // gaps 3,5

	info, err := svc.Gaps("d")
	if err != nil {
		t.Fatal(err)
	}
	wantOpen := []Gap{{Start: 3, End: 3}, {Start: 5, End: 5}}
	if fmt.Sprint(info.Open) != fmt.Sprint(wantOpen) {
		t.Fatalf("open gaps = %+v, want %+v", info.Open, wantOpen)
	}

	mustIngest(t, svc, "d", eventAt(3, 3))
	info, _ = svc.Gaps("d")
	if fmt.Sprint(info.Open) != fmt.Sprint([]Gap{{Start: 5, End: 5}}) {
		t.Fatalf("after filling 3: %+v", info.Open)
	}
}

func TestDuplicateSameContentReturnsOriginalResultIdempotently(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)

	// 首次 buffered。
	first := mustIngest(t, svc, "d", eventAt(2, 20))
	if first.Status != ResultBuffered {
		t.Fatalf("first: %+v", first)
	}
	dup, err := svc.Ingest("d", eventAt(2, 20))
	if err != nil || !dup.Duplicate || dup.Status != ResultBuffered {
		t.Fatalf("duplicate of buffered: %+v err=%v", dup, err)
	}
	// 补齐缺口后再重复：汇总不得变化，且仍返回首次结果。
	mustIngest(t, svc, "d", eventAt(1, 10))
	before, _ := svc.Summary("d")
	dup2, err := svc.Ingest("d", eventAt(2, 20))
	if err != nil || !dup2.Duplicate || dup2.Status != ResultBuffered {
		t.Fatalf("duplicate after ack: %+v err=%v", dup2, err)
	}
	after, _ := svc.Summary("d")
	if after.EventCount != before.EventCount || after.Sum != before.Sum {
		t.Fatalf("summary changed on duplicate: before=%+v after=%+v", before, after)
	}

	// acked 事件的重复。
	dupAcked, err := svc.Ingest("d", eventAt(1, 10))
	if err != nil || !dupAcked.Duplicate || dupAcked.Status != ResultAcked {
		t.Fatalf("duplicate acked: %+v err=%v", dupAcked, err)
	}
}

func TestConflictSameSequenceDifferentContent(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 10))

	bad := eventAt(1, 999)
	out, err := svc.Ingest("d", bad)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("want ErrConflict, got out=%+v err=%v", out, err)
	}
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Outcome.Reason != ReasonConflict {
		t.Fatalf("want RejectedError(conflict), got %v", err)
	}
	// 汇总不被改写。
	sum, _ := svc.Summary("d")
	if sum.Sum != 10 || sum.EventCount != 1 {
		t.Fatalf("summary changed by conflict: %+v", sum)
	}
	// 拒绝原因可查。
	recs, err := svc.ListRejections("d", 0)
	if err != nil || len(recs) != 1 || recs[0].Reason != ReasonConflict {
		t.Fatalf("rejections: %+v err=%v", recs, err)
	}
	// 相同冲突再次提交幂等：拒绝记录不重复追加。
	_, err = svc.Ingest("d", bad)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second conflict: %v", err)
	}
	recs, _ = svc.ListRejections("d", 0)
	if len(recs) != 1 {
		t.Fatalf("conflict rejection not idempotent: %d records", len(recs))
	}
}

func TestOutOfWindowRejectedThenAcceptedWhenWindowMoves(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 3) // 允许 seq <= acked+3

	out, err := svc.Ingest("d", eventAt(5, 50))
	if err == nil || out.Status != ResultRejected || out.Reason != ReasonOutOfWindow {
		t.Fatalf("seq5 should be out of window: %+v err=%v", out, err)
	}
	// 拒绝不推进任何状态。
	sum, _ := svc.Summary("d")
	if sum.AckedSeq != 0 || sum.EventCount != 0 {
		t.Fatalf("rejected event changed state: %+v", sum)
	}
	// 窗口前移后可重新接收。
	mustIngest(t, svc, "d", eventAt(1, 10))
	mustIngest(t, svc, "d", eventAt(2, 20)) // acked=2, 上限变为 5
	out = mustIngest(t, svc, "d", eventAt(5, 50))
	if out.Status != ResultBuffered {
		t.Fatalf("seq5 now buffered, got %+v", out)
	}
	// 之前的拒绝记录仍保留。
	recs, _ := svc.ListRejections("d", 0)
	if len(recs) != 1 || recs[0].Reason != ReasonOutOfWindow {
		t.Fatalf("rejection history lost: %+v", recs)
	}
}

func TestWindowBoundary(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 3) // acked=0 时允许 seq <= 0+1+3 = 4

	// seq=4 恰为窗口上沿，接收。
	out := mustIngest(t, svc, "d", eventAt(4, 40))
	if out.Status != ResultBuffered {
		t.Fatalf("seq4 at window edge: %+v", out)
	}
	// seq=5 超窗。
	rejected, err := svc.Ingest("d", eventAt(5, 50))
	if err == nil || rejected.Reason != ReasonOutOfWindow {
		t.Fatalf("seq5 beyond window: %+v %v", rejected, err)
	}
}

func TestZeroWindowRequiresStrictContiguous(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 0)
	mustIngest(t, svc, "d", eventAt(1, 1))
	out, err := svc.Ingest("d", eventAt(2, 2))
	if err != nil || out.Status != ResultAcked {
		t.Fatalf("contiguous with window 0: %+v %v", out, err)
	}
	// window=0 时上沿为 acked+1，seq=3 恰为下一期望序号可收，seq=4 超窗。
	out, err = svc.Ingest("d", eventAt(4, 4))
	if err == nil || out.Reason != ReasonOutOfWindow {
		t.Fatalf("gap with window 0: %+v %v", out, err)
	}
}

func TestWatermarkNeverMovesAckedBackward(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	for seq := int64(1); seq <= 5; seq++ {
		mustIngest(t, svc, "d", eventAt(seq, float64(seq)*10))
	}
	// acked=5；把水位推到一个更低的位置只抬 ClosedSeq，不动汇总与确认。
	res, err := svc.AdvanceWatermark("d", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Advanced || res.AckedSeq != 5 || res.ClosedSeq != 3 || len(res.NewlySealed) != 0 {
		t.Fatalf("advance below acked: %+v", res)
	}
	sum, _ := svc.Summary("d")
	if sum.AckedSeq != 5 || sum.EventCount != 5 || sum.Sum != 150 {
		t.Fatalf("summary changed: %+v", sum)
	}
	// 再向上推到 8：6、8 缺失，7 未到 → 封闭 [6,6] 与 [8,8]？
	// 实际 6、7、8 都没事件，应合并为单个封闭缺口 [6,8]。
	res, err = svc.AdvanceWatermark("d", 8)
	if err != nil {
		t.Fatal(err)
	}
	if res.AckedSeq != 8 || res.ClosedSeq != 8 {
		t.Fatalf("advance to 8: %+v", res)
	}
	if fmt.Sprint(res.NewlySealed) != fmt.Sprint([]Gap{{Start: 6, End: 8, Sealed: true}}) {
		t.Fatalf("merged sealed gap: %+v", res.NewlySealed)
	}
	// 汇总仍只有 1..5。
	sum, _ = svc.Summary("d")
	if sum.EventCount != 5 || sum.Sum != 150 {
		t.Fatalf("summary after sealing: %+v", sum)
	}
	// 被封区间内补交全部迟到。
	out, err := svc.Ingest("d", eventAt(7, 70))
	if err == nil || out.Reason != ReasonLate {
		t.Fatalf("seq7 should be late, got %+v %v", out, err)
	}
}

func TestEventTimeMustBeStrictlyIncreasing(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	e1 := eventAt(1, 1)
	e2 := eventAt(2, 2)
	mustIngest(t, svc, "d", e1)
	mustIngest(t, svc, "d", e2)

	// 在缺口 3、已收 4... 场景下，乱序填补必须满足时间序。
	mustIngest(t, svc, "d", eventAt(4, 4))
	lower := Event{Sequence: 3, EventTime: e1.EventTime, Metric: "temp", Value: 3}
	if _, err := svc.Ingest("d", lower); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("non-increasing time should be invalid, got %v", err)
	}

	// 顺序确认到 4 后，紧邻的 5 也必须晚于最后一个已确认事件的时间。
	okFill := Event{Sequence: 3, EventTime: e2.EventTime.Add(time.Nanosecond), Metric: "temp", Value: 3}
	mustIngest(t, svc, "d", okFill)
	tooEarly := Event{Sequence: 5, EventTime: e2.EventTime, Metric: "temp", Value: 5}
	if _, err := svc.Ingest("d", tooEarly); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("seq5 earlier than last acked should be invalid, got %v", err)
	}
}

func TestWatermarkSealsGapsAndSettlesBufferedOnce(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 10))
	mustIngest(t, svc, "d", eventAt(2, 20))
	mustIngest(t, svc, "d", eventAt(4, 40)) // 缺 3
	mustIngest(t, svc, "d", eventAt(6, 60)) // 缺 3、5

	res, err := svc.AdvanceWatermark("d", 6)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Advanced || res.ClosedSeq != 6 || res.AckedSeq != 6 {
		t.Fatalf("advance: %+v", res)
	}
	wantSealed := []Gap{{Start: 3, End: 3, Sealed: true}, {Start: 5, End: 5, Sealed: true}}
	if fmt.Sprint(res.NewlySealed) != fmt.Sprint(wantSealed) {
		t.Fatalf("newly sealed: %+v", res.NewlySealed)
	}

	sum, _ := svc.Summary("d")
	if sum.EventCount != 4 || sum.Sum != 130 {
		t.Fatalf("summary after close: %+v", sum)
	}

	// 推进后，缺口上方连续事件继续推进；封闭缺口永久保留。
	info, _ := svc.Gaps("d")
	if len(info.Sealed) != 2 {
		t.Fatalf("sealed gaps: %+v", info)
	}

	// 重复推进（更小或相等水位）是幂等空操作，汇总不变。
	res2, err := svc.AdvanceWatermark("d", 5)
	if err != nil || res2.Advanced {
		t.Fatalf("backward advance should be no-op: %+v %v", res2, err)
	}
	res3, _ := svc.AdvanceWatermark("d", 6)
	if res3.Advanced {
		t.Fatal("equal advance should be no-op")
	}
	sum2, _ := svc.Summary("d")
	if sum2.EventCount != 4 || sum2.Sum != 130 {
		t.Fatalf("summary changed on idempotent advance: %+v", sum2)
	}
}

func TestLateEventAfterWatermarkRejectedAndDoesNotChangeSummary(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 10))
	if _, err := svc.AdvanceWatermark("d", 3); err != nil {
		t.Fatal(err)
	}
	// 缺口 2、3 已封闭；现在补交 seq2 → 迟到。
	out, err := svc.Ingest("d", eventAt(2, 20))
	if err == nil || out.Reason != ReasonLate {
		t.Fatalf("late event: %+v %v", out, err)
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 1 || sum.Sum != 10 || sum.AckedSeq != 3 || sum.ClosedSeq != 3 {
		t.Fatalf("late event changed summary: %+v", sum)
	}
	info, _ := svc.Gaps("d")
	if len(info.Sealed) != 1 || info.Sealed[0].Start != 2 || info.Sealed[0].End != 3 {
		t.Fatalf("sealed gaps after late: %+v", info)
	}
}

func TestWatermarkDrainsBufferedAboveTarget(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 10))
	mustIngest(t, svc, "d", eventAt(2, 20))
	mustIngest(t, svc, "d", eventAt(3, 30))
	mustIngest(t, svc, "d", eventAt(4, 40))
	// 关闭水位只推到 2（无缺口），但 3、4 已连续到达，应一并确认。
	res, err := svc.AdvanceWatermark("d", 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.AckedSeq != 4 || res.ClosedSeq != 2 {
		t.Fatalf("expected drain above target: %+v", res)
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 4 || sum.Sum != 100 {
		t.Fatalf("summary: %+v", sum)
	}
}

func TestConcurrentGapFillNoLostAdvancement(t *testing.T) {
	for _, store := range newConcurrentStores(t) {
		t.Run(store.name, func(t *testing.T) {
			svc := NewService(store.make(t))
			mustReg(t, svc, "dev", 1000)
			const n = 200
			// 先乱序放入 2..n（全部 buffered），再并发补交 1。
			var seqs []int64
			for seq := int64(2); seq <= n; seq++ {
				seqs = append(seqs, seq)
			}
			// shuffle（确定的伪随机）
			for i := len(seqs) - 1; i > 0; i-- {
				j := int((int64(i+1)*1103515245 + 12345) % int64(i+1))
				seqs[i], seqs[j] = seqs[j], seqs[i]
			}
			for _, seq := range seqs {
				mustIngest(t, svc, "dev", eventAt(seq, float64(seq)))
			}

			// 并发：多个 goroutine 重复提交 seq1 / 读取推进结果。
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					out, err := svc.Ingest("dev", eventAt(1, 1))
					if err != nil {
						t.Errorf("ingest seq1: %v", err)
						return
					}
					if out.AckedSeq != n {
						t.Errorf("acked=%d, want %d", out.AckedSeq, n)
					}
				}()
			}
			wg.Wait()

			sum, err := svc.Summary("dev")
			if err != nil {
				t.Fatal(err)
			}
			if sum.AckedSeq != n {
				t.Fatalf("acked=%d want %d", sum.AckedSeq, n)
			}
			if sum.EventCount != n {
				t.Fatalf("eventCount=%d want %d (lost or double advancement)", sum.EventCount, n)
			}
			var want float64
			for seq := int64(1); seq <= n; seq++ {
				want += float64(seq)
			}
			if math.Abs(sum.Sum-want) > 1e-6 {
				t.Fatalf("sum=%v want %v", sum.Sum, want)
			}
			info, _ := svc.Gaps("dev")
			if len(info.Open) != 0 {
				t.Fatalf("open gaps remain: %+v", info.Open)
			}
		})
	}
}

func TestConcurrentWatermarkAndLateEventDeterministic(t *testing.T) {
	for _, store := range newConcurrentStores(t) {
		t.Run(store.name, func(t *testing.T) {
			svc := NewService(store.make(t))
			mustReg(t, svc, "dev", 100)
			mustIngest(t, svc, "dev", eventAt(1, 1))
			mustIngest(t, svc, "dev", eventAt(3, 3)) // 缺 2

			// 并发推进水位到 2 与补交 seq2。无论谁先完成，最终状态必须确定：
			// seq2 要么被计入，要么被记为一次 late，二者恰好其一。
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				_, _ = svc.AdvanceWatermark("dev", 2)
			}()
			go func() {
				defer wg.Done()
				out, err := svc.Ingest("dev", eventAt(2, 2))
				switch {
				case err == nil && out.Status == ResultAcked:
				case err != nil && out != nil && out.Reason == ReasonLate:
				default:
					t.Errorf("unexpected seq2 result: %+v err=%v", out, err)
				}
			}()
			wg.Wait()

			sum, _ := svc.Summary("dev")
			recs, _ := svc.ListRejections("dev", 0)
			nLate := 0
			for _, r := range recs {
				if r.Sequence == 2 && r.Reason == ReasonLate {
					nLate++
				}
			}
			if nLate > 1 {
				t.Fatalf("at most one late record expected, got %d", nLate)
			}
			if nLate == 1 {
				// 水位先提交：seq2 被拒，缺口封闭；seq3 在同一结算事务之后
				// 经 drain 被确认。计数为 seq1、seq3 共 2。
				if sum.EventCount != 2 || sum.Sum != 4 {
					t.Fatalf("watermark-first outcome summary: %+v", sum)
				}
				info, _ := svc.Gaps("dev")
				if len(info.Sealed) != 1 || info.Sealed[0].Start != 2 || info.Sealed[0].End != 2 {
					t.Fatalf("sealed: %+v", info.Sealed)
				}
			} else {
				// 事件先提交：seq2 计入，缺口消失，推进水位时无新封闭缺口。
				if sum.EventCount != 3 || sum.Sum != 6 {
					t.Fatalf("event-first outcome summary: %+v", sum)
				}
				info, _ := svc.Gaps("dev")
				if len(info.Sealed) != 0 {
					t.Fatalf("should be no sealed gaps: %+v", info.Sealed)
				}
			}
			if sum.AckedSeq != 3 || sum.ClosedSeq != 2 {
				t.Fatalf("final positions: acked=%d closed=%d", sum.AckedSeq, sum.ClosedSeq)
			}
		})
	}
}

func TestListEventsPreservesRawAndAckFlag(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(3, 3))
	mustIngest(t, svc, "d", eventAt(2, 2))

	recs, err := svc.ListEvents("d", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("recs: %+v", recs)
	}
	for i, r := range recs {
		if r.Event.Sequence != int64(i+1) {
			t.Fatalf("order: %+v", recs)
		}
		if !r.Acknowledged {
			t.Fatalf("all should be acknowledged: %+v", r)
		}
	}
}

func TestUnknownDeviceErrors(t *testing.T) {
	svc := NewService(NewMemoryStore())
	if _, err := svc.Summary("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("summary: %v", err)
	}
	if _, err := svc.Ingest("nope", eventAt(1, 1)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ingest: %v", err)
	}
}

func mustReg(t *testing.T, svc *Service, id string, window int64) {
	t.Helper()
	if _, err := svc.RegisterDevice(id, window); err != nil {
		t.Fatal(err)
	}
}
