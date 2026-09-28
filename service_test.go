package gotelemetryingest

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)

func ev(seq uint64, value float64) Event {
	return Event{DeviceID: "dev-1", Seq: seq, EventTime: t0.Add(time.Duration(seq) * time.Second), Value: value}
}

func newService(t *testing.T) *Service {
	t.Helper()
	store, err := OpenStore("")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return NewService(store)
}

func mustRegister(t *testing.T, svc *Service, id string, window uint64) {
	t.Helper()
	if _, err := svc.RegisterDevice(id, window); err != nil {
		t.Fatalf("register device: %v", err)
	}
}

func TestOutOfOrderWithinWindowAndGapBlocking(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 10)

	// 乱序到达：3 先于 1、2。Ack 不能越过缺口。
	for _, seq := range []uint64{3, 1} {
		res, err := svc.Ingest(ev(seq, float64(seq)))
		if err != nil {
			t.Fatalf("ingest seq %d: %v", seq, err)
		}
		if res.Status != StatusAccepted {
			t.Fatalf("seq %d: got status %q", seq, res.Status)
		}
	}
	_, ack, _, err := svc.Summary("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if ack != 1 {
		t.Fatalf("ack = %d, want 1 (gap at 2 must block advancement)", ack)
	}

	gaps, err := svc.Gaps("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps.Missing) != 1 || gaps.Missing[0] != 2 {
		t.Fatalf("missing = %v, want [2]", gaps.Missing)
	}
	if len(gaps.Buffered) != 1 || gaps.Buffered[0] != 3 {
		t.Fatalf("buffered = %v, want [3]", gaps.Buffered)
	}

	// 填补缺口 2 后，Ack 连续推进到 3，缓冲事件恰好一次计入汇总。
	if _, err := svc.Ingest(ev(2, 2)); err != nil {
		t.Fatal(err)
	}
	sum, ack, _, err := svc.Summary("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if ack != 3 {
		t.Fatalf("ack = %d, want 3", ack)
	}
	if sum.Count != 3 || sum.TotalValue != 1+2+3 {
		t.Fatalf("summary = %+v, want count=3 total=6", sum)
	}
}

func TestDuplicateSameContentIsIdempotent(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 10)

	if _, err := svc.Ingest(ev(1, 5)); err != nil {
		t.Fatal(err)
	}
	res, err := svc.Ingest(ev(1, 5))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusDuplicate {
		t.Fatalf("status = %q, want duplicate", res.Status)
	}
	sum, _, _, err := svc.Summary("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if sum.Count != 1 || sum.TotalValue != 5 {
		t.Fatalf("summary = %+v, want count=1 total=5 (no double count)", sum)
	}
}

func TestSameSeqDifferentContentConflicts(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 10)

	if _, err := svc.Ingest(ev(1, 5)); err != nil {
		t.Fatal(err)
	}
	bad := ev(1, 6)
	_, err := svc.Ingest(bad)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
	// 事件时间不同也算内容不同。
	bad2 := ev(1, 5)
	bad2.EventTime = bad2.EventTime.Add(time.Hour)
	if _, err := svc.Ingest(bad2); !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v, want ErrConflict", err)
	}
}

func TestBeyondWindowRejectedAndRecorded(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 5)

	res, err := svc.Ingest(ev(6, 1)) // Ack=0, 窗口为 (0,5]
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRejected || res.Reason != ReasonBeyondWindow {
		t.Fatalf("res = %+v, want rejected/beyond_window", res)
	}
	rej, err := svc.Rejections("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(rej) != 1 || rej[0].Reason != ReasonBeyondWindow || rej[0].Seq != 6 {
		t.Fatalf("rejections = %+v", rej)
	}
	sum, ack, _, _ := svc.Summary("dev-1")
	if sum.Count != 0 || ack != 0 {
		t.Fatalf("rejected event must not affect summary: %+v ack=%d", sum, ack)
	}
}

func TestLateEventAfterWatermarkRejected(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 10)

	if _, err := svc.Ingest(ev(1, 1)); err != nil {
		t.Fatal(err)
	}
	d, err := svc.GetDevice("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	// 关闭水位推进到 t0+5s。
	if _, err := svc.AdvanceWatermark("dev-1", t0.Add(5*time.Second), d.Version); err != nil {
		t.Fatal(err)
	}
	// 事件时间 <= 水位 → 迟到拒绝。
	res, err := svc.Ingest(ev(2, 2)) // event_time = t0+2s
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRejected || res.Reason != ReasonLateEvent {
		t.Fatalf("res = %+v, want rejected/late_event", res)
	}
	// 迟到事件不得改写已完成的汇总。
	sum, ack, _, _ := svc.Summary("dev-1")
	if sum.Count != 1 || sum.TotalValue != 1 || ack != 1 {
		t.Fatalf("summary mutated by late event: %+v ack=%d", sum, ack)
	}
	rej, _ := svc.Rejections("dev-1")
	if len(rej) != 1 || rej[0].Reason != ReasonLateEvent {
		t.Fatalf("rejections = %+v", rej)
	}
	// 晚于水位的事件仍可正常接收。
	res, err = svc.Ingest(ev(2, 2))
	if err != nil {
		t.Fatal(err)
	}
	_ = res
	late := ev(3, 3) // t0+3s，仍迟到
	if res, err := svc.Ingest(late); err != nil || res.Status != StatusRejected {
		t.Fatalf("late seq 3: res=%+v err=%v", res, err)
	}
	onTime := ev(4, 4)
	onTime.EventTime = t0.Add(6 * time.Second)
	if res, err := svc.Ingest(onTime); err != nil || res.Status != StatusAccepted {
		t.Fatalf("on-time seq 4: res=%+v err=%v", res, err)
	}
}

func TestWatermarkRequiresCurrentVersion(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 10)

	d, _ := svc.GetDevice("dev-1")
	if _, err := svc.AdvanceWatermark("dev-1", t0.Add(time.Minute), d.Version); err != nil {
		t.Fatal(err)
	}
	// 用旧版本再次推进 → 冲突，保证并发推进基于同一版本得出确定结果。
	if _, err := svc.AdvanceWatermark("dev-1", t0.Add(2*time.Minute), d.Version); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
	// 水位不允许回退。
	d, _ = svc.GetDevice("dev-1")
	if _, err := svc.AdvanceWatermark("dev-1", t0, d.Version); !errors.Is(err, ErrWatermarkRegression) {
		t.Fatalf("err = %v, want ErrWatermarkRegression", err)
	}
}

func TestConcurrentGapFillingNoLostAdvancement(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 200)

	const n = 100
	var wg sync.WaitGroup
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			res, err := svc.Ingest(ev(seq, float64(seq)))
			if err != nil {
				t.Errorf("ingest seq %d: %v", seq, err)
				return
			}
			if res.Status != StatusAccepted {
				t.Errorf("seq %d: status %q", seq, res.Status)
			}
		}(uint64(i))
	}
	wg.Wait()

	sum, ack, _, err := svc.Summary("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if ack != n {
		t.Fatalf("ack = %d, want %d", ack, n)
	}
	wantTotal := float64(n*(n+1)) / 2
	if sum.Count != n || sum.TotalValue != wantTotal {
		t.Fatalf("summary = %+v, want count=%d total=%v", sum, n, wantTotal)
	}
}

func TestRestartDoesNotDoubleCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")

	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(store)
	mustRegister(t, svc, "dev-1", 10)
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := svc.Ingest(ev(seq, float64(seq))); err != nil {
			t.Fatal(err)
		}
	}

	// 模拟重启：从同一文件重新打开。
	store2, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := NewService(store2)
	sum, ack, _, err := svc2.Summary("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if ack != 3 || sum.Count != 3 || sum.TotalValue != 6 {
		t.Fatalf("after restart: summary=%+v ack=%d, want count=3 total=6 ack=3", sum, ack)
	}

	// 重启后重复提交已确认事件：幂等返回，不重复累加。
	res, err := svc2.Ingest(ev(2, 2))
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusDuplicate {
		t.Fatalf("status = %q, want duplicate", res.Status)
	}
	sum, _, _, _ = svc2.Summary("dev-1")
	if sum.Count != 3 || sum.TotalValue != 6 {
		t.Fatalf("double count after restart: %+v", sum)
	}

	// 重启后拒绝记录也保留。
	late := ev(4, 4)
	d, _ := svc2.GetDevice("dev-1")
	if _, err := svc2.AdvanceWatermark("dev-1", t0.Add(time.Hour), d.Version); err != nil {
		t.Fatal(err)
	}
	if res, err := svc2.Ingest(late); err != nil || res.Status != StatusRejected {
		t.Fatalf("late after restart: res=%+v err=%v", res, err)
	}
	store3, _ := OpenStore(path)
	svc3 := NewService(store3)
	rej, _ := svc3.Rejections("dev-1")
	if len(rej) != 1 || rej[0].Reason != ReasonLateEvent {
		t.Fatalf("rejections after restart: %+v", rej)
	}
}

func TestConcurrentWatermarkAndLateEventDeterministic(t *testing.T) {
	for i := 0; i < 50; i++ {
		svc := newService(t)
		mustRegister(t, svc, "dev-1", 10)
		if _, err := svc.Ingest(ev(1, 1)); err != nil {
			t.Fatal(err)
		}
		d, _ := svc.GetDevice("dev-1")

		late := Event{DeviceID: "dev-1", Seq: 2, EventTime: t0.Add(2 * time.Second), Value: 2}
		wm := t0.Add(5 * time.Second)

		var wg sync.WaitGroup
		var res *IngestResult
		var wmErr error
		wg.Add(2)
		go func() { defer wg.Done(); res, _ = svc.Ingest(late) }()
		go func() { defer wg.Done(); _, wmErr = svc.AdvanceWatermark("dev-1", wm, d.Version) }()
		wg.Wait()

		sum, _, _, _ := svc.Summary("dev-1")
		rej, _ := svc.Rejections("dev-1")
		// 两种串行化结果必居其一，且状态与结果自洽：
		// 事件先提交 → 被接收并计入汇总；水位先推进 → 事件被记为迟到拒绝。
		switch res.Status {
		case StatusAccepted:
			if sum.Count != 2 || len(rej) != 0 {
				t.Fatalf("accepted but summary=%+v rejections=%+v", sum, rej)
			}
		case StatusRejected:
			if res.Reason != ReasonLateEvent || sum.Count != 1 || len(rej) != 1 {
				t.Fatalf("rejected but res=%+v summary=%+v rejections=%+v", res, sum, rej)
			}
		default:
			t.Fatalf("unexpected status %q (wmErr=%v)", res.Status, wmErr)
		}
	}
}

func TestEventQueries(t *testing.T) {
	svc := newService(t)
	mustRegister(t, svc, "dev-1", 10)
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := svc.Ingest(ev(seq, float64(seq))); err != nil {
			t.Fatal(err)
		}
	}
	events, err := svc.Events("dev-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Seq != 1 || events[2].Seq != 3 {
		t.Fatalf("events = %+v", events)
	}
	one, err := svc.Event("dev-1", 2)
	if err != nil {
		t.Fatal(err)
	}
	if one.Seq != 2 || one.Value != 2 {
		t.Fatalf("event = %+v", one)
	}
	if _, err := svc.Event("dev-1", 99); err == nil {
		t.Fatal("want error for missing event")
	}
	if _, err := svc.Events("nope"); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("err = %v, want ErrDeviceNotFound", err)
	}
}

func TestRegisterDeviceValidation(t *testing.T) {
	svc := newService(t)
	if _, err := svc.RegisterDevice("", 10); err == nil {
		t.Fatal("want error for empty id")
	}
	if _, err := svc.RegisterDevice("dev-1", 0); err == nil {
		t.Fatal("want error for zero window")
	}
	mustRegister(t, svc, "dev-1", 10)
	if _, err := svc.RegisterDevice("dev-1", 10); !errors.Is(err, ErrDeviceExists) {
		t.Fatalf("err = %v, want ErrDeviceExists", err)
	}
	if _, err := svc.Ingest(ev(1, 1)); err != nil {
		t.Fatal(err)
	}
	unknown := ev(1, 1)
	unknown.DeviceID = "nope"
	if _, err := svc.Ingest(unknown); !errors.Is(err, ErrDeviceNotFound) {
		t.Fatalf("err = %v, want ErrDeviceNotFound", err)
	}
}

func TestManyDevicesIsolated(t *testing.T) {
	svc := newService(t)
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("dev-%d", i)
		mustRegister(t, svc, id, 10)
		e := ev(1, float64(i+1))
		e.DeviceID = id
		if _, err := svc.Ingest(e); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		sum, ack, _, err := svc.Summary(fmt.Sprintf("dev-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		if sum.Count != 1 || sum.TotalValue != float64(i+1) || ack != 1 {
			t.Fatalf("dev-%d: summary=%+v ack=%d", i, sum, ack)
		}
	}
}
