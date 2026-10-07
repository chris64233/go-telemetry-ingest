package gotelemetryingest

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func calEventAt(seq int64, v float64) Event {
	return Event{
		Sequence:  seq,
		EventTime: time.Date(2026, 10, 1, 0, 0, int(seq), 0, time.UTC),
		Metric:    "temp",
		Value:     v,
	}
}

func mustApplyCal(t *testing.T, svc *Service, dev, id, digest string, seq int64, operator string) *CalibrationResult {
	t.Helper()
	res, err := svc.ApplyCalibration(dev, CalibrationRequest{
		ID: id, ParameterDigest: digest, EffectiveSeq: seq, Operator: operator,
	})
	if err != nil {
		t.Fatalf("ApplyCalibration(%s@%d): %v", id, seq, err)
	}
	return res
}

func TestBaselineCalibrationBoundBeforeSwitch(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)

	out := mustIngest(t, svc, "d", calEventAt(1, 10))
	if out.CalibrationID != BaselineCalibrationID {
		t.Fatalf("seq1 bound to %q, want baseline", out.CalibrationID)
	}
	recs, err := svc.ListEvents("d", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if recs[0].CalibrationID != BaselineCalibrationID ||
		recs[0].CalibrationEffectiveSeq != 1 || recs[0].CalibrationDigest != "" {
		t.Fatalf("adoption basis not persisted: %+v", recs[0])
	}
	sum, _ := svc.Summary("d")
	if sum.CurrentCalibrationID != BaselineCalibrationID {
		t.Fatalf("current calibration: %s", sum.CurrentCalibrationID)
	}
	if sum.Current.EventCount != 1 || sum.Current.Sum != 10 {
		t.Fatalf("baseline current agg: %+v", sum.Current)
	}
}

func TestCalibrationSwitchBoundaryBindsSingleVersion(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)

	mustIngest(t, svc, "d", calEventAt(1, 10))
	mustApplyCal(t, svc, "d", "c1", "digest-c1", 3, "ops")

	o2 := mustIngest(t, svc, "d", calEventAt(2, 20))
	if o2.CalibrationID != BaselineCalibrationID {
		t.Fatalf("seq2 = %s, want baseline", o2.CalibrationID)
	}
	o3 := mustIngest(t, svc, "d", calEventAt(3, 30))
	if o3.CalibrationID != "c1" {
		t.Fatalf("seq3 = %s, want c1", o3.CalibrationID)
	}
	o4 := mustIngest(t, svc, "d", calEventAt(4, 40))
	if o4.CalibrationID != "c1" {
		t.Fatalf("seq4 = %s, want c1", o4.CalibrationID)
	}

	cals, current, err := svc.ListCalibrations("d")
	if err != nil {
		t.Fatal(err)
	}
	if len(cals) != 2 || cals[1].ID != "c1" || cals[1].ParameterDigest != "digest-c1" ||
		cals[1].EffectiveSeq != 3 || cals[1].Operator != "ops" {
		t.Fatalf("calibration history wrong: %+v", cals)
	}
	if current != "c1" {
		t.Fatalf("current = %s, want c1 (acked=4)", current)
	}

	sum, _ := svc.Summary("d")
	if sum.CurrentCalibrationID != "c1" || sum.Current.EventCount != 2 || sum.Current.Sum != 70 {
		t.Fatalf("current agg wrong: %+v", sum)
	}
	if len(sum.History) != 2 {
		t.Fatalf("history entries: %d", len(sum.History))
	}
	hist := sum.History[0]
	if hist.CalibrationID != BaselineCalibrationID || hist.Current ||
		hist.EventCount != 2 || hist.Sum != 30 {
		t.Fatalf("baseline history/diagnostic entry wrong: %+v", hist)
	}
	if !sum.History[1].Current {
		t.Fatalf("c1 must be marked current: %+v", sum.History[1])
	}
	if sum.EventCount != 4 || sum.Sum != 100 {
		t.Fatalf("overall summary wrong: %+v", sum)
	}
}

func TestEffectiveSeqCannotBeAtOrBeforeAcked(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEventAt(1, 10))
	mustIngest(t, svc, "d", calEventAt(2, 20))

	for _, seq := range []int64{1, 2} {
		_, err := svc.ApplyCalibration("d", CalibrationRequest{
			ID: fmt.Sprintf("bad%d", seq), ParameterDigest: "x",
			EffectiveSeq: seq, Operator: "ops",
		})
		if !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("effectiveSeq=%d: want ErrInvalidArgument, got %v", seq, err)
		}
	}
	sum, _ := svc.Summary("d")
	if sum.CurrentCalibrationID != BaselineCalibrationID {
		t.Fatalf("current changed after failed calibration: %s", sum.CurrentCalibrationID)
	}
	cals, current, _ := svc.ListCalibrations("d")
	if len(cals) != 1 || current != BaselineCalibrationID {
		t.Fatalf("calibration history changed: %+v", cals)
	}
}

func TestCalibrationReplayIdempotentAndConflict(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 100)
	first := mustApplyCal(t, svc, "d", "c1", "dig", 10, "ops")
	if first.Duplicate {
		t.Fatal("first apply must not be duplicate")
	}

	replay, err := svc.ApplyCalibration("d", CalibrationRequest{
		ID: "c1", ParameterDigest: "dig", EffectiveSeq: 10, Operator: "ops",
	})
	if err != nil || !replay.Duplicate || replay.CurrentCalibrationID != first.CurrentCalibrationID {
		t.Fatalf("replay: %+v err=%v", replay, err)
	}

	cases := []CalibrationRequest{
		{ID: "c1", ParameterDigest: "other", EffectiveSeq: 10, Operator: "ops"},
		{ID: "c1", ParameterDigest: "dig", EffectiveSeq: 11, Operator: "ops"},
		{ID: "c1", ParameterDigest: "dig", EffectiveSeq: 10, Operator: "other"},
	}
	for _, req := range cases {
		if _, err := svc.ApplyCalibration("d", req); !errors.Is(err, ErrCalibrationConflict) {
			t.Fatalf("req=%+v: want conflict, got %v", req, err)
		}
	}

	cals, current, _ := svc.ListCalibrations("d")
	if len(cals) != 2 || current != BaselineCalibrationID {
		t.Fatalf("state changed after conflicting replays: %+v current=%s", cals, current)
	}

	mustReg(t, svc, "d2", 100)
	_, err = svc.ApplyCalibration("d2", CalibrationRequest{
		ID: "c1", ParameterDigest: "dig", EffectiveSeq: 5, Operator: "ops",
	})
	if !errors.Is(err, ErrCalibrationConflict) {
		t.Fatalf("same id on other device: want conflict, got %v", err)
	}
}

func TestCalibrationEffectiveSeqMustAdvance(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 100)
	mustApplyCal(t, svc, "d", "c1", "d1", 10, "ops")
	_, err := svc.ApplyCalibration("d", CalibrationRequest{
		ID: "c2", ParameterDigest: "d2", EffectiveSeq: 5, Operator: "ops",
	})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("earlier effectiveSeq: want invalid argument, got %v", err)
	}
}

func TestBufferedReadingsKeepBindingAfterSwitch(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)

	o2 := mustIngest(t, svc, "d", calEventAt(2, 20))
	if o2.Status != ResultBuffered {
		t.Fatalf("seq2 status = %s", o2.Status)
	}
	mustApplyCal(t, svc, "d", "c1", "dig", 2, "ops")
	mustIngest(t, svc, "d", calEventAt(1, 10))

	recs, _ := svc.ListEvents("d", 0, 10)
	bySeq := map[int64]EventRecord{}
	for _, r := range recs {
		bySeq[r.Event.Sequence] = r
	}
	if bySeq[2].CalibrationID != BaselineCalibrationID {
		t.Fatalf("buffered seq2 rebound to %s", bySeq[2].CalibrationID)
	}
	o3 := mustIngest(t, svc, "d", calEventAt(3, 30))
	if o3.CalibrationID != "c1" {
		t.Fatalf("seq3 = %s, want c1", o3.CalibrationID)
	}

	sum, _ := svc.Summary("d")
	var base, c1 CalibrationAggregate
	for _, h := range sum.History {
		switch h.CalibrationID {
		case BaselineCalibrationID:
			base = h
		case "c1":
			c1 = h
		}
	}
	if base.EventCount != 2 || base.Sum != 30 {
		t.Fatalf("baseline agg after switch: %+v", base)
	}
	if c1.EventCount != 1 || c1.Sum != 30 || !c1.Current {
		t.Fatalf("c1 agg: %+v", c1)
	}
}

func TestLateReadingUnderOldCalibrationRecordedNotAggregated(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEventAt(1, 10))
	mustApplyCal(t, svc, "d", "c1", "dig", 3, "ops")
	mustIngest(t, svc, "d", calEventAt(3, 30))
	mustIngest(t, svc, "d", calEventAt(4, 40))

	if _, err := svc.AdvanceWatermark("d", 4); err != nil {
		t.Fatal(err)
	}
	sum, _ := svc.Summary("d")
	var base *CalibrationAggregate
	for i := range sum.History {
		if sum.History[i].CalibrationID == BaselineCalibrationID {
			base = &sum.History[i]
		}
	}
	if base == nil || !base.Frozen || base.EventCount != 1 || base.Sum != 10 {
		t.Fatalf("baseline not frozen as expected: %+v", base)
	}

	out, err := svc.Ingest("d", calEventAt(2, 99))
	if err == nil {
		t.Fatal("late ingest must return RejectedError")
	}
	var rej *RejectedError
	if !errors.As(err, &rej) || rej.Outcome.Reason != ReasonLate {
		t.Fatalf("want late rejection, got %v", err)
	}
	if out.CalibrationID != BaselineCalibrationID {
		t.Fatalf("late reading basis = %s, want baseline", out.CalibrationID)
	}
	sum2, _ := svc.Summary("d")
	if sum2.Current.EventCount != sum.Current.EventCount ||
		sum2.History[0].EventCount != base.EventCount ||
		sum2.History[0].Sum != base.Sum {
		t.Fatalf("late reading rewrote aggregation: before=%+v after=%+v", sum, sum2)
	}
	recs, _ := svc.ListRejections("d", 10)
	if len(recs) != 1 || recs[0].Reason != ReasonLate ||
		recs[0].CalibrationID != BaselineCalibrationID {
		t.Fatalf("late rejection record: %+v", recs)
	}
}

func TestDuplicateReadingKeepsCalibrationBasis(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustApplyCal(t, svc, "d", "c1", "dig", 2, "ops")
	first := mustIngest(t, svc, "d", calEventAt(2, 20))
	if first.CalibrationID != "c1" {
		t.Fatalf("first = %s", first.CalibrationID)
	}
	dup, err := svc.Ingest("d", calEventAt(2, 20))
	if err != nil || !dup.Duplicate || dup.CalibrationID != "c1" {
		t.Fatalf("duplicate outcome: %+v err=%v", dup, err)
	}
	_, err = svc.Ingest("d", calEventAt(2, 21))
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("same seq different content: want ErrConflict, got %v", err)
	}
}

func TestConcurrentIngestCalibrationWatermarkSummary(t *testing.T) {
	svc := NewService(NewMemoryStore())
	const n = 100
	mustReg(t, svc, "dev", n)
	// 先登记两个未来生效的标定版本；并发中试图重放同号但改摘要的请求
	// 必须得到冲突，且不改变版本。
	mustApplyCal(t, svc, "dev", "c1", "dig1", 40, "ops")
	mustApplyCal(t, svc, "dev", "c2", "dig2", 80, "ops")

	var wg sync.WaitGroup
	errCh := make(chan error, n*3)
	ingestDone := make(chan struct{})

	// 100 条读数乱序重复上报；水位分段推进；汇总查询与全部写操作并发；
	// 另一协程重放标定（幂等）并尝试冲突改写（必须失败）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(ingestDone)
		for i := 0; i < n; i++ {
			seq := int64((i*37)%n + 1)
			if _, err := svc.Ingest("dev", calEventAt(seq, float64(seq))); err != nil {
				var rej *RejectedError
				if !errors.As(err, &rej) {
					errCh <- err
				}
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			if _, err := svc.ApplyCalibration("dev", CalibrationRequest{
				ID: "c1", ParameterDigest: "dig1", EffectiveSeq: 40, Operator: "ops",
			}); err != nil {
				errCh <- fmt.Errorf("replay: %w", err)
			}
		}
		if _, err := svc.ApplyCalibration("dev", CalibrationRequest{
			ID: "c2", ParameterDigest: "tampered", EffectiveSeq: 80, Operator: "ops",
		}); !errors.Is(err, ErrCalibrationConflict) {
			errCh <- fmt.Errorf("tampered replay want conflict, got %v", err)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ingestDone
		for _, target := range []int64{60, n} {
			if _, err := svc.AdvanceWatermark("dev", target); err != nil {
				errCh <- err
			}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if _, err := svc.Summary("dev"); err != nil {
				errCh <- err
			}
		}
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent op error: %v", err)
	}

	// 水位关闭到 n：全部 100 个序号恰好结算一次。
	sum, err := svc.Summary("dev")
	if err != nil {
		t.Fatal(err)
	}
	if sum.ClosedSeq != n || sum.AckedSeq != n {
		t.Fatalf("watermark/acked = %d/%d", sum.ClosedSeq, sum.AckedSeq)
	}
	if sum.EventCount != n {
		t.Fatalf("eventCount=%d want %d", sum.EventCount, n)
	}
	var want float64
	for seq := int64(1); seq <= n; seq++ {
		want += float64(seq)
	}
	if diff := mathAbs(sum.Sum - want); diff > 1e-6 {
		t.Fatalf("sum=%v want %v", sum.Sum, want)
	}
	// 三个版本的聚合计数必须覆盖全部读数且互不重叠。
	if len(sum.History) != 3 {
		t.Fatalf("calibration history: %+v", sum.History)
	}
	var countByCal int64
	for _, h := range sum.History {
		if !h.Current && !h.Frozen {
			t.Fatalf("historical calibration %s should be frozen after close", h.CalibrationID)
		}
		countByCal += h.EventCount
	}
	if countByCal != n {
		t.Fatalf("per-calibration counts=%d want %d", countByCal, n)
	}
	// 当前版本为最后一个生效的 c2。
	if sum.CurrentCalibrationID != "c2" || sum.Current.CalibrationID != "c2" {
		t.Fatalf("current = %s/%s", sum.CurrentCalibrationID, sum.Current.CalibrationID)
	}

	// 每条已确认读数都保存且仅保存一个标定版本及依据。
	recs, err := svc.ListEvents("dev", 0, 500)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int64]bool{}
	for _, r := range recs {
		if seen[r.Event.Sequence] {
			t.Fatalf("seq %d stored twice", r.Event.Sequence)
		}
		seen[r.Event.Sequence] = true
		if r.CalibrationID == "" {
			t.Fatalf("seq %d missing calibration binding", r.Event.Sequence)
		}
		var eff int64
		switch r.CalibrationID {
		case BaselineCalibrationID:
			eff = 1
		case "c1":
			eff = 40
		case "c2":
			eff = 80
		default:
			t.Fatalf("unknown calibration %s", r.CalibrationID)
		}
		if r.CalibrationEffectiveSeq != eff {
			t.Fatalf("seq %d basis effectiveSeq=%d want %d",
				r.Event.Sequence, r.CalibrationEffectiveSeq, eff)
		}
	}
}

func mathAbs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func TestCalibrationStateSurvivesFileStoreRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(fs)
	mustReg(t, svc, "d", 20)
	mustIngest(t, svc, "d", calEventAt(1, 10))
	mustApplyCal(t, svc, "d", "c1", "dig1", 3, "ops")
	mustIngest(t, svc, "d", calEventAt(2, 20))
	mustIngest(t, svc, "d", calEventAt(3, 30))
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	fs2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	svc2 := NewService(fs2)

	cals, current, err := svc2.ListCalibrations("d")
	if err != nil {
		t.Fatal(err)
	}
	if len(cals) != 2 || current != "c1" {
		t.Fatalf("calibration history after restart: %+v current=%s", cals, current)
	}
	if cals[1].Operator != "ops" || cals[1].ParameterDigest != "dig1" {
		t.Fatalf("calibration record not fixed after restart: %+v", cals[1])
	}

	recs, _ := svc2.ListEvents("d", 0, 10)
	if len(recs) != 3 ||
		recs[0].CalibrationID != BaselineCalibrationID ||
		recs[2].CalibrationID != "c1" ||
		recs[2].CalibrationDigest != "dig1" {
		t.Fatalf("bindings/basis lost after restart: %+v", recs)
	}

	sum, _ := svc2.Summary("d")
	if sum.CurrentCalibrationID != "c1" || sum.Current.Sum != 30 ||
		len(sum.History) != 2 || sum.History[0].Sum != 30 {
		t.Fatalf("per-calibration aggregates lost after restart: %+v", sum)
	}

	// 重放同号标定仍幂等；换设备仍冲突。
	replay, err := svc2.ApplyCalibration("d", CalibrationRequest{
		ID: "c1", ParameterDigest: "dig1", EffectiveSeq: 3, Operator: "ops",
	})
	if err != nil || !replay.Duplicate {
		t.Fatalf("replay after restart: %+v err=%v", replay, err)
	}
}
