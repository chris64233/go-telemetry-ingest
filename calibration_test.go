package gotelemetryingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func calEvent(seq int64, v float64) Event {
	return Event{
		Sequence:  seq,
		EventTime: time.Date(2026, 10, 7, 0, 0, int(seq), 0, time.UTC),
		Metric:    "temp",
		Value:     v,
	}
}

func applyCal(t *testing.T, svc *Service, dev, cid string, eff int64, scale, offset float64, op string) *ApplyCalibrationResult {
	t.Helper()
	res, err := svc.ApplyCalibration(dev, cid, eff, CalibrationParams{Scale: scale, Offset: offset}, op)
	if err != nil {
		t.Fatalf("ApplyCalibration(%s): %v", cid, err)
	}
	return res
}

func TestBaselineCalibrationCreatedOnRegister(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)

	cals, err := svc.ListCalibrations("d")
	if err != nil {
		t.Fatal(err)
	}
	if len(cals) != 1 || cals[0].CalibrationID != BaselineCalibrationID ||
		cals[0].EffectiveSeq != 1 || cals[0].Params.Scale != 1 || cals[0].Params.Offset != 0 {
		t.Fatalf("baseline: %+v", cals)
	}
	if cals[0].Digest != digestParams(CalibrationParams{Scale: 1, Offset: 0}) {
		t.Fatalf("baseline digest mismatch: %s", cals[0].Digest)
	}

	mustIngest(t, svc, "d", calEvent(1, 10))
	sum, _ := svc.Summary("d")
	if sum.CurrentCalibration.CalibrationID != BaselineCalibrationID {
		t.Fatalf("current calibration: %+v", sum.CurrentCalibration)
	}
	if len(sum.Segments) != 1 || !sum.Segments[0].Current || sum.Segments[0].Sum != 10 {
		t.Fatalf("baseline segment: %+v", sum.Segments)
	}
}

func TestCalibrationSwitchBoundaryAndSingleBinding(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEvent(1, 10))
	applyCal(t, svc, "d", "cal-2", 3, 2, 1, "alice")

	out4 := mustIngest(t, svc, "d", calEvent(4, 40))
	if out4.Status != ResultBuffered || out4.CalibrationID != "" {
		t.Fatalf("buffered event must not bind early: %+v", out4)
	}
	out2 := mustIngest(t, svc, "d", calEvent(2, 20))
	if out2.Status != ResultAcked || out2.AckedSeq != 2 {
		t.Fatalf("seq2: %+v", out2)
	}
	if out2.CalibrationID != BaselineCalibrationID || out2.CalibratedValue != 20 ||
		out2.Basis != CalBasisAcked {
		t.Fatalf("seq2 binding: %+v", out2)
	}
	out3 := mustIngest(t, svc, "d", calEvent(3, 30))
	if out3.Status != ResultAcked || out3.CalibrationID != "cal-2" || out3.CalibratedValue != 61 {
		t.Fatalf("seq3 binding: %+v", out3)
	}
	recs, _ := svc.ListEvents("d", 0, 0)
	var r4 EventRecord
	for _, r := range recs {
		if r.Event.Sequence == 4 {
			r4 = r
		}
	}
	if r4.CalibrationID != "cal-2" || r4.CalibratedValue != 81 || r4.Basis != CalBasisAcked {
		t.Fatalf("seq4 record binding: %+v", r4)
	}

	sum, _ := svc.Summary("d")
	if sum.EventCount != 4 || sum.Sum != 172 {
		t.Fatalf("total summary: %+v", sum)
	}
	if len(sum.Segments) != 2 {
		t.Fatalf("segments: %+v", sum.Segments)
	}
	segBase, segCal := sum.Segments[0], sum.Segments[1]
	if segBase.CalibrationID != BaselineCalibrationID || segBase.Current ||
		segBase.EventCount != 2 || segBase.Sum != 30 {
		t.Fatalf("historical baseline segment: %+v", segBase)
	}
	if segCal.CalibrationID != "cal-2" || !segCal.Current ||
		segCal.EventCount != 2 || segCal.Sum != 142 || segCal.Min != 61 || segCal.Max != 81 {
		t.Fatalf("current cal segment: %+v", segCal)
	}
	if sum.CurrentCalibration.CalibrationID != "cal-2" {
		t.Fatalf("current calibration: %+v", sum.CurrentCalibration)
	}
}

func TestEffectiveSeqCannotBeBeforeAcked(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEvent(1, 1))
	mustIngest(t, svc, "d", calEvent(2, 2))

	if _, err := svc.ApplyCalibration("d", "late-cal", 2, CalibrationParams{Scale: 2, Offset: 0}, "bob"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("effectiveSeq==acked should be invalid: %v", err)
	}
	if _, err := svc.ApplyCalibration("d", "late-cal", 1, CalibrationParams{Scale: 2, Offset: 0}, "bob"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("effectiveSeq<acked should be invalid: %v", err)
	}
	sum, _ := svc.Summary("d")
	if sum.CurrentCalibration.CalibrationID != BaselineCalibrationID {
		t.Fatalf("failed calibration must not change active version: %+v", sum.CurrentCalibration)
	}
	cals, _ := svc.ListCalibrations("d")
	if len(cals) != 1 {
		t.Fatalf("failed calibration must not be recorded: %+v", cals)
	}
	if _, err := svc.ApplyCalibration("d", "bad", 3, CalibrationParams{Scale: 0, Offset: 0}, "bob"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("scale=0 should be invalid: %v", err)
	}
	if _, err := svc.ApplyCalibration("d", "bad", 3, CalibrationParams{Scale: 1, Offset: 0}, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty operator should be invalid: %v", err)
	}
}

func TestCalibrationReplayIdempotentAndConflict(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	first := applyCal(t, svc, "d", "cal-x", 5, 1.5, -0.25, "alice")
	if first.Replayed {
		t.Fatalf("first apply must not be a replay")
	}
	replay := applyCal(t, svc, "d", "cal-x", 5, 1.5, -0.25, "alice")
	if !replay.Replayed || replay.Calibration.Digest != first.Calibration.Digest {
		t.Fatalf("replay: %+v", replay)
	}
	cals, _ := svc.ListCalibrations("d")
	if len(cals) != 2 {
		t.Fatalf("replay must not duplicate the record: %+v", cals)
	}

	if _, err := svc.ApplyCalibration("d", "cal-x", 5, CalibrationParams{Scale: 1.6, Offset: -0.25}, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("params change should conflict: %v", err)
	}
	if _, err := svc.ApplyCalibration("d", "cal-x", 6, CalibrationParams{Scale: 1.5, Offset: -0.25}, "alice"); !errors.Is(err, ErrConflict) {
		t.Fatalf("effectiveSeq change should conflict: %v", err)
	}
	if _, err := svc.ApplyCalibration("d", "cal-x", 5, CalibrationParams{Scale: 1.5, Offset: -0.25}, "mallory"); !errors.Is(err, ErrConflict) {
		t.Fatalf("operator change should conflict: %v", err)
	}
	sum, _ := svc.Summary("d")
	if sum.CurrentCalibration.CalibrationID != "cal-x" {
		t.Fatalf("active version changed after conflict: %+v", sum.CurrentCalibration)
	}
}

func TestLateReadingRecordedButNeverRewritesAggregate(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEvent(1, 10))
	applyCal(t, svc, "d", "cal-2", 5, 2, 0, "alice")
	if _, err := svc.AdvanceWatermark("d", 3); err != nil {
		t.Fatal(err)
	}

	out, err := svc.Ingest("d", calEvent(2, 20))
	if err == nil || out.Reason != ReasonLate || out.CalibrationID != BaselineCalibrationID ||
		out.CalibratedValue != 20 || out.Basis != CalBasisLate {
		t.Fatalf("late diagnostic outcome: %+v err=%v", out, err)
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 1 || sum.Sum != 10 || sum.AckedSeq != 3 {
		t.Fatalf("late reading changed aggregate: %+v", sum)
	}
	if sum.DiagnosticLateCount != 1 {
		t.Fatalf("diagnostic count: %d", sum.DiagnosticLateCount)
	}
	lates, _ := svc.ListLateReadings("d", 0)
	if len(lates) != 1 || lates[0].Event.Sequence != 2 ||
		lates[0].CalibrationID != BaselineCalibrationID || lates[0].CalibratedValue != 20 {
		t.Fatalf("late readings: %+v", lates)
	}

	out2, err2 := svc.Ingest("d", calEvent(2, 20))
	if err2 == nil || !out2.Duplicate || out2.Reason != ReasonLate {
		t.Fatalf("late replay: %+v err=%v", out2, err2)
	}
	lates, _ = svc.ListLateReadings("d", 0)
	if len(lates) != 1 {
		t.Fatalf("late replay duplicated diagnostic: %d", len(lates))
	}
	if _, err := svc.Ingest("d", calEvent(2, 999)); !errors.Is(err, ErrConflict) {
		t.Fatalf("late reading different content should conflict: %v", err)
	}
	lates, _ = svc.ListLateReadings("d", 0)
	if lates[0].Event.Value != 20 {
		t.Fatalf("diagnostic record overwritten: %+v", lates[0])
	}

	mustIngest(t, svc, "d", calEvent(5, 100))
	// seq4 已封闭，seq5 需水位推进到 5 才按 cal-2 结算（raw=100 → 200）。
	if _, err := svc.AdvanceWatermark("d", 5); err != nil {
		t.Fatal(err)
	}
	sum, _ = svc.Summary("d")
	if sum.Sum != 210 {
		t.Fatalf("aggregate with new calibration: %+v", sum)
	}
}

func TestBufferedEventsBindAtWatermarkSettle(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEvent(3, 30))
	mustIngest(t, svc, "d", calEvent(2, 20))
	applyCal(t, svc, "d", "cal-2", 2, 10, 0, "alice")
	res, err := svc.AdvanceWatermark("d", 3)
	if err != nil || !res.Advanced {
		t.Fatalf("advance: %+v %v", res, err)
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 2 || sum.Sum != 500 {
		t.Fatalf("settled summary: %+v", sum)
	}
	recs, _ := svc.ListEvents("d", 0, 0)
	for _, r := range recs {
		if r.CalibrationID != "cal-2" || r.Basis != CalBasisSettled ||
			r.CalibratedValue != r.Event.Value*10 {
			t.Fatalf("settled event binding: %+v", r)
		}
	}
}

func TestConcurrentCalibrationIngestSummary(t *testing.T) {
	for _, store := range newConcurrentStores(t) {
		t.Run(store.name, func(t *testing.T) {
			svc := NewService(store.make(t))
			mustReg(t, svc, "dev", 1000)
			// seq1..100 先在 baseline 下确认。
			for seq := int64(1); seq <= 100; seq++ {
				mustIngest(t, svc, "dev", calEvent(seq, float64(seq)))
			}
			// 标定自 101 生效（scale=2）；与读数 101..300、汇总查询并发。
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				if _, err := svc.ApplyCalibration("dev", "cal-2", 101, CalibrationParams{Scale: 2, Offset: 0}, "alice"); err != nil {
					t.Errorf("apply calibration: %v", err)
				}
			}()
			go func() {
				defer wg.Done()
				for seq := int64(101); seq <= 300; seq++ {
					if _, err := svc.Ingest("dev", calEvent(seq, float64(seq))); err != nil {
						t.Errorf("ingest %d: %v", seq, err)
					}
				}
			}()
			// 并发查询汇总不得读到撕裂状态。
			for i := 0; i < 20; i++ {
				if _, err := svc.Summary("dev"); err != nil {
					t.Errorf("summary: %v", err)
				}
			}
			wg.Wait()

			sum, err := svc.Summary("dev")
			if err != nil {
				t.Fatal(err)
			}
			if sum.AckedSeq != 300 || sum.EventCount != 300 {
				t.Fatalf("positions: %+v", sum)
			}
			// 每条读数恰好绑定一个标定版本：分段计数之和必须等于总数。
			var segCount int64
			var segSum float64
			for _, seg := range sum.Segments {
				segCount += seg.EventCount
				segSum += seg.Sum
			}
			if segCount != 300 || segSum != sum.Sum {
				t.Fatalf("segment partition mismatch: count=%d sum=%v vs %+v", segCount, segSum, sum)
			}
			var baseCount, calCount int64
			var baseSum, calSum float64
			for seq := int64(1); seq <= 300; seq++ {
				if seq <= 100 {
					baseCount++
					baseSum += float64(seq)
				} else {
					calCount++
					calSum += 2 * float64(seq)
				}
			}
			for _, seg := range sum.Segments {
				switch seg.CalibrationID {
				case BaselineCalibrationID:
					if seg.EventCount != baseCount || seg.Sum != baseSum || seg.Current {
						t.Fatalf("baseline seg: %+v want count=%d sum=%v", seg, baseCount, baseSum)
					}
				case "cal-2":
					if seg.EventCount != calCount || seg.Sum != calSum || !seg.Current {
						t.Fatalf("cal-2 seg: %+v want count=%d sum=%v", seg, calCount, calSum)
					}
				default:
					t.Fatalf("unexpected segment %+v", seg)
				}
			}
			// 每条事件的采用依据都已持久化。
			recs, _ := svc.ListEvents("dev", 0, 500)
			if len(recs) != 300 {
				t.Fatalf("event records: %d", len(recs))
			}
			for _, r := range recs {
				if r.CalibrationID == "" || r.Basis == "" {
					t.Fatalf("event missing calibration basis: %+v", r)
				}
				want := BaselineCalibrationID
				if r.Event.Sequence >= 101 {
					want = "cal-2"
				}
				if r.CalibrationID != want {
					t.Fatalf("seq %d bound %s want %s", r.Event.Sequence, r.CalibrationID, want)
				}
			}
		})
	}
}

func TestHTTPCalibrationAndDiagnostics(t *testing.T) {
	h := NewHTTPServer(NewService(NewMemoryStore()), nil)
	do := func(method, path, body string) (int, map[string]any) {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, bytes.NewBufferString(body))
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		out := map[string]any{}
		if w.Body.Len() > 0 {
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatalf("decode %s: %v body=%s", path, err, w.Body.String())
			}
		}
		return w.Code, out
	}

	if code, _ := do(http.MethodPut, "/v1/devices/d", `{"window":10}`); code != http.StatusCreated {
		t.Fatalf("register code=%d", code)
	}
	// 标定历史初始只有 baseline。
	code, out := do(http.MethodGet, "/v1/devices/d/calibrations", "")
	if code != 200 || len(out["calibrations"].([]any)) != 1 {
		t.Fatalf("calibrations: code=%d %v", code, out)
	}
	// 切换标定：201。
	code, out = do(http.MethodPut, "/v1/devices/d/calibrations/cal-2",
		`{"effectiveSeq":3,"scale":2,"offset":1,"operator":"alice"}`)
	if code != http.StatusCreated {
		t.Fatalf("apply calibration code=%d %v", code, out)
	}
	digest := out["calibration"].(map[string]any)["digest"].(string)
	if digest == "" {
		t.Fatalf("digest missing: %v", out)
	}
	// 重放相同内容：200 replayed=true。
	code, out = do(http.MethodPut, "/v1/devices/d/calibrations/cal-2",
		`{"effectiveSeq":3,"scale":2,"offset":1,"operator":"alice"}`)
	if code != http.StatusOK || out["replayed"] != true {
		t.Fatalf("replay: code=%d %v", code, out)
	}
	// 参数变化：409。
	code, _ = do(http.MethodPut, "/v1/devices/d/calibrations/cal-2",
		`{"effectiveSeq":3,"scale":3,"offset":1,"operator":"alice"}`)
	if code != http.StatusConflict {
		t.Fatalf("conflict calibration code=%d", code)
	}
	// 非法入参：400。
	code, _ = do(http.MethodPut, "/v1/devices/d/calibrations/cal-3",
		`{"effectiveSeq":0,"scale":1,"offset":0,"operator":"bob"}`)
	if code != http.StatusBadRequest {
		t.Fatalf("invalid calibration code=%d", code)
	}

	// 读数确认并查询汇总：分段区分当前/历史。
	ingest := func(seq int64, v float64) {
		t.Helper()
		b, _ := json.Marshal(eventRequest{
			Sequence:  seq,
			EventTime: calEvent(seq, v).EventTime.Format(time.RFC3339Nano),
			Metric:    "temp", Value: v,
		})
		code, out = do(http.MethodPost, "/v1/devices/d/events", string(b))
		if code != http.StatusOK {
			t.Fatalf("ingest %d: code=%d %v", seq, code, out)
		}
	}
	ingest(1, 10)
	ingest(2, 20)
	ingest(3, 30)
	code, out = do(http.MethodGet, "/v1/devices/d/summary", "")
	if code != 200 {
		t.Fatalf("summary code=%d", code)
	}
	segs := out["segments"].([]any)
	if len(segs) != 2 {
		t.Fatalf("segments: %v", segs)
	}
	if out["currentCalibration"].(map[string]any)["calibrationId"] != "cal-2" {
		t.Fatalf("current calibration: %v", out["currentCalibration"])
	}
	// 读数序号与采用标定版本在原始事件查询中可见。
	code, out = do(http.MethodGet, "/v1/devices/d/events", "")
	evs := out["events"].([]any)
	if evs[2].(map[string]any)["calibrationId"] != "cal-2" {
		t.Fatalf("seq3 calibration binding: %v", evs[2])
	}

	// 关闭水位后迟到读数 → 409 late，并进入历史诊断区。
	code, _ = do(http.MethodPost, "/v1/devices/d/watermark", `{"closedSeq":5}`)
	if code != 200 {
		t.Fatalf("watermark code=%d", code)
	}
	code, out = do(http.MethodPost, "/v1/devices/d/events",
		`{"sequence":4,"eventTime":"2026-10-07T00:00:04Z","metric":"temp","value":40}`)
	if code != http.StatusConflict || out["reason"] != ReasonLate ||
		out["calibrationBasis"] != CalBasisLate {
		t.Fatalf("late event: code=%d %v", code, out)
	}
	code, out = do(http.MethodGet, "/v1/devices/d/diagnostics/late", "")
	if code != 200 || len(out["lateReadings"].([]any)) != 1 {
		t.Fatalf("diagnostics: code=%d %v", code, out)
	}
	lr := out["lateReadings"].([]any)[0].(map[string]any)
	if lr["calibrationId"] != "cal-2" || lr["calibratedValue"].(float64) != 81 {
		t.Fatalf("late reading diagnostic: %v", lr)
	}
}

func TestCalibrationPersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(fs)
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", calEvent(1, 10))
	applyCal(t, svc, "d", "cal-2", 2, 3, 1, "alice")
	mustIngest(t, svc, "d", calEvent(2, 20))
	if _, err := svc.AdvanceWatermark("d", 2); err != nil {
		t.Fatal(err)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}

	fs2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	svc2 := NewService(fs2)

	cals, err := svc2.ListCalibrations("d")
	if err != nil || len(cals) != 2 {
		t.Fatalf("calibration history after restart: %+v err=%v", cals, err)
	}
	sum, err := svc2.Summary("d")
	if err != nil {
		t.Fatal(err)
	}
	// raw 10 → 10（baseline），raw 20 → 61（cal-2）。
	if sum.EventCount != 2 || sum.Sum != 71 {
		t.Fatalf("summary after restart: %+v", sum)
	}
	if len(sum.Segments) != 2 || sum.Segments[1].CalibrationID != "cal-2" ||
		sum.Segments[1].Sum != 61 || !sum.Segments[1].Current {
		t.Fatalf("segments after restart: %+v", sum.Segments)
	}
	recs, _ := svc2.ListEvents("d", 0, 0)
	for _, r := range recs {
		if r.CalibrationID == "" || r.Basis == "" {
			t.Fatalf("calibration basis lost after restart: %+v", r)
		}
	}
	// 相同标定号重放在重启后仍返回原结果。
	res, err := svc2.ApplyCalibration("d", "cal-2", 2,
		CalibrationParams{Scale: 3, Offset: 1}, "alice")
	if err != nil || !res.Replayed {
		t.Fatalf("calibration replay after restart: %+v %v", res, err)
	}
}
