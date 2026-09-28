package gotelemetryingest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHTTPAPI(t *testing.T) {
	h := NewHandler(newService(t))

	rec := do(t, h, "POST", "/devices", map[string]any{"id": "dev-1", "window_size": 10})
	if rec.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}

	// 乱序接收 seq=2（seq=1 未到，Ack 不推进）。
	rec = do(t, h, "POST", "/devices/dev-1/events",
		map[string]any{"seq": 2, "event_time": t0.Add(2_000_000_000), "value": 2})
	if rec.Code != http.StatusCreated {
		t.Fatalf("ingest 2: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "GET", "/devices/dev-1/gaps", nil)
	var gaps GapReport
	if err := json.NewDecoder(rec.Body).Decode(&gaps); err != nil {
		t.Fatal(err)
	}
	if gaps.Ack != 0 || len(gaps.Missing) != 1 || gaps.Missing[0] != 1 {
		t.Fatalf("gaps = %+v", gaps)
	}

	// 重复提交同内容 → 200 duplicate；不同内容 → 409。
	rec = do(t, h, "POST", "/devices/dev-1/events",
		map[string]any{"seq": 2, "event_time": t0.Add(2_000_000_000), "value": 2})
	if rec.Code != http.StatusOK {
		t.Fatalf("duplicate: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "POST", "/devices/dev-1/events",
		map[string]any{"seq": 2, "event_time": t0.Add(2_000_000_000), "value": 99})
	if rec.Code != http.StatusConflict {
		t.Fatalf("conflict: %d %s", rec.Code, rec.Body)
	}

	// 补缺口后汇总更新。
	do(t, h, "POST", "/devices/dev-1/events",
		map[string]any{"seq": 1, "event_time": t0.Add(1_000_000_000), "value": 1})
	rec = do(t, h, "GET", "/devices/dev-1/summary", nil)
	var sumResp struct {
		Summary Summary `json:"summary"`
		Ack     uint64  `json:"ack"`
		Version uint64  `json:"version"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&sumResp); err != nil {
		t.Fatal(err)
	}
	if sumResp.Ack != 2 || sumResp.Summary.Count != 2 || sumResp.Summary.TotalValue != 3 {
		t.Fatalf("summary = %+v", sumResp)
	}

	// 水位推进：错误版本 → 409；正确版本 → 200。
	rec = do(t, h, "POST", "/devices/dev-1/watermark",
		map[string]any{"watermark": t0.Add(3_000_000_000), "expected_version": 999})
	if rec.Code != http.StatusConflict {
		t.Fatalf("watermark stale version: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "POST", "/devices/dev-1/watermark",
		map[string]any{"watermark": t0.Add(3_000_000_000), "expected_version": sumResp.Version})
	if rec.Code != http.StatusOK {
		t.Fatalf("watermark: %d %s", rec.Code, rec.Body)
	}

	// 迟到事件被拒绝并可在 rejections 中查询。
	rec = do(t, h, "POST", "/devices/dev-1/events",
		map[string]any{"seq": 3, "event_time": t0.Add(1_500_000_000), "value": 3})
	if rec.Code != http.StatusOK {
		t.Fatalf("late ingest: %d %s", rec.Code, rec.Body)
	}
	var res IngestResult
	if err := json.NewDecoder(rec.Body).Decode(&res); err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusRejected || res.Reason != ReasonLateEvent {
		t.Fatalf("res = %+v", res)
	}
	rec = do(t, h, "GET", "/devices/dev-1/rejections", nil)
	var rej []Rejection
	if err := json.NewDecoder(rec.Body).Decode(&rej); err != nil {
		t.Fatal(err)
	}
	if len(rej) != 1 || rej[0].Reason != ReasonLateEvent {
		t.Fatalf("rejections = %+v", rej)
	}

	// 原始事件查询与未知设备。
	rec = do(t, h, "GET", "/devices/dev-1/events?seq=1", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get event: %d %s", rec.Code, rec.Body)
	}
	rec = do(t, h, "GET", "/devices/nope/summary", nil)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown device: %d", rec.Code)
	}
}
