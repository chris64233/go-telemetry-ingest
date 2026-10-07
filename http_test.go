package gotelemetryingest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T, store Store) (*httptest.Server, *Service) {
	t.Helper()
	svc := NewService(store)
	return httptest.NewServer(NewHTTPServer(svc, nil)), svc
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var r *http.Request
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = httptest.NewRequest(method, path, bytes.NewReader(b))
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

func TestHTTPEndToEnd(t *testing.T) {
	srv, _ := newTestServer(t, NewMemoryStore())
	defer srv.Close()
	client := srv.Client()

	post := func(path string, body string) (int, map[string]any) {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	put := func(path, body string) int {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+path, strings.NewReader(body))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	get := func(path string) (int, map[string]any) {
		resp, err := client.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	// 登记。
	if code := put("/v1/devices/sensor-a", `{"window":3}`); code != http.StatusCreated {
		t.Fatalf("register status=%d", code)
	}
	// 重复登记同配置 → 200 幂等；不同配置 → 409。
	if code := put("/v1/devices/sensor-a", `{"window":3}`); code != http.StatusOK {
		t.Fatalf("idempotent register=%d", code)
	}
	if code := put("/v1/devices/sensor-a", `{"window":9}`); code != http.StatusConflict {
		t.Fatalf("conflicting register=%d", code)
	}

	ts := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	ev := func(seq int64, v float64) string {
		b, _ := json.Marshal(eventRequest{
			Sequence:  seq,
			EventTime: ts.Add(time.Duration(seq) * time.Second).Format(time.RFC3339Nano),
			Metric:    "temp", Value: v,
		})
		return string(b)
	}

	// 乱序：2 先到 buffered，1 补齐后一次性 acked 到 2。
	code, out := post("/v1/devices/sensor-a/events", ev(2, 20))
	if code != 200 || out["status"] != "buffered" {
		t.Fatalf("ev2 code=%d out=%v", code, out)
	}
	code, out = post("/v1/devices/sensor-a/events", ev(2, 20))
	if code != 200 || out["duplicate"] != true {
		t.Fatalf("dup: code=%d out=%v", code, out)
	}
	code, out = post("/v1/devices/sensor-a/events", ev(1, 10))
	if code != 200 || out["status"] != "acked" || out["ackedSeq"].(float64) != 2 {
		t.Fatalf("ev1: code=%d out=%v", code, out)
	}

	// 冲突：同序号不同内容 → 409 reason=conflict。
	bad := eventRequest{Sequence: 1, EventTime: ts.Add(time.Second).Format(time.RFC3339Nano), Metric: "temp", Value: 999}
	badBody, _ := json.Marshal(bad)
	code, out = post("/v1/devices/sensor-a/events", string(badBody))
	if code != http.StatusConflict || out["reason"] != ReasonConflict {
		t.Fatalf("conflict: code=%d out=%v", code, out)
	}

	// 超窗：acked=2,window=3，seq=7 超上限 6。
	code, out = post("/v1/devices/sensor-a/events", ev(7, 70))
	if code != http.StatusConflict || out["reason"] != ReasonOutOfWindow {
		t.Fatalf("out-of-window: code=%d out=%v", code, out)
	}

	// 水位推进到 4（seq3 缺失 → 封闭缺口；seq4 已在窗口内提前到达则结算）。
	code, out = post("/v1/devices/sensor-a/events", ev(4, 40))
	if code != 200 || out["status"] != "buffered" {
		t.Fatalf("ev4: code=%d out=%v", code, out)
	}
	code, out = post("/v1/devices/sensor-a/watermark", `{"closedSeq":4}`)
	if code != 200 || out["closedSeq"].(float64) != 4 {
		t.Fatalf("watermark: code=%d out=%v", code, out)
	}

	// 迟到事件 seq3 → 409 late。
	code, out = post("/v1/devices/sensor-a/events", ev(3, 30))
	if code != http.StatusConflict || out["reason"] != ReasonLate {
		t.Fatalf("late: code=%d out=%v", code, out)
	}

	// 汇总：seq1、2、4 共 3 条，和 70。
	code, out = get("/v1/devices/sensor-a/summary")
	if code != 200 || out["eventCount"].(float64) != 3 || out["sum"].(float64) != 70 {
		t.Fatalf("summary: code=%d out=%v", code, out)
	}

	// 缺口：sealed 3..3。
	code, out = get("/v1/devices/sensor-a/gaps")
	if code != 200 {
		t.Fatalf("gaps code=%d", code)
	}
	sealed := out["sealed"].([]any)
	if len(sealed) != 1 || sealed[0].(map[string]any)["start"].(float64) != 3 {
		t.Fatalf("sealed gaps: %v", sealed)
	}

	// 原始事件：3 条（1、2、4）。
	code, out = get("/v1/devices/sensor-a/events")
	events := out["events"].([]any)
	if code != 200 || len(events) != 3 {
		t.Fatalf("events: code=%d %v", code, out)
	}

	// 拒绝记录：conflict、out_of_window、late 共 3 条。
	code, out = get("/v1/devices/sensor-a/rejections")
	if code != 200 || len(out["rejections"].([]any)) != 3 {
		t.Fatalf("rejections: code=%d %v", code, out)
	}

	// 设备不存在 → 404；非法 JSON → 400。
	code, _ = get("/v1/devices/ghost/summary")
	if code != http.StatusNotFound {
		t.Fatalf("missing device code=%d", code)
	}
	code, _ = post("/v1/devices/sensor-a/events", `{not json`)
	if code != http.StatusBadRequest {
		t.Fatalf("bad json code=%d", code)
	}

	// 设备列表。
	code, out = get("/v1/devices")
	if code != 200 || len(out["deviceIds"].([]any)) != 1 {
		t.Fatalf("list: code=%d %v", code, out)
	}
}

// TestHTTPCalibrationFlow 用 ResponseRecorder 验证标定相关 HTTP 接口，
// 不依赖端口绑定。
func TestHTTPCalibrationFlow(t *testing.T) {
	svc := NewService(NewMemoryStore())
	h := NewHTTPServer(svc, nil)

	code, out := doJSON(t, h, http.MethodPut, "/v1/devices/d", map[string]int64{"window": 10})
	if code != http.StatusCreated {
		t.Fatalf("register: %d %v", code, out)
	}

	// 设备视图：含基线标定。
	code, out = doJSON(t, h, http.MethodGet, "/v1/devices/d", nil)
	if code != http.StatusOK || out["currentCalibrationId"] != BaselineCalibrationID {
		t.Fatalf("device view: %d %v", code, out)
	}
	cals := out["calibrations"].([]any)
	if len(cals) != 1 {
		t.Fatalf("baseline calibration missing: %v", cals)
	}

	// 登记标定版本 c1，seq3 生效。
	calBody := map[string]any{
		"calibrationId": "c1", "parameterDigest": "sha256:abc",
		"effectiveSeq": 3, "operator": "ops",
	}
	code, out = doJSON(t, h, http.MethodPut, "/v1/devices/d/calibrations", calBody)
	if code != http.StatusCreated || out["currentCalibrationId"] != BaselineCalibrationID {
		t.Fatalf("apply calibration: %d %v", code, out)
	}
	// 相同标定号重放 → 200 duplicate=true。
	code, out = doJSON(t, h, http.MethodPut, "/v1/devices/d/calibrations", calBody)
	if code != http.StatusOK || out["duplicate"] != true {
		t.Fatalf("calibration replay: %d %v", code, out)
	}
	// 参数变化重放 → 409。
	tampered := map[string]any{
		"calibrationId": "c1", "parameterDigest": "sha256:zzz",
		"effectiveSeq": 3, "operator": "ops",
	}
	code, out = doJSON(t, h, http.MethodPut, "/v1/devices/d/calibrations", tampered)
	if code != http.StatusConflict {
		t.Fatalf("calibration conflict: %d %v", code, out)
	}
	// 生效序号早于确认读数 → 400。
	evBody := map[string]any{
		"sequence": 1, "eventTime": "2026-10-01T00:00:01Z",
		"metric": "temp", "value": 10,
	}
	code, _ = doJSON(t, h, http.MethodPost, "/v1/devices/d/events", evBody)
	if code != http.StatusOK {
		t.Fatalf("event1: %d", code)
	}
	oldCal := map[string]any{
		"calibrationId": "old", "parameterDigest": "x",
		"effectiveSeq": 1, "operator": "ops",
	}
	code, out = doJSON(t, h, http.MethodPut, "/v1/devices/d/calibrations", oldCal)
	if code != http.StatusBadRequest {
		t.Fatalf("stale calibration: %d %v", code, out)
	}

	// seq2 基线、seq3 起 c1。
	evBody = map[string]any{
		"sequence": 2, "eventTime": "2026-10-01T00:00:02Z",
		"metric": "temp", "value": 20,
	}
	code, out = doJSON(t, h, http.MethodPost, "/v1/devices/d/events", evBody)
	if code != http.StatusOK || out["calibrationId"] != BaselineCalibrationID {
		t.Fatalf("event2 binding: %d %v", code, out)
	}
	evBody = map[string]any{
		"sequence": 3, "eventTime": "2026-10-01T00:00:03Z",
		"metric": "temp", "value": 30,
	}
	code, out = doJSON(t, h, http.MethodPost, "/v1/devices/d/events", evBody)
	if code != http.StatusOK || out["calibrationId"] != "c1" {
		t.Fatalf("event3 binding: %d %v", code, out)
	}

	// 标定历史与汇总。
	code, out = doJSON(t, h, http.MethodGet, "/v1/devices/d/calibrations", nil)
	if code != http.StatusOK || out["currentCalibrationId"] != "c1" ||
		len(out["calibrations"].([]any)) != 2 {
		t.Fatalf("calibration history: %d %v", code, out)
	}
	code, out = doJSON(t, h, http.MethodGet, "/v1/devices/d/summary", nil)
	if code != http.StatusOK {
		t.Fatalf("summary: %d", code)
	}
	current := out["current"].(map[string]any)
	if current["calibrationId"] != "c1" || current["eventCount"].(float64) != 1 {
		t.Fatalf("current agg: %v", current)
	}
	history := out["history"].([]any)
	h0 := history[0].(map[string]any)
	if h0["calibrationId"] != BaselineCalibrationID || h0["current"] != false ||
		h0["eventCount"].(float64) != 2 {
		t.Fatalf("history baseline: %v", h0)
	}

	// 读数查询含标定采用依据。
	code, out = doJSON(t, h, http.MethodGet, "/v1/devices/d/events?from=3", nil)
	evs := out["events"].([]any)
	e3 := evs[0].(map[string]any)
	if e3["calibrationId"] != "c1" ||
		e3["calibrationEffectiveSeq"].(float64) != 3 ||
		e3["calibrationDigest"] != "sha256:abc" {
		t.Fatalf("event adoption basis: %v", e3)
	}

	// 同标定号用于另一台设备 → 409。
	doJSON(t, h, http.MethodPut, "/v1/devices/other", map[string]int64{"window": 10})
	code, out = doJSON(t, h, http.MethodPut, "/v1/devices/other/calibrations", calBody)
	if code != http.StatusConflict {
		t.Fatalf("cross-device calibration id: %d %v", code, out)
	}
}

func TestHTTPServerWithFileStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(NewHTTPServer(NewService(fs), nil))
	defer srv.Close()
	defer fs.Close()

	reg, err := http.NewRequest(http.MethodPut, srv.URL+"/v1/devices/d", strings.NewReader(`{"window":2}`))
	if err != nil {
		t.Fatal(err)
	}
	regResp, err := http.DefaultClient.Do(reg)
	if err != nil {
		t.Fatal(err)
	}
	regResp.Body.Close()
	if regResp.StatusCode != http.StatusCreated {
		t.Fatalf("register status=%d", regResp.StatusCode)
	}

	resp, err := http.Post(srv.URL+"/v1/devices/d/events", "application/json",
		strings.NewReader(`{"sequence":1,"eventTime":"2026-09-28T00:00:01Z","metric":"m","value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}
