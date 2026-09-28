package gotelemetryingest

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTP API（所有时间均为 RFC3339Nano UTC）：
//
//	PUT    /v1/devices/:id                    登记设备        {"window": 10}
//	POST   /v1/devices/:id/events             接收事件
//	      {"sequence":1,"eventTime":"2026-09-28T00:00:00Z","metric":"m","value":1.5}
//	POST   /v1/devices/:id/watermark          推进关闭水位    {"closedSeq": 5}
//	GET    /v1/devices/:id/gaps               缺口查询
//	GET    /v1/devices/:id/events?from=&limit= 原始事件查询
//	GET    /v1/devices/:id/summary            汇总查询
//	GET    /v1/devices/:id/rejections?limit=  拒绝原因查询
//	GET    /v1/devices                        设备列表
//
// 事件成功接收返回 200；重复提交返回 200 且 duplicate=true；
// 冲突/超窗/迟到返回 409 并在 body 中给出 reason；设备不存在返回 404；
// 入参非法返回 400；并发版本冲突由服务层自动重试，调用方不可见。
const (
	apiPrefix = "/v1/devices"
)

// Server 包装 Service 提供 HTTP 接口。
type Server struct {
	svc    *Service
	logger *log.Logger
}

// NewHTTPServer 创建 HTTP handler（可挂载到任意 mux）。
func NewHTTPServer(svc *Service, logger *log.Logger) http.Handler {
	if logger == nil {
		logger = log.New(log.Writer(), "telemetry ", log.LstdFlags)
	}
	s := &Server{svc: svc, logger: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/devices", s.listDevices)
	mux.HandleFunc("PUT /v1/devices/{id}", s.registerDevice)
	mux.HandleFunc("POST /v1/devices/{id}/events", s.ingest)
	mux.HandleFunc("POST /v1/devices/{id}/watermark", s.advanceWatermark)
	mux.HandleFunc("GET /v1/devices/{id}/gaps", s.getGaps)
	mux.HandleFunc("GET /v1/devices/{id}/events", s.listEvents)
	mux.HandleFunc("GET /v1/devices/{id}/summary", s.getSummary)
	mux.HandleFunc("GET /v1/devices/{id}/rejections", s.listRejections)
	return mux
}

type registerRequest struct {
	Window int64 `json:"window"`
}

type eventRequest struct {
	Sequence  int64   `json:"sequence"`
	EventTime string  `json:"eventTime"`
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
}

type watermarkRequest struct {
	ClosedSeq int64 `json:"closedSeq"`
}

type errorResponse struct {
	Error  string `json:"error"`
	Reason string `json:"reason,omitempty"`
	Detail string `json:"detail,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, status int, code, reason, detail string) {
	writeJSON(w, status, errorResponse{Error: code, Reason: reason, Detail: detail})
}

// mapServiceError 把领域错误映射为 HTTP 状态码。
func mapServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "", err.Error())
	case errors.Is(err, ErrDeviceExists):
		writeError(w, http.StatusConflict, "device_exists", "", err.Error())
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "invalid_argument", "", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "", err.Error())
	}
}

func (s *Server) listDevices(w http.ResponseWriter, r *http.Request) {
	ids, err := s.svc.ListDevices()
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deviceIds": ids})
}

func (s *Server) registerDevice(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "", err.Error())
		return
	}
	d, err := s.svc.RegisterDevice(id, req.Window)
	if err != nil {
		if errors.Is(err, ErrDeviceExists) {
			// 登记幂等：同配置已存在时返回 200；配置不一致返回 409。
			st, lerr := s.svc.store.Load(id)
			if lerr == nil {
				if st.Window == req.Window {
					writeJSON(w, http.StatusOK, dOrConfig(st))
					return
				}
				writeError(w, http.StatusConflict, "device_exists", "",
					fmt.Sprintf("device %q already registered with window %d", id, st.Window))
				return
			}
		}
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, d)
}

func dOrConfig(st *deviceState) *Device {
	return &Device{ID: st.ID, Window: st.Window, RegisteredAt: st.RegisteredAt}
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req eventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "", err.Error())
		return
	}
	ts, err := time.Parse(time.RFC3339Nano, req.EventTime)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_argument", "",
			"eventTime must be RFC3339, e.g. 2026-09-28T00:00:00Z")
		return
	}
	outcome, err := s.svc.Ingest(id, Event{
		Sequence:  req.Sequence,
		EventTime: ts.UTC(),
		Metric:    req.Metric,
		Value:     req.Value,
	})
	if err != nil {
		var rej *RejectedError
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, rej.Outcome)
			return
		}
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, outcome)
}

func (s *Server) advanceWatermark(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req watermarkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "", err.Error())
		return
	}
	res, err := s.svc.AdvanceWatermark(id, req.ClosedSeq)
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) getGaps(w http.ResponseWriter, r *http.Request) {
	info, err := s.svc.Gaps(r.PathValue("id"))
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var from, limit int64
	if v := strings.TrimSpace(q.Get("from")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_argument", "", "from must be an integer")
			return
		}
		from = n
	}
	if v := strings.TrimSpace(q.Get("limit")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid_argument", "", "limit must be a non-negative integer")
			return
		}
		limit = n
	}
	events, err := s.svc.ListEvents(r.PathValue("id"), from, limit)
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

func (s *Server) getSummary(w http.ResponseWriter, r *http.Request) {
	sum, err := s.svc.Summary(r.PathValue("id"))
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) listRejections(w http.ResponseWriter, r *http.Request) {
	var limit int64
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid_argument", "", "limit must be a non-negative integer")
			return
		}
		limit = n
	}
	recs, err := s.svc.ListRejections(r.PathValue("id"), limit)
	if err != nil {
		mapServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rejections": recs})
}

// ListenAndServe 启动 HTTP 服务的便捷封装。
func ListenAndServe(addr, dataDir string) error {
	var store Store = NewMemoryStore()
	if strings.TrimSpace(dataDir) != "" {
		fs, err := OpenFileStore(dataDir)
		if err != nil {
			return fmt.Errorf("open store: %w", err)
		}
		store = fs
	}
	svc := NewService(store)
	logger := log.New(log.Writer(), "telemetry ", log.LstdFlags)
	logger.Printf("listening on %s (dataDir=%q)", addr, dataDir)
	return http.ListenAndServe(addr, NewHTTPServer(svc, logger))
}
