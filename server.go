package gotelemetryingest

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// NewHandler 返回暴露 Service 的 HTTP 处理器。
//
//	POST   /devices                       登记设备 {"id","window_size"}
//	GET    /devices/{id}                  设备状态（含汇总）
//	POST   /devices/{id}/events           接收事件 {"seq","event_time","value","payload"}
//	GET    /devices/{id}/events           原始事件列表（?seq=N 查单条）
//	POST   /devices/{id}/watermark        推进水位 {"watermark","expected_version"}
//	GET    /devices/{id}/gaps             缺口查询
//	GET    /devices/{id}/summary          汇总查询
//	GET    /devices/{id}/rejections       拒绝记录查询
func NewHandler(svc *Service) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /devices", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID         string `json:"id"`
			WindowSize uint64 `json:"window_size"`
		}
		if !decode(w, r, &req) {
			return
		}
		d, err := svc.RegisterDevice(req.ID, req.WindowSize)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, d)
	})

	mux.HandleFunc("GET /devices/{id}", func(w http.ResponseWriter, r *http.Request) {
		d, err := svc.GetDevice(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, d)
	})

	mux.HandleFunc("POST /devices/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		var ev Event
		if !decode(w, r, &ev) {
			return
		}
		ev.DeviceID = r.PathValue("id")
		res, err := svc.Ingest(ev)
		if err != nil {
			writeError(w, err)
			return
		}
		status := http.StatusOK
		if res.Status == StatusAccepted {
			status = http.StatusCreated
		}
		writeJSON(w, status, res)
	})

	mux.HandleFunc("GET /devices/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if q := r.URL.Query().Get("seq"); q != "" {
			seq, perr := strconv.ParseUint(q, 10, 64)
			if perr != nil {
				writeError(w, errors.New("invalid seq query parameter"))
				return
			}
			ev, err := svc.Event(id, seq)
			if err != nil {
				writeError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, ev)
			return
		}
		events, err := svc.Events(id)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, events)
	})

	mux.HandleFunc("POST /devices/{id}/watermark", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Watermark       time.Time `json:"watermark"`
			ExpectedVersion uint64    `json:"expected_version"`
		}
		if !decode(w, r, &req) {
			return
		}
		d, err := svc.AdvanceWatermark(r.PathValue("id"), req.Watermark, req.ExpectedVersion)
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, d)
	})

	mux.HandleFunc("GET /devices/{id}/gaps", func(w http.ResponseWriter, r *http.Request) {
		rep, err := svc.Gaps(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, rep)
	})

	mux.HandleFunc("GET /devices/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		sum, ack, version, err := svc.Summary(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"summary": sum,
			"ack":     ack,
			"version": version,
		})
	})

	mux.HandleFunc("GET /devices/{id}/rejections", func(w http.ResponseWriter, r *http.Request) {
		rej, err := svc.Rejections(r.PathValue("id"))
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, rej)
	})

	return mux
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, err)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, ErrDeviceNotFound),
		strings.Contains(err.Error(), "event not found"):
		status = http.StatusNotFound
	case errors.Is(err, ErrConflict),
		errors.Is(err, ErrVersionConflict),
		errors.Is(err, ErrDeviceExists),
		errors.Is(err, ErrWatermarkRegression):
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
