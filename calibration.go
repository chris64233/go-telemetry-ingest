package gotelemetryingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
)

// digestParams 计算标定参数的稳定摘要（SHA256 十六进制）。
func digestParams(p CalibrationParams) string {
	// 用规范化 JSON 保证字段顺序固定、摘要可复现。
	b, _ := json.Marshal(struct {
		Scale  float64 `json:"scale"`
		Offset float64 `json:"offset"`
	}{Scale: p.Scale, Offset: p.Offset})
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ApplyCalibration 为设备登记/切换一个标定版本。
//
// 标定记录固定设备、参数摘要、生效序号与操作者：
//   - calibrationID 设备内唯一、非空；baseline 为系统保留版本号；
//   - effectiveSeq 必须严格大于已确认读数序号 AckedSeq（不能早于已经确认
//     的读数），且不小于上一个标定版本的生效序号；
//   - 参数必须有限且 scale > 0，operator 非空；
//   - 相同标定号重放：设备、参数摘要、生效序号、操作者完全一致时幂等
//     返回原结果（Replayed=true）；任一字段变化（设备/参数/序号/操作者）
//     返回冲突（errors.Is(err, ErrConflict)）。
//
// 标定失败（非法入参/冲突）不改变当前有效版本。
func (s *Service) ApplyCalibration(deviceID, calibrationID string, effectiveSeq int64, params CalibrationParams, operator string) (*ApplyCalibrationResult, error) {
	if deviceID == "" || calibrationID == "" {
		return nil, fmt.Errorf("%w: device id and calibration id are required", ErrInvalidArgument)
	}
	if calibrationID == BaselineCalibrationID {
		return nil, fmt.Errorf("%w: calibration id %q is reserved", ErrInvalidArgument, BaselineCalibrationID)
	}
	if effectiveSeq <= 0 {
		return nil, fmt.Errorf("%w: effectiveSeq must be >= 1", ErrInvalidArgument)
	}
	if math.IsNaN(params.Scale) || math.IsInf(params.Scale, 0) ||
		math.IsNaN(params.Offset) || math.IsInf(params.Offset, 0) {
		return nil, fmt.Errorf("%w: calibration params must be finite", ErrInvalidArgument)
	}
	if params.Scale <= 0 {
		return nil, fmt.Errorf("%w: scale must be > 0", ErrInvalidArgument)
	}
	if operator == "" {
		return nil, fmt.Errorf("%w: operator is required", ErrInvalidArgument)
	}

	mu := s.deviceLock(deviceID)
	mu.Lock()
	defer mu.Unlock()

	for {
		st, err := s.store.Load(deviceID)
		if err != nil {
			return nil, err
		}

		// 相同标定号重放：与既有记录逐字段比较，完全一致才幂等。
		for _, existing := range st.Calibrations {
			if existing.CalibrationID != calibrationID {
				continue
			}
			if existing.DeviceID == deviceID &&
				existing.EffectiveSeq == effectiveSeq &&
				existing.Operator == operator &&
				existing.Params == params {
				return &ApplyCalibrationResult{Calibration: existing, Replayed: true}, nil
			}
			return nil, fmt.Errorf("%w: calibration %q already exists with different device/params/effectiveSeq/operator",
				ErrConflict, calibrationID)
		}

		// 生效序号不能早于已经确认的读数：必须严格大于 AckedSeq。
		if effectiveSeq <= st.AckedSeq {
			return nil, fmt.Errorf("%w: effectiveSeq %d must be greater than ackedSeq %d",
				ErrInvalidArgument, effectiveSeq, st.AckedSeq)
		}
		last := st.currentCalibration()
		if effectiveSeq < last.EffectiveSeq {
			return nil, fmt.Errorf("%w: effectiveSeq %d must be >= current effectiveSeq %d",
				ErrInvalidArgument, effectiveSeq, last.EffectiveSeq)
		}

		now := s.now().UTC()
		rec := Calibration{
			CalibrationID: calibrationID,
			DeviceID:      deviceID,
			EffectiveSeq:  effectiveSeq,
			Params:        params,
			Digest:        digestParams(params),
			Operator:      operator,
			CreatedAt:     now,
		}
		prev := st.clone()
		st.Calibrations = append(st.Calibrations, rec)
		sort.SliceStable(st.Calibrations, func(i, j int) bool {
			return st.Calibrations[i].EffectiveSeq < st.Calibrations[j].EffectiveSeq
		})
		st.SegmentStats[calibrationID] = &calibStat{}
		st.Version++
		st.UpdatedAt = now
		if err := s.store.UpdateDevice(prev, st); err != nil {
			if errors.Is(err, ErrConflictVersion) {
				continue
			}
			return nil, err
		}
		return &ApplyCalibrationResult{Calibration: rec}, nil
	}
}

// ListCalibrations 返回设备标定历史（按生效序号升序，含 baseline）。
func (s *Service) ListCalibrations(deviceID string) ([]Calibration, error) {
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	return append([]Calibration(nil), st.Calibrations...), nil
}

// ListLateReadings 返回历史诊断区中的旧标定迟到读数（按记录时间倒序）。
// 这些读数可以被记录，但永远不会回写当前聚合。
func (s *Service) ListLateReadings(deviceID string, limit int64) ([]LateReading, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	st, err := s.store.Load(deviceID)
	if err != nil {
		return nil, err
	}
	n := int64(len(st.LateReadings))
	start := n - limit
	if start < 0 {
		start = 0
	}
	out := make([]LateReading, 0, n-start)
	for i := n - 1; i >= start; i-- {
		out = append(out, st.LateReadings[i])
	}
	return out, nil
}
