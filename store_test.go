package gotelemetryingest

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type storeFactory struct {
	name string
	make func(t *testing.T) Store
}

func newConcurrentStores(t *testing.T) []storeFactory {
	return []storeFactory{
		{name: "memory", make: func(t *testing.T) Store { return NewMemoryStore() }},
		{name: "file", make: func(t *testing.T) Store {
			dir := filepath.Join(t.TempDir(), "data")
			st, err := OpenFileStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			return st
		}},
	}
}

// flakyStore 在前 failFirst 次 UpdateDevice 上注入版本冲突，其余委托给内部存储。
type flakyStore struct {
	Store
	failLeft int64
}

func (f *flakyStore) UpdateDevice(prev, next *deviceState) error {
	if atomic.AddInt64(&f.failLeft, -1) >= 0 {
		return ErrConflictVersion
	}
	return f.Store.UpdateDevice(prev, next)
}

func TestCASRetrySucceedsAfterVersionConflict(t *testing.T) {
	// 前两次提交（seq1 首次落库前）注入冲突：服务必须基于新版本重试，
	// 最终恰好确认一次。
	svc := NewService(&flakyStore{Store: NewMemoryStore(), failLeft: 2})
	if _, err := svc.RegisterDevice("d", 5); err != nil {
		t.Fatal(err)
	}
	out, err := svc.Ingest("d", eventAt(1, 10))
	if err != nil {
		t.Fatalf("ingest should retry past version conflicts: %v", err)
	}
	if out.Status != ResultAcked || out.AckedSeq != 1 {
		t.Fatalf("%+v", out)
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 1 || sum.Sum != 10 {
		t.Fatalf("%+v", sum)
	}
}

func TestMemoryStoreCASRejectsStaleVersion(t *testing.T) {
	st := NewMemoryStore()
	if _, err := st.RegisterDevice(Device{ID: "d", Window: 1}, time.Now()); err != nil {
		t.Fatal(err)
	}
	s1, _ := st.Load("d")
	s2 := s1.clone()
	s2.Version++
	if err := st.UpdateDevice(s1, s2); err != nil {
		t.Fatalf("first CAS should succeed: %v", err)
	}
	// 用过期的 s1 再提交一次。
	s3 := s1.clone()
	s3.Version++
	err := st.UpdateDevice(s1, s3)
	if !errors.Is(err, ErrConflictVersion) {
		t.Fatalf("stale CAS: %v", err)
	}
}

func TestFileStoreRestartNoDoubleCount(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	restart := func() (*FileStore, *Service) {
		fs, err := OpenFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		return fs, NewService(fs)
	}

	fs, svc := restart()
	if _, err := svc.RegisterDevice("d", 10); err != nil {
		t.Fatal(err)
	}
	mustIngest(t, svc, "d", eventAt(2, 20)) // buffered
	mustIngest(t, svc, "d", eventAt(1, 10)) // drains to 2
	mustIngest(t, svc, "d", eventAt(3, 30))
	fs.Close()

	fs, svc = restart()
	sum, err := svc.Summary("d")
	if err != nil {
		t.Fatal(err)
	}
	if sum.AckedSeq != 3 || sum.EventCount != 3 || sum.Sum != 60 {
		t.Fatalf("after restart: %+v", sum)
	}
	// 重启后重复提交旧事件：幂等返回首次处理结果（seq2 首次为 buffered），
	// 汇总不变。
	out, err := svc.Ingest("d", eventAt(2, 20))
	if err != nil || !out.Duplicate || out.Status != ResultBuffered {
		t.Fatalf("duplicate after restart: %+v %v", out, err)
	}
	// 当前确认位置应已恢复。
	if out.AckedSeq != 3 {
		t.Fatalf("acked after restart: %d", out.AckedSeq)
	}
	mustIngest(t, svc, "d", eventAt(4, 40))
	sum2, _ := svc.Summary("d")
	if sum2.EventCount != 4 || sum2.Sum != 100 {
		t.Fatalf("after post-restart ingest: %+v", sum2)
	}
	// 水位状态也应持久化：关闭缺口 5，重启后迟到事件仍被拒。
	if _, err := svc.AdvanceWatermark("d", 5); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	fs, svc = restart()
	defer fs.Close()
	sum3, _ := svc.Summary("d")
	if sum3.AckedSeq != 5 || sum3.ClosedSeq != 5 || sum3.EventCount != 4 || sum3.Sum != 100 {
		t.Fatalf("second restart: %+v", sum3)
	}
	out, err = svc.Ingest("d", eventAt(5, 50))
	if err == nil || out.Reason != ReasonLate {
		t.Fatalf("seq5 should remain sealed after restart: %+v %v", out, err)
	}
	recs, _ := svc.ListRejections("d", 0)
	if len(recs) != 1 || recs[0].Reason != ReasonLate {
		t.Fatalf("rejections after restart: %+v", recs)
	}
}

func TestFileStoreSnapshotCompactionAndRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 极小阈值，强制每帧后压缩快照。
	fs.SetSnapshotThreshold(1)
	svc := NewService(fs)
	if _, err := svc.RegisterDevice("a", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.RegisterDevice("b", 3); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 20; seq++ {
		mustIngest(t, svc, "a", eventAt(seq, float64(seq)))
	}
	if _, err := svc.AdvanceWatermark("b", 5); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	// 快照文件应当存在，WAL 被清空过。
	if _, err := os.Stat(filepath.Join(dir, snapName)); err != nil {
		t.Fatalf("snapshot missing: %v", err)
	}
	fs2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs2.Close()
	svc2 := NewService(fs2)
	sum, err := svc2.Summary("a")
	if err != nil {
		t.Fatal(err)
	}
	if sum.AckedSeq != 20 || sum.EventCount != 20 || sum.Sum != 210 {
		t.Fatalf("a after compaction restart: %+v", sum)
	}
	sumB, _ := svc2.Summary("b")
	if sumB.ClosedSeq != 5 || sumB.AckedSeq != 5 || sumB.EventCount != 0 {
		t.Fatalf("b after compaction restart: %+v", sumB)
	}
	ids, _ := svc2.ListDevices()
	if len(ids) != 2 || ids[0] != "a" || ids[1] != "b" {
		t.Fatalf("devices: %v", ids)
	}
}

func TestFileStoreReplayTornFrame(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(fs)
	if _, err := svc.RegisterDevice("d", 10); err != nil {
		t.Fatal(err)
	}
	for seq := int64(1); seq <= 3; seq++ {
		mustIngest(t, svc, "d", eventAt(seq, float64(seq)))
	}
	fs.Close()

	// 模拟写到一半崩溃：向 WAL 追加垃圾半帧（错误 magic 或截断）。
	walPath := filepath.Join(dir, walName)
	good, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte{}, good...), []byte(frameMagic)...)
	torn = append(torn, frameDevice, 0, 0, 0, 0, 0, 0, 0, 42, 'x', 'y')
	if err := os.WriteFile(walPath, torn, 0o644); err != nil {
		t.Fatal(err)
	}

	fs2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("reopen with torn tail: %v", err)
	}
	defer fs2.Close()
	sum, err := NewService(fs2).Summary("d")
	if err != nil {
		t.Fatal(err)
	}
	if sum.AckedSeq != 3 || sum.EventCount != 3 || sum.Sum != 6 {
		t.Fatalf("recovered state: %+v", sum)
	}
	// 半帧已被截断，再写入的帧应正常落盘并在再次重启后存活。
	svc2 := NewService(fs2)
	mustIngest(t, svc2, "d", eventAt(4, 4))
	fs2.Close()

	fs3, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs3.Close()
	sum, _ = NewService(fs3).Summary("d")
	if sum.AckedSeq != 4 || sum.EventCount != 4 || sum.Sum != 10 {
		t.Fatalf("state after torn-tail recovery + write: %+v", sum)
	}
}
