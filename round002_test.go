package gotelemetryingest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 本轮（002）针对需求边界补充的自动化测试。
//
// 覆盖：
//   - WAL 尾帧 CRC 损坏（帧完整但校验失败）按半帧截断恢复；
//   - readFrames 对短读 ReaderAt（n>0, io.EOF）的健壮处理；
//   - 水位封闭缺口后窗口随确认位置整体上移；
//   - 相邻封闭缺口在二次水位推进时不产生重叠/交叉记录；
//   - 相同拒绝提交幂等、窗口移动后可重新接收；
//   - 乱序 drain 后 min/max/首末事件时间正确；
//   - 事件入参校验；
//   - 原始事件查询分页；
//   - 封闭缺口、拒绝记录在多次水位推进/重启后保持一致。

// TestFileStoreReplayCorruptedCRCTail 写入 3 个事件后，把最后一帧的
// CRC 区域改坏（帧长度仍完整）。旧实现会因 CRC 错误直接拒绝打开存储；
// 正确行为是截断到前一个完好帧，恢复出 seq1、seq2 的汇总。
func TestFileStoreReplayCorruptedCRCTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(fs)
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(2, 2))
	mustIngest(t, svc, "d", eventAt(3, 3))
	fs.Close()

	walPath := filepath.Join(dir, walName)
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(raw)) < frameHeader+frameTrailer {
		t.Fatalf("wal too short: %d", len(raw))
	}
	// 翻转最后一帧 CRC 的首字节，帧的长度字段保持完好。
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(walPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	fs2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("store should recover from corrupted tail frame: %v", err)
	}
	defer fs2.Close()
	sum, err := NewService(fs2).Summary("d")
	if err != nil {
		t.Fatal(err)
	}
	// 最后一帧（seq3 的完整状态）被丢弃，恢复到 seq2。
	if sum.AckedSeq != 2 || sum.EventCount != 2 || sum.Sum != 3 {
		t.Fatalf("recovered summary after CRC-tail truncation: %+v", sum)
	}

	// 截断后可继续写入并跨重启存活。
	svc2 := NewService(fs2)
	mustIngest(t, svc2, "d", eventAt(3, 30))
	fs2.Close()

	fs3, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer fs3.Close()
	sum, _ = NewService(fs3).Summary("d")
	if sum.AckedSeq != 3 || sum.EventCount != 3 || sum.Sum != 33 {
		t.Fatalf("summary after recovery+write+restart: %+v", sum)
	}
}

// TestFileStoreReplayTornHeaderTail 尾部是一个 magic 正确但头部不完整
// 的半帧，必须截断而不是打不开。
func TestFileStoreReplayTornHeaderTail(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	fs, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(fs)
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 1))
	fs.Close()

	walPath := filepath.Join(dir, walName)
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte{}, raw...), []byte(frameMagic)...)
	torn = append(torn, frameDevice, 0, 0) // 只写了 3 字节头
	if err := os.WriteFile(walPath, torn, 0o644); err != nil {
		t.Fatal(err)
	}

	fs2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatalf("torn header tail should be truncated, got %v", err)
	}
	defer fs2.Close()
	sum, _ := NewService(fs2).Summary("d")
	if sum.AckedSeq != 1 || sum.EventCount != 1 || sum.Sum != 1 {
		t.Fatalf("recovered: %+v", sum)
	}
}

// shortReadReaderAt 在读取触及末尾时模拟某些 ReaderAt 的行为：返回比
// 请求少的字节并附带 io.EOF（而不是 io.ReadFull 期望的短读语义）。
type shortReadReaderAt struct{ data []byte }

func (s *shortReadReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(s.data)) {
		return 0, io.EOF
	}
	n := copy(p, s.data[off:])
	if n < len(p) {
		return n, io.EOF // 关键：短读 + EOF
	}
	return n, nil
}

func TestReadFramesToleratesShortReadAtEOF(t *testing.T) {
	var payload = []byte("{}")
	var buf bytes.Buffer
	if err := appendFrame(&buf, frameDevice, payload); err != nil {
		t.Fatal(err)
	}
	good := buf.Bytes()
	var got [][]byte
	goodBytes, err := readFrames(&shortReadReaderAt{data: good}, int64(len(good)),
		func(typ byte, p []byte) error {
			got = append(got, append([]byte(nil), p...))
			return nil
		})
	if err != nil {
		t.Fatalf("full frame should parse despite short reads: %v", err)
	}
	if goodBytes != int64(len(good)) || len(got) != 1 || string(got[0]) != string(payload) {
		t.Fatalf("goodBytes=%d frames=%v", goodBytes, got)
	}

	// 半帧 + 短读：返回 ErrUnexpectedEOF 与已完好读取的字节数。
	torn := append(append([]byte{}, good...), good[:5]...)
	got = nil
	goodBytes, err = readFrames(&shortReadReaderAt{data: torn}, int64(len(torn)),
		func(typ byte, p []byte) error {
			got = append(got, p)
			return nil
		})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("torn frame: want ErrUnexpectedEOF, got %v", err)
	}
	if goodBytes != int64(len(good)) || len(got) != 1 {
		t.Fatalf("goodBytes=%d frames=%d", goodBytes, len(got))
	}
}

// TestWatermarkMovesWindowUpperEdge 水位封闭缺口后，确认位置整体前移，
// 乱序窗口上沿随之移动；窗口语义没有被水位永久“锁死”。
func TestWatermarkMovesWindowUpperEdge(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 3) // 初始上沿 = 0+1+3 = 4
	mustIngest(t, svc, "d", eventAt(1, 1))
	// 封闭缺口 2：acked、closed 都到 2。
	if _, err := svc.AdvanceWatermark("d", 2); err != nil {
		t.Fatal(err)
	}
	// 新上沿 = 2+1+3 = 6：seq6 可缓冲，seq7 超窗。
	out := mustIngest(t, svc, "d", eventAt(6, 6))
	if out.Status != ResultBuffered || out.AckedSeq != 2 {
		t.Fatalf("seq6 within shifted window: %+v", out)
	}
	rejected, err := svc.Ingest("d", eventAt(7, 7))
	if err == nil || rejected.Reason != ReasonOutOfWindow {
		t.Fatalf("seq7 beyond shifted window: %+v err=%v", rejected, err)
	}
	// 缺口 3..5 仍开放（尚未被水位封闭）。
	info, _ := svc.Gaps("d")
	if fmt.Sprint(info.Open) != fmt.Sprint([]Gap{{Start: 3, End: 5}}) {
		t.Fatalf("open gap after seal: %+v", info.Open)
	}
	if fmt.Sprint(info.Sealed) != fmt.Sprint([]Gap{{Start: 2, End: 2, Sealed: true}}) {
		t.Fatalf("sealed gap: %+v", info.Sealed)
	}
}

// TestRepeatedWatermarksDoNotOverlapSealedGaps 分批推进水位时，新封闭
// 缺口必须与既有封闭缺口相邻而不重叠，且区间内 buffered 事件只计一次。
func TestRepeatedWatermarksDoNotOverlapSealedGaps(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 20)
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(3, 3)) // 缺 2
	// 第一次：封闭 2，并 drain 到 3。
	r1, err := svc.AdvanceWatermark("d", 2)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r1.NewlySealed) != fmt.Sprint([]Gap{{Start: 2, End: 2, Sealed: true}}) ||
		r1.AckedSeq != 3 {
		t.Fatalf("first advance: %+v", r1)
	}
	// 第二次：4、5 全缺，6 已提前到达 → 封闭 [4,5]，结算 6 并 drain。
	mustIngest(t, svc, "d", eventAt(6, 6))
	r2, err := svc.AdvanceWatermark("d", 6)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(r2.NewlySealed) != fmt.Sprint([]Gap{{Start: 4, End: 5, Sealed: true}}) {
		t.Fatalf("second advance sealed: %+v", r2.NewlySealed)
	}
	if r2.AckedSeq != 6 || r2.ClosedSeq != 6 {
		t.Fatalf("positions: %+v", r2)
	}
	info, _ := svc.Gaps("d")
	if len(info.Sealed) != 2 ||
		info.Sealed[0].Start != 2 || info.Sealed[1].Start != 4 ||
		info.Sealed[1].End != 5 {
		t.Fatalf("sealed gaps should be disjoint: %+v", info.Sealed)
	}
	// 计入汇总的是 1、3、6（恰好一次）。
	sum, _ := svc.Summary("d")
	if sum.EventCount != 3 || sum.Sum != 10 {
		t.Fatalf("summary: %+v", sum)
	}
}

// TestRepeatedRejectionIdempotentThenWindowMoves 同一次超窗提交重复发
// 送只保留一条拒绝记录；窗口前移后同一内容应能被正常接收。
func TestRepeatedRejectionIdempotentThenWindowMoves(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 1) // 初始上沿 = 2
	out1, err1 := svc.Ingest("d", eventAt(3, 3))
	if err1 == nil || out1.Reason != ReasonOutOfWindow {
		t.Fatalf("first rejection: %+v %v", out1, err1)
	}
	out2, err2 := svc.Ingest("d", eventAt(3, 3))
	if err2 == nil || !out2.Duplicate || out2.Reason != ReasonOutOfWindow {
		t.Fatalf("identical rejection must be idempotent: %+v %v", out2, err2)
	}
	recs, _ := svc.ListRejections("d", 0)
	if len(recs) != 1 {
		t.Fatalf("want exactly one rejection record, got %d", len(recs))
	}
	// 窗口前移：acked=2 时上沿=3。
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(2, 2))
	accepted := mustIngest(t, svc, "d", eventAt(3, 3))
	if accepted.Status != ResultAcked || accepted.AckedSeq != 3 {
		t.Fatalf("previously rejected seq now accepted: %+v", accepted)
	}
	// 历史拒绝记录仍然保留（不删除），但汇总只计一次。
	recs, _ = svc.ListRejections("d", 0)
	if len(recs) != 1 {
		t.Fatalf("rejection history must be retained: %d", len(recs))
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 3 || sum.Sum != 6 {
		t.Fatalf("summary: %+v", sum)
	}
}

// TestOutOfOrderDrainSummaryMinMaxTimes 乱序到达最终 drain 时，
// min/max 与首末事件时间必须按序号语义正确（而非按到达顺序）。
func TestOutOfOrderDrainSummaryMinMaxTimes(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(3, -5))
	mustIngest(t, svc, "d", eventAt(2, 42))
	mustIngest(t, svc, "d", eventAt(4, 7))
	mustIngest(t, svc, "d", eventAt(1, 100)) // 触发 drain 1..4

	sum, _ := svc.Summary("d")
	if sum.EventCount != 4 || sum.Sum != 144 || sum.Min != -5 || sum.Max != 100 {
		t.Fatalf("aggregates wrong: %+v", sum)
	}
	if !sum.FirstEventTime.Equal(eventAt(1, 100).EventTime) ||
		!sum.LastEventTime.Equal(eventAt(4, 7).EventTime) {
		t.Fatalf("event time range wrong: first=%v last=%v",
			sum.FirstEventTime, sum.LastEventTime)
	}
}

func TestIngestValidation(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	base := eventAt(1, 1)
	cases := []struct {
		name  string
		event Event
	}{
		{"zero sequence", func() Event { e := base; e.Sequence = 0; return e }()},
		{"negative sequence", func() Event { e := base; e.Sequence = -1; return e }()},
		{"empty metric", func() Event { e := base; e.Metric = ""; return e }()},
		{"zero event time", func() Event { e := base; e.EventTime = time.Time{}; return e }()},
		{"nan value", func() Event { e := base; e.Value = math.NaN(); return e }()},
		{"+inf value", func() Event { e := base; e.Value = math.Inf(1); return e }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := svc.Ingest("d", tc.event)
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("want ErrInvalidArgument, got out=%+v err=%v", out, err)
			}
		})
	}
	sum, _ := svc.Summary("d")
	if sum.EventCount != 0 || sum.AckedSeq != 0 {
		t.Fatalf("invalid events must not change state: %+v", sum)
	}
	if _, err := svc.RegisterDevice("", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty device id: %v", err)
	}
	if _, err := svc.RegisterDevice("x", -1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative window: %v", err)
	}
	if _, err := svc.AdvanceWatermark("d", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("non-positive watermark: %v", err)
	}
}

// TestListEventsPagination from/limit 分页按序号稳定工作，Acknowledged
// 标记准确区分已确认与缓冲事件。
func TestListEventsPagination(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 10)
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(3, 3)) // buffered（缺 2）

	page1, err := svc.ListEvents("d", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 1 || page1[0].Event.Sequence != 1 || !page1[0].Acknowledged {
		t.Fatalf("page1: %+v", page1)
	}
	page2, _ := svc.ListEvents("d", 2, 1)
	if len(page2) != 1 || page2[0].Event.Sequence != 3 || page2[0].Acknowledged {
		t.Fatalf("page2 buffered event: %+v", page2)
	}
	if page2[0].FirstResult != ResultBuffered {
		t.Fatalf("first result should be buffered: %+v", page2[0])
	}
	// limit 上限保护：超大 limit 退化为默认 100。
	big, _ := svc.ListEvents("d", 0, 1<<20)
	if len(big) != 2 {
		t.Fatalf("clamped limit: %d", len(big))
	}
}

// TestRejectionsOrderAndLimit 拒绝记录按时间倒序返回，limit 生效。
func TestRejectionsOrderAndLimit(t *testing.T) {
	svc := NewService(NewMemoryStore())
	mustReg(t, svc, "d", 0)
	// seq2 超窗被拒，重复 3 次仍只有一条；再制造不同内容拒绝。
	_, _ = svc.Ingest("d", eventAt(2, 2))
	_, _ = svc.Ingest("d", eventAt(2, 2))
	_, _ = svc.Ingest("d", eventAt(3, 3))
	recs, err := svc.ListRejections("d", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || recs[0].Sequence != 3 {
		t.Fatalf("latest-only: %+v", recs)
	}
	all, _ := svc.ListRejections("d", 0)
	if len(all) != 2 || all[0].Sequence != 3 || all[1].Sequence != 2 {
		t.Fatalf("reverse chronological order: %+v", all)
	}
}

// TestFileStoreSealedGapsSurviveRepeatedRestarts 封闭缺口与拒绝记录
// 与汇总在同一持久化边界落盘，多次重启后仍一致、不可再填补。
func TestFileStoreSealedGapsSurviveRepeatedRestarts(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	open := func() (*FileStore, *Service) {
		fs, err := OpenFileStore(dir)
		if err != nil {
			t.Fatal(err)
		}
		return fs, NewService(fs)
	}

	fs, svc := open()
	mustReg(t, svc, "d", 5)
	mustIngest(t, svc, "d", eventAt(1, 1))
	mustIngest(t, svc, "d", eventAt(4, 4)) // 缺 2、3
	if _, err := svc.AdvanceWatermark("d", 4); err != nil {
		t.Fatal(err)
	}
	fs.Close()

	for i := 0; i < 2; i++ {
		fs, svc = open()
		info, err := svc.Gaps("d")
		if err != nil {
			t.Fatal(err)
		}
		if len(info.Sealed) != 1 ||
			info.Sealed[0].Start != 2 || info.Sealed[0].End != 3 {
			t.Fatalf("restart %d sealed gaps: %+v", i, info.Sealed)
		}
		// 被封序号仍迟到拒绝。
		out, err := svc.Ingest("d", eventAt(2, 2))
		if err == nil || out.Reason != ReasonLate {
			t.Fatalf("restart %d seq2 late: %+v err=%v", i, out, err)
		}
		sum, _ := svc.Summary("d")
		if sum.EventCount != 2 || sum.Sum != 5 || sum.AckedSeq != 4 || sum.ClosedSeq != 4 {
			t.Fatalf("restart %d summary: %+v", i, sum)
		}
		fs.Close()
	}
}
