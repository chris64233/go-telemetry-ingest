package gotelemetryingest

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// FileStore 是基于 WAL + 快照的持久化 Store。
//
// 持久化模型：
//   - 每次 Register/Update 把一台设备的【完整状态】序列化为一个 CRC 校验帧，
//     追加到 WAL 并 fsync。一帧 = 一个持久化事务边界：汇总、确认位置、
//     水位、事件、拒绝记录要么一起生效，要么都不生效。
//   - WAL 超过 snapshotThreshold 后，在提交锁内把全部设备状态写入快照，
//     fsync + 原子 rename，然后清空 WAL。
//   - 打开时先载入快照，再顺序重放 WAL（同设备后帧覆盖前帧）。
//     遇到半帧或 CRC 错误说明上次写入中途崩溃：在该位置截断 WAL。
//
// 该实现面向单进程部署（文件锁之外不再做多进程协调）。
type FileStore struct {
	dir     string
	mu      sync.Mutex // 保护 map 与文件追加/快照压缩的串行化
	devices map[string]*deviceState

	f *os.File

	walBytes  int64
	snapBytes int64
}

const (
	frameMagic   = "GTIW"
	frameDevice  = 1
	frameSnap    = 2
	frameHeader  = 4 + 1 + 8 // magic + type + length
	frameTrailer = 4         // crc32

	defaultSnapshotThreshold = 4 << 20 // 4 MiB

	walName   = "wal.log"
	snapName  = "snapshot.gti"
	tmpSuffix = ".tmp"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// OpenFileStore 打开（必要时创建）dir 下的持久化存储并执行恢复。
func OpenFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, errors.New("FileStore: empty dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("FileStore: mkdir: %w", err)
	}
	s := &FileStore{devices: make(map[string]*deviceState), dir: dir}

	if err := s.loadSnapshot(); err != nil {
		return nil, err
	}
	if err := s.openAndReplayWAL(); err != nil {
		return nil, err
	}
	return s, nil
}

// SetSnapshotThreshold 调整触发快照压缩的 WAL 字节数（主要用于测试）。
// 返回 FileStore 以便链式调用；必须在无并发提交时设置。
func (s *FileStore) SetSnapshotThreshold(n int64) *FileStore {
	s.mu.Lock()
	s.snapBytes = n
	s.mu.Unlock()
	return s
}

func (s *FileStore) threshold() int64 {
	if s.snapBytes > 0 {
		return s.snapBytes
	}
	return defaultSnapshotThreshold
}

type deviceFrame struct {
	State *deviceState `json:"state"`
}

type snapshotFrame struct {
	Devices []*deviceState `json:"devices"`
}

// appendFrame 将一帧写入 w。调用方负责加锁与 fsync。
func appendFrame(w io.Writer, typ byte, payload []byte) error {
	var buf [frameHeader + frameTrailer]byte
	copy(buf[0:4], frameMagic)
	buf[4] = typ
	binary.BigEndian.PutUint64(buf[5:13], uint64(len(payload)))
	crc := crc32.Checksum(append(buf[4:13], payload...), crcTable)
	if _, err := w.Write(buf[:frameHeader]); err != nil {
		return err
	}
	if _, err := w.Write(payload); err != nil {
		return err
	}
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.Write(crcBuf[:]); err != nil {
		return err
	}
	return nil
}

// readFrames 从 r 顺序读取帧。handle 处理一帧；返回（读取字节数, error）。
// 干净结束时返回 nil；遇到尾部半帧（含短读）、magic 或 CRC 错误时停止并
// 返回 io.ErrUnexpectedEOF，由调用方按 goodBytes 截断；真正的底层读取
// 错误原样返回，不得静默截断。
func readFrames(r io.ReaderAt, size int64, handle func(typ byte, payload []byte) error) (int64, error) {
	var off int64
	for off < size {
		var hdr [frameHeader]byte
		// ReadAt 在超过数据末尾时可能以 (n>0, io.EOF) 返回短读；
		// io.ReadFull 把「读到的字节少于请求」统一成 ErrUnexpectedEOF。
		if _, err := io.ReadFull(io.NewSectionReader(r, off, frameHeader), hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return off, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return off, io.ErrUnexpectedEOF // 半帧
			}
			return off, err
		}
		if string(hdr[0:4]) != frameMagic {
			// 帧边界已损坏（例如写到一半崩溃且尾部恰好凑够长度）：
			// 追加式 WAL 中这只能出现在当前尾部，交给调用方截断。
			return off, io.ErrUnexpectedEOF
		}
		typ := hdr[4]
		plen := int64(binary.BigEndian.Uint64(hdr[5:13]))
		if plen < 0 || off > size-frameHeader-plen-frameTrailer {
			return off, io.ErrUnexpectedEOF // 载荷或 CRC 不完整
		}
		payload := make([]byte, plen)
		if _, err := io.ReadFull(io.NewSectionReader(r, off+frameHeader, plen), payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, io.ErrUnexpectedEOF
			}
			return off, err
		}
		var crcBuf [4]byte
		if _, err := io.ReadFull(io.NewSectionReader(r, off+frameHeader+plen, frameTrailer), crcBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return off, io.ErrUnexpectedEOF
			}
			return off, err
		}
		want := crc32.Checksum(append(hdr[4:13], payload...), crcTable)
		if binary.BigEndian.Uint32(crcBuf[:]) != want {
			// CRC 不匹配：该帧写到一半或页缓存撕裂。WAL 只追加且
			// 帧内含完整设备状态，此帧之后不可能再有有效帧，按尾部
			// 半帧截断（与 magic 损坏同样处理）。
			return off, io.ErrUnexpectedEOF
		}
		if err := handle(typ, payload); err != nil {
			return off, err
		}
		off += frameHeader + plen + frameTrailer
	}
	return off, nil
}

func (s *FileStore) applyDeviceFrame(payload []byte) error {
	var fr deviceFrame
	if err := json.Unmarshal(payload, &fr); err != nil {
		return fmt.Errorf("decode device frame: %w", err)
	}
	if fr.State == nil || fr.State.ID == "" {
		return errors.New("device frame missing state")
	}
	if fr.State.Events == nil {
		fr.State.Events = make(map[int64]*storedEvent)
	}
	s.devices[fr.State.ID] = fr.State
	return nil
}

func (s *FileStore) loadSnapshot() error {
	path := filepath.Join(s.dir, snapName)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	ok := false
	_, err = readFrames(bytes.NewReader(data), int64(len(data)), func(typ byte, payload []byte) error {
		if typ != frameSnap {
			return fmt.Errorf("unexpected frame type %d in snapshot", typ)
		}
		var sf snapshotFrame
		if err := json.Unmarshal(payload, &sf); err != nil {
			return fmt.Errorf("decode snapshot: %w", err)
		}
		for _, st := range sf.Devices {
			if st == nil || st.ID == "" {
				return errors.New("snapshot frame has empty device")
			}
			if st.Events == nil {
				st.Events = make(map[int64]*storedEvent)
			}
			s.devices[st.ID] = st
		}
		ok = true
		return nil
	})
	// 快照经 tmp+fsync+原子 rename 落盘：任何损坏（含半帧/CRC）都说明
	// 介质或文件系统异常，不能静默丢弃已确认状态，保持致命错误。
	if err != nil {
		return fmt.Errorf("recover snapshot: %w", err)
	}
	if !ok {
		// 快照损坏：不应静默使用，避免丢状态。
		return fmt.Errorf("snapshot unreadable at %s (%d bytes)", path, info.Size())
	}
	return nil
}

func (s *FileStore) openAndReplayWAL() error {
	path := filepath.Join(s.dir, walName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	size := info.Size()
	goodBytes, err := readFrames(f, size, func(typ byte, payload []byte) error {
		switch typ {
		case frameDevice:
			return s.applyDeviceFrame(payload)
		default:
			return fmt.Errorf("unexpected frame type %d in wal", typ)
		}
	})
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		f.Close()
		return fmt.Errorf("replay wal: %w", err)
	}
	// 尾部半帧 / CRC 错误：截断到最后一个完好帧。
	if goodBytes < size {
		if err := f.Truncate(goodBytes); err != nil {
			f.Close()
			return fmt.Errorf("truncate torn wal: %w", err)
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if _, err := f.Seek(goodBytes, io.SeekStart); err != nil {
		f.Close()
		return err
	}
	s.f = f
	s.walBytes = goodBytes
	return nil
}

func (s *FileStore) RegisterDevice(d Device, now time.Time) (*deviceState, error) {
	if d.ID == "" {
		return nil, ErrInvalidArgument
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.devices[d.ID]; ok {
		return nil, ErrDeviceExists
	}
	st := newDeviceState(d)
	if st.RegisteredAt.IsZero() {
		st.RegisteredAt = now
	}
	if err := s.persistLocked(st); err != nil {
		return nil, err
	}
	s.devices[st.ID] = st
	return st.clone(), nil
}

func (s *FileStore) Load(id string) (*deviceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.devices[id]
	if !ok {
		return nil, ErrNotFound
	}
	return st.clone(), nil
}

func (s *FileStore) UpdateDevice(prev, next *deviceState) error {
	if prev == nil || next == nil || prev.ID != next.ID {
		return ErrInvalidArgument
	}
	if next.Version != prev.Version+1 {
		return ErrConflictVersion
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.devices[prev.ID]
	if !ok {
		return ErrNotFound
	}
	if cur.Version != prev.Version {
		return ErrConflictVersion
	}
	if err := s.persistLocked(next); err != nil {
		return err
	}
	s.devices[prev.ID] = next
	return nil
}

// persistLocked 把设备状态作为一帧追加到 WAL 并 fsync，必要时压缩快照。
// pending 是本次正在提交的设备状态：若触发压缩，快照必须包含它而不是
// map 中尚未更新的旧状态。
func (s *FileStore) persistLocked(pending *deviceState) error {
	payload, err := json.Marshal(deviceFrame{State: pending})
	if err != nil {
		return fmt.Errorf("encode frame: %w", err)
	}
	if err := appendFrame(s.f, frameDevice, payload); err != nil {
		return fmt.Errorf("write wal: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("fsync wal: %w", err)
	}
	s.walBytes += int64(frameHeader + len(payload) + frameTrailer)
	if s.walBytes >= s.threshold() {
		if err := s.compactLocked(pending); err != nil {
			return err
		}
	}
	return nil
}

// compactLocked 在提交锁内写出全量快照并清空 WAL。
//
// 崩溃情形：
//   - 写 tmp 期间崩溃：tmp 被忽略，snapshot 与 wal 均完好。
//   - rename 之后、清空 WAL 之前崩溃：重放时先载快照再重放 WAL，
//     快照中已包含这些设备帧，重复应用得到相同状态（幂等）。
func (s *FileStore) compactLocked(pending *deviceState) error {
	ids := make([]string, 0, len(s.devices)+1)
	for id := range s.devices {
		if pending != nil && id == pending.ID {
			continue // 用 pending 替换 map 中的旧状态
		}
		ids = append(ids, id)
	}
	if pending != nil {
		ids = append(ids, pending.ID)
	}
	sort.Strings(ids)
	states := make([]*deviceState, 0, len(ids))
	for _, id := range ids {
		if pending != nil && id == pending.ID {
			states = append(states, pending)
		} else {
			states = append(states, s.devices[id])
		}
	}
	payload, err := json.Marshal(snapshotFrame{Devices: states})
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	tmpPath := filepath.Join(s.dir, snapName+tmpSuffix)
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}
	if err := appendFrame(tmp, frameSnap, payload); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("fsync snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(s.dir, snapName)); err != nil {
		return fmt.Errorf("rename snapshot: %w", err)
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}

	// 快照已落盘，清空 WAL。
	if err := s.f.Truncate(0); err != nil {
		return fmt.Errorf("reset wal: %w", err)
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("fsync wal reset: %w", err)
	}
	s.walBytes = 0
	return nil
}

func (s *FileStore) ListDevices() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.devices))
	for id := range s.devices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f != nil {
		err := s.f.Close()
		s.f = nil
		return err
	}
	return nil
}
