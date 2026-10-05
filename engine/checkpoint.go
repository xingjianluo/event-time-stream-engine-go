package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// 检查点文件格式标识与当前版本。
const (
	checkpointMagic   = "event-time-stream-engine/checkpoint"
	CheckpointVersion = 2
)

// Checkpoint 是一次原子提交的全部续传状态：
// 引擎快照（水位线、窗口、去重表）与数据源位置一起提交，保证跨进程不丢不重。
type Checkpoint struct {
	Format    string
	Version   int
	CreatedAt int64
	Snapshot  Snapshot
	Positions map[int]uint64 // 分区 -> 下一条待投递记录的偏移
	Anchors   map[int]string // 分区 -> 已投递最后一条记录的指纹（校验数据未被替换）
}

// 检查点加载失败的可辨识原因，用 errors.Is 判断。
var (
	// ErrNoCheckpoint 表示指定位置不存在任何检查点（首次运行），不是损坏。
	ErrNoCheckpoint = errors.New("no checkpoint found")
	// ErrCheckpointCorrupt 表示字节内容损坏、魔数不符或结构无法解析。
	ErrCheckpointCorrupt = errors.New("checkpoint corrupt")
	// ErrUnsupportedVersion 表示检查点版本来自更新的程序，当前版本无法安全读取。
	ErrUnsupportedVersion = errors.New("checkpoint version unsupported")
)

// wireCheckpoint 是检查点文件的 JSON 线格式（v2）。
type wireCheckpoint struct {
	Format    string         `json:"format"`
	Version   int            `json:"version"`
	CreatedAt int64          `json:"createdAt"`
	Snapshot  Snapshot       `json:"snapshot"`
	Positions map[int]uint64 `json:"positions"`
	Anchors   map[int]string `json:"anchors"`
	Checksum  string         `json:"checksum,omitempty"`
}

// EncodeCheckpoint 把检查点编码为可落盘的字节。
func EncodeCheckpoint(cp Checkpoint) ([]byte, error) {
	cp.Format = checkpointMagic
	cp.Version = CheckpointVersion
	if cp.CreatedAt == 0 {
		cp.CreatedAt = time.Now().UnixMilli()
	}
	w := wireCheckpoint{
		Format:    cp.Format,
		Version:   cp.Version,
		CreatedAt: cp.CreatedAt,
		Snapshot:  cp.Snapshot,
		Positions: cp.Positions,
		Anchors:   cp.Anchors,
	}
	if w.Positions == nil {
		w.Positions = map[int]uint64{}
	}
	if w.Anchors == nil {
		w.Anchors = map[int]string{}
	}
	w.Checksum = checksumOf(w.Format, w.Version, w.CreatedAt, w.Snapshot, w.Positions, w.Anchors)
	data, err := json.MarshalIndent(w, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode checkpoint: %w", err)
	}
	return data, nil
}

// DecodeCheckpoint 解析检查点字节，兼容旧版快照格式；无法识别时返回可辨识错误。
// 支持：
//   - v2：带 format/version 的检查点信封，含数据位置与锚点；
//   - v1：旧版本裸 Snapshot（无信封、无位置），按“从头投递”语义恢复，
//     由恢复出来的去重表保证重投递不重复。
//
// 未知的更高版本明确拒绝（ErrUnsupportedVersion），不猜测、不崩溃。
func DecodeCheckpoint(data []byte) (Checkpoint, error) {
	if len(data) == 0 {
		return Checkpoint{}, fmt.Errorf("%w: empty payload", ErrCheckpointCorrupt)
	}
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		return Checkpoint{}, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
	}

	_, hasVersion := probe["version"]
	_, hasFormat := probe["format"]
	_, hasSnapshot := probe["snapshot"]

	// 带信封的检查点。
	if hasVersion || hasFormat {
		var w wireCheckpoint
		if err := json.Unmarshal(data, &w); err != nil {
			return Checkpoint{}, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
		}
		if w.Format != checkpointMagic {
			return Checkpoint{}, fmt.Errorf("%w: unexpected format marker %q", ErrCheckpointCorrupt, w.Format)
		}
		switch {
		case w.Version < CheckpointVersion:
			// 当前唯一更旧的带信封版本不存在；保留分支以明确语义。
			return Checkpoint{}, fmt.Errorf("%w: legacy envelope version %d", ErrUnsupportedVersion, w.Version)
		case w.Version > CheckpointVersion:
			return Checkpoint{}, fmt.Errorf("%w: version=%d (this build understands up to %d)",
				ErrUnsupportedVersion, w.Version, CheckpointVersion)
		}
		// 校验和（若存在则必须匹配；早期 v2 写出可能无 checksum）。
		if w.Checksum != "" {
			want := checksumOf(w.Format, w.Version, w.CreatedAt, w.Snapshot, w.Positions, w.Anchors)
			if want != w.Checksum {
				return Checkpoint{}, fmt.Errorf("%w: checksum mismatch", ErrCheckpointCorrupt)
			}
		}
		return Checkpoint{
			Format:    w.Format,
			Version:   w.Version,
			CreatedAt: w.CreatedAt,
			Snapshot:  w.Snapshot,
			Positions: orEmptyPositions(w.Positions),
			Anchors:   orEmptyAnchors(w.Anchors),
		}, nil
	}

	// 无信封：要么是 v1 裸 Snapshot，要么是无法识别的内容。
	if !hasSnapshot {
		if looksLikeSnapshotV1(probe) {
			var snap Snapshot
			if err := json.Unmarshal(data, &snap); err != nil {
				return Checkpoint{}, fmt.Errorf("%w: %v", ErrCheckpointCorrupt, err)
			}
			return Checkpoint{
				Format:    checkpointMagic,
				Version:   1,
				CreatedAt: 0,
				Snapshot:  snap,
				Positions: map[int]uint64{},
				Anchors:   map[int]string{},
			}, nil
		}
		return Checkpoint{}, fmt.Errorf("%w: unrecognized checkpoint payload (no format/version marker)",
			ErrCheckpointCorrupt)
	}
	return Checkpoint{}, fmt.Errorf("%w: payload has snapshot but no envelope", ErrCheckpointCorrupt)
}

// encodeSnapshotV1 按旧版本（v1）的裸快照形状编码，仅用于兼容验证。
func encodeSnapshotV1(snap Snapshot) ([]byte, error) {
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode v1 snapshot: %w", err)
	}
	return data, nil
}

// looksLikeSnapshotV1 判断无信封 JSON 是否为 v1 裸 Snapshot：
// 顶层键必须全部来自 Snapshot 字段集合，且至少含 Watermarks 或 Committed。
func looksLikeSnapshotV1(m map[string]json.RawMessage) bool {
	known := map[string]bool{"watermarks": true, "committed": true, "windows": true, "seen": true}
	hit := false
	for k := range m {
		if !known[k] {
			return false
		}
		if k == "watermarks" || k == "committed" {
			hit = true
		}
	}
	return hit
}

func orEmptyPositions(m map[int]uint64) map[int]uint64 {
	if m == nil {
		return map[int]uint64{}
	}
	return m
}

func orEmptyAnchors(m map[int]string) map[int]string {
	if m == nil {
		return map[int]string{}
	}
	return m
}

// checksumOf 对检查点关键内容做稳定哈希，用于识别落盘内容被篡改/截断。
func checksumOf(format string, version int, createdAt int64, snap Snapshot,
	positions map[int]uint64, anchors map[int]string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%d|%d|", format, version, createdAt)
	wm := snap.Watermarks
	keys := make([]int, 0, len(wm))
	for k := range wm {
		keys = append(keys, k)
	}
	// map 遍历顺序不稳定，按分区排序保证哈希稳定。
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		fmt.Fprintf(h, "wm:%d=%d;", k, wm[k])
	}
	fmt.Fprintf(h, "committed:%d;", snap.Committed)
	pk := make([]int, 0, len(positions))
	for k := range positions {
		pk = append(pk, k)
	}
	for i := 0; i < len(pk); i++ {
		for j := i + 1; j < len(pk); j++ {
			if pk[j] < pk[i] {
				pk[i], pk[j] = pk[j], pk[i]
			}
		}
	}
	for _, k := range pk {
		fmt.Fprintf(h, "pos:%d=%d;", k, positions[k])
	}
	ak := make([]int, 0, len(anchors))
	for k := range anchors {
		ak = append(ak, k)
	}
	for i := 0; i < len(ak); i++ {
		for j := i + 1; j < len(ak); j++ {
			if ak[j] < ak[i] {
				ak[i], ak[j] = ak[j], ak[i]
			}
		}
	}
	for _, k := range ak {
		fmt.Fprintf(h, "anchor:%d=%s;", k, anchors[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}
