package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Position 记录一个分区的消费位置：已消费事件的最大序号及该事件 ID。
// 序号用于从源续传，事件 ID 用于校验位置指向的数据是否被替换。
type Position struct {
	Seq    uint64 `json:"seq"`
	LastID string `json:"last_id"`
}

// Checkpoint 是可持久化的进度：完整快照 + 格式版本。
// 版本兼容范围：
//   - v1：上一版直接序列化的 Snapshot（无版本包装、无 Offsets 字段）；
//   - v2：当前版本，带版本包装，Snapshot 含各分区消费位置 Offsets；
//   - 更高版本：明确拒绝（ReasonUnsupportedVersion），不猜、不崩。
type Checkpoint struct {
	Version  int      `json:"version"`
	Snapshot Snapshot `json:"snapshot"`
}

// 进度格式版本。
const (
	CheckpointV1      = 1 // 上一版裸 Snapshot
	CheckpointV2      = 2 // 当前版本，含消费位置
	CurrentCheckpoint = CheckpointV2
)

// CheckpointError 描述一次被拒绝的 checkpoint 读取，Reason 可区分。
type CheckpointError struct {
	Reason string
	Detail string
}

func (e *CheckpointError) Error() string {
	return "checkpoint rejected [" + e.Reason + "]: " + e.Detail
}

// checkpoint 读取拒绝原因，可用 errors.As 拿到 *CheckpointError 后比较 Reason。
const (
	ReasonCorrupt            = "corrupt"             // 数据损坏，无法解析
	ReasonUnsupportedVersion = "unsupported-version" // 版本高于当前可识别范围
)

// MarshalCheckpoint 将快照序列化为当前版本的持久化字节流。
func MarshalCheckpoint(snap Snapshot) ([]byte, error) {
	return json.Marshal(Checkpoint{Version: CurrentCheckpoint, Snapshot: snap})
}

// UnmarshalCheckpoint 解析持久化字节流。
// 上一版（v1，裸 Snapshot）可识别；更高版本或损坏数据返回 *CheckpointError。
func UnmarshalCheckpoint(data []byte) (Snapshot, error) {
	// 先探测是否存在版本包装：v2 起为 {"version":N,"snapshot":{...}}，
	// v1 为裸 Snapshot（无 version 字段）。
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return Snapshot{}, &CheckpointError{
			Reason: ReasonCorrupt,
			Detail: fmt.Sprintf("not valid JSON: %v", err),
		}
	}

	switch {
	case probe.Version == 0:
		// v1：上一版直接序列化的裸 Snapshot，无 Offsets 字段。
		var snap Snapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			return Snapshot{}, &CheckpointError{
				Reason: ReasonCorrupt,
				Detail: fmt.Sprintf("v1 snapshot undecodable: %v", err),
			}
		}
		return snap, nil
	case probe.Version <= CurrentCheckpoint:
		var ck Checkpoint
		if err := json.Unmarshal(data, &ck); err != nil {
			return Snapshot{}, &CheckpointError{
				Reason: ReasonCorrupt,
				Detail: fmt.Sprintf("v%d checkpoint undecodable: %v", probe.Version, err),
			}
		}
		return ck.Snapshot, nil
	default:
		return Snapshot{}, &CheckpointError{
			Reason: ReasonUnsupportedVersion,
			Detail: fmt.Sprintf("checkpoint version %d > supported %d", probe.Version, CurrentCheckpoint),
		}
	}
}

// CheckpointStore 是进度的持久化存取接口。
type CheckpointStore interface {
	Save(snap Snapshot) error
	Load() (Snapshot, error)
}

// FileCheckpointStore 把进度以 JSON 形式写入本地文件，
// 进程退出后文件仍在，新进程可加载续传。
type FileCheckpointStore struct {
	Path string
}

// Save 原子写入 checkpoint 文件（先写临时文件再改名，避免崩溃留下半个文件）。
func (s *FileCheckpointStore) Save(snap Snapshot) error {
	data, err := MarshalCheckpoint(snap)
	if err != nil {
		return err
	}
	tmp := s.Path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.Path)
}

// Load 读取并解析 checkpoint 文件；文件不存在返回包装 os.ErrNotExist 的错误，
// 版本不兼容或数据损坏返回 *CheckpointError（可 errors.As 区分）。
func (s *FileCheckpointStore) Load() (Snapshot, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Snapshot{}, fmt.Errorf("checkpoint %s: %w", filepath.Base(s.Path), err)
		}
		return Snapshot{}, err
	}
	return UnmarshalCheckpoint(data)
}
