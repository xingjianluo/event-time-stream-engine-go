package engine

import (
	"errors"
	"path/filepath"
	"testing"
)

// TestCheckpointFileRoundTrip 验证检查点落盘后在新句柄里能完整读回（跨进程语义）。
func TestCheckpointFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "cp.json")
	cp := Checkpoint{
		Snapshot:  sampleSnapshot(),
		Positions: map[int]uint64{0: 5, 1: 4},
		Anchors:   map[int]string{0: "a0", 1: "a1"},
	}
	if err := SaveCheckpointFile(path, cp); err != nil {
		t.Fatal(err)
	}
	t.Logf("SCENARIO checkpoint-file-roundtrip saved to %s", path)

	got, err := LoadCheckpointFile(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Version != CheckpointVersion || got.Positions[0] != 5 || got.Anchors[1] != "a1" {
		t.Fatalf("loaded checkpoint mismatch: %+v", got)
	}
	t.Logf("PASS checkpoint read back: version=%d committed=%d positions=%v",
		got.Version, got.Snapshot.Committed, got.Positions)

	// 不应残留临时文件。
	if _, err := LoadCheckpointFile(path + ".tmp"); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("temp file leaked or wrong error: %v", err)
	}
	t.Log("PASS no temp file left behind (atomic rename committed)")
}

// TestCheckpointFileMissing 验证首次运行（无检查点）返回可辨识的 ErrNoCheckpoint。
func TestCheckpointFileMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.json")
	_, err := LoadCheckpointFile(path)
	if !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("expected ErrNoCheckpoint, got %v", err)
	}
	t.Logf("SCENARIO/PASS first run with no checkpoint -> ErrNoCheckpoint: %v", err)
}

// TestCheckpointFileOverwrite 验证重复保存覆盖旧内容，读回的总是最新提交。
func TestCheckpointFileOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	first := Checkpoint{Snapshot: Snapshot{Committed: 100, Watermarks: map[int]int64{0: 100}}}
	second := Checkpoint{
		Snapshot:  Snapshot{Committed: 2400, Watermarks: map[int]int64{0: 2400}},
		Positions: map[int]uint64{0: 9},
	}
	if err := SaveCheckpointFile(path, first); err != nil {
		t.Fatal(err)
	}
	if err := SaveCheckpointFile(path, second); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpointFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Snapshot.Committed != 2400 || got.Positions[0] != 9 {
		t.Fatalf("expected latest checkpoint, got committed=%d positions=%v",
			got.Snapshot.Committed, got.Positions)
	}
	t.Logf("SCENARIO/PASS checkpoint overwrite: latest committed=%d wins", got.Snapshot.Committed)
}

// TestCheckpointFileCorrupt 验证磁盘上坏掉的文件被明确报为 corrupt。
func TestCheckpointFileCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cp.json")
	if err := writeFileAtomic(path, []byte("@@@ not json @@@")); err != nil {
		t.Fatal(err)
	}
	_, err := LoadCheckpointFile(path)
	if !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
	}
	t.Logf("SCENARIO/PASS corrupt checkpoint file rejected: %v", err)
}
