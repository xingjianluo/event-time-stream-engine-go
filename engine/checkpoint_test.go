package engine

import (
	"errors"
	"strings"
	"testing"
)

// sampleSnapshot 构造一个含窗口与去重条目的非空快照。
func sampleSnapshot() Snapshot {
	return Snapshot{
		Watermarks: map[int]int64{0: 1200, 1: 1200},
		Committed:  1200,
		Windows: map[string]map[int64]windowState{
			"a": {0: {Sum: 3, Count: 2, Fired: true}},
		},
		Seen: []string{"e1", "e2", "e3"},
	}
}

// TestCheckpointRoundTrip 验证 v2 检查点编码后能原样解码，位置与锚点不丢。
func TestCheckpointRoundTrip(t *testing.T) {
	cp := Checkpoint{
		Snapshot:  sampleSnapshot(),
		Positions: map[int]uint64{0: 3, 1: 2},
		Anchors:   map[int]string{0: "abc0", 1: "abc1"},
	}
	data, err := EncodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SCENARIO checkpoint-v2-roundtrip encoded %d bytes", len(data))

	got, err := DecodeCheckpoint(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Version != CheckpointVersion || got.Format != checkpointMagic {
		t.Fatalf("header mismatch: version=%d format=%q", got.Version, got.Format)
	}
	if got.Snapshot.Committed != 1200 || len(got.Snapshot.Seen) != 3 {
		t.Fatalf("snapshot lost: %+v", got.Snapshot)
	}
	if got.Positions[0] != 3 || got.Positions[1] != 2 || got.Anchors[0] != "abc0" {
		t.Fatalf("positions/anchors lost: %v %v", got.Positions, got.Anchors)
	}
	if got.Snapshot.Windows["a"][0].Sum != 3 || !got.Snapshot.Windows["a"][0].Fired {
		t.Fatalf("window state lost: %+v", got.Snapshot.Windows)
	}
	t.Logf("PASS v2 round-trip: committed=%d positions=%v anchors=%v",
		got.Snapshot.Committed, got.Positions, got.Anchors)
}

// TestCheckpointLegacyV1 验证旧版本裸快照能被认出并按 v1 语义恢复（无位置=从头投递）。
func TestCheckpointLegacyV1(t *testing.T) {
	data, err := encodeSnapshotV1(sampleSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SCENARIO legacy-v1-snapshot encoded %d bytes", len(data))

	cp, err := DecodeCheckpoint(data)
	if err != nil {
		t.Fatalf("legacy v1 snapshot must be accepted, got: %v", err)
	}
	if cp.Version != 1 {
		t.Fatalf("expected recognized version 1, got %d", cp.Version)
	}
	if cp.Snapshot.Committed != 1200 || len(cp.Snapshot.Seen) != 3 {
		t.Fatalf("v1 snapshot content lost: %+v", cp.Snapshot)
	}
	if len(cp.Positions) != 0 {
		t.Fatalf("v1 checkpoint must carry no positions (means replay-from-start), got %v", cp.Positions)
	}
	t.Logf("PASS legacy v1 accepted: version=%d committed=%d positions=empty (replay from start)",
		cp.Version, cp.Snapshot.Committed)

	// v1 快照恢复到新引擎也必须成功，且结论可用。
	eng := NewEngine(recoveryCfg, NewMemSink(), testHooks(t))
	if err := eng.Restore(cp.Snapshot); err != nil {
		t.Fatalf("restore from v1: %v", err)
	}
	if eng.Watermark() != 1200 {
		t.Fatalf("restored watermark=%d want 1200", eng.Watermark())
	}
	t.Logf("PASS v1 snapshot restored into fresh engine, watermark=%d", eng.Watermark())
}

// TestCheckpointUnknownVersion 验证更新版本的检查点被明确拒绝，不猜不崩。
func TestCheckpointUnknownVersion(t *testing.T) {
	cp := Checkpoint{
		Version:  CheckpointVersion + 1,
		Format:   checkpointMagic,
		Snapshot: sampleSnapshot(),
	}
	// 手工构造更高版本信封（EncodeCheckpoint 只会写当前版本）。
	data := []byte(`{"format":"` + checkpointMagic + `","version":99,"snapshot":{` +
		`"watermarks":{"0":1200},"committed":1200,"windows":{},"seen":[]}}`)
	_ = cp
	t.Log("SCENARIO future-version-checkpoint must be rejected with unsupported-version")

	_, err := DecodeCheckpoint(data)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
	t.Logf("PASS future version rejected: %v", err)
}

// TestCheckpointCorrupt 验证损坏/无法识别的内容被明确报为 corrupt。
func TestCheckpointCorrupt(t *testing.T) {
	cases := map[string][]byte{
		"empty":                         {},
		"not-json":                      []byte("{this is not json"),
		"unknown-shape":                 []byte(`{"hello":"world","foo":42}`),
		"wrong-magic":                   []byte(`{"format":"some-other-tool","version":2}`),
		"snapshot-only-no-envelope-bad": []byte(`{"nope":1}`),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeCheckpoint(data)
			if !errors.Is(err, ErrCheckpointCorrupt) {
				t.Fatalf("expected ErrCheckpointCorrupt, got %v", err)
			}
			t.Logf("PASS corrupt input %q rejected: %v", name, err)
		})
	}
}

// TestCheckpointChecksumMismatch 验证落盘内容被改写后因校验和不符被拒。
func TestCheckpointChecksumMismatch(t *testing.T) {
	cp := Checkpoint{Snapshot: sampleSnapshot(), Positions: map[int]uint64{0: 1}}
	data, err := EncodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	// 篡改水位线字节（1200 -> 1299），保持 JSON 仍可解析。
	tampered := strings.Replace(string(data), `"0": 1200`, `"0": 1299`, 1)
	if tampered == string(data) {
		// JSON 缩进形状不同时的兜底：直接替换 committed。
		tampered = strings.Replace(string(data), `"committed": 1200`, `"committed": 1299`, 1)
	}
	if _, err := DecodeCheckpoint([]byte(tampered)); !errors.Is(err, ErrCheckpointCorrupt) {
		t.Fatalf("tampered checkpoint must be corrupt, got %v", err)
	}
	t.Log("PASS tampered checkpoint rejected by checksum")
}
