package engine

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

// ckptCfg 是跨进程场景的统一配置。
var ckptCfg = Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 10000}

// ckptSources 构造两个分区的事件脚本：事件带分区内单调 Seq。
// 水位线由驱动方显式推进（与事件注入分离），保证与源间交错无关的确定性。
func ckptSources() []*Source {
	return []*Source{
		{Partition: 0, Steps: []Step{
			EventStep(Event{ID: "s0-1", Key: "a", Value: 1, EventTime: 100, Seq: 1}),
			EventStep(Event{ID: "s0-2", Key: "b", Value: 5, EventTime: 300, Seq: 2}),
			// <-- 中断点：进程在此快照并退出
			EventStep(Event{ID: "s0-3", Key: "a", Value: 3, EventTime: 1100, Seq: 3}),
			EventStep(Event{ID: "s0-4", Key: "b", Value: 7, EventTime: 1500, Seq: 4}),
		}},
		{Partition: 1, Steps: []Step{
			EventStep(Event{ID: "s1-1", Key: "a", Value: 2, EventTime: 200, Seq: 1}),
			EventStep(Event{ID: "s1-2", Key: "a", Value: 10, EventTime: 150, Seq: 2}), // 窗口内乱序
			// <-- 中断点
			EventStep(Event{ID: "s1-3", Key: "c", Value: 9, EventTime: 2500, Seq: 3}),
		}},
	}
}

// splitAt 把源的事件脚本切成“中断点前（含）”与“中断点后”两段。
func splitAt(s *Source, n int) (before, after *Source) {
	return &Source{Partition: s.Partition, Steps: s.Steps[:n]},
		&Source{Partition: s.Partition, Steps: s.Steps[n:]}
}

// writeResults 把汇的最终结果写盘（模拟进程退出后仍存在的持久输出）。
func writeResults(path string, results map[string]WindowResult) error {
	data, err := json.Marshal(results)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// runCkptBaseline 单进程从头跑到尾，作为“一次都没停”的对照组。
func runCkptBaseline(t *testing.T) map[string]WindowResult {
	t.Helper()
	sink := NewMemSink()
	eng := NewEngine(ckptCfg, sink, Hooks{})
	Inject(eng, ckptSources()...)
	eng.AdvanceWatermark(0, 3499)
	eng.AdvanceWatermark(1, 3499)
	return sink.Results()
}

// TestCheckpointHelperProcess 不是真正的测试，而是被主测试以子进程方式拉起，
// 模拟“进程真的退出再重启”：phase1 消费到中断点、进度与输出写盘、退出；
// phase2 新进程读盘恢复、从续传位置接着消费、把最终结果写盘。
func TestCheckpointHelperProcess(t *testing.T) {
	phase := os.Getenv("GO_CKPT_HELPER")
	if phase == "" {
		return
	}
	ckptFile := os.Getenv("GO_CKPT_FILE")
	resultFile := os.Getenv("GO_CKPT_RESULT")
	store := &FileCheckpointStore{Path: ckptFile}
	srcs := ckptSources()

	switch phase {
	case "phase1":
		sink := NewMemSink()
		eng := NewEngine(ckptCfg, sink, Hooks{})
		b0, _ := splitAt(srcs[0], 2)
		b1, _ := splitAt(srcs[1], 2)
		Inject(eng, b0, b1)
		eng.AdvanceWatermark(0, 1200)
		eng.AdvanceWatermark(1, 1200)
		if err := store.Save(eng.Snapshot()); err != nil {
			os.Exit(2)
		}
		if err := writeResults(resultFile, sink.Results()); err != nil {
			os.Exit(2)
		}
		os.Exit(0) // 进程彻底退出，内存状态全部消失
	case "phase2":
		snap, err := store.Load()
		if err != nil {
			os.Exit(3)
		}
		sink := NewMemSink()
		eng := NewEngine(ckptCfg, sink, Hooks{})
		if err := eng.Restore(snap); err != nil {
			os.Exit(4)
		}
		// 数据源从上次提交的位置继续，而不是整批重灌。
		offsets := eng.Offsets()
		resumed := make([]*Source, 0, len(srcs))
		for _, s := range srcs {
			r, err := s.ResumeFrom(offsets[s.Partition])
			if err != nil {
				os.Exit(5)
			}
			resumed = append(resumed, r)
		}
		Inject(eng, resumed...)
		eng.AdvanceWatermark(0, 3499)
		eng.AdvanceWatermark(1, 3499)
		if err := writeResults(resultFile, sink.Results()); err != nil {
			os.Exit(6)
		}
		os.Exit(0)
	}
	os.Exit(7) // 未知阶段
}

// runHelper 以子进程方式运行当前测试二进制的指定阶段。
func runHelper(t *testing.T, phase, ckptFile, resultFile string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestCheckpointHelperProcess")
	cmd.Env = append(os.Environ(),
		"GO_CKPT_HELPER="+phase,
		"GO_CKPT_FILE="+ckptFile,
		"GO_CKPT_RESULT="+resultFile,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper %s failed: %v\n%s", phase, err, out)
	}
	t.Logf("HELPER %s exited cleanly (process restart boundary crossed)", phase)
}

func readResults(t *testing.T, path string) map[string]WindowResult {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]WindowResult
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

// TestCheckpointAcrossProcesses 验证进程彻底退出后，新进程从磁盘进度恢复，
// 最终结论与“从头跑到尾”完全一致，不丢不重。
func TestCheckpointAcrossProcesses(t *testing.T) {
	base := runCkptBaseline(t)

	dir := t.TempDir()
	ckptFile := filepath.Join(dir, "checkpoint.json")
	result1 := filepath.Join(dir, "result-phase1.json")
	result2 := filepath.Join(dir, "result-phase2.json")

	runHelper(t, "phase1", ckptFile, result1)
	if _, err := os.Stat(ckptFile); err != nil {
		t.Fatalf("checkpoint file not persisted: %v", err)
	}
	runHelper(t, "phase2", ckptFile, result2)

	// 合并两个进程各自的持久输出（幂等键覆盖），模拟外部持久汇。
	merged := readResults(t, result1)
	for k, v := range readResults(t, result2) {
		merged[k] = v
	}

	if !reflect.DeepEqual(base, merged) {
		for _, k := range sortedKeys(base) {
			t.Logf("  %s base=%+v got=%+v", k, base[k], merged[k])
		}
		t.Fatal("cross-process recovered results differ from no-restart baseline")
	}
	for _, k := range sortedKeys(merged) {
		t.Logf("RECOVERED %s -> %+v", k, merged[k])
	}
}

// TestCheckpointRepeatedRestore 验证同一份存盘进度反复恢复，结论稳定一致。
func TestCheckpointRepeatedRestore(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "checkpoint.json")}

	srcs := ckptSources()
	eng := NewEngine(ckptCfg, NewMemSink(), Hooks{})
	b0, _ := splitAt(srcs[0], 2)
	b1, _ := splitAt(srcs[1], 2)
	Inject(eng, b0, b1)
	eng.AdvanceWatermark(0, 1200)
	eng.AdvanceWatermark(1, 1200)
	if err := store.Save(eng.Snapshot()); err != nil {
		t.Fatal(err)
	}

	var first map[string]WindowResult
	for run := 1; run <= 3; run++ {
		snap, err := store.Load()
		if err != nil {
			t.Fatal(err)
		}
		sink := NewMemSink()
		e := NewEngine(ckptCfg, sink, Hooks{})
		if err := e.Restore(snap); err != nil {
			t.Fatalf("run %d restore: %v", run, err)
		}
		offsets := e.Offsets()
		var resumed []*Source
		for _, s := range ckptSources() {
			r, err := s.ResumeFrom(offsets[s.Partition])
			if err != nil {
				t.Fatalf("run %d resume: %v", run, err)
			}
			resumed = append(resumed, r)
		}
		Inject(e, resumed...)
		e.AdvanceWatermark(0, 3499)
		e.AdvanceWatermark(1, 3499)
		got := sink.Results()
		if first == nil {
			first = got
		} else if !reflect.DeepEqual(first, got) {
			t.Fatalf("run %d: repeated restore diverged", run)
		}
		t.Logf("run %d: %d results, identical to previous runs", run, len(got))
	}
}

// TestCheckpointV1Compatibility 验证上一版（v1，裸 Snapshot、无 Offsets）
// 存下来的进度仍可被新版本识别并恢复。
func TestCheckpointV1Compatibility(t *testing.T) {
	v1 := Snapshot{
		Watermarks: map[int]int64{0: 1200, 1: 1200},
		Committed:  1200,
		Windows: map[string]map[int64]windowState{
			"a": {0: {Sum: 3, Count: 2, Fired: true}},
		},
		Seen: []string{"s0-1", "s1-1"},
	}
	data, err := json.Marshal(v1) // 上一版直接序列化 Snapshot，无版本包装
	if err != nil {
		t.Fatal(err)
	}

	snap, err := UnmarshalCheckpoint(data)
	if err != nil {
		t.Fatalf("v1 checkpoint should be accepted: %v", err)
	}
	if len(snap.Offsets) != 0 {
		t.Fatalf("v1 has no offsets, got %v", snap.Offsets)
	}

	eng := NewEngine(ckptCfg, NewMemSink(), Hooks{})
	if err := eng.Restore(snap); err != nil {
		t.Fatalf("restore v1: %v", err)
	}
	if eng.Watermark() != 1200 {
		t.Fatalf("restored watermark = %d, want 1200", eng.Watermark())
	}
	// v1 无消费位置：Offsets 为空，调用方应整批重灌并依赖去重表吸收重复。
	if len(eng.Offsets()) != 0 {
		t.Fatalf("v1 restore should have empty offsets, got %v", eng.Offsets())
	}
	t.Logf("v1 checkpoint accepted: committed=%d windows=%d seen=%d offsets=none",
		snap.Committed, len(snap.Windows), len(snap.Seen))
}

// TestCheckpointRejected 验证认不了的进度被明确拒绝：更高版本与损坏数据
// 返回可区分的 *CheckpointError，不猜、不崩。
func TestCheckpointRejected(t *testing.T) {
	// 更高版本：明确拒绝。
	future, _ := json.Marshal(Checkpoint{Version: CurrentCheckpoint + 1})
	_, err := UnmarshalCheckpoint(future)
	var ce *CheckpointError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CheckpointError, got %v", err)
	}
	if ce.Reason != ReasonUnsupportedVersion {
		t.Fatalf("expected reason %q, got %q", ReasonUnsupportedVersion, ce.Reason)
	}
	t.Logf("future version rejected: %v", err)

	// 损坏数据：明确拒绝。
	_, err = UnmarshalCheckpoint([]byte("{not json"))
	ce = nil
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CheckpointError, got %v", err)
	}
	if ce.Reason != ReasonCorrupt {
		t.Fatalf("expected reason %q, got %q", ReasonCorrupt, ce.Reason)
	}
	t.Logf("corrupt data rejected: %v", err)

	// 文件不存在：包装 os.ErrNotExist，可与格式错误区分。
	store := &FileCheckpointStore{Path: filepath.Join(t.TempDir(), "missing.json")}
	_, err = store.Load()
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
	t.Logf("missing file reported: %v", err)
}
