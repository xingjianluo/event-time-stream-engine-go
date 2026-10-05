package engine

import (
	"errors"
	"reflect"
	"testing"
)

// resumeSteps 构造确定性的两分区序列（事件与水位线混合）。
func resumeSteps(p int) []Step {
	if p == 0 {
		return []Step{
			EventStep(Event{ID: "p0-e1", Key: "a", Value: 1, EventTime: 100}),
			EventStep(Event{ID: "p0-e2", Key: "a", Value: 2, EventTime: 200}),
			WatermarkStep(1200),
			EventStep(Event{ID: "p0-e3", Key: "a", Value: 4, EventTime: 1100}),
			WatermarkStep(3499),
		}
	}
	return []Step{
		EventStep(Event{ID: "p1-e1", Key: "b", Value: 7, EventTime: 300}),
		WatermarkStep(1200),
		EventStep(Event{ID: "p1-e2", Key: "b", Value: 9, EventTime: 1500}),
		EventStep(Event{ID: "p1-e3", Key: "a", Value: 10, EventTime: 150}), // 窗口0迟到修正
		WatermarkStep(3499),
	}
}

// deliverN 从分区当前光标确定性地投递前 n 条记录。
func deliverN(t *testing.T, eng *Engine, p *RecordedPartition, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		r, ok := p.Next()
		if !ok {
			t.Fatalf("partition %d exhausted early at %d", p.Partition, i)
		}
		if r.Event != nil {
			if err := eng.Ingest(*r.Event); err != nil {
				t.Fatal(err)
			}
		} else {
			eng.AdvanceWatermark(p.Partition, *r.Watermark)
		}
	}
}

// runResumeBaseline 按与“分段续传”完全相同的确定顺序不分段从头跑到尾：
// 先 p0 前3步、再 p1 前2步（= 中断点之前的顺序），之后两分区剩余记录并发
// 交错（= 恢复后阶段），作为“一次都没停”的对照组。
func runResumeBaseline(t *testing.T) map[string]WindowResult {
	t.Helper()
	sink := NewMemSink()
	eng := NewEngine(recoveryCfg, sink, testHooks(t))
	p0 := NewRecordedPartition(0, resumeSteps(0))
	p1 := NewRecordedPartition(1, resumeSteps(1))
	_ = p0.Open(0, "")
	_ = p1.Open(0, "")
	deliverN(t, eng, p0, 3)
	deliverN(t, eng, p1, 2)
	InjectRecorded(eng, p0, p1)
	return sink.Results()
}

// checkpointAtPause 跑到固定中断点（p0 前3步、p1 前2步）并提交检查点。
func checkpointAtPause(t *testing.T, eng *Engine) Checkpoint {
	t.Helper()
	p0 := NewRecordedPartition(0, resumeSteps(0))
	p1 := NewRecordedPartition(1, resumeSteps(1))
	_ = p0.Open(0, "")
	_ = p1.Open(0, "")
	deliverN(t, eng, p0, 3)
	deliverN(t, eng, p1, 2)
	return Checkpoint{
		Snapshot:  eng.Snapshot(),
		Positions: map[int]uint64{0: p0.Position(), 1: p1.Position()},
		Anchors:   map[int]string{0: p0.Anchor(), 1: p1.Anchor()},
	}
}

// TestResumeExactlyOnce 验证从提交位置续传，跨中断点的记录恰好处理一次，
// 最终结论与“从头跑到尾、一次没停”完全一致。
func TestResumeExactlyOnce(t *testing.T) {
	base := runResumeBaseline(t)

	// 阶段 1：进程跑到中断点并提交检查点（汇里已有窗口0的输出）。
	sink := NewMemSink()
	eng := NewEngine(recoveryCfg, sink, testHooks(t))
	cp := checkpointAtPause(t, eng)
	t.Logf("SCENARIO resume-exactly-once checkpoint at positions=%v anchors0=%q anchors1=%q",
		cp.Positions, cp.Anchors[0][:8], cp.Anchors[1][:8])

	// 阶段 2：新进程、重建数据源，从检查点位置打开后继续。
	// 已投递前缀不会被再灌一遍（汇沿用同一持久存储，此处用内存对象模拟）。
	eng2 := NewEngine(recoveryCfg, sink, Hooks{})
	if err := eng2.Restore(cp.Snapshot); err != nil {
		t.Fatalf("restore: %v", err)
	}
	q0 := NewRecordedPartition(0, resumeSteps(0))
	q1 := NewRecordedPartition(1, resumeSteps(1))
	if err := OpenPositions(cp, q0, q1); err != nil {
		t.Fatalf("resume positions must validate: %v", err)
	}
	if q0.Position() != cp.Positions[0] || q1.Position() != cp.Positions[1] {
		t.Fatalf("cursors not at saved positions: %d/%d vs %v", q0.Position(), q1.Position(), cp.Positions)
	}
	rejected := InjectRecorded(eng2, q0, q1)
	if len(rejected) != 0 {
		t.Fatalf("no events should be rejected on resume, got %v", rejected)
	}

	if q0.Position() != q0.Len() || q1.Position() != q1.Len() {
		t.Fatal("partition not fully drained after resume")
	}
	got := sink.Results()
	if !reflect.DeepEqual(base, got) {
		for k := range base {
			if base[k] != got[k] {
				t.Errorf("  %s baseline=%+v resumed=%+v", k, base[k], got[k])
			}
		}
		t.Fatal("resumed results differ from no-interruption baseline")
	}
	t.Logf("PASS exactly-once resume: positions %v -> end, results identical to baseline (%d windows)",
		cp.Positions, len(got))
}

// TestResumeRepeatedStable 验证同一份检查点反复用来恢复，结论稳定一致。
func TestResumeRepeatedStable(t *testing.T) {
	base := runResumeBaseline(t)

	sink := NewMemSink()
	engPre := NewEngine(recoveryCfg, sink, Hooks{})
	cp := checkpointAtPause(t, engPre)
	t.Log("SCENARIO restore twice from the same checkpoint")

	for attempt := 1; attempt <= 2; attempt++ {
		eng := NewEngine(recoveryCfg, sink, testHooks(t))
		if err := eng.Restore(cp.Snapshot); err != nil {
			t.Fatalf("attempt %d restore: %v", attempt, err)
		}
		q0 := NewRecordedPartition(0, resumeSteps(0))
		q1 := NewRecordedPartition(1, resumeSteps(1))
		if err := OpenPositions(cp, q0, q1); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
		InjectRecorded(eng, q0, q1)
		got := sink.Results()
		if !reflect.DeepEqual(base, got) {
			t.Fatalf("attempt %d results differ from baseline", attempt)
		}
		t.Logf("PASS repeated resume attempt=%d identical results (%d windows)", attempt, len(got))
	}
}

// TestResumePositionAhead 验证保存位置比现有数据更靠后时被明确拒绝。
func TestResumePositionAhead(t *testing.T) {
	p := NewRecordedPartition(0, resumeSteps(0))
	err := p.Open(uint64(len(resumeSteps(0)))+10, "")
	var pe *PositionError
	if !errors.As(err, &pe) || pe.Reason != ReasonPositionAhead {
		t.Fatalf("expected position-ahead, got %v", err)
	}
	t.Logf("SCENARIO/PASS position ahead of data rejected: %v", err)
}

// TestResumePositionChanged 验证锚点记录被替换时明确拒绝，不会悄悄跳过。
func TestResumePositionChanged(t *testing.T) {
	original := NewRecordedPartition(0, resumeSteps(0))
	_ = original.Open(0, "")
	_, _ = original.Next()
	r, _ := original.Next() // 偏移 1 是 p0-e2
	goodAnchor := recordFingerprint(r)
	if original.Position() != 2 {
		t.Fatalf("setup cursor=%d want 2", original.Position())
	}

	// 数据源在偏移 1 处被换成另一条事件（同长度，不是截断）。
	changed := append([]Step{}, resumeSteps(0)...)
	changed[1] = EventStep(Event{ID: "p0-e2", Key: "a", Value: 999, EventTime: 201})
	swapped := NewRecordedPartition(0, changed)
	err := swapped.Open(2, goodAnchor)
	var pe *PositionError
	if !errors.As(err, &pe) || pe.Reason != ReasonPositionChanged {
		t.Fatalf("expected position-changed, got %v", err)
	}
	t.Logf("SCENARIO/PASS swapped record at anchor rejected: %v", err)
	if swapped.Position() != 0 {
		t.Fatalf("rejected open must not advance cursor, got %d", swapped.Position())
	}

	// 正确数据 + 正确锚点必须成功，且打开后下一条就是偏移 2。
	fresh := NewRecordedPartition(0, resumeSteps(0))
	if err := fresh.Open(2, goodAnchor); err != nil {
		t.Fatalf("matching anchor should open: %v", err)
	}
	next, ok := fresh.Next()
	if !ok || next.Offset != 2 {
		t.Fatalf("expected first post-resume record offset=2, got %+v ok=%v", next, ok)
	}
	t.Log("PASS unchanged data with matching anchor resumes at saved position, prefix skipped")
}

// TestResumeMissingPartition 验证检查点里的分区在当前数据源中消失时被明确拒绝。
func TestResumeMissingPartition(t *testing.T) {
	cp := Checkpoint{
		Positions: map[int]uint64{0: 3, 7: 1},
		Anchors:   map[int]string{0: "x", 7: "y"},
	}
	p0 := NewRecordedPartition(0, resumeSteps(0))
	err := OpenPositions(cp, p0)
	var pe *PositionError
	if !errors.As(err, &pe) || pe.Reason != ReasonPositionChanged || pe.Partition != 7 {
		t.Fatalf("expected position-changed for missing partition 7, got %v", err)
	}
	t.Logf("SCENARIO/PASS missing partition rejected: %v", err)
}

// TestResumeAnchorAtZero 验证位置为 0 却带着非空锚点（形状自相矛盾）时被拒绝。
func TestResumeAnchorAtZero(t *testing.T) {
	p := NewRecordedPartition(0, resumeSteps(0))
	err := p.Open(0, "nonempty")
	var pe *PositionError
	if !errors.As(err, &pe) || pe.Reason != ReasonPositionChanged {
		t.Fatalf("expected position-changed, got %v", err)
	}
	t.Logf("SCENARIO/PASS zero-offset with anchor rejected: %v", err)

	if err := p.Open(0, ""); err != nil {
		t.Fatalf("clean start should open: %v", err)
	}
	if _, ok := p.Next(); !ok {
		t.Fatal("first record missing after clean open")
	}
}

// TestResumeFingerprintStable 验证同一记录指纹稳定、不同内容指纹不同。
func TestResumeFingerprintStable(t *testing.T) {
	ev := Event{ID: "e", Key: "k", Value: 1, EventTime: 100}
	r1 := Record{Offset: 0, Event: &ev}
	r2 := Record{Offset: 99, Event: &ev} // 偏移不同不算内容变化
	if recordFingerprint(r1) != recordFingerprint(r2) {
		t.Fatal("fingerprint must ignore offset")
	}
	other := Event{ID: "e", Key: "k", Value: 2, EventTime: 100}
	if recordFingerprint(r1) == recordFingerprint(Record{Event: &other}) {
		t.Fatal("different content must have different fingerprint")
	}
	wm := int64(1200)
	if recordFingerprint(r1) == recordFingerprint(Record{Watermark: &wm}) {
		t.Fatal("event and watermark must differ")
	}
	t.Log("SCENARIO/PASS fingerprint stable across offsets, distinct across content")
}
