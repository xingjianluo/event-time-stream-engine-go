package engine

import (
	"errors"
	"reflect"
	"testing"
)

// TestResumeFromOffset 验证重启后数据源从上次提交的位置继续投递：
// 跨过中断点的数据只被算一次，最终结论与“从头跑到尾”一致。
func TestResumeFromOffset(t *testing.T) {
	base := runCkptBaseline(t)

	srcs := ckptSources()

	// 中断前：消费每个分区的前 2 条事件。
	sink1 := NewMemSink()
	eng1 := NewEngine(ckptCfg, sink1, testHooks(t))
	b0, _ := splitAt(srcs[0], 2)
	b1, _ := splitAt(srcs[1], 2)
	Inject(eng1, b0, b1)
	eng1.AdvanceWatermark(0, 1200)
	eng1.AdvanceWatermark(1, 1200)
	snap := eng1.Snapshot()
	t.Logf("CHECKPOINT offsets=%v committed=%d", snap.Offsets, snap.Committed)

	// 重启：恢复状态，数据源按位置续传。
	sink2 := NewMemSink()
	eng2 := NewEngine(ckptCfg, sink2, testHooks(t))
	if err := eng2.Restore(snap); err != nil {
		t.Fatal(err)
	}
	var resumed []*Source
	for _, s := range ckptSources() {
		r, err := s.ResumeFrom(snap.Offsets[s.Partition])
		if err != nil {
			t.Fatalf("resume partition %d: %v", s.Partition, err)
		}
		t.Logf("RESUME partition=%d from=%+v remaining-steps=%d",
			s.Partition, snap.Offsets[s.Partition], len(r.Steps))
		resumed = append(resumed, r)
	}
	Inject(eng2, resumed...)
	eng2.AdvanceWatermark(0, 3499)
	eng2.AdvanceWatermark(1, 3499)

	// 中断点前的事件没有重投：恢复后的引擎只消费了续传部分。
	if got := len(eng2.Offsets()); got != 2 {
		t.Fatalf("expected offsets for 2 partitions, got %d", got)
	}

	merged := sink1.Results()
	for k, v := range sink2.Results() {
		merged[k] = v
	}
	if !reflect.DeepEqual(base, merged) {
		for _, k := range sortedKeys(base) {
			t.Logf("  %s base=%+v got=%+v", k, base[k], merged[k])
		}
		t.Fatal("offset-resumed results differ from no-restart baseline (loss or duplication)")
	}
	for _, k := range sortedKeys(merged) {
		t.Logf("RESUMED-FINAL %s -> %+v", k, merged[k])
	}
}

// TestResumeOffsetRejected 验证位置与源中数据对不上时被明确拒绝，
// 且三种情况（位置靠后 / 数据被截断 / 数据被替换）的 Reason 可区分。
func TestResumeOffsetRejected(t *testing.T) {
	src := &Source{Partition: 0, Steps: []Step{
		EventStep(Event{ID: "e1", Key: "a", Value: 1, EventTime: 100, Seq: 1}),
		EventStep(Event{ID: "e2", Key: "a", Value: 2, EventTime: 200, Seq: 2}),
		EventStep(Event{ID: "e3", Key: "a", Value: 3, EventTime: 300, Seq: 3}),
	}}

	// 情况 1：位置比现有数据靠后。
	_, err := src.ResumeFrom(Position{Seq: 9, LastID: "e9"})
	var se *SeekError
	if !errors.As(err, &se) || se.Reason != ReasonOffsetBeyondEnd {
		t.Fatalf("expected %q, got %v", ReasonOffsetBeyondEnd, err)
	}
	t.Logf("beyond-end rejected: %v", err)

	// 情况 2：位置指向的数据已不在（源被截断，只剩 Seq>=3）。
	trimmed := &Source{Partition: 0, Steps: src.Steps[2:]}
	_, err = trimmed.ResumeFrom(Position{Seq: 2, LastID: "e2"})
	se = nil
	if !errors.As(err, &se) || se.Reason != ReasonOffsetTrimmed {
		t.Fatalf("expected %q, got %v", ReasonOffsetTrimmed, err)
	}
	t.Logf("trimmed rejected: %v", err)

	// 情况 3：位置指向的数据被换过（同 Seq 不同 ID）。
	replaced := &Source{Partition: 0, Steps: []Step{
		EventStep(Event{ID: "e1", Key: "a", Value: 1, EventTime: 100, Seq: 1}),
		EventStep(Event{ID: "eX", Key: "a", Value: 99, EventTime: 200, Seq: 2}), // 被替换
		EventStep(Event{ID: "e3", Key: "a", Value: 3, EventTime: 300, Seq: 3}),
	}}
	_, err = replaced.ResumeFrom(Position{Seq: 2, LastID: "e2"})
	se = nil
	if !errors.As(err, &se) || se.Reason != ReasonOffsetMismatch {
		t.Fatalf("expected %q, got %v", ReasonOffsetMismatch, err)
	}
	t.Logf("mismatch rejected: %v", err)

	// 合法位置：从 Seq=2 之后继续，只剩 e3。
	r, err := src.ResumeFrom(Position{Seq: 2, LastID: "e2"})
	if err != nil {
		t.Fatalf("valid resume should succeed: %v", err)
	}
	if len(r.Steps) != 1 || r.Steps[0].Event.ID != "e3" {
		t.Fatalf("expected only e3 after resume, got %+v", r.Steps)
	}
	t.Logf("valid resume from seq=2: remaining=%d step(s)", len(r.Steps))

	// 零值位置：从头开始，不校验。
	r, err = src.ResumeFrom(Position{})
	if err != nil || len(r.Steps) != 3 {
		t.Fatalf("zero position should replay all, got %d steps, err=%v", len(r.Steps), err)
	}
}
