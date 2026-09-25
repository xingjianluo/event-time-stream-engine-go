package engine

import (
	"errors"
	"reflect"
	"testing"
)

var recoveryCfg = Config{WindowSize: 1000, AllowedLateness: 500, MaxSeenIDs: 10000}

// phase1 / phase2 把一批事件切成重启前与恢复后两段。
func phase1() []Event {
	return []Event{
		{ID: "p1-1", Key: "a", Value: 1, EventTime: 100, Partition: 0},
		{ID: "p1-2", Key: "a", Value: 2, EventTime: 200, Partition: 1},
		{ID: "p1-3", Key: "b", Value: 5, EventTime: 300, Partition: 0},
	}
}

func phase2() []Event {
	return []Event{
		{ID: "p2-1", Key: "a", Value: 3, EventTime: 1100, Partition: 1},
		{ID: "p2-2", Key: "b", Value: 7, EventTime: 1500, Partition: 0},
		// 迟到但在保留期内的修正事件（水位线 1200 < 窗口保留期 1500）
		{ID: "p2-3", Key: "a", Value: 10, EventTime: 150, Partition: 1},
	}
}

// runBaseline 无重启地跑完整段流，作为对照组。
func runBaseline(t *testing.T) map[string]WindowResult {
	t.Helper()
	sink := NewMemSink()
	eng := NewEngine(recoveryCfg, sink, testHooks(t))
	for _, ev := range phase1() {
		if err := eng.Ingest(ev); err != nil {
			t.Fatal(err)
		}
	}
	eng.AdvanceWatermark(0, 1200)
	eng.AdvanceWatermark(1, 1200)
	for _, ev := range phase2() {
		if err := eng.Ingest(ev); err != nil {
			t.Fatal(err)
		}
	}
	eng.AdvanceWatermark(0, 3499)
	eng.AdvanceWatermark(1, 3499)
	return sink.Results()
}

// TestRestartRecovery 验证在已提交水位线处快照并重启后，结果无重复无丢失。
func TestRestartRecovery(t *testing.T) {
	base := runBaseline(t)

	sink := NewMemSink()
	eng := NewEngine(recoveryCfg, sink, testHooks(t))

	// 阶段 1：消费部分事件并推进水位线到 1200。
	for _, ev := range phase1() {
		if err := eng.Ingest(ev); err != nil {
			t.Fatal(err)
		}
	}
	eng.AdvanceWatermark(0, 1200)
	eng.AdvanceWatermark(1, 1200)

	// 在已提交水位线处快照，模拟进程崩溃。
	snap := eng.Snapshot()
	t.Logf("SNAPSHOT committed=%d partitions=%v windows=%d seen=%d",
		snap.Committed, snap.Watermarks, len(snap.Windows), len(snap.Seen))

	// 阶段 2：新进程从快照恢复，继续消费。
	sink2 := NewMemSink()
	eng2 := NewEngine(recoveryCfg, sink2, testHooks(t))
	if err := eng2.Restore(snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// 崩溃恢复后消息中间件重投递阶段 1 的事件：必须被恢复的去重表吸收。
	for _, ev := range phase1() {
		if err := eng2.Ingest(ev); err != nil {
			t.Fatalf("redelivered event should be absorbed, got: %v", err)
		}
	}
	for _, ev := range phase2() {
		if err := eng2.Ingest(ev); err != nil {
			t.Fatal(err)
		}
	}
	eng2.AdvanceWatermark(0, 3499)
	eng2.AdvanceWatermark(1, 3499)

	// 合并两个汇（崩溃前输出 + 恢复后输出）。窗口 [0,1000) 在崩溃前已触发，
	// 恢复后迟到事件 p2-3 引发修正性重触发，幂等键覆盖后得到最终值。
	merged := make(map[string]WindowResult)
	for k, v := range sink.Results() {
		merged[k] = v
	}
	for k, v := range sink2.Results() {
		merged[k] = v
	}

	if !reflect.DeepEqual(base, merged) {
		for k := range base {
			t.Logf("  %s base=%+v recovered=%+v", k, base[k], merged[k])
		}
		t.Fatal("recovered results differ from no-restart baseline (loss or duplication)")
	}
	for k, v := range merged {
		t.Logf("RECOVERED %s -> %+v", k, v)
	}
}

// TestRestoreRejected 验证水位线回退与分区冲突的恢复请求被明确拒绝且原因可区分。
func TestRestoreRejected(t *testing.T) {
	eng := NewEngine(recoveryCfg, NewMemSink(), testHooks(t))
	if err := eng.Ingest(Event{ID: "x", Key: "a", Value: 1, EventTime: 100}); err != nil {
		t.Fatal(err)
	}
	eng.AdvanceWatermark(0, 500)
	eng.AdvanceWatermark(1, 800)

	// 先取一个合法快照，再推进水位线，之后旧快照即过期。
	old := eng.Snapshot()
	eng.AdvanceWatermark(0, 2000)
	eng.AdvanceWatermark(1, 2000)

	err := eng.Restore(old)
	var re *RestoreError
	if !errors.As(err, &re) {
		t.Fatalf("expected *RestoreError, got %v", err)
	}
	if re.Reason != ReasonWatermarkRegression {
		t.Fatalf("expected reason %q, got %q", ReasonWatermarkRegression, re.Reason)
	}
	t.Logf("regression restore rejected: %v", err)

	// 分区冲突：快照全局水位线不低，但某分区水位线低于当前。
	conflict := Snapshot{
		Watermarks: map[int]int64{0: 3000, 1: 100}, // 分区 1 当前为 2000
		Committed:  3000,
	}
	err = eng.Restore(conflict)
	re = nil
	if !errors.As(err, &re) {
		t.Fatalf("expected *RestoreError, got %v", err)
	}
	if re.Reason != ReasonPartitionConflict {
		t.Fatalf("expected reason %q, got %q", ReasonPartitionConflict, re.Reason)
	}
	t.Logf("conflicting restore rejected: %v", err)

	// 合法恢复仍然成功。
	fresh := NewEngine(recoveryCfg, NewMemSink(), testHooks(t))
	if err := fresh.Restore(old); err != nil {
		t.Fatalf("valid restore should succeed: %v", err)
	}
	if fresh.Watermark() != old.Committed {
		t.Fatalf("restored watermark = %d, want %d", fresh.Watermark(), old.Committed)
	}
}
