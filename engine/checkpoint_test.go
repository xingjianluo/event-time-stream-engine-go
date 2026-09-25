package engine

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// driveToCheckpoint 跑阶段1+2+3 并在水位线 30 处返回引擎。
func driveToCheckpoint(t *testing.T) (*Engine, BatchFixture) {
	t.Helper()
	fx := MakeBatchFixture()
	eng := runOneOrder(t, fx.PreWM20Events(), fx) // 内部已推进到 wm=30
	return eng, fx
}

// driveToWM20 只跑阶段1+2 并在水位线 20 处返回引擎
// （窗口已触发但去重表尚未清理，适合验证恢复后重放去重）。
func driveToWM20(t *testing.T) (*Engine, BatchFixture) {
	t.Helper()
	fx := MakeBatchFixture()
	eng := New(testConfig())
	eng.SetLogger(testLogger{t})
	for _, ev := range fx.PreWM20Events() {
		mustProc(t, eng, ev)
	}
	if _, err := eng.AdvanceWatermark(0, fx.Base.Add(20*time.Second)); err != nil {
		t.Fatal(err)
	}
	return eng, fx
}

func assertSameResults(t *testing.T, a, b *Engine, tag string) {
	t.Helper()
	ra, rb := resultFingerprint(t, a), resultFingerprint(t, b)
	if len(ra) != len(rb) {
		t.Fatalf("[%s] result count %d != %d", tag, len(ra), len(rb))
	}
	for k := range ra {
		if _, ok := rb[k]; !ok {
			t.Fatalf("[%s] restored engine missing %+v", tag, k)
		}
	}
	t.Logf("[%s] 恢复前后聚合结论一致（%d 个窗口）", tag, len(ra))
}

// TestSnapshotRestoreNoDuplicateNoLoss 在已提交水位线 20（窗口已触发、
// 去重表仍完整）处做快照、用新引擎恢复，再重放历史事件：结果不重复不丢失。
func TestSnapshotRestoreNoDuplicateNoLoss(t *testing.T) {
	eng, fx := driveToWM20(t)
	before := SnapshotViewResults(eng)
	cp := eng.Snapshot()
	t.Logf("快照 watermark=%s windows=%d sink=%d dedup=%d",
		cp.CommittedWatermark.Format("15:04:05"), len(cp.Windows),
		len(cp.SinkEntries), len(cp.DedupEntries))

	eng2 := New(testConfig())
	eng2.SetLogger(testLogger{t})
	if err := eng2.Restore(cp); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !eng2.Watermark().Equal(eng.Watermark()) {
		t.Fatalf("restored wm=%s want %s", eng2.Watermark(), eng.Watermark())
	}
	assertSameResults(t, eng, eng2, "restore")

	// 模拟消息队列重放：快照前已接受的事件重投时，
	// 要么被去重拦截（仍可能影响存活窗口），要么被迟到策略拒绝（窗口已彻底关闭），
	// 二者都不得再次改变聚合结论。
	var dup, late int
	for _, ev := range fx.PreWM20Events() {
		err := eng2.ProcessEvent(ev)
		switch {
		case errors.Is(err, ErrDuplicateEvent):
			dup++
		case errors.Is(err, ErrLateEventDropped):
			late++
		case err != nil:
			t.Fatalf("replay unexpected error: %v", err)
		default:
			t.Fatalf("replayed event %s unexpectedly accepted after restore", ev.ID)
		}
	}
	// wm=20、切点=20-5-10=5：@11/@12/@15 共3条走去重，@1/@3 共2条超迟到。
	if dup != 3 || late != 2 {
		t.Fatalf("replay stats: dup=%d want3, late=%d want2", dup, late)
	}
	t.Logf("恢复后重放: 重复拦截=%d 迟到拦截=%d（5 条历史事件全部幂等处理，结论不变）",
		dup, late)

	after := SnapshotViewResults(eng2)
	if len(after) != len(before) {
		t.Fatalf("result count changed after replay: %d -> %d", len(before), len(after))
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("result %+v changed version %d -> %d after replay", k, v, after[k])
		}
	}
	logFinalResults(t, eng2, "after-restore-replay")

	// 继续消费新事件（模拟重启后继续）：允许迟到内修正仍正常工作，无丢失。
	err := eng2.ProcessEvent(Event{ID: "new-1", Key: "a", Value: 7,
		EventTime: fx.Base.Add(17 * time.Second)})
	if err != nil {
		t.Fatalf("post-restore within-lateness event rejected: %v", err)
	}
	r := findResult(eng2, "a", fx.Base.Add(10*time.Second))
	if r.Result.Sum != 13 { // 2(a-03)+4(a-04)+7(new)
		t.Fatalf("post-restore continuation sum=%d want 13 (lost events?)", r.Result.Sum)
	}
	t.Logf("恢复后继续消费验证通过: a 窗口 sum=%d count=%d", r.Result.Sum, r.Result.Count)
}

// TestRestoreRejectsOlderWatermark 已运行到更新提交点的引擎，
// 恢复更旧水位线的快照必须被拒绝。
func TestRestoreRejectsOlderWatermark(t *testing.T) {
	eng, fx := driveToCheckpoint(t) // 已提交 wm=30
	cp := eng.Snapshot()
	cp.CommittedWatermark = fx.Base.Add(20 * time.Second) // 伪造更旧快照点

	err := eng.Restore(cp)
	if !errors.Is(err, ErrWatermarkRegression) {
		t.Fatalf("want ErrWatermarkRegression, got %v", err)
	}
	t.Logf("旧水位线恢复被拒绝: %v", err)

	// 全新引擎（当前提交点为零值）恢复任意非负快照点应被允许。
	fresh := New(testConfig())
	if err := fresh.Restore(eng.Snapshot()); err != nil {
		t.Fatalf("fresh engine restore should succeed: %v", err)
	}
}

// TestRestoreRejectsConflict 快照与当前输出冲突必须被拒绝。
func TestRestoreRejectsConflict(t *testing.T) {
	eng, _ := driveToCheckpoint(t)
	cp := eng.Snapshot()

	// 先恢复到 cp，再产生一个"更新的修正版本"，然后恢复同一 cp：冲突。
	eng2 := New(testConfig())
	if err := eng2.Restore(cp); err != nil {
		t.Fatal(err)
	}
	// 手动往输出汇写一个更高版本，模拟已确认的更新输出。
	first := cp.SinkEntries[0]
	higher := ResultLogEntry{Result: first.Result, Version: first.Version + 10}
	eng2.sink.Write(higher.Result, higher.Version)

	err := eng2.Restore(cp)
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("want ErrSnapshotConflict, got %v", err)
	}
	t.Logf("冲突快照恢复被拒绝: %v", err)
}

// TestCheckpointFileRoundTrip JSON 文件快照往返一致。
func TestCheckpointFileRoundTrip(t *testing.T) {
	eng, _ := driveToCheckpoint(t)
	cp := eng.Snapshot()
	path := filepath.Join(t.TempDir(), "cp.json")
	if err := SaveCheckpoint(path, cp); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.CommittedWatermark.Equal(cp.CommittedWatermark) {
		t.Fatalf("wm mismatch %s != %s", loaded.CommittedWatermark, cp.CommittedWatermark)
	}
	eng3 := New(testConfig())
	if err := eng3.Restore(loaded); err != nil {
		t.Fatalf("restore from file: %v", err)
	}
	assertSameResults(t, eng, eng3, "file-roundtrip")
}

// TestWatermarkRegressionRejected 分区水位线回退必须被拒绝。
func TestWatermarkRegressionRejected(t *testing.T) {
	eng := New(testConfig())
	if _, err := eng.AdvanceWatermark(0, time.Unix(100, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	_, err := eng.AdvanceWatermark(0, time.Unix(99, 0).UTC())
	if !errors.Is(err, ErrWatermarkRegression) {
		t.Fatalf("want ErrWatermarkRegression, got %v", err)
	}
	t.Logf("分区水位线回退被拒绝: %v", err)
}
