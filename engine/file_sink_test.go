package engine

import (
	"path/filepath"
	"testing"
)

// TestFileSinkPersistence 验证进程退出后重开同一文件，历史结果仍在且幂等覆盖。
func TestFileSinkPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")

	s1, err := OpenFileSink(path, RetentionPolicy{RetainMillis: 100000})
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Write(WindowResult{Key: "a", Window: Window{Start: 0, End: 1000}, Sum: 1, Count: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Write(WindowResult{Key: "a", Window: Window{Start: 0, End: 1000}, Sum: 5, Count: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s1.Write(WindowResult{Key: "b", Window: Window{Start: 0, End: 1000}, Sum: 9, Count: 1}); err != nil {
		t.Fatal(err)
	}
	t.Log("SCENARIO file-sink persistence: 3 writes (2 of them same idempotent key) then reopen")

	// “新进程”：用新句柄重新打开同一文件。
	s2, err := OpenFileSink(path, RetentionPolicy{RetainMillis: 100000})
	if err != nil {
		t.Fatal(err)
	}
	if s2.Len() != 2 {
		t.Fatalf("expected 2 results after reopen, got %d", s2.Len())
	}
	r, ok := s2.Get("a", Window{Start: 0, End: 1000})
	if !ok || r.Sum != 5 || r.Count != 2 {
		t.Fatalf("latest result not persisted: %+v ok=%v", r, ok)
	}
	t.Logf("PASS results survive real reopen: %d windows, a=%+v", s2.Len(), r)
}

// TestFileSinkExpirePersists 验证回收动作跨重开生效：旧结果既离开内存也离开磁盘。
func TestFileSinkExpirePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	s, err := OpenFileSink(path, RetentionPolicy{RetainMillis: 1000})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(WindowResult{Key: "a", Window: Window{Start: 0, End: 1000}, Sum: 1, Count: 1})
	_ = s.Write(WindowResult{Key: "a", Window: Window{Start: 2000, End: 3000}, Sum: 2, Count: 1})

	rep := s.Expire(2000, 0) // 窗口0过期点 2000 <= 2000 → 回收
	if rep.After != 1 || len(rep.Evicted) != 1 {
		t.Fatalf("expected 1 eviction, got %+v", rep)
	}
	t.Logf("SCENARIO file-sink expire: %+v", rep.Evicted[0])

	reopened, err := OpenFileSink(path, RetentionPolicy{RetainMillis: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Len() != 1 {
		t.Fatalf("eviction not persisted: %d results on disk", reopened.Len())
	}
	if _, ok := reopened.Get("a", Window{Start: 0, End: 1000}); ok {
		t.Fatal("evicted result still present on disk after reopen")
	}
	if _, ok := reopened.Get("a", Window{Start: 2000, End: 3000}); !ok {
		t.Fatal("retained result missing on disk")
	}
	t.Log("PASS eviction durable across reopen, retained window intact")
}

// TestFileSinkProtectionFloor 验证落盘汇同样遵守允许迟到保护下限。
func TestFileSinkProtectionFloor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	s, err := OpenFileSink(path, RetentionPolicy{RetainMillis: 100})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Write(WindowResult{Key: "a", Window: Window{Start: 0, End: 1000}, Sum: 1, Count: 1})
	rep := s.Expire(1200, 500) // 策略 100 想回收，但保护下限 500 保住该结果
	if rep.RetainMillis != 500 || rep.After != 1 {
		t.Fatalf("protected result evicted: %+v", rep)
	}
	t.Log("SCENARIO/PASS file sink honors AllowedLateness protection floor")
}

// TestFileSinkRejectsCorruptFile 验证坏掉的结果文件被明确报错而不是静默清空。
func TestFileSinkRejectsCorruptFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "results.json")
	if err := writeFileAtomic(path, []byte("not json")); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileSink(path, RetentionPolicy{}); err == nil {
		t.Fatal("corrupt result file must produce an error")
	} else {
		t.Logf("SCENARIO/PASS corrupt result file rejected: %v", err)
	}
}
