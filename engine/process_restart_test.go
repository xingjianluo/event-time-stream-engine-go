package engine_test

// 本文件通过 os/exec 真正两次拉起独立进程（cmd/worker），
// 验证“进程彻底退出后再新起进程”从磁盘续传，不丢不重。
// 用独立的 _test 包，避免触发被测包的 TestMain 之类机制。

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildWorker 编译 worker 二进制，返回路径（测试结束自动清理）。
func buildWorker(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "worker")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/worker")
	cmd.Dir = repoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}
	return bin
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(wd) // engine/ -> repo root
}

func runWorker(t *testing.T, bin, dir string, phase int) string {
	t.Helper()
	cmd := exec.Command(bin, "-dir", dir, "-phase", phaseString(phase))
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err != nil {
		t.Fatalf("worker phase %d failed: %v\n%s", phase, err, buf.String())
	}
	return buf.String()
}

func phaseString(p int) string {
	if p == 1 {
		return "1"
	}
	return "2"
}

type resultsFile struct {
	Format string                  `json:"format"`
	Values map[string]WindowResult `json:"results"`
}

// WindowResult 的外部包形状（字段一致即可解析）。
type WindowResult struct {
	Key    string
	Window Window
	Sum    int64
	Count  int64
}
type Window struct{ Start, End int64 }

func readResults(t *testing.T, path string) map[string]WindowResult {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rf resultsFile
	if err := json.Unmarshal(data, &rf); err != nil {
		t.Fatal(err)
	}
	return rf.Values
}

// wantResults 是“一次都没停、从头跑到尾”的期望结论。
var wantResults = map[string][2]int64{
	"a@[0,1000)":    {13, 3}, // 1+2+10(迟到修正)
	"a@[1000,2000)": {4, 1},
	"b@[0,1000)":    {7, 1},
	"b@[1000,2000)": {9, 1},
}

// TestCrossProcessRestart 真正起两个进程：进程1提交检查点后退出，进程2续传。
func TestCrossProcessRestart(t *testing.T) {
	bin := buildWorker(t)
	dir := t.TempDir()

	t.Log("SCENARIO cross-process restart: process 1 starts fresh, commits midpoint, exits")
	log1 := runWorker(t, bin, dir, 1)
	if !strings.Contains(log1, "FRESH start") || !strings.Contains(log1, "COMMITTED") {
		t.Fatalf("phase1 log missing markers:\n%s", log1)
	}
	if _, err := os.Stat(filepath.Join(dir, "checkpoint.json")); err != nil {
		t.Fatalf("checkpoint not persisted before phase2: %v", err)
	}
	// 阶段 1 只应处理到中断点（3 个事件），不是全部。
	if got := strings.Count(log1, "ACCEPT"); got != 3 {
		t.Fatalf("phase1 should process exactly 3 prefix events, got %d", got)
	}

	t.Log("SCENARIO cross-process restart: process 2 is a brand-new process, resumes from disk")
	log2 := runWorker(t, bin, dir, 2)
	if !strings.Contains(log2, "RESUMED from checkpoint") {
		t.Fatalf("phase2 did not resume from checkpoint:\n%s", log2)
	}
	// 后缀只有 4 条事件记录（p0-e3, p1-e2, p1-e3 + p1 水位线不算事件）。
	if got := strings.Count(log2, "ACCEPT"); got != 3 {
		t.Fatalf("phase2 should process exactly 3 suffix events, got %d", got)
	}
	if strings.Contains(log2, "w-p0-e1") || strings.Contains(log2, "w-p1-e1") {
		t.Fatal("phase2 must not re-deliver already-committed prefix records")
	}

	got := readResults(t, filepath.Join(dir, "results.json"))
	if len(got) != len(wantResults) {
		t.Fatalf("expected %d final windows, got %d: %v", len(wantResults), len(got), got)
	}
	for k, want := range wantResults {
		r, ok := got[k]
		if !ok {
			t.Errorf("missing result %s", k)
			continue
		}
		if r.Sum != want[0] || r.Count != want[1] {
			t.Errorf("%s got sum=%d count=%d, want sum=%d count=%d", k, r.Sum, r.Count, want[0], want[1])
		}
	}
	t.Log("PASS real process restart: final results identical to uninterrupted run")

	// 再用同一份检查点起一次“新进程”：结果必须稳定一致（反复恢复不漂移）。
	log3 := runWorker(t, bin, dir, 2)
	if strings.Count(log3, "ACCEPT") != 0 {
		t.Fatalf("third process must deliver nothing (all positions at end), see log:\n%s", log3)
	}
	got2 := readResults(t, filepath.Join(dir, "results.json"))
	if len(got2) != len(wantResults) {
		t.Fatalf("repeated resume changed result count: %d", len(got2))
	}
	for k := range wantResults {
		if got2[k] != got[k] {
			t.Fatalf("repeated resume changed result %s: %+v vs %+v", k, got2[k], got[k])
		}
	}
	t.Log("PASS re-opening same checkpoint repeatedly yields stable, identical results")
}

// TestCrossProcessRejectsTamperedSource 验证数据与保存位置对不上时，
// 新进程必须明确拒绝而不是从头重来或跳过。
func TestCrossProcessRejectsTamperedSource(t *testing.T) {
	bin := buildWorker(t)
	dir := t.TempDir()
	_ = runWorker(t, bin, dir, 1)

	// worker 的数据源是编译进二进制的固定数据；要模拟“锚点记录被换过”，
	// 直接改检查点里的锚点为错误值，等价于位置指向的数据内容对不上。
	cpPath := filepath.Join(dir, "checkpoint.json")
	raw, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	anchors := wire["anchors"].(map[string]any)
	anchors["0"] = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	// 校验和会随之失配；删除 checksum 以单独验证“锚点不匹配”这条路径。
	delete(wire, "checksum")
	rewritten, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cpPath, rewritten, 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "-dir", dir, "-phase", "2")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	if err == nil {
		t.Fatalf("worker must fail when anchor mismatches, but it succeeded:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "position-changed") {
		t.Fatalf("error must distinguish position-changed, got:\n%s", buf.String())
	}
	t.Logf("SCENARIO/PASS tampered anchor rejected across processes: %s",
		firstLineContaining(buf.String(), "position-changed"))
}

// TestCrossProcessRejectsFutureVersion 验证新版本检查点被新进程明确拒绝、不崩。
func TestCrossProcessRejectsFutureVersion(t *testing.T) {
	bin := buildWorker(t)
	dir := t.TempDir()
	_ = runWorker(t, bin, dir, 1)

	cpPath := filepath.Join(dir, "checkpoint.json")
	var wire map[string]any
	raw, _ := os.ReadFile(cpPath)
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	wire["version"] = 99
	delete(wire, "checksum")
	rewritten, _ := json.Marshal(wire)
	if err := os.WriteFile(cpPath, rewritten, 0o644); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(bin, "-dir", dir, "-phase", "2")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Run(); err == nil {
		t.Fatal("future-version checkpoint must be rejected")
	}
	if !strings.Contains(buf.String(), "version unsupported") {
		t.Fatalf("expected unsupported-version error, got:\n%s", buf.String())
	}
	t.Log("SCENARIO/PASS future checkpoint version rejected by resumed process")
}

func firstLineContaining(haystack, needle string) string {
	for _, line := range strings.Split(haystack, "\n") {
		if strings.Contains(line, needle) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// TestCrossProcessAcceptsLegacyV1 验证旧版本（v1 裸快照）检查点能被新进程认出：
// 没有数据位置 -> 从头重放 -> 恢复出的去重表吸收重投递，最终结论仍正确不重。
func TestCrossProcessAcceptsLegacyV1(t *testing.T) {
	bin := buildWorker(t)
	dir := t.TempDir()
	_ = runWorker(t, bin, dir, 1) // 先产生一个正常的 v2 状态目录

	// 用手工构造的 v1 裸 Snapshot（无信封、无位置）覆盖检查点文件。
	// 内容必须对应 worker phase1 之后的引擎状态：wm 1200、窗口 a/b、seen 三个前缀事件。
	v1 := map[string]any{
		"watermarks": map[string]int64{"0": 1200, "1": 1200},
		"committed":  int64(1200),
		"windows": map[string]map[string]map[string]any{
			"a": {"0": {"Sum": 3, "Count": 2, "Fired": true}},
			"b": {"0": {"Sum": 7, "Count": 1, "Fired": false}},
		},
		"seen": []string{"w-p0-e1", "w-p0-e2", "w-p1-e1"},
	}
	raw, err := json.MarshalIndent(v1, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checkpoint.json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Log("SCENARIO legacy v1 snapshot placed for a brand-new process to discover")

	log2 := runWorker(t, bin, dir, 2)
	if !strings.Contains(log2, "RESUMED from checkpoint") {
		t.Fatalf("v1 checkpoint should be recognized:\n%s", log2)
	}
	// 无位置信息 -> 数据源从头打开：前缀 3 事件被去重表吸收（DROP duplicate），
	// 后缀 3 事件被接受。全程无重复计数。
	if accepted := strings.Count(log2, "ACCEPT"); accepted != 3 {
		t.Fatalf("v1 resume should accept 3 new events, got %d:\n%s", accepted, log2)
	}
	got := readResults(t, filepath.Join(dir, "results.json"))
	if len(got) != len(wantResults) {
		t.Fatalf("v1 resume final window count=%d want %d", len(got), len(wantResults))
	}
	for k, want := range wantResults {
		r, ok := got[k]
		if !ok || r.Sum != want[0] || r.Count != want[1] {
			t.Fatalf("v1 resume %s = %+v ok=%v, want sum=%d count=%d", k, r, ok, want[0], want[1])
		}
	}
	t.Log("PASS legacy v1 checkpoint accepted; replay-from-start deduped; results correct")
}
