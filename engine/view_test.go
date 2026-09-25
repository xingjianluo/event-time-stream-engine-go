package engine

// SnapshotViewResults 供测试使用：窗口结果 -> 版本号。
func SnapshotViewResults(eng *Engine) map[WindowResult]int64 {
	return eng.SnapshotView()
}
