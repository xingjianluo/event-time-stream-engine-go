package engine

import "testing"

// testLogger 把引擎内部日志（事件接受/拒绝、窗口触发、水位线提交）
// 转发到 testing.T，go test -v 时可直接看到完整事件/窗口/水位线变化。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf(format, args...)
}
