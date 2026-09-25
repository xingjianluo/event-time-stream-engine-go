package engine

import (
	"errors"
	"time"
)

// Event 表示一条来自某个分区（来源）的输入事件。
type Event struct {
	ID        string
	Key       string
	Value     int64
	EventTime time.Time
	Partition int
	Offset    int64
}

// WindowResult 表示某个键在某个窗口上的聚合结果。
type WindowResult struct {
	Key         string
	WindowStart time.Time
	WindowEnd   time.Time
	Sum         int64
	Count       int64
}

// ResultLogEntry 是输出汇中一条幂等写入记录。
type ResultLogEntry struct {
	Result  WindowResult
	Version int64
}

var (
	// ErrLateEventDropped 事件超过允许迟到时间，被明确拒绝。
	ErrLateEventDropped = errors.New("late event dropped: beyond allowed lateness")
	// ErrDuplicateEvent 重复投递的事件被去重忽略。
	ErrDuplicateEvent = errors.New("duplicate event ignored")
	// ErrWatermarkRegression 恢复请求的水位线高于快照水位线，或发生回退。
	ErrWatermarkRegression = errors.New("watermark regression rejected")
	// ErrSnapshotConflict 恢复快照与当前状态冲突。
	ErrSnapshotConflict = errors.New("snapshot restore conflict")
	// ErrEngineClosed 引擎已关闭。
	ErrEngineClosed = errors.New("engine closed")
)
