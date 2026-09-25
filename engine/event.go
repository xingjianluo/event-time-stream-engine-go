package engine

// Event 表示一条进入引擎的流事件。
type Event struct {
	ID        string // 全局唯一事件 ID，用于幂等去重
	Key       string // 聚合键
	Value     int64  // 参与聚合的数值
	EventTime int64  // 事件时间（毫秒）
	Partition int    // 来源分区
	Seq       uint64 // 分区内单调递增序号，用于同键顺序语义
}
