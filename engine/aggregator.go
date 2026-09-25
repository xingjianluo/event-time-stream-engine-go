package engine

// Aggregator 定义有状态聚合器接口（本实现使用求和+计数）。
type Aggregator interface {
	Add(value int64)
	Value() (sum int64, count int64)
}

// sumAggregator 求和计数器。
type sumAggregator struct {
	sum   int64
	count int64
}

func newSumAggregator() *sumAggregator {
	return &sumAggregator{}
}

func (a *sumAggregator) Add(value int64) {
	a.sum += value
	a.count++
}

func (a *sumAggregator) Value() (int64, int64) {
	return a.sum, a.count
}
