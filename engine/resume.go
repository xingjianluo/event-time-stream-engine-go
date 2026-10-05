package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
)

// 位置校验拒绝原因。
const (
	ReasonPositionAhead   = "position-ahead"   // 保存的位置比当前数据更靠后
	ReasonPositionChanged = "position-changed" // 位置指向的数据已不存在或被替换
)

// PositionError 描述一次被明确拒绝的续传定位，Reason 可区分不匹配类型。
// 拒绝时数据源保持未打开状态，调用方必须显式处置（停止或人工介入），
// 引擎不会悄悄从头重来，也不会悄悄跳过。
type PositionError struct {
	Reason    string
	Detail    string
	Partition int
	Position  uint64
}

func (e *PositionError) Error() string {
	return fmt.Sprintf("resume rejected [%s] partition=%d position=%d: %s",
		e.Reason, e.Partition, e.Position, e.Detail)
}

// Record 是可续传数据源中的一条记录：投递事件或推进水位线。
type Record struct {
	Offset    uint64
	Event     *Event
	Watermark *int64
}

// recordWire 是指纹计算用的稳定 JSON 形状（不含偏移，偏移是位置不是内容）。
type recordWire struct {
	Kind      string `json:"kind"`
	ID        string `json:"id,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     int64  `json:"value,omitempty"`
	EventTime int64  `json:"eventTime,omitempty"`
	Watermark int64  `json:"watermark,omitempty"`
}

// RecordedPartition 是带偏移光标的分区记录序列，可从上次提交位置继续投递。
type RecordedPartition struct {
	Partition int
	records   []Record
	cursor    uint64
}

// NewRecordedPartition 用步骤序列构造可续传分区，偏移从 0 起按序分配。
func NewRecordedPartition(partition int, steps []Step) *RecordedPartition {
	recs := make([]Record, len(steps))
	for i, st := range steps {
		recs[i] = Record{Offset: uint64(i), Event: st.Event, Watermark: st.Watermark}
	}
	return &RecordedPartition{Partition: partition, records: recs}
}

// Len 返回分区记录总数。
func (p *RecordedPartition) Len() uint64 { return uint64(len(p.records)) }

// Open 校验并定位到 start（下一条待投递记录的偏移）：
//   - start 超过当前记录总数：position-ahead（保存的位置比数据更靠后）；
//   - start>0 但 start-1 处记录指纹与 anchor 不符：position-changed
//     （位置指向的数据已被替换/截断重写）；
//   - start==0 时 anchor 必须为空。
//
// 定位成功后 Next 从 start 开始，已投递前缀不会被再灌一遍。
func (p *RecordedPartition) Open(start uint64, anchor string) error {
	if start > uint64(len(p.records)) {
		return &PositionError{
			Reason:    ReasonPositionAhead,
			Detail:    fmt.Sprintf("saved offset %d is beyond %d available records", start, len(p.records)),
			Partition: p.Partition,
			Position:  start,
		}
	}
	if start == 0 {
		if anchor != "" {
			return &PositionError{
				Reason:    ReasonPositionChanged,
				Detail:    "offset is 0 but anchor fingerprint is non-empty",
				Partition: p.Partition,
				Position:  start,
			}
		}
		p.cursor = 0
		return nil
	}
	want := recordFingerprint(p.records[start-1])
	if anchor != want {
		return &PositionError{
			Reason: ReasonPositionChanged,
			Detail: fmt.Sprintf("record at offset %d changed: anchor mismatch (saved=%s actual=%s)",
				start-1, truncateAnchor(anchor), truncateAnchor(want)),
			Partition: p.Partition,
			Position:  start,
		}
	}
	p.cursor = start
	return nil
}

// Next 返回光标处记录并前进；已到末尾返回 false。
func (p *RecordedPartition) Next() (Record, bool) {
	if int(p.cursor) >= len(p.records) {
		return Record{}, false
	}
	r := p.records[p.cursor]
	p.cursor++
	return r, true
}

// Position 返回当前光标（下一条待投递偏移）。
func (p *RecordedPartition) Position() uint64 { return p.cursor }

// Anchor 返回已投递最后一条记录的指纹；尚未投递任何记录时为空串。
func (p *RecordedPartition) Anchor() string {
	if p.cursor == 0 {
		return ""
	}
	return recordFingerprint(p.records[p.cursor-1])
}

// recordFingerprint 计算记录内容指纹，用于识别记录是否被替换。
func recordFingerprint(r Record) string {
	w := recordWire{}
	switch {
	case r.Event != nil:
		w.Kind = "event"
		w.ID = r.Event.ID
		w.Key = r.Event.Key
		w.Value = r.Event.Value
		w.EventTime = r.Event.EventTime
	case r.Watermark != nil:
		w.Kind = "watermark"
		w.Watermark = *r.Watermark
	default:
		w.Kind = "empty"
	}
	data, _ := json.Marshal(w)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func truncateAnchor(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12] + "..."
}

// OpenPositions 用检查点中的位置一次性打开多个分区：
//   - 检查点里有位置、但当前数据源缺失的分区，按 position-changed 拒绝
//     （分区指向的数据整体不在了）；
//   - 各分区自身的越界/指纹不匹配由 RecordedPartition.Open 区分。
func OpenPositions(cp Checkpoint, parts ...*RecordedPartition) error {
	byPartition := make(map[int]*RecordedPartition, len(parts))
	for _, p := range parts {
		byPartition[p.Partition] = p
	}
	// 确定性顺序：先检查缺失分区，再按分区号打开。
	missing := make([]int, 0)
	for part := range cp.Positions {
		if _, ok := byPartition[part]; !ok {
			missing = append(missing, part)
		}
	}
	for i := 0; i < len(missing); i++ {
		for j := i + 1; j < len(missing); j++ {
			if missing[j] < missing[i] {
				missing[i], missing[j] = missing[j], missing[i]
			}
		}
	}
	for _, part := range missing {
		return &PositionError{
			Reason:    ReasonPositionChanged,
			Detail:    fmt.Sprintf("saved partition %d no longer present in source data", part),
			Partition: part,
			Position:  cp.Positions[part],
		}
	}
	ordered := make([]int, 0, len(parts))
	for _, p := range parts {
		ordered = append(ordered, p.Partition)
	}
	for i := 0; i < len(ordered); i++ {
		for j := i + 1; j < len(ordered); j++ {
			if ordered[j] < ordered[i] {
				ordered[i], ordered[j] = ordered[j], ordered[i]
			}
		}
	}
	for _, part := range ordered {
		p := byPartition[part]
		if err := p.Open(cp.Positions[part], cp.Anchors[part]); err != nil {
			return err
		}
	}
	return nil
}

// InjectRecorded 并发地从各分区当前光标投递到末尾，语义同 Inject：
// 源内严格按序、源间并发；跨过分界点的记录恰好被投递一次。
func InjectRecorded(e *Engine, parts ...*RecordedPartition) []Event {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var rejected []Event

	for _, p := range parts {
		wg.Add(1)
		go func(p *RecordedPartition) {
			defer wg.Done()
			for {
				r, ok := p.Next()
				if !ok {
					return
				}
				switch {
				case r.Event != nil:
					ev := *r.Event
					ev.Partition = p.Partition
					if err := e.Ingest(ev); err != nil {
						if _, ok := err.(*LateEventError); ok {
							mu.Lock()
							rejected = append(rejected, ev)
							mu.Unlock()
						}
					}
				case r.Watermark != nil:
					e.AdvanceWatermark(p.Partition, *r.Watermark)
				}
			}
		}(p)
	}
	wg.Wait()
	return rejected
}
