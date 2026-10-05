package engine

import "fmt"

// SeekError 描述一次被拒绝的续传请求，Reason 可区分。
type SeekError struct {
	Reason   string
	Detail   string
	Position Position
}

func (e *SeekError) Error() string {
	return "seek rejected [" + e.Reason + "]: " + e.Detail
}

// 续传拒绝原因，可用 errors.As 拿到 *SeekError 后比较 Reason。
const (
	ReasonOffsetBeyondEnd = "offset-beyond-end" // 位置比源中现有数据还靠后
	ReasonOffsetTrimmed   = "offset-trimmed"    // 位置指向的数据已不在（被截断）
	ReasonOffsetMismatch  = "offset-mismatch"   // 位置指向的数据被替换（ID 对不上）
)

// ResumeFrom 从上次提交的消费位置继续：返回只包含该位置之后步骤的新源。
// 位置与源中数据对不上时返回 *SeekError，且不同情况 Reason 不同，
// 绝不悄悄从头重来，也绝不悄悄跳过。
//
// 校验规则（pos 为零值 Position{} 时表示从头开始，不校验）：
//   - 源中存在 Seq == pos.Seq 的事件：其 ID 必须等于 pos.LastID，
//     否则说明位置指向的数据已被替换（ReasonOffsetMismatch）；
//   - 源中所有事件 Seq 都小于 pos.Seq：位置比现有数据靠后（ReasonOffsetBeyondEnd）；
//   - 源中只有 Seq 更大的事件、找不到 pos.Seq 本身：位置指向的数据
//     已被截断（ReasonOffsetTrimmed）。
func (s *Source) ResumeFrom(pos Position) (*Source, error) {
	if pos.Seq == 0 && pos.LastID == "" {
		return &Source{Partition: s.Partition, Steps: append([]Step(nil), s.Steps...)}, nil
	}

	cut := -1 // 位置事件在 Steps 中的下标
	maxSeq := uint64(0)
	hasAny := false
	for i, st := range s.Steps {
		if st.Event == nil {
			continue
		}
		hasAny = true
		if st.Event.Seq > maxSeq {
			maxSeq = st.Event.Seq
		}
		if st.Event.Seq == pos.Seq {
			if st.Event.ID != pos.LastID {
				return nil, &SeekError{
					Reason:   ReasonOffsetMismatch,
					Detail:   fmt.Sprintf("partition %d: event at seq %d has id %q, position expects %q", s.Partition, pos.Seq, st.Event.ID, pos.LastID),
					Position: pos,
				}
			}
			cut = i
		}
	}

	if cut < 0 {
		if !hasAny || maxSeq < pos.Seq {
			return nil, &SeekError{
				Reason:   ReasonOffsetBeyondEnd,
				Detail:   fmt.Sprintf("partition %d: position seq %d beyond source max seq %d", s.Partition, pos.Seq, maxSeq),
				Position: pos,
			}
		}
		return nil, &SeekError{
			Reason:   ReasonOffsetTrimmed,
			Detail:   fmt.Sprintf("partition %d: position seq %d no longer present (max seq %d)", s.Partition, pos.Seq, maxSeq),
			Position: pos,
		}
	}

	// 位置之后的事件与水位线步骤原样保留，源内顺序不变。
	return &Source{Partition: s.Partition, Steps: append([]Step(nil), s.Steps[cut+1:]...)}, nil
}
