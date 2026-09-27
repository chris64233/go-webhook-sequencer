package webhooksequencer

import "time"

// Event 是来源推送的一条事件。同一来源内 Seq 单调递增，
// EventID 全局唯一，Digest 是负载摘要。
type Event struct {
	SourceID string `json:"source_id"`
	Seq      int64  `json:"seq"`
	EventID  string `json:"event_id"`
	Digest   string `json:"digest"`
}

// ReceiptStatus 表示一次接收的结果类型。
type ReceiptStatus string

const (
	// StatusBuffered 表示事件已持久化，但因前方存在序号缺口暂未释放。
	StatusBuffered ReceiptStatus = "buffered"
	// StatusReleased 表示事件（及其后已缓冲的连续事件）已释放进 outbox。
	StatusReleased ReceiptStatus = "released"
)

// Receipt 是 Receive 的返回结果。重复提交同一事件时返回首次的结果，
// 并将 Duplicate 置为 true。
type Receipt struct {
	Status ReceiptStatus `json:"status"`
	// Released 是本次调用触发释放进 outbox 的事件标识（按序号升序）。
	Released []string `json:"released,omitempty"`
	// NextSeq 是本次调用后来源期望的下一个序号。
	NextSeq   int64 `json:"next_seq"`
	Duplicate bool  `json:"duplicate"`
}

// DeliveryStatus 是 outbox 投递项的状态。
type DeliveryStatus string

const (
	DeliveryPending DeliveryStatus = "pending"
	DeliveryLeased  DeliveryStatus = "leased"
	DeliveryAcked   DeliveryStatus = "acked"
)

// Delivery 是 outbox 中的一条待投递记录。
// Key 是稳定幂等键（来源 + 事件标识），崩溃重投时保持不变。
type Delivery struct {
	Key        string         `json:"key"`
	SourceID   string         `json:"source_id"`
	Seq        int64          `json:"seq"`
	EventID    string         `json:"event_id"`
	Digest     string         `json:"digest"`
	Status     DeliveryStatus `json:"status"`
	Attempts   int            `json:"attempts"`
	Owner      string         `json:"owner,omitempty"`
	LeaseUntil time.Time      `json:"lease_until,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// Backlog 描述一个来源的积压情况。
type Backlog struct {
	SourceID string `json:"source_id"`
	// NextSeq 是来源期望的下一个序号；BufferedSeqs 中的最小值若大于它即存在缺口。
	NextSeq int64 `json:"next_seq"`
	// BufferedSeqs 是已持久化但因缺口未释放的序号（升序）。
	BufferedSeqs []int64 `json:"buffered_seqs,omitempty"`
	// Pending/Leased/Acked 是该来源 outbox 中各状态的投递数。
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Acked   int `json:"acked"`
}

// deliveryKey 生成稳定幂等键：同一来源同一事件无论重投多少次都相同。
func deliveryKey(sourceID, eventID string) string {
	return sourceID + "/" + eventID
}
