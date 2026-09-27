package webhooksequencer

import "time"

// Event 是来源系统上报的 Webhook 事件。
//
// Seq 为来源内单调递增序号；ID 为事件唯一标识；Digest 为负载摘要
// （例如 SHA-256），服务本身不重新计算摘要，只依据三方字段做幂等与冲突判定。
// Payload 为可选原始负载，会在投递时原样转发给下游。
type Event struct {
	SourceID string `json:"source_id"`
	Seq      int64  `json:"seq"`
	ID       string `json:"id"`
	Digest   string `json:"digest"`
	Payload  []byte `json:"payload,omitempty"`
}

// ReceiptStatus 描述事件被接收后的即时状态。
type ReceiptStatus string

const (
	// StatusBuffered 表示事件已持久化，但因存在序号缺口暂不能投递。
	StatusBuffered ReceiptStatus = "buffered"
	// StatusQueued 表示事件（连同可能被它补齐的连续序号）已进入投递 outbox。
	StatusQueued ReceiptStatus = "queued"
)

// Receipt 是事件接收结果。重复请求会返回首次接收时的原始结果，Duplicate 为 true。
type Receipt struct {
	SourceID string `json:"source_id"`
	Seq      int64  `json:"seq"`
	EventID  string `json:"event_id"`

	// Duplicate 为 true 表示这是一次完全相同的重复提交（标识、序号、负载摘要均一致）。
	Duplicate bool `json:"duplicate"`
	// Status 为首次接收时的状态，重复提交原样返回。
	Status ReceiptStatus `json:"status"`
	// IdempotencyKey 是该事件后续每次下游投递都会携带的稳定幂等键；
	// 事件因缺口被缓存时为空。
	IdempotencyKey string `json:"idempotency_key,omitempty"`

	// ReleasedCount 是本次接收在缺口补齐后一次性连续释放到 outbox 的事件数。
	ReleasedCount int `json:"released_count"`
	// Watermark 是接收完成后来源的释放水位（下一个期望释放的序号）。
	Watermark int64 `json:"watermark"`
}

// Delivery 是一次下游投递尝试。同一个 Key 在重试、崩溃恢复等场景下保持不变。
type Delivery struct {
	// Key 为稳定幂等键，格式为 "<sourceID>:<seq>"。
	Key string `json:"key"`
	// Attempt 为第几次尝试，从 1 开始，同时作为租约 fencing token。
	Attempt int   `json:"attempt"`
	Event   Event `json:"event"`
}

// SourceInfo 描述一个已初始化的来源。
type SourceInfo struct {
	ID string `json:"id"`
	// StartSeq 为该来源的起始序号（首个期望事件的序号）。
	StartSeq int64 `json:"start_seq"`
	// NextSeq 为当前释放水位：下一个等待进入 outbox 的序号。
	NextSeq   int64     `json:"next_seq"`
	CreatedAt time.Time `json:"created_at"`
}

// Backlog 是来源积压查询结果。
type Backlog struct {
	SourceID string `json:"source_id"`
	// NextSeq 为释放水位，序号小于 NextSeq 的事件都已进入 outbox。
	NextSeq int64 `json:"next_seq"`
	// LastDeliveredSeq 为已确认投递的最大连续序号；来源尚无投递时为 StartSeq-1。
	LastDeliveredSeq int64 `json:"last_delivered_seq"`
	// Buffered 为已持久化但因序号缺口尚未释放的事件数。
	Buffered int64 `json:"buffered"`
	// PendingDelivery 为已进入 outbox 但尚未确认投递成功的事件数
	// （含待发送、租约占用中、等待重试）。
	PendingDelivery int64 `json:"pending_delivery"`
}
