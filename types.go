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
	// DeliveryDead 表示达到最大投递次数，已进入死信，等待人工处置。
	DeliveryDead DeliveryStatus = "dead"
	// DeliverySkipped 表示操作员已决定跳过该事件，是终态：
	// 任何迟到的确认都不能把它恢复为已投递。
	DeliverySkipped DeliveryStatus = "skipped"
)

// Delivery 是 outbox 中的一条待投递记录。
// Key 是稳定幂等键（来源 + 事件标识），崩溃重投与死信重放时保持不变。
type Delivery struct {
	Key      string         `json:"key"`
	SourceID string         `json:"source_id"`
	Seq      int64          `json:"seq"`
	EventID  string         `json:"event_id"`
	Digest   string         `json:"digest"`
	Status   DeliveryStatus `json:"status"`
	// Attempts 是当前投递轮次内被领取的次数；死信重试后归零开始新一轮，
	// 完整历史见领取尝试日志（Attempt）。
	Attempts int    `json:"attempts"`
	Owner    string `json:"owner,omitempty"`
	// LeaseID 是当前租约的编号，单调递增；只有持有当前 LeaseID 的
	// 发送者才能确认成功，租约被接管后旧发送者的确认会被拒绝。
	LeaseID    int64     `json:"lease_id,omitempty"`
	LeaseUntil time.Time `json:"lease_until,omitempty"`
	// LastError/LastFailedAt 记录最后一次发送失败，随死信记录保存。
	LastError    string    `json:"last_error,omitempty"`
	LastFailedAt time.Time `json:"last_failed_at,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// DeadLetterStatus 是死信记录的状态。
type DeadLetterStatus string

const (
	// DeadLetterOpen 表示死信待处置（重试或跳过）。
	DeadLetterOpen DeadLetterStatus = "open"
	// DeadLetterRetried 表示操作员已选择重试，事件已重新入队待投递。
	DeadLetterRetried DeadLetterStatus = "retried"
	// DeadLetterSkipped 表示操作员已决定跳过，该序号已处置。
	DeadLetterSkipped DeadLetterStatus = "skipped"
)

// DeadLetter 是达到最大投递次数后进入死信的记录。
// 只保存稳定投递键、来源序号、负载摘要与最后一次失败等引用信息，
// 不复制也不改写原事件负载。
type DeadLetter struct {
	Key          string           `json:"key"`
	SourceID     string           `json:"source_id"`
	Seq          int64            `json:"seq"`
	EventID      string           `json:"event_id"`
	Digest       string           `json:"digest"`
	Attempts     int              `json:"attempts"`
	LastError    string           `json:"last_error,omitempty"`
	LastFailedAt time.Time        `json:"last_failed_at,omitempty"`
	DeadAt       time.Time        `json:"dead_at"`
	Status       DeadLetterStatus `json:"status"`
}

// DecisionType 是处置决定的类型。
type DecisionType string

const (
	// DecisionSkip 表示跳过该序号的事件，允许连续序列继续推进。
	DecisionSkip DecisionType = "skip"
)

// Decision 是操作员对死信的处置决定。ID 全局唯一；
// 用同一 ID 重复提交相同决定是幂等 no-op。
type Decision struct {
	ID        string       `json:"id"`
	Type      DecisionType `json:"type"`
	Key       string       `json:"key"`
	SourceID  string       `json:"source_id"`
	Seq       int64        `json:"seq"`
	Reason    string       `json:"reason"`
	DecidedAt time.Time    `json:"decided_at"`
}

// AttemptStatus 是一次领取尝试的结果。
type AttemptStatus string

const (
	// AttemptActive 表示租约有效，等待确认或失败上报。
	AttemptActive AttemptStatus = "active"
	// AttemptAcked 表示本次尝试已确认成功。
	AttemptAcked AttemptStatus = "acked"
	// AttemptFailed 表示本次尝试发送失败（租约保留到期）。
	AttemptFailed AttemptStatus = "failed"
	// AttemptExpired 表示租约到期未确认，投递可被重新领取。
	AttemptExpired AttemptStatus = "expired"
)

// Attempt 记录一次领取（租约）尝试，用于审计与排查。
// ID 即租约编号，单调递增。
type Attempt struct {
	ID         int64         `json:"id"`
	Key        string        `json:"key"`
	SourceID   string        `json:"source_id"`
	Seq        int64         `json:"seq"`
	Owner      string        `json:"owner"`
	LeasedAt   time.Time     `json:"leased_at"`
	LeaseUntil time.Time     `json:"lease_until"`
	Status     AttemptStatus `json:"status"`
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
	// DeadLetters 是该来源待处置的死信数量。
	DeadLetters int `json:"dead_letters"`
	// BlockedSeq 是当前阻塞来源确认推进的最小死信号；0 表示未阻塞。
	BlockedSeq int64 `json:"blocked_seq,omitempty"`
}

// deliveryKey 生成稳定幂等键：同一来源同一事件无论重投多少次都相同。
func deliveryKey(sourceID, eventID string) string {
	return sourceID + "/" + eventID
}
