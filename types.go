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
	// DeliveryDead 表示达到最大投递次数，进入死信等待人工处置。
	DeliveryDead DeliveryStatus = "dead"
	// DeliverySkipped 表示被操作员跳过处置，与 acked 一样是终态。
	DeliverySkipped DeliveryStatus = "skipped"
)

// Delivery 是 outbox 中的一条待投递记录。
// Key 是稳定幂等键（来源 + 事件标识），崩溃重投与死信重放时保持不变。
// Attempts 是单调递增的领取栅栏号：只有当前尝试能够确认或上报失败，
// 租约被接管后旧尝试的确认会被拒绝。
type Delivery struct {
	Key      string         `json:"key"`
	SourceID string         `json:"source_id"`
	Seq      int64          `json:"seq"`
	EventID  string         `json:"event_id"`
	Digest   string         `json:"digest"`
	Status   DeliveryStatus `json:"status"`
	Attempts int            `json:"attempts"`
	// Failures 是本次处置周期内的连续失败次数，死信重试后清零。
	Failures int `json:"failures"`
	// LastFailure 是最近一次失败原因。
	LastFailure string    `json:"last_failure,omitempty"`
	Owner       string    `json:"owner,omitempty"`
	LeaseUntil  time.Time `json:"lease_until,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// DeadLetter 是事件达到最大投递次数后的死信记录。
// 只保存稳定投递键、进入死信时的来源序号和最后一次失败，
// 不复制也不改写原事件负载。重试后再次失败会产生新的轮次记录。
type DeadLetter struct {
	// Episode 是该投递进入死信的轮次（从 1 开始），与 Key 组合唯一。
	Episode   int       `json:"episode"`
	Key       string    `json:"key"`
	SourceID  string    `json:"source_id"`
	Seq       int64     `json:"seq"`
	LastError string    `json:"last_error"`
	Failures  int       `json:"failures"`
	DeadAt    time.Time `json:"dead_at"`
	// ResolvedAt 非零表示已处置；Resolution 记录处置方式。
	ResolvedAt time.Time `json:"resolved_at,omitempty"`
	Resolution string    `json:"resolution,omitempty"`
	DecisionID string    `json:"decision_id,omitempty"`
}

// 死信处置方式。
const (
	// ResolutionRetried 表示操作员选择重试，投递重新进入待投递。
	ResolutionRetried = "retried"
	// ResolutionSkipped 表示操作员明确跳过，该序号记为已处置。
	ResolutionSkipped = "skipped"
)

// DecisionKind 是处置决定的类型。
type DecisionKind string

const (
	DecisionRetry DecisionKind = "retry"
	DecisionSkip  DecisionKind = "skip"
)

// Decision 是操作员对一条死信的处置决定。ID 全局唯一并持久化，
// 同一决定号重放时幂等返回首次决定，不会产生重复副作用。
type Decision struct {
	ID        string       `json:"id"`
	Kind      DecisionKind `json:"kind"`
	SourceID  string       `json:"source_id"`
	Seq       int64        `json:"seq"`
	Key       string       `json:"key"`
	Reason    string       `json:"reason,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
}

// AttemptOutcome 是一次领取尝试的结果。
type AttemptOutcome string

const (
	AttemptLeased  AttemptOutcome = "leased"
	AttemptAcked   AttemptOutcome = "acked"
	AttemptFailed  AttemptOutcome = "failed"
	AttemptExpired AttemptOutcome = "expired"
)

// ClaimAttempt 记录一次领取尝试：谁、何时、第几号栅栏、结果如何。
// ID 为 投递键#栅栏号，与租约一一对应。
type ClaimAttempt struct {
	ID         string         `json:"id"`
	Key        string         `json:"key"`
	SourceID   string         `json:"source_id"`
	Seq        int64          `json:"seq"`
	Owner      string         `json:"owner"`
	Attempt    int            `json:"attempt"`
	LeasedAt   time.Time      `json:"leased_at"`
	LeaseUntil time.Time      `json:"lease_until"`
	Outcome    AttemptOutcome `json:"outcome"`
}

// Backlog 描述一个来源的积压情况。
type Backlog struct {
	SourceID string `json:"source_id"`
	// NextSeq 是来源期望的下一个序号；BufferedSeqs 中的最小值若大于它即存在缺口。
	NextSeq int64 `json:"next_seq"`
	// BufferedSeqs 是已持久化但因缺口未释放的序号（升序）。
	BufferedSeqs []int64 `json:"buffered_seqs,omitempty"`
	// Pending/Leased/Acked/Dead/Skipped 是该来源 outbox 中各状态的投递数。
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Acked   int `json:"acked"`
	Dead    int `json:"dead"`
	Skipped int `json:"skipped"`
	// BlockedSeq 是当前阻塞序号（最小的未处置死信序号），0 表示未阻塞。
	// 阻塞期间该序号及之后的投递不可领取，之后的序号不可确认。
	BlockedSeq int64 `json:"blocked_seq,omitempty"`
}

// deliveryKey 生成稳定幂等键：同一来源同一事件无论重投多少次都相同。
func deliveryKey(sourceID, eventID string) string {
	return sourceID + "/" + eventID
}
