package webhooksequencer

import (
	"strconv"
	"time"
)

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
	// DeliveryDead 表示死信：超过最大尝试次数或窗口过期转入人工，
	// 不再被领取；只能由重放计划重新入队。确认是终态，死信上的
	// 迟到确认不再改变状态。
	DeliveryDead DeliveryStatus = "dead"
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
	// Pending/Leased/Acked/Dead 是该来源 outbox 中各状态的投递数。
	Pending int `json:"pending"`
	Leased  int `json:"leased"`
	Acked   int `json:"acked"`
	Dead    int `json:"dead"`
}

// deliveryKey 生成稳定幂等键：同一来源同一事件无论重投多少次都相同。
func deliveryKey(sourceID, eventID string) string {
	return sourceID + "/" + eventID
}

// manualKey 生成人工处理条目的键：来源 + 原始序号。
func manualKey(sourceID string, seq int64) string {
	return sourceID + "/" + strconv.FormatInt(seq, 10)
}

// ReplayStatus 是重放计划的状态。
type ReplayStatus string

const (
	// ReplayActive 表示窗口未过期，重放正在推进。
	ReplayActive ReplayStatus = "active"
	// ReplayExpired 表示窗口已过期，未确认序号已转入人工处理。
	ReplayExpired ReplayStatus = "expired"
	// ReplayCompleted 表示范围内所有序号均已确认或人工处理完毕。
	ReplayCompleted ReplayStatus = "completed"
)

// ReplayPlan 是一次死信重放的计划。计划固定事件范围 [FromSeq, ToSeq]、
// 截止时间和当前确认位置；创建后范围与版本不可变更：
// 相同范围的重复请求返回原计划，范围不同返回 ReplayConflictError。
type ReplayPlan struct {
	SourceID string `json:"source_id"`
	FromSeq  int64  `json:"from_seq"`
	ToSeq    int64  `json:"to_seq"`
	// Deadline 是重放窗口的截止时间；过期后未确认序号转入人工处理。
	Deadline time.Time `json:"deadline"`
	// Position 是当前确认位置：下一个等待确认或人工处理的序号，
	// 只会单调前进，迟到的旧重放不能使其回退。
	Position  int64        `json:"position"`
	Version   int64        `json:"version"`
	Status    ReplayStatus `json:"status"`
	CreatedAt time.Time    `json:"created_at"`
}

// ManualStatus 是人工处理条目的状态。
type ManualStatus string

const (
	ManualPending  ManualStatus = "pending"
	ManualResolved ManualStatus = "resolved"
)

// ManualEntry 是窗口过期后转入人工处理的一条记录。
// 保留事件的原始序号与标识，不删除事件、不重新编号；
// EventID 为空表示该序号的事件从未到达（缺口）。
type ManualEntry struct {
	SourceID   string       `json:"source_id"`
	Seq        int64        `json:"seq"`
	EventID    string       `json:"event_id,omitempty"`
	Digest     string       `json:"digest,omitempty"`
	Status     ManualStatus `json:"status"`
	CreatedAt  time.Time    `json:"created_at"`
	ResolvedAt time.Time    `json:"resolved_at,omitempty"`
}
