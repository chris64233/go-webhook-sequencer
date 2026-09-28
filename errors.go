package webhooksequencer

import (
	"errors"
	"fmt"
)

// 哨兵错误，调用方可用 errors.Is 判断。
var (
	// ErrInvalidArgument 表示请求参数不合法（空标识、非正序号等）。
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrSourceExists 表示来源已初始化。
	ErrSourceExists = errors.New("source already exists")
	// ErrSourceNotFound 表示来源尚未初始化。
	ErrSourceNotFound = errors.New("source not found")
	// ErrDeliveryNotFound 表示待确认的投递不存在。
	ErrDeliveryNotFound = errors.New("delivery not found")
	// ErrStaleLease 表示确认者持有的租约已被接管或已失效：
	// 旧发送者不得确认当前任务，迟到的确认也不能改变终态。
	ErrStaleLease = errors.New("stale lease")
	// ErrSourceBlocked 表示来源被更早序号的死信阻塞，后续序号不得绕过它确认。
	ErrSourceBlocked = errors.New("source blocked by dead letter")
	// ErrDeadLetterNotFound 表示死信记录不存在。
	ErrDeadLetterNotFound = errors.New("dead letter not found")
	// ErrDeadLetterNotOpen 表示死信已被处置（重试或跳过），不能重复处置。
	ErrDeadLetterNotOpen = errors.New("dead letter already disposed")
	// ErrDeadLetterOrder 表示同一来源的死信必须按序号从小到大处置。
	ErrDeadLetterOrder = errors.New("dead letters must be disposed in seq order")
	// ErrDecisionConflict 表示决定号已被用于其他死信。
	ErrDecisionConflict = errors.New("decision id already used")
)

// SeqConflictError 表示序号冲突：同一序号对应了不同的事件，
// 或同一事件标识被提交到了不同的序号位置。
type SeqConflictError struct {
	SourceID        string
	Seq             int64
	ExistingEventID string
	IncomingEventID string
}

func (e *SeqConflictError) Error() string {
	return fmt.Sprintf("seq conflict on source %q: seq %d already bound to event %q, got %q",
		e.SourceID, e.Seq, e.ExistingEventID, e.IncomingEventID)
}

// PayloadConflictError 表示负载冲突：事件标识相同但负载摘要不同。
type PayloadConflictError struct {
	SourceID       string
	EventID        string
	ExistingDigest string
	IncomingDigest string
}

func (e *PayloadConflictError) Error() string {
	return fmt.Sprintf("payload conflict on source %q event %q: digest %q != %q",
		e.SourceID, e.EventID, e.ExistingDigest, e.IncomingDigest)
}
