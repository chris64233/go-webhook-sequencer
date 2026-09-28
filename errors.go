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
	// ErrStaleAttempt 表示确认/上报来自过期的领取尝试（租约已被接管或已处置）。
	ErrStaleAttempt = errors.New("stale claim attempt")
	// ErrSourceBlocked 表示来源被未处置的死信阻塞，后续序号暂时不能确认。
	ErrSourceBlocked = errors.New("source blocked by dead letter")
	// ErrDeliveryDead 表示投递处于死信状态，须先由操作员处置。
	ErrDeliveryDead = errors.New("delivery is dead-lettered")
	// ErrDeliveryDisposed 表示投递已被跳过处置（终态），迟到确认不能恢复它。
	ErrDeliveryDisposed = errors.New("delivery disposed by skip decision")
	// ErrNotDeadLetter 表示重试/跳过的目标不是未处置的死信。
	ErrNotDeadLetter = errors.New("not an unresolved dead letter")
	// ErrDeadLetterOrder 表示未按序号顺序处置：存在更靠前的未处置死信。
	ErrDeadLetterOrder = errors.New("dead letters must be resolved in seq order")
	// ErrDecisionConflict 表示决定号已被内容不同的决定占用。
	ErrDecisionConflict = errors.New("decision id conflict")
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
