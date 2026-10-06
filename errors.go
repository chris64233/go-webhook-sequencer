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
	// ErrReplayNotFound 表示来源尚无重放计划。
	ErrReplayNotFound = errors.New("replay plan not found")
	// ErrManualNotFound 表示人工处理条目不存在或已处理。
	ErrManualNotFound = errors.New("manual entry not found")
	// ErrManualOrder 表示试图跳过缺口位置处理后面的人工条目。
	ErrManualOrder = errors.New("manual entries must be resolved in seq order")
)

// ReplayConflictError 表示重放计划冲突：来源已存在固定范围与版本的计划，
// 新请求的范围不同。相同范围的重复请求不会报错，而是返回原计划。
type ReplayConflictError struct {
	SourceID        string
	Existing        ReplayPlan
	IncomingFromSeq int64
	IncomingToSeq   int64
}

func (e *ReplayConflictError) Error() string {
	return fmt.Sprintf("replay conflict on source %q: existing plan [%d,%d] v%d, got [%d,%d]",
		e.SourceID, e.Existing.FromSeq, e.Existing.ToSeq, e.Existing.Version,
		e.IncomingFromSeq, e.IncomingToSeq)
}

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
