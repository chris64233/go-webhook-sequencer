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
