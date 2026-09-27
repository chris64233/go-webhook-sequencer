package webhooksequencer

import (
	"errors"
	"fmt"
)

// 可通过 errors.Is 判定的错误哨兵。
var (
	// ErrNotFound 表示来源或投递记录不存在。
	ErrNotFound = errors.New("webhooksequencer: not found")
	// ErrAlreadyExists 表示创建的资源已存在（且关键属性冲突）。
	ErrAlreadyExists = errors.New("webhooksequencer: already exists")
	// ErrInvalidArgument 表示请求参数不合法（缺字段、序号越界等）。
	ErrInvalidArgument = errors.New("webhooksequencer: invalid argument")
	// ErrSequenceConflict 表示同一个序号对应了不同事件
	// （事件标识不同，或标识相同但负载摘要不同）。
	ErrSequenceConflict = errors.New("webhooksequencer: sequence conflict")
	// ErrPayloadConflict 表示相同事件标识对应了不同内容
	// （负载摘要不同，或绑定到了另一个序号）。
	ErrPayloadConflict = errors.New("webhooksequencer: payload conflict")
	// ErrLeaseLost 表示确认/释放租约时携带的 attempt 已过期
	// （租约已被其他发送者重新领取），本次操作被 fencing 拒绝。
	ErrLeaseLost = errors.New("webhooksequencer: lease lost")
)

// ConflictKind 标识冲突的具体类别。
type ConflictKind int

const (
	conflictUnknown ConflictKind = iota
	// ConflictSequence 为序号冲突：同一序号绑定了不同事件。
	ConflictSequence
	// ConflictPayload 为负载冲突：同一事件标识绑定了不同内容。
	ConflictPayload
)

// ConflictError 携带冲突详情，同时可被 errors.Is 匹配到对应哨兵错误。
type ConflictError struct {
	Kind     ConflictKind
	SourceID string
	Seq      int64
	EventID  string
	// Existing 为库中已存在事件的关键属性。
	Existing conflictSide
	// Incoming 为本次提交事件的关键属性。
	Incoming conflictSide
}

type conflictSide struct {
	Seq    int64
	ID     string
	Digest string
}

func (e *ConflictError) Error() string {
	kind := "payload"
	sentinel := ErrPayloadConflict
	if e.Kind == ConflictSequence {
		kind = "sequence"
		sentinel = ErrSequenceConflict
	}
	return fmt.Sprintf("%s: source=%q event=%q seq=%d conflicts with existing event=%q seq=%d (%s)",
		sentinel, e.SourceID, e.EventID, e.Seq, e.Existing.ID, e.Existing.Seq, kind)
}

// Is 让 ConflictError 同时支持 errors.Is(err, ErrSequenceConflict) /
// errors.Is(err, ErrPayloadConflict)。
func (e *ConflictError) Is(target error) bool {
	switch target {
	case ErrSequenceConflict:
		return e.Kind == ConflictSequence
	case ErrPayloadConflict:
		return e.Kind == ConflictPayload
	default:
		return false
	}
}

// InvalidArgumentError 携带字段级参数错误详情，匹配 ErrInvalidArgument。
type InvalidArgumentError struct {
	Field  string
	Reason string
}

func (e *InvalidArgumentError) Error() string {
	return fmt.Sprintf("%s: field %q: %s", ErrInvalidArgument, e.Field, e.Reason)
}

func (e *InvalidArgumentError) Is(target error) bool {
	return target == ErrInvalidArgument
}

func invalidArg(field, reason string) error {
	return &InvalidArgumentError{Field: field, Reason: reason}
}
