package webhooksequencer

import (
	"fmt"
	"sync"
)

// keyMutex 按来源加锁：同一来源串行，不同来源并行。
type keyMutex struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func (k *keyMutex) lock(key string) func() {
	k.mu.Lock()
	if k.m == nil {
		k.m = make(map[string]*sync.Mutex)
	}
	l, ok := k.m[key]
	if !ok {
		l = &sync.Mutex{}
		k.m[key] = l
	}
	k.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// Service 提供来源初始化、事件接收、投递确认与积压查询。
type Service struct {
	store *Store
	locks keyMutex
}

// NewService 基于给定存储构造服务。
func NewService(store *Store) *Service {
	return &Service{store: store}
}

// InitSource 初始化来源，序号从 startSeq（须 >= 1）开始期望。
func (s *Service) InitSource(sourceID string, startSeq int64) error {
	if sourceID == "" || startSeq < 1 {
		return fmt.Errorf("%w: source id 不能为空且 startSeq 必须 >= 1", ErrInvalidArgument)
	}
	return s.store.InitSource(sourceID, startSeq)
}

// Receive 接收一条事件，保证同一来源严格有序释放：
//   - 标识、序号、负载摘要完全相同：幂等返回首次结果（Duplicate=true）；
//   - 标识相同但摘要不同：PayloadConflictError；
//   - 标识相同但序号不同，或序号已被其他事件占用：SeqConflictError；
//   - 序号大于期望值：持久化后缓冲，等待缺口补齐；
//   - 缺口补齐后按连续序号依次释放进 outbox。
func (s *Service) Receive(ev Event) (Receipt, error) {
	if ev.SourceID == "" || ev.EventID == "" || ev.Digest == "" || ev.Seq <= 0 {
		return Receipt{}, fmt.Errorf("%w: source_id/event_id/digest 不能为空且 seq 必须为正", ErrInvalidArgument)
	}
	unlock := s.locks.lock(ev.SourceID)
	defer unlock()

	src, err := s.store.Source(ev.SourceID)
	if err != nil {
		return Receipt{}, err
	}

	if rec, ok := src.ByID[ev.EventID]; ok {
		switch {
		case rec.Event.Seq == ev.Seq && rec.Event.Digest == ev.Digest:
			receipt := rec.Receipt
			receipt.Duplicate = true
			return receipt, nil
		case rec.Event.Digest != ev.Digest:
			return Receipt{}, &PayloadConflictError{
				SourceID:       ev.SourceID,
				EventID:        ev.EventID,
				ExistingDigest: rec.Event.Digest,
				IncomingDigest: ev.Digest,
			}
		default:
			return Receipt{}, &SeqConflictError{
				SourceID:        ev.SourceID,
				Seq:             ev.Seq,
				ExistingEventID: ev.EventID + "@" + fmt.Sprint(rec.Event.Seq),
				IncomingEventID: ev.EventID + "@" + fmt.Sprint(ev.Seq),
			}
		}
	}
	if existing, ok := src.BySeq[ev.Seq]; ok {
		return Receipt{}, &SeqConflictError{
			SourceID:        ev.SourceID,
			Seq:             ev.Seq,
			ExistingEventID: existing,
			IncomingEventID: ev.EventID,
		}
	}

	rec := &eventRecord{Event: ev}
	src.ByID[ev.EventID] = rec
	src.BySeq[ev.Seq] = ev.EventID
	src.Buffered[ev.Seq] = true

	// 从期望序号起连续推进，把缓冲中已就绪的事件按序释放进 outbox。
	var deliveries []Delivery
	var released []string
	now := s.store.now()
	for src.Buffered[src.NextSeq] {
		id := src.BySeq[src.NextSeq]
		deliveries = append(deliveries, Delivery{
			Key:       deliveryKey(ev.SourceID, id),
			SourceID:  ev.SourceID,
			Seq:       src.NextSeq,
			EventID:   id,
			Digest:    src.ByID[id].Event.Digest,
			Status:    DeliveryPending,
			CreatedAt: now,
			UpdatedAt: now,
		})
		released = append(released, id)
		delete(src.Buffered, src.NextSeq)
		src.NextSeq++
	}

	receipt := Receipt{Status: StatusBuffered, NextSeq: src.NextSeq}
	if len(released) > 0 {
		receipt.Status = StatusReleased
		receipt.Released = released
	}
	rec.Receipt = receipt

	if err := s.store.SaveSource(src, deliveries); err != nil {
		return Receipt{}, err
	}
	return receipt, nil
}

// AckDelivery 确认一条投递成功。attempt 必须是领取时拿到的尝试号：
// 只有当前领取尝试能够确认，租约被接管后旧发送者的确认返回
// ErrStaleAttempt。确认成功后该投递永远不会重新待投递。
func (s *Service) AckDelivery(key string, attempt int) error {
	return s.store.Ack(key, attempt)
}

// RetryDeadLetter 操作员选择重试来源 sourceID 中序号 seq 的死信。
// 投递回到待投递，由下一轮领取生成新的尝试号，但沿用原稳定投递键。
// decisionID 必须唯一；用同一决定号重放本操作幂等返回首次决定。
// 同一来源存在多个死信时，必须先处置序号最小的一条。
func (s *Service) RetryDeadLetter(sourceID string, seq int64, decisionID string) (Decision, error) {
	if sourceID == "" || decisionID == "" {
		return Decision{}, fmt.Errorf("%w: source id 与 decision id 不能为空", ErrInvalidArgument)
	}
	return s.store.RetryDeadLetter(sourceID, seq, decisionID)
}

// SkipDeadLetter 操作员明确跳过来源 sourceID 中序号 seq 的死信。
// 必须给出跳过原因 reason 和唯一决定号 decisionID。该序号被记为已处置
// （终态 skipped），之后连续序列才允许继续释放；跳过后任何迟到成功
// 确认都不能把它恢复为已投递。
func (s *Service) SkipDeadLetter(sourceID string, seq int64, decisionID, reason string) (Decision, error) {
	if sourceID == "" || decisionID == "" || reason == "" {
		return Decision{}, fmt.Errorf("%w: source id、decision id 与 reason 均不能为空", ErrInvalidArgument)
	}
	return s.store.SkipDeadLetter(sourceID, seq, decisionID, reason)
}

// DeadLetters 查询死信。sourceID 为空时查询全部来源；
// unresolvedOnly 为真时只返回等待处置的记录，按（来源，序号）升序。
func (s *Service) DeadLetters(sourceID string, unresolvedOnly bool) []DeadLetter {
	return s.store.DeadLetters(sourceID, unresolvedOnly)
}

// Attempts 查询领取尝试记录。sourceID 为空时查询全部来源。
func (s *Service) Attempts(sourceID string) []ClaimAttempt {
	return s.store.Attempts(sourceID)
}

// Decisions 查询全部处置决定（重试/跳过），按创建顺序升序。
func (s *Service) Decisions() []Decision {
	return s.store.Decisions()
}

// Backlog 查询来源积压：期望序号、缺口后缓冲的序号、outbox 各状态数量、
// 以及当前阻塞序号（最小的未处置死信序号）。
func (s *Service) Backlog(sourceID string) (Backlog, error) {
	return s.store.Backlog(sourceID)
}
