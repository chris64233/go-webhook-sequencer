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

// AckDelivery 确认一次领取尝试投递成功，之后该投递永远不会重新待投递。
// 只有持有当前租约（leaseID 来自 Claim 返回的 Delivery.LeaseID）的发送者
// 才能确认；租约被接管后的迟到确认返回 ErrStaleLease，
// 来源被死信阻塞时返回 ErrSourceBlocked。
func (s *Service) AckDelivery(key string, leaseID int64) error {
	return s.store.Ack(key, leaseID)
}

// RetryDeadLetter 重试死信：沿用原投递键重新入队，后续领取会生成新的
// 领取尝试。同一来源存在多个死信时必须按序号从小到大处置，
// 否则返回 ErrDeadLetterOrder。
func (s *Service) RetryDeadLetter(key string) (DeadLetter, error) {
	if key == "" {
		return DeadLetter{}, fmt.Errorf("%w: key 不能为空", ErrInvalidArgument)
	}
	return s.store.RetryDeadLetter(key)
}

// SkipDeadLetter 跳过死信：需要非空原因与全局唯一的决定号。
// 跳过生效后该序号记为已处置，连续序列继续推进；任何迟到的确认
// 都不能把它恢复为已投递。同一决定号重复提交相同决定是幂等 no-op。
func (s *Service) SkipDeadLetter(key, decisionID, reason string) (Decision, error) {
	if key == "" || decisionID == "" || reason == "" {
		return Decision{}, fmt.Errorf("%w: key/decisionID/reason 均不能为空", ErrInvalidArgument)
	}
	return s.store.SkipDeadLetter(key, decisionID, reason)
}

// DeadLetters 查询死信记录（按来源、序号升序）；sourceID 为空时返回全部来源。
func (s *Service) DeadLetters(sourceID string) []DeadLetter {
	return s.store.DeadLetters(sourceID)
}

// DeliveryAttempts 查询一条投递的领取尝试历史（按租约编号升序）。
func (s *Service) DeliveryAttempts(key string) []Attempt {
	return s.store.DeliveryAttempts(key)
}

// Decisions 查询处置决定（按来源、序号升序）；sourceID 为空时返回全部来源。
func (s *Service) Decisions(sourceID string) []Decision {
	return s.store.Decisions(sourceID)
}

// BlockingSeq 查询当前阻塞来源确认推进的最小死信号；0 表示未阻塞。
func (s *Service) BlockingSeq(sourceID string) int64 {
	return s.store.BlockingSeq(sourceID)
}

// Backlog 查询来源积压：期望序号、缺口后缓冲的序号、outbox 各状态数量、
// 待处置死信数量与当前阻塞序号。
func (s *Service) Backlog(sourceID string) (Backlog, error) {
	return s.store.Backlog(sourceID)
}
