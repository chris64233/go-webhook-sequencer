package webhooksequencer

import (
	"fmt"
	"sync"
	"time"
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

// AckDelivery 确认一条投递成功，之后该投递永远不会重新待投递。
func (s *Service) AckDelivery(key string) error {
	return s.store.Ack(key)
}

// Backlog 查询来源积压：期望序号、缺口后缓冲的序号、outbox 各状态数量。
func (s *Service) Backlog(sourceID string) (Backlog, error) {
	return s.store.Backlog(sourceID)
}

// PlanReplay 为来源的 [fromSeq, toSeq] 区间制定死信重放计划：
// 计划固定事件范围、起始序号、截止时间和当前确认位置。
//   - 已成功投递（已确认）的事件不会重新入队；
//   - 从确认位置起按原序号连续入队，遇到序号缺口必须停住；
//   - 相同范围的重复请求返回原计划；范围或版本改变返回 ReplayConflictError。
func (s *Service) PlanReplay(sourceID string, fromSeq, toSeq int64, window time.Duration) (ReplayPlan, error) {
	if sourceID == "" || fromSeq < 1 || toSeq < fromSeq || window <= 0 {
		return ReplayPlan{}, fmt.Errorf("%w: source_id 不能为空，1 <= fromSeq <= toSeq 且 window 必须为正", ErrInvalidArgument)
	}
	unlock := s.locks.lock(sourceID)
	defer unlock()
	return s.store.PlanReplay(sourceID, fromSeq, toSeq, window)
}

// ExpireReplay 结算窗口已过期的重放计划：未确认序号按原始顺序转入
// 人工处理，对应投递转为死信；事件保留原序号，不删除、不重新编号。
func (s *Service) ExpireReplay(sourceID string) (ReplayPlan, error) {
	if sourceID == "" {
		return ReplayPlan{}, fmt.Errorf("%w: source_id 不能为空", ErrInvalidArgument)
	}
	unlock := s.locks.lock(sourceID)
	defer unlock()
	return s.store.ExpirePlan(sourceID)
}

// ReplayStatus 查询来源的重放计划与人工处理队列（按原始序号升序）。
func (s *Service) ReplayStatus(sourceID string) (ReplayPlan, []ManualEntry, error) {
	return s.store.ReplayStatus(sourceID)
}

// ResolveManual 人工处理缺口位置的条目。只能从当前确认位置继续，
// 不能把后面的事件重排成另一条序列；已处理的序号不可重复消费。
func (s *Service) ResolveManual(sourceID string, seq int64) (ReplayPlan, error) {
	if sourceID == "" || seq <= 0 {
		return ReplayPlan{}, fmt.Errorf("%w: source_id 不能为空且 seq 必须为正", ErrInvalidArgument)
	}
	unlock := s.locks.lock(sourceID)
	defer unlock()
	return s.store.ResolveManual(sourceID, seq)
}
