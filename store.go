package webhooksequencer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// outboxStatus 是 outbox 记录的生命周期状态。
type outboxStatus string

const (
	// statusPending：已进入 outbox，等待被领取发送。
	statusPending outboxStatus = "pending"
	// statusLeased：已被某个发送协程租约占用；租约过期前他人不可领取。
	statusLeased outboxStatus = "leased"
	// statusDelivered：下游已确认成功，终态，永不再投递。
	statusDelivered outboxStatus = "delivered"
)

// storedEvent 是已持久化的接收事件。
type storedEvent struct {
	ev      Event
	queued  bool     // 是否已释放进入 outbox
	key     string   // 释放后分配的稳定幂等键
	receipt *Receipt // 首次接收的原始结果，重复提交时原样返回
}

// outboxEntry 是一条持久化 outbox 记录。
type outboxEntry struct {
	Key       string
	SourceID  string
	Seq       int64
	Event     Event
	Status    outboxStatus
	Attempts  int
	CreatedAt time.Time
	// LeaseDeadline 为当前租约的到期时刻；零值表示从未被租约。
	LeaseDeadline time.Time
}

// sourceState 是单个来源的全部状态。每个来源拥有独立互斥锁，
// 因此不同来源的接收与投递可以完全并行。
type sourceState struct {
	mu sync.Mutex

	id       string
	startSeq int64
	// nextSeq 为释放水位：所有 seq < nextSeq 的事件均已连续进入 outbox。
	nextSeq int64
	// lastDelivered 为已确认投递的最大连续序号，初始为 startSeq-1。
	lastDelivered int64
	createdAt     time.Time

	// buffered 持有 seq >= nextSeq 的已持久化事件（可能存在缺口）。
	bySeq map[int64]*storedEvent
	byID  map[string]*storedEvent

	// queue 为按释放顺序排列的 outbox 记录，队头即下一个可投递事件。
	queue   []*outboxEntry
	head    int
	pending int // queue 中非 delivered 的数量

	// entries 为 key -> outbox 记录的来源内索引，记录创建后不移除。
	// 放在来源状态内，与 bySeq/queue 一样由 mu 保护，避免跨来源共享 map。
	entries map[string]*outboxEntry
}

// MemoryStore 是 Store 的进程内参考实现。
//
// 它用每来源互斥锁实现“同源严格有序、异源并行”，用 outbox 队头租约
// 模拟持久化投递语义：进程崩溃用“租约到期未确认”来建模，过期后由其他
// 发送者重新领取，重试携带同一幂等键；delivered 为终态。生产环境可用
// 同一组方法映射到 SQL 事务（events / outbox 两张表 + 行锁）。
type MemoryStore struct {
	sourceRegMu sync.Mutex
	sources     map[string]*sourceState
}

// NewMemoryStore 创建空存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		sources: make(map[string]*sourceState),
	}
}

func (s *MemoryStore) source(id string) *sourceState {
	s.sourceRegMu.Lock()
	defer s.sourceRegMu.Unlock()
	return s.sources[id]
}

// InitSource 初始化来源。startSeq 为首个期望事件的序号。
// 对同一 startSeq 重复初始化是幂等的；已存在但 startSeq 不同则冲突。
func (s *MemoryStore) InitSource(id string, startSeq int64) (SourceInfo, error) {
	if strings.TrimSpace(id) == "" {
		return SourceInfo{}, invalidArg("source_id", "must not be empty")
	}
	s.sourceRegMu.Lock()
	if st, ok := s.sources[id]; ok {
		s.sourceRegMu.Unlock()
		st.mu.Lock()
		defer st.mu.Unlock()
		if st.startSeq != startSeq {
			return SourceInfo{}, fmt.Errorf("%w: source %q already initialized with startSeq=%d, got %d",
				ErrAlreadyExists, id, st.startSeq, startSeq)
		}
		return st.info(), nil
	}
	st := &sourceState{
		id:            id,
		startSeq:      startSeq,
		nextSeq:       startSeq,
		lastDelivered: startSeq - 1,
		createdAt:     time.Now().UTC(),
		bySeq:         make(map[int64]*storedEvent),
		byID:          make(map[string]*storedEvent),
		entries:       make(map[string]*outboxEntry),
	}
	s.sources[id] = st
	s.sourceRegMu.Unlock()

	st.mu.Lock()
	defer st.mu.Unlock()
	return st.info(), nil
}

func (st *sourceState) info() SourceInfo {
	return SourceInfo{
		ID:        st.id,
		StartSeq:  st.startSeq,
		NextSeq:   st.nextSeq,
		CreatedAt: st.createdAt,
	}
}

// GetSource 查询来源信息；不存在返回 ErrNotFound。
func (s *MemoryStore) GetSource(id string) (SourceInfo, error) {
	st := s.source(id)
	if st == nil {
		return SourceInfo{}, ErrNotFound
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.info(), nil
}

func validateEvent(ev Event) error {
	if strings.TrimSpace(ev.SourceID) == "" {
		return invalidArg("source_id", "must not be empty")
	}
	if strings.TrimSpace(ev.ID) == "" {
		return invalidArg("event_id", "must not be empty")
	}
	if ev.Digest == "" {
		return invalidArg("digest", "must not be empty")
	}
	return nil
}

// Receive 接收一个事件。
//
// 判定规则（在来源临界区内原子完成）：
//   - 标识、序号、负载摘要与已存事件完全相同：返回首次接收的原始结果（幂等）；
//   - 同一序号绑定了不同标识：序号冲突 ErrSequenceConflict；
//   - 同一标识绑定了不同负载摘要，或绑定到另一个序号：负载冲突 ErrPayloadConflict；
//   - 正常事件先持久化；随后从释放水位起连续释放所有无缺口事件进入 outbox。
func (s *MemoryStore) Receive(ev Event) (*Receipt, error) {
	if err := validateEvent(ev); err != nil {
		return nil, err
	}
	st := s.source(ev.SourceID)
	if st == nil {
		return nil, ErrNotFound
	}

	st.mu.Lock()
	defer st.mu.Unlock()

	if ev.Seq < st.startSeq {
		return nil, invalidArg("seq", "sequence is before the source start sequence")
	}

	// 1) 序号维度判定。
	if same, ok := st.bySeq[ev.Seq]; ok {
		if same.ev.ID == ev.ID && same.ev.Digest == ev.Digest {
			// 完全重复：原样返回首次结果（不再次触发释放），仅标记本次为重复提交。
			r := cloneReceipt(same.receipt)
			r.Duplicate = true
			return r, nil
		}
		// 同序号不同标识 → 序号冲突；标识相同但摘要不同 → 负载冲突。
		return nil, st.conflict(ev, same, same.ev.ID == ev.ID)
	}

	// 2) 标识维度判定：同一标识出现在另一个序号（或摘要不同）→ 负载冲突。
	if other, ok := st.byID[ev.ID]; ok {
		return nil, st.conflict(ev, other, true)
	}

	se := &storedEvent{ev: ev}
	st.bySeq[ev.Seq] = se
	st.byID[ev.ID] = se

	receipt := &Receipt{
		SourceID:  ev.SourceID,
		Seq:       ev.Seq,
		EventID:   ev.ID,
		Watermark: st.nextSeq,
	}

	// 3) 缺口补齐推进：从水位开始连续释放，全部在同一临界区完成，
	//    并发接收相邻事件时不会漏投、重投或乱序。
	//    对新事件而言，能进入释放批次当且仅当它自身就是水位补齐点，
	//    即 ReleasedCount>0 时本事件一定已释放。
	for {
		head, ok := st.bySeq[st.nextSeq]
		if !ok {
			break
		}
		key := idempotencyKey(st.id, st.nextSeq)
		entry := &outboxEntry{
			Key:       key,
			SourceID:  st.id,
			Seq:       st.nextSeq,
			Event:     cloneEvent(head.ev),
			Status:    statusPending,
			CreatedAt: time.Now().UTC(),
		}
		head.queued = true
		head.key = key
		st.queue = append(st.queue, entry)
		st.entries[key] = entry
		st.pending++
		st.nextSeq++
		receipt.ReleasedCount++
	}

	if receipt.ReleasedCount > 0 {
		receipt.Status = StatusQueued
		receipt.IdempotencyKey = idempotencyKey(st.id, ev.Seq)
	} else {
		receipt.Status = StatusBuffered
	}
	receipt.Watermark = st.nextSeq
	// 保存首次接收的原始结果；之后完全相同的重复提交原样返回。
	se.receipt = cloneReceipt(receipt)

	return receipt, nil
}

// conflict 构造冲突错误。payloadLike=true 表示归类为负载冲突，
// 否则归类为序号冲突。
func (st *sourceState) conflict(in Event, existing *storedEvent, payloadLike bool) error {
	kind := ConflictSequence
	if payloadLike {
		kind = ConflictPayload
	}
	return &ConflictError{
		Kind:     kind,
		SourceID: in.SourceID,
		Seq:      in.Seq,
		EventID:  in.ID,
		Existing: conflictSide{Seq: existing.ev.Seq, ID: existing.ev.ID, Digest: existing.ev.Digest},
		Incoming: conflictSide{Seq: in.Seq, ID: in.ID, Digest: in.Digest},
	}
}

// ClaimDue 领取至多 max 个到期待投递任务。
//
// 顺序保证：每个来源只有队头记录可被领取——队头未确认（delivered）前，
// 后续记录绝不发放给发送方；队头租约过期则允许重新租约（attempt+1）。
// 因此同一来源任意时刻最多只有一个事件在途，不同来源则可在同一批中并行。
func (s *MemoryStore) ClaimDue(now time.Time, lease time.Duration, max int) []*Delivery {
	if lease <= 0 {
		panic("webhooksequencer: lease duration must be positive")
	}
	if max <= 0 {
		return nil
	}
	out := make([]*Delivery, 0, max)

	s.sourceRegMu.Lock()
	srcs := make([]*sourceState, 0, len(s.sources))
	for _, st := range s.sources {
		srcs = append(srcs, st)
	}
	s.sourceRegMu.Unlock()

	for _, st := range srcs {
		if len(out) >= max {
			break
		}
		st.mu.Lock()
		if st.head >= len(st.queue) {
			st.mu.Unlock()
			continue
		}
		head := st.queue[st.head]
		switch head.Status {
		case statusPending:
			head.Status = statusLeased
			head.Attempts++
			head.LeaseDeadline = now.Add(lease)
			out = append(out, &Delivery{Key: head.Key, Attempt: head.Attempts, Event: cloneEvent(head.Event)})
		case statusLeased:
			if !head.LeaseDeadline.After(now) {
				// 租约过期：前任发送者疑似崩溃，重新发放，幂等键不变。
				head.Attempts++
				head.LeaseDeadline = now.Add(lease)
				out = append(out, &Delivery{Key: head.Key, Attempt: head.Attempts, Event: cloneEvent(head.Event)})
			}
		}
		st.mu.Unlock()
	}
	return out
}

// Ack 确认一次投递成功。delivered 是终态：重复确认（含旧租约迟到的确认
// 落在终态之后）返回 nil 且不改变任何状态，已确认记录永远不会重新回到待投递。
//
// attempt 为领取时拿到的租约 token：若该租约已过期并被他人重新领取，
// 旧持有者的确认返回 ErrLeaseLost，不能把新租约的在途投递误标为完成。
func (s *MemoryStore) Ack(key string, attempt int) error {
	sourceID, _, ok := parseIdempotencyKey(key)
	if !ok {
		return invalidArg("key", "malformed idempotency key")
	}
	st := s.source(sourceID)
	if st == nil {
		return ErrNotFound
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	entry, ok := st.entries[key]
	if !ok {
		return ErrNotFound
	}
	if entry.Status == statusDelivered {
		// 终态幂等：任何迟到的确认都无副作用。
		return nil
	}
	if entry.Status != statusLeased || entry.Attempts != attempt {
		return fmt.Errorf("%w: key=%s ack attempt=%d current=%v",
			ErrLeaseLost, key, attempt, entry.Status)
	}
	// 只有队头可以被确认（正常流程中也只有队头会被发放）。
	cur := st.queue[st.head]
	if cur.Key != key {
		return invalidArg("key", "delivery is not at the head of the source outbox")
	}
	entry.Status = statusDelivered
	entry.LeaseDeadline = time.Time{}
	st.pending--
	st.lastDelivered = entry.Seq
	st.head++
	return nil
}

// Release 主动放弃租约（例如下游返回可重试错误、发送协程优雅退出），
// 记录立即回到 pending，等待下一轮领取。过期租约（attempt 不匹配）的释放
// 返回 ErrLeaseLost 且无副作用——新租约不受旧持有者影响。
func (s *MemoryStore) Release(key string, attempt int) error {
	sourceID, _, ok := parseIdempotencyKey(key)
	if !ok {
		return invalidArg("key", "malformed idempotency key")
	}
	st := s.source(sourceID)
	if st == nil {
		return ErrNotFound
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	entry, ok := st.entries[key]
	if !ok {
		return ErrNotFound
	}
	if entry.Status == statusDelivered {
		return nil
	}
	if entry.Status != statusLeased || entry.Attempts != attempt {
		return fmt.Errorf("%w: key=%s release attempt=%d current=%v",
			ErrLeaseLost, key, attempt, entry.Status)
	}
	entry.Status = statusPending
	entry.LeaseDeadline = time.Time{}
	return nil
}

// GetBacklog 查询来源积压。
func (s *MemoryStore) GetBacklog(sourceID string) (*Backlog, error) {
	st := s.source(sourceID)
	if st == nil {
		return nil, ErrNotFound
	}
	st.mu.Lock()
	defer st.mu.Unlock()

	var buffered int64
	for seq := range st.bySeq {
		if seq >= st.nextSeq {
			buffered++
		}
	}
	return &Backlog{
		SourceID:         sourceID,
		NextSeq:          st.nextSeq,
		LastDeliveredSeq: st.lastDelivered,
		Buffered:         buffered,
		PendingDelivery:  int64(st.pending),
	}, nil
}

// idempotencyKey 生成稳定幂等键 "<sourceID>:<seq>"。
// sourceID 与 seq 一一绑定，解析时按最后一个 ':' 切分。
func idempotencyKey(sourceID string, seq int64) string {
	return sourceID + ":" + strconv.FormatInt(seq, 10)
}

func parseIdempotencyKey(key string) (string, int64, bool) {
	idx := strings.LastIndex(key, ":")
	if idx <= 0 || idx == len(key)-1 {
		return "", 0, false
	}
	seq, err := strconv.ParseInt(key[idx+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return key[:idx], seq, true
}

func cloneEvent(ev Event) Event {
	if ev.Payload != nil {
		ev.Payload = append([]byte(nil), ev.Payload...)
	}
	return ev
}

func cloneReceipt(r *Receipt) *Receipt {
	if r == nil {
		return nil
	}
	c := *r
	return &c
}
