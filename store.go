package webhooksequencer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// DefaultMaxAttempts 是默认的最大投递次数：达到后投递进入死信。
const DefaultMaxAttempts = 5

// eventRecord 记录一条已接收事件及其首次接收结果（用于幂等重放）。
type eventRecord struct {
	Event   Event   `json:"event"`
	Receipt Receipt `json:"receipt"`
}

// sourceState 是单个来源的持久化状态。
type sourceState struct {
	ID      string                  `json:"id"`
	NextSeq int64                   `json:"next_seq"`
	ByID    map[string]*eventRecord `json:"by_id"`
	// BySeq 记录所有已知序号（含已释放的）对应的事件标识，用于冲突检测。
	BySeq map[int64]string `json:"by_seq"`
	// Buffered 是因前方缺口而暂未释放的序号集合。
	Buffered map[int64]bool `json:"buffered"`
}

func (s *sourceState) clone() *sourceState {
	c := &sourceState{
		ID:       s.ID,
		NextSeq:  s.NextSeq,
		ByID:     make(map[string]*eventRecord, len(s.ByID)),
		BySeq:    make(map[int64]string, len(s.BySeq)),
		Buffered: make(map[int64]bool, len(s.Buffered)),
	}
	for k, v := range s.ByID {
		rec := *v
		if v.Receipt.Released != nil {
			rec.Receipt.Released = append([]string(nil), v.Receipt.Released...)
		}
		c.ByID[k] = &rec
	}
	for k, v := range s.BySeq {
		c.BySeq[k] = v
	}
	for k, v := range s.Buffered {
		c.Buffered[k] = v
	}
	return c
}

type storeData struct {
	Sources     map[string]*sourceState `json:"sources"`
	Deliveries  map[string]*Delivery    `json:"deliveries"`
	DeadLetters map[string]*DeadLetter  `json:"dead_letters"`
	// Decisions 按决定号索引，保证决定号全局唯一、重复提交幂等。
	Decisions map[string]*Decision `json:"decisions"`
	// Attempts 按租约编号索引的领取尝试日志。
	Attempts    map[int64]*Attempt `json:"attempts"`
	NextLeaseID int64              `json:"next_lease_id"`
}

func newStoreData() storeData {
	return storeData{
		Sources:     make(map[string]*sourceState),
		Deliveries:  make(map[string]*Delivery),
		DeadLetters: make(map[string]*DeadLetter),
		Decisions:   make(map[string]*Decision),
		Attempts:    make(map[int64]*Attempt),
	}
}

// Store 是来源状态与 outbox 的持久化存储。
// 通过 NewMemoryStore（纯内存）或 OpenFileStore（JSON 文件快照）构造。
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
	// MaxAttempts 是最大投递次数：达到后投递进入死信，等待人工处置。
	// <= 0 表示不启用死信（无限重试）。
	MaxAttempts int
	// now 可替换，便于测试租约过期。
	now func() time.Time
}

// NewMemoryStore 返回纯内存存储，进程退出即丢失。
func NewMemoryStore() *Store {
	return &Store{data: newStoreData(), now: time.Now, MaxAttempts: DefaultMaxAttempts}
}

// OpenFileStore 打开（或创建）文件持久化存储。每次变更整体原子落盘。
// 加载时所有未确认的租约会被回收为待投递：上一进程已退出，
// 其持有的租约不可能再被确认，恢复后由新的 worker 重新领取。
// 死信记录与处置决定原样恢复，不会重复生成。
func OpenFileStore(path string) (*Store, error) {
	s := &Store{path: path, data: newStoreData(), now: time.Now, MaxAttempts: DefaultMaxAttempts}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, fmt.Errorf("load store %s: %w", path, err)
		}
		// 兼容旧快照：逐字段补齐缺失的集合。
		fresh := newStoreData()
		if s.data.Sources == nil {
			s.data.Sources = fresh.Sources
		}
		if s.data.Deliveries == nil {
			s.data.Deliveries = fresh.Deliveries
		}
		if s.data.DeadLetters == nil {
			s.data.DeadLetters = fresh.DeadLetters
		}
		if s.data.Decisions == nil {
			s.data.Decisions = fresh.Decisions
		}
		if s.data.Attempts == nil {
			s.data.Attempts = fresh.Attempts
		}
		for _, d := range s.data.Deliveries {
			if d.Status == DeliveryLeased {
				if a, ok := s.data.Attempts[d.LeaseID]; ok && a.Status == AttemptActive {
					a.Status = AttemptExpired
				}
				d.Status = DeliveryPending
				d.Owner = ""
				d.LeaseUntil = time.Time{}
				d.LeaseID = 0
			}
		}
	case errors.Is(err, os.ErrNotExist):
		if err := s.persistLocked(); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("read store %s: %w", path, err)
	}
	return s, nil
}

// persistLocked 将全量状态原子写入文件（临时文件 + rename）。调用方须持有锁。
func (s *Store) persistLocked() error {
	if s.path == "" {
		return nil
	}
	b, err := json.Marshal(s.data)
	if err != nil {
		return fmt.Errorf("marshal store: %w", err)
	}
	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("write store: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("write store: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync store: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close store: %w", err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("rename store: %w", err)
	}
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		dir.Sync()
		dir.Close()
	}
	return nil
}

// InitSource 初始化来源，期望序号从 startSeq 开始。
func (s *Store) InitSource(id string, startSeq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Sources[id]; ok {
		return fmt.Errorf("%w: %q", ErrSourceExists, id)
	}
	s.data.Sources[id] = &sourceState{
		ID:       id,
		NextSeq:  startSeq,
		ByID:     make(map[string]*eventRecord),
		BySeq:    make(map[int64]string),
		Buffered: make(map[int64]bool),
	}
	return s.persistLocked()
}

// Source 返回来源状态的深拷贝；调用方修改后需通过 SaveSource 写回。
func (s *Store) Source(id string) (*sourceState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.data.Sources[id]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrSourceNotFound, id)
	}
	return src.clone(), nil
}

// SaveSource 原子写回来源状态并追加其新释放的 outbox 投递项。
func (s *Store) SaveSource(src *sourceState, deliveries []Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Sources[src.ID]; !ok {
		return fmt.Errorf("%w: %q", ErrSourceNotFound, src.ID)
	}
	s.data.Sources[src.ID] = src.clone()
	for i := range deliveries {
		d := deliveries[i]
		s.data.Deliveries[d.Key] = &d
	}
	return s.persistLocked()
}

// blockedSeqsLocked 返回每个来源当前阻塞确认推进的最小死信号（仅统计待处置的）。
func (s *Store) blockedSeqsLocked() map[string]int64 {
	m := make(map[string]int64)
	for _, dl := range s.data.DeadLetters {
		if dl.Status != DeadLetterOpen {
			continue
		}
		if cur, ok := m[dl.SourceID]; !ok || dl.Seq < cur {
			m[dl.SourceID] = dl.Seq
		}
	}
	return m
}

// deadLetterLocked 把达到最大投递次数的投递转入死信。调用方须持有锁。
func (s *Store) deadLetterLocked(d *Delivery, now time.Time) {
	if a, ok := s.data.Attempts[d.LeaseID]; ok && a.Status == AttemptActive {
		a.Status = AttemptExpired
	}
	d.Status = DeliveryDead
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.LeaseID = 0
	d.UpdatedAt = now
	s.data.DeadLetters[d.Key] = &DeadLetter{
		Key:          d.Key,
		SourceID:     d.SourceID,
		Seq:          d.Seq,
		EventID:      d.EventID,
		Digest:       d.Digest,
		Attempts:     d.Attempts,
		LastError:    d.LastError,
		LastFailedAt: d.LastFailedAt,
		DeadAt:       now,
		Status:       DeadLetterOpen,
	}
}

// Claim 领取最多 limit 条待投递记录：pending 或租约已过期的 leased。
// 返回按（来源， 序号）排序的副本，保证同一来源按序发送。
// 达到最大投递次数的投递在此转入死信；被死信阻塞的来源，
// 其后续序号不得绕过死信被领取。
func (s *Store) Claim(owner string, limit int, lease time.Duration) []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()

	// 第一遍：把达到最大投递次数的投递转入死信。
	for _, d := range s.data.Deliveries {
		if s.MaxAttempts <= 0 || d.Attempts < s.MaxAttempts {
			continue
		}
		switch d.Status {
		case DeliveryPending:
			s.deadLetterLocked(d, now)
		case DeliveryLeased:
			if !now.Before(d.LeaseUntil) {
				s.deadLetterLocked(d, now)
			}
		}
	}

	blocked := s.blockedSeqsLocked()
	var picked []*Delivery
	for _, d := range s.data.Deliveries {
		switch d.Status {
		case DeliveryPending:
		case DeliveryLeased:
			if now.Before(d.LeaseUntil) {
				continue
			}
			// 旧租约已过期，对应尝试标记为 expired。
			if a, ok := s.data.Attempts[d.LeaseID]; ok && a.Status == AttemptActive {
				a.Status = AttemptExpired
			}
		default:
			continue
		}
		// 来源存在更早序号的待处置死信时，后续序号不得绕过它确认。
		if minSeq, ok := blocked[d.SourceID]; ok && d.Seq > minSeq {
			continue
		}
		picked = append(picked, d)
	}
	sort.Slice(picked, func(i, j int) bool {
		if picked[i].SourceID != picked[j].SourceID {
			return picked[i].SourceID < picked[j].SourceID
		}
		return picked[i].Seq < picked[j].Seq
	})
	if limit > 0 && len(picked) > limit {
		picked = picked[:limit]
	}
	out := make([]Delivery, 0, len(picked))
	for _, d := range picked {
		s.data.NextLeaseID++
		id := s.data.NextLeaseID
		d.Status = DeliveryLeased
		d.Owner = owner
		d.LeaseUntil = now.Add(lease)
		d.LeaseID = id
		d.Attempts++
		d.UpdatedAt = now
		s.data.Attempts[id] = &Attempt{
			ID:         id,
			Key:        d.Key,
			SourceID:   d.SourceID,
			Seq:        d.Seq,
			Owner:      owner,
			LeasedAt:   now,
			LeaseUntil: d.LeaseUntil,
			Status:     AttemptActive,
		}
		out = append(out, *d)
	}
	// 租约变更落盘失败不致命：内存状态仍然一致，重启后租约会被回收重放。
	_ = s.persistLocked()
	return out
}

// Ack 确认投递成功，只有持有当前租约（leaseID）的发送者才能确认：
//   - 租约被接管后，旧发送者的迟到确认返回 ErrStaleLease，不改变状态；
//   - 已进入死信或已被跳过的投递是终态，迟到确认同样返回 ErrStaleLease；
//   - 来源被更早序号的死信阻塞时返回 ErrSourceBlocked；
//   - 已确认的投递重复确认是幂等 no-op。
func (s *Store) Ack(key string, leaseID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	switch d.Status {
	case DeliveryAcked:
		return nil
	case DeliveryDead, DeliverySkipped:
		return fmt.Errorf("%w: %q 已进入终态 %s", ErrStaleLease, key, d.Status)
	}
	if d.Status != DeliveryLeased || d.LeaseID != leaseID {
		return fmt.Errorf("%w: %q", ErrStaleLease, key)
	}
	if minSeq, ok := s.blockedSeqsLocked()[d.SourceID]; ok && d.Seq > minSeq {
		return fmt.Errorf("%w: source %q blocked at seq %d", ErrSourceBlocked, d.SourceID, minSeq)
	}
	d.Status = DeliveryAcked
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.UpdatedAt = s.now()
	if a, ok := s.data.Attempts[leaseID]; ok {
		a.Status = AttemptAcked
	}
	return s.persistLocked()
}

// Fail 记录一次发送失败（保存为最后一次失败详情），租约保留到期。
// 租约已被接管的迟到失败报告直接忽略。
func (s *Store) Fail(key string, leaseID int64, cause error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok || d.Status != DeliveryLeased || d.LeaseID != leaseID {
		return
	}
	now := s.now()
	d.LastError = cause.Error()
	d.LastFailedAt = now
	d.UpdatedAt = now
	if a, ok := s.data.Attempts[leaseID]; ok && a.Status == AttemptActive {
		a.Status = AttemptFailed
	}
	_ = s.persistLocked()
}

// checkOrderLocked 校验同一来源内没有序号更小的待处置死信。调用方须持有锁。
func (s *Store) checkOrderLocked(dl *DeadLetter) error {
	for _, other := range s.data.DeadLetters {
		if other.SourceID == dl.SourceID && other.Status == DeadLetterOpen && other.Seq < dl.Seq {
			return fmt.Errorf("%w: source %q 需先处置序号 %d", ErrDeadLetterOrder, dl.SourceID, other.Seq)
		}
	}
	return nil
}

// RetryDeadLetter 重试死信：沿用原投递键重新入队，生成新的领取尝试。
// 同一来源存在多个死信时必须按序号从小到大处置。
func (s *Store) RetryDeadLetter(key string) (DeadLetter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dl, ok := s.data.DeadLetters[key]
	if !ok {
		return DeadLetter{}, fmt.Errorf("%w: %q", ErrDeadLetterNotFound, key)
	}
	if dl.Status != DeadLetterOpen {
		return DeadLetter{}, fmt.Errorf("%w: %q 状态 %s", ErrDeadLetterNotOpen, key, dl.Status)
	}
	if err := s.checkOrderLocked(dl); err != nil {
		return DeadLetter{}, err
	}
	d, ok := s.data.Deliveries[key]
	if !ok {
		return DeadLetter{}, fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	now := s.now()
	// 沿用原投递键重新入队；Attempts 归零开始新一轮投递，
	// 历史尝试保留在领取尝试日志中。
	d.Status = DeliveryPending
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.LeaseID = 0
	d.Attempts = 0
	d.UpdatedAt = now
	dl.Status = DeadLetterRetried
	return *dl, s.persistLocked()
}

// SkipDeadLetter 跳过死信：需要非空原因与全局唯一的决定号。
// 同一决定号重复提交相同决定是幂等 no-op；决定号被用于其他死信时
// 返回 ErrDecisionConflict。跳过后该序号记为已处置，连续序列继续推进，
// 任何迟到的确认都不能把它恢复为已投递。
func (s *Store) SkipDeadLetter(key, decisionID, reason string) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if dec, ok := s.data.Decisions[decisionID]; ok {
		if dec.Key != key {
			return Decision{}, fmt.Errorf("%w: %q 已用于 %q", ErrDecisionConflict, decisionID, dec.Key)
		}
		return *dec, nil
	}
	dl, ok := s.data.DeadLetters[key]
	if !ok {
		return Decision{}, fmt.Errorf("%w: %q", ErrDeadLetterNotFound, key)
	}
	if dl.Status != DeadLetterOpen {
		return Decision{}, fmt.Errorf("%w: %q 状态 %s", ErrDeadLetterNotOpen, key, dl.Status)
	}
	if err := s.checkOrderLocked(dl); err != nil {
		return Decision{}, err
	}
	d, ok := s.data.Deliveries[key]
	if !ok {
		return Decision{}, fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	now := s.now()
	if a, ok := s.data.Attempts[d.LeaseID]; ok && a.Status == AttemptActive {
		a.Status = AttemptExpired
	}
	d.Status = DeliverySkipped
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.LeaseID = 0
	d.UpdatedAt = now
	dl.Status = DeadLetterSkipped
	dec := &Decision{
		ID:        decisionID,
		Type:      DecisionSkip,
		Key:       key,
		SourceID:  dl.SourceID,
		Seq:       dl.Seq,
		Reason:    reason,
		DecidedAt: now,
	}
	s.data.Decisions[decisionID] = dec
	return *dec, s.persistLocked()
}

// Delivery 按键查询投递记录。
func (s *Store) Delivery(key string) (Delivery, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok {
		return Delivery{}, false
	}
	return *d, true
}

// DeadLetters 返回死信记录，按（来源， 序号）升序；sourceID 为空时返回全部来源。
func (s *Store) DeadLetters(sourceID string) []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DeadLetter
	for _, dl := range s.data.DeadLetters {
		if sourceID == "" || dl.SourceID == sourceID {
			out = append(out, *dl)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// DeliveryAttempts 返回一条投递的领取尝试历史，按租约编号升序。
func (s *Store) DeliveryAttempts(key string) []Attempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Attempt
	for _, a := range s.data.Attempts {
		if a.Key == key {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Decisions 返回处置决定，按（来源， 序号）升序；sourceID 为空时返回全部来源。
func (s *Store) Decisions(sourceID string) []Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Decision
	for _, dec := range s.data.Decisions {
		if sourceID == "" || dec.SourceID == sourceID {
			out = append(out, *dec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// BlockingSeq 返回当前阻塞来源确认推进的最小死信号；0 表示未阻塞。
func (s *Store) BlockingSeq(sourceID string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blockedSeqsLocked()[sourceID]
}

// Backlog 返回来源的积压情况。
func (s *Store) Backlog(sourceID string) (Backlog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.data.Sources[sourceID]
	if !ok {
		return Backlog{}, fmt.Errorf("%w: %q", ErrSourceNotFound, sourceID)
	}
	b := Backlog{SourceID: sourceID, NextSeq: src.NextSeq}
	for seq := range src.Buffered {
		b.BufferedSeqs = append(b.BufferedSeqs, seq)
	}
	sort.Slice(b.BufferedSeqs, func(i, j int) bool { return b.BufferedSeqs[i] < b.BufferedSeqs[j] })
	for _, d := range s.data.Deliveries {
		if d.SourceID != sourceID {
			continue
		}
		switch d.Status {
		case DeliveryPending:
			b.Pending++
		case DeliveryLeased:
			b.Leased++
		case DeliveryAcked:
			b.Acked++
		}
	}
	for _, dl := range s.data.DeadLetters {
		if dl.SourceID == sourceID && dl.Status == DeadLetterOpen {
			b.DeadLetters++
			if b.BlockedSeq == 0 || dl.Seq < b.BlockedSeq {
				b.BlockedSeq = dl.Seq
			}
		}
	}
	return b, nil
}
