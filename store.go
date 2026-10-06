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
	Sources    map[string]*sourceState `json:"sources"`
	Deliveries map[string]*Delivery    `json:"deliveries"`
	// Plans 按来源保存重放计划（每个来源同时只有一份，范围与版本固定）。
	Plans map[string]*ReplayPlan `json:"plans"`
	// Manual 按 来源/序号 保存人工处理条目，保留原始序号。
	Manual map[string]*ManualEntry `json:"manual"`
}

func newStoreData() storeData {
	return storeData{
		Sources:    make(map[string]*sourceState),
		Deliveries: make(map[string]*Delivery),
		Plans:      make(map[string]*ReplayPlan),
		Manual:     make(map[string]*ManualEntry),
	}
}

// Store 是来源状态与 outbox 的持久化存储。
// 通过 NewMemoryStore（纯内存）或 OpenFileStore（JSON 文件快照）构造。
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
	// now 可替换，便于测试租约过期。
	now func() time.Time
}

// NewMemoryStore 返回纯内存存储，进程退出即丢失。
func NewMemoryStore() *Store {
	return &Store{data: newStoreData(), now: time.Now}
}

// OpenFileStore 打开（或创建）文件持久化存储。每次变更整体原子落盘。
// 加载时所有未确认的租约会被回收为待投递：上一进程已退出，
// 其持有的租约不可能再被确认，恢复后由新的 worker 重新领取。
func OpenFileStore(path string) (*Store, error) {
	s := &Store{path: path, data: newStoreData(), now: time.Now}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, fmt.Errorf("load store %s: %w", path, err)
		}
		if s.data.Sources == nil || s.data.Deliveries == nil {
			s.data = newStoreData()
		}
		if s.data.Plans == nil {
			s.data.Plans = make(map[string]*ReplayPlan)
		}
		if s.data.Manual == nil {
			s.data.Manual = make(map[string]*ManualEntry)
		}
		for _, d := range s.data.Deliveries {
			if d.Status == DeliveryLeased {
				d.Status = DeliveryPending
				d.Owner = ""
				d.LeaseUntil = time.Time{}
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
	// 新事件释放后，激活的重放计划从确认位置继续入队（缺口处停住）。
	if p, ok := s.data.Plans[src.ID]; ok && p.Status == ReplayActive {
		s.syncPlanLocked(s.data.Sources[src.ID], p, s.now())
	}
	return s.persistLocked()
}

// syncPlanLocked 推进重放计划：先把确认位置移过已确认/已人工处理的序号，
// 再从确认位置起把连续可用的事件入队；遇到序号缺口（事件未到达）必须停住，
// 后续事件不得提前入队。调用方须持有锁。
func (s *Store) syncPlanLocked(src *sourceState, p *ReplayPlan, now time.Time) {
	for p.Position <= p.ToSeq {
		if e, ok := s.data.Manual[manualKey(src.ID, p.Position)]; ok && e.Status == ManualResolved {
			p.Position++
			continue
		}
		id, ok := src.BySeq[p.Position]
		if !ok {
			break // 缺口：事件未到达
		}
		d, ok := s.data.Deliveries[deliveryKey(src.ID, id)]
		if !ok || d.Status != DeliveryAcked {
			break
		}
		p.Position++
	}
	if p.Status != ReplayActive {
		// 窗口过期后不再自动入队：剩余序号只能逐条人工处理。
		if p.Position > p.ToSeq {
			p.Status = ReplayCompleted
		}
		return
	}
	for seq := p.Position; seq <= p.ToSeq; seq++ {
		id, ok := src.BySeq[seq]
		if !ok {
			break // 缺口：后续事件不得提前发送
		}
		key := deliveryKey(src.ID, id)
		d, ok := s.data.Deliveries[key]
		switch {
		case ok && d.Status == DeliveryAcked:
			// 已成功投递的事件不得重新入队。
		case ok && d.Status == DeliveryDead:
			d.Status = DeliveryPending
			d.Owner = ""
			d.LeaseUntil = time.Time{}
			d.Attempts = 0
			d.UpdatedAt = now
		case !ok:
			s.data.Deliveries[key] = &Delivery{
				Key:       key,
				SourceID:  src.ID,
				Seq:       seq,
				EventID:   id,
				Digest:    src.ByID[id].Event.Digest,
				Status:    DeliveryPending,
				CreatedAt: now,
				UpdatedAt: now,
			}
		}
	}
	if p.Position > p.ToSeq {
		p.Status = ReplayCompleted
	}
}

// PlanReplay 为来源的 [fromSeq, toSeq] 区间创建死信重放计划。
// 相同范围的重复请求返回原计划（幂等，窗口参数被忽略）；
// 范围或版本不同返回 ReplayConflictError。
func (s *Store) PlanReplay(sourceID string, fromSeq, toSeq int64, window time.Duration) (ReplayPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.data.Sources[sourceID]
	if !ok {
		return ReplayPlan{}, fmt.Errorf("%w: %q", ErrSourceNotFound, sourceID)
	}
	if p, ok := s.data.Plans[sourceID]; ok {
		if p.FromSeq == fromSeq && p.ToSeq == toSeq {
			return *p, nil
		}
		return ReplayPlan{}, &ReplayConflictError{
			SourceID:        sourceID,
			Existing:        *p,
			IncomingFromSeq: fromSeq,
			IncomingToSeq:   toSeq,
		}
	}
	now := s.now()
	p := &ReplayPlan{
		SourceID:  sourceID,
		FromSeq:   fromSeq,
		ToSeq:     toSeq,
		Deadline:  now.Add(window),
		Position:  fromSeq,
		Version:   1,
		Status:    ReplayActive,
		CreatedAt: now,
	}
	s.data.Plans[sourceID] = p
	s.syncPlanLocked(src, p, now)
	if err := s.persistLocked(); err != nil {
		return ReplayPlan{}, err
	}
	return *p, nil
}

// ExpirePlan 在窗口过期后结算计划：未确认的序号按原始顺序转入人工处理，
// 对应投递转为死信；事件本身保留，不删除、不重新编号。窗口未过期或
// 计划已终结时是幂等 no-op。
func (s *Store) ExpirePlan(sourceID string) (ReplayPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.data.Sources[sourceID]
	if !ok {
		return ReplayPlan{}, fmt.Errorf("%w: %q", ErrSourceNotFound, sourceID)
	}
	p, ok := s.data.Plans[sourceID]
	if !ok {
		return ReplayPlan{}, fmt.Errorf("%w: %q", ErrReplayNotFound, sourceID)
	}
	now := s.now()
	if p.Status != ReplayActive || now.Before(p.Deadline) {
		return *p, nil
	}
	p.Status = ReplayExpired
	for seq := p.Position; seq <= p.ToSeq; seq++ {
		key := manualKey(sourceID, seq)
		if _, ok := s.data.Manual[key]; ok {
			continue
		}
		entry := &ManualEntry{
			SourceID:  sourceID,
			Seq:       seq,
			Status:    ManualPending,
			CreatedAt: now,
		}
		if id, ok := src.BySeq[seq]; ok {
			entry.EventID = id
			entry.Digest = src.ByID[id].Event.Digest
			if d, ok := s.data.Deliveries[deliveryKey(sourceID, id)]; ok && d.Status != DeliveryAcked {
				d.Status = DeliveryDead
				d.Owner = ""
				d.LeaseUntil = time.Time{}
				d.UpdatedAt = now
			}
		}
		s.data.Manual[key] = entry
	}
	if err := s.persistLocked(); err != nil {
		return ReplayPlan{}, err
	}
	return *p, nil
}

// ResolveManual 人工处理缺口位置的条目：只能从当前确认位置（缺口）开始，
// 处理后确认位置继续前进；已处理的序号不可重复消费。
func (s *Store) ResolveManual(sourceID string, seq int64) (ReplayPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.data.Sources[sourceID]
	if !ok {
		return ReplayPlan{}, fmt.Errorf("%w: %q", ErrSourceNotFound, sourceID)
	}
	p, ok := s.data.Plans[sourceID]
	if !ok {
		return ReplayPlan{}, fmt.Errorf("%w: %q", ErrReplayNotFound, sourceID)
	}
	entry, ok := s.data.Manual[manualKey(sourceID, seq)]
	if !ok || entry.Status != ManualPending {
		return ReplayPlan{}, fmt.Errorf("%w: %q seq %d", ErrManualNotFound, sourceID, seq)
	}
	if seq != p.Position {
		return ReplayPlan{}, fmt.Errorf("%w: 只能处理缺口位置 %d，收到 %d", ErrManualOrder, p.Position, seq)
	}
	entry.Status = ManualResolved
	entry.ResolvedAt = s.now()
	s.syncPlanLocked(src, p, s.now())
	if err := s.persistLocked(); err != nil {
		return ReplayPlan{}, err
	}
	return *p, nil
}

// ReplayStatus 返回来源的重放计划与人工处理条目（按原始序号升序）。
func (s *Store) ReplayStatus(sourceID string) (ReplayPlan, []ManualEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data.Sources[sourceID]; !ok {
		return ReplayPlan{}, nil, fmt.Errorf("%w: %q", ErrSourceNotFound, sourceID)
	}
	p, ok := s.data.Plans[sourceID]
	if !ok {
		return ReplayPlan{}, nil, fmt.Errorf("%w: %q", ErrReplayNotFound, sourceID)
	}
	var entries []ManualEntry
	for _, e := range s.data.Manual {
		if e.SourceID == sourceID {
			entries = append(entries, *e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Seq < entries[j].Seq })
	return *p, entries, nil
}

// Claim 领取最多 limit 条待投递记录：pending 或租约已过期的 leased。
// 返回按（来源， 序号）排序的副本，保证同一来源按序发送。
func (s *Store) Claim(owner string, limit int, lease time.Duration) []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var picked []*Delivery
	for _, d := range s.data.Deliveries {
		switch d.Status {
		case DeliveryPending:
			picked = append(picked, d)
		case DeliveryLeased:
			if !now.Before(d.LeaseUntil) {
				picked = append(picked, d)
			}
		}
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
		d.Status = DeliveryLeased
		d.Owner = owner
		d.LeaseUntil = now.Add(lease)
		d.Attempts++
		d.UpdatedAt = now
		out = append(out, *d)
	}
	// 租约变更落盘失败不致命：内存状态仍然一致，重启后租约会被回收重放。
	_ = s.persistLocked()
	return out
}

// Ack 确认投递成功。已确认的投递永远不会再回到待投递状态；
// 重复确认是幂等 no-op。死信上的迟到确认也是 no-op：
// 迟到的旧重放不能覆盖已经确认或转入人工的序号。
func (s *Store) Ack(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	if d.Status == DeliveryAcked || d.Status == DeliveryDead {
		return nil
	}
	d.Status = DeliveryAcked
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.UpdatedAt = s.now()
	// 确认可能推进激活中的重放计划，并触发后续事件入队。
	if p, ok := s.data.Plans[d.SourceID]; ok && p.Status == ReplayActive &&
		d.Seq >= p.FromSeq && d.Seq <= p.ToSeq {
		s.syncPlanLocked(s.data.Sources[d.SourceID], p, s.now())
	}
	return s.persistLocked()
}

// Fail 记录一次发送失败：达到 maxAttempts 的投递转为死信，
// 不再被领取，只能由重放计划重新入队或转入人工处理。
func (s *Store) Fail(key string, maxAttempts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok || d.Status != DeliveryLeased {
		return
	}
	if maxAttempts > 0 && d.Attempts >= maxAttempts {
		d.Status = DeliveryDead
		d.Owner = ""
		d.LeaseUntil = time.Time{}
		d.UpdatedAt = s.now()
		_ = s.persistLocked()
	}
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
		case DeliveryDead:
			b.Dead++
		}
	}
	return b, nil
}
