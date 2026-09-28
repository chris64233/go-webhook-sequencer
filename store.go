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
	Sources     map[string]*sourceState  `json:"sources"`
	Deliveries  map[string]*Delivery     `json:"deliveries"`
	DeadLetters map[string]*DeadLetter   `json:"dead_letters"`
	Decisions   map[string]*Decision     `json:"decisions"`
	Attempts    map[string]*ClaimAttempt `json:"attempts"`
}

func newStoreData() storeData {
	return storeData{
		Sources:     make(map[string]*sourceState),
		Deliveries:  make(map[string]*Delivery),
		DeadLetters: make(map[string]*DeadLetter),
		Decisions:   make(map[string]*Decision),
		Attempts:    make(map[string]*ClaimAttempt),
	}
}

// DefaultMaxAttempts 是单条投递在两次处置之间允许的默认最大失败次数。
const DefaultMaxAttempts = 8

// Store 是来源状态与 outbox 的持久化存储。
// 通过 NewMemoryStore（纯内存）或 OpenFileStore（JSON 文件快照）构造。
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
	// now 可替换，便于测试租约过期。
	now func() time.Time
	// MaxAttempts 是单条投递连续失败多少次后进入死信，默认 DefaultMaxAttempts。
	// 属于运行时策略，不持久化；进程重启后需重新设置。
	MaxAttempts int
}

// NewMemoryStore 返回纯内存存储，进程退出即丢失。
func NewMemoryStore() *Store {
	return &Store{data: newStoreData(), now: time.Now, MaxAttempts: DefaultMaxAttempts}
}

// OpenFileStore 打开（或创建）文件持久化存储。每次变更整体原子落盘。
// 加载时所有未确认的租约会被回收为待投递：上一进程已退出，
// 其持有的租约不可能再被确认，恢复后由新的 worker 重新领取。
func OpenFileStore(path string) (*Store, error) {
	s := &Store{path: path, data: newStoreData(), now: time.Now, MaxAttempts: DefaultMaxAttempts}
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &s.data); err != nil {
			return nil, fmt.Errorf("load store %s: %w", path, err)
		}
		if s.data.Sources == nil || s.data.Deliveries == nil {
			s.data = newStoreData()
		}
		// 兼容旧版本快照：补齐新增集合。
		if s.data.DeadLetters == nil {
			s.data.DeadLetters = make(map[string]*DeadLetter)
		}
		if s.data.Decisions == nil {
			s.data.Decisions = make(map[string]*Decision)
		}
		if s.data.Attempts == nil {
			s.data.Attempts = make(map[string]*ClaimAttempt)
		}
		for _, d := range s.data.Deliveries {
			if d.Status == DeliveryLeased {
				d.Status = DeliveryPending
				d.Owner = ""
				d.LeaseUntil = time.Time{}
			}
		}
		// 进程已退出，未终结的领取尝试不可能再被确认，标记过期。
		for _, a := range s.data.Attempts {
			if a.Outcome == AttemptLeased {
				a.Outcome = AttemptExpired
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

// blockedSeqLocked 返回来源当前阻塞序号（最小的未处置死信序号），0 表示未阻塞。
func (s *Store) blockedSeqLocked(sourceID string) int64 {
	var blocked int64
	for _, dl := range s.data.DeadLetters {
		if dl.SourceID != sourceID || !dl.ResolvedAt.IsZero() {
			continue
		}
		if blocked == 0 || dl.Seq < blocked {
			blocked = dl.Seq
		}
	}
	return blocked
}

// Claim 领取最多 limit 条待投递记录：pending 或租约已过期的 leased。
// 被未处置死信阻塞的来源只释放阻塞序号之前的记录；阻塞序号本身处于
// dead 状态不会被领取，其之后的记录一律不释放，避免后续序号绕过死信。
// 返回按（来源，序号）排序的副本，保证同一来源按序发送。
// 每次领取递增投递的 Attempts 作为栅栏号并登记一条 ClaimAttempt。
func (s *Store) Claim(owner string, limit int, lease time.Duration) []Delivery {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	blocked := make(map[string]int64)
	blockedFor := func(sourceID string) int64 {
		b, ok := blocked[sourceID]
		if !ok {
			b = s.blockedSeqLocked(sourceID)
			blocked[sourceID] = b
		}
		return b
	}
	var picked []*Delivery
	for _, d := range s.data.Deliveries {
		switch d.Status {
		case DeliveryPending:
			// 阻塞序号（处于 dead）本身及其之后的记录都不得领取，
			// 避免后续序号绕过未处置死信。
			if b := blockedFor(d.SourceID); b != 0 && d.Seq >= b {
				continue
			}
			picked = append(picked, d)
		case DeliveryLeased:
			if now.Before(d.LeaseUntil) {
				continue
			}
			// 租约过期才可接管；阻塞序号之后的记录不允许接管
			// （阻塞序号本身是 dead，不可能处于 leased）。
			if b := blockedFor(d.SourceID); b != 0 && d.Seq >= b {
				continue
			}
			picked = append(picked, d)
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
		d.Attempts++
		d.Status = DeliveryLeased
		d.Owner = owner
		d.LeaseUntil = now.Add(lease)
		d.UpdatedAt = now
		a := &ClaimAttempt{
			ID:         attemptID(d.Key, d.Attempts),
			Key:        d.Key,
			SourceID:   d.SourceID,
			Seq:        d.Seq,
			Owner:      owner,
			Attempt:    d.Attempts,
			LeasedAt:   now,
			LeaseUntil: d.LeaseUntil,
			Outcome:    AttemptLeased,
		}
		s.data.Attempts[a.ID] = a
		out = append(out, *d)
	}
	// 租约变更落盘失败不致命：内存状态仍然一致，重启后租约会被回收重放。
	_ = s.persistLocked()
	return out
}

// attemptID 生成领取尝试标识：同一投递的每次领取都不同。
func attemptID(key string, attempt int) string {
	return fmt.Sprintf("%s#%d", key, attempt)
}

// deadLetterKey 生成某轮死信记录的存储键。
func deadLetterKey(key string, episode int) string {
	return fmt.Sprintf("%s@%d", key, episode)
}

// Ack 确认投递成功。只有当前领取尝试（栅栏号匹配且租约仍属于该持有者）
// 能够确认；旧发送者在租约被接管后的确认返回 ErrStaleAttempt。
// 若该序号已被跳过处置，返回 ErrDeliveryDisposed；被前方死信阻塞时
// 返回 ErrSourceBlocked。已确认的投递永远不会再回到待投递状态。
func (s *Store) Ack(key string, attempt int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.ackLocked(key, attempt)
	// 即使返回错误也可能已登记旧尝试过期，统一落盘。
	_ = s.persistLocked()
	return err
}

func (s *Store) ackLocked(key string, attempt int) error {
	d, ok := s.data.Deliveries[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	if d.Status == DeliveryAcked {
		return nil
	}
	if d.Status == DeliverySkipped {
		return fmt.Errorf("%w: %q seq %d", ErrDeliveryDisposed, key, d.Seq)
	}
	if d.Status == DeliveryDead {
		return fmt.Errorf("%w: %q seq %d", ErrDeliveryDead, key, d.Seq)
	}
	// 只有处于 leased（已被当前领取尝试持有）的投递可以确认。
	// pending（如重试后尚未被重新领取）上的迟到确认按过期尝试拒绝。
	if d.Status != DeliveryLeased {
		return fmt.Errorf("%w: key %q not leased (status %s)", ErrStaleAttempt, key, d.Status)
	}
	// leased：栅栏校验。当前持有尝试才可以确认。
	if d.Attempts != attempt {
		if a := s.data.Attempts[attemptID(key, attempt)]; a != nil {
			a.Outcome = AttemptExpired
		}
		return fmt.Errorf("%w: key %q attempt %d, current %d", ErrStaleAttempt, key, attempt, d.Attempts)
	}
	// 前方存在未处置死信：不得确认当前序号，连续序列不能越过死信推进。
	if b := s.blockedSeqLocked(d.SourceID); b != 0 && b < d.Seq {
		return fmt.Errorf("%w: source %q blocked at seq %d, ack of seq %d rejected",
			ErrSourceBlocked, d.SourceID, b, d.Seq)
	}
	d.Status = DeliveryAcked
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.UpdatedAt = s.now()
	if a := s.data.Attempts[attemptID(key, attempt)]; a != nil {
		a.Outcome = AttemptAcked
	}
	return nil
}

// ReportFailure 上报一次发送失败。栅栏规则与 Ack 相同：只有当前尝试可以
// 上报。连续失败达到 MaxAttempts 后投递进入死信（登记 DeadLetter，只保存
// 投递键、序号与最后一次失败，不触碰原事件负载），租约释放。
func (s *Store) ReportFailure(key string, attempt int, cause string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	if d.Status == DeliverySkipped || d.Status == DeliveryAcked {
		// 终态之后的迟到失败不产生任何效果。
		return nil
	}
	if d.Status == DeliveryDead {
		return fmt.Errorf("%w: %q seq %d", ErrDeliveryDead, key, d.Seq)
	}
	// 只有当前领取尝试持有（leased）时才接受失败上报；重试后回到 pending
	// 的旧尝试上报按过期拒绝。
	if d.Status != DeliveryLeased {
		return fmt.Errorf("%w: key %q not leased (status %s)", ErrStaleAttempt, key, d.Status)
	}
	if d.Attempts != attempt {
		if a := s.data.Attempts[attemptID(key, attempt)]; a != nil {
			a.Outcome = AttemptExpired
		}
		return fmt.Errorf("%w: key %q attempt %d, current %d", ErrStaleAttempt, key, attempt, d.Attempts)
	}
	now := s.now()
	d.Failures++
	d.LastFailure = cause
	d.UpdatedAt = now
	if a := s.data.Attempts[attemptID(key, attempt)]; a != nil {
		a.Outcome = AttemptFailed
	}
	max := s.MaxAttempts
	if max <= 0 {
		max = DefaultMaxAttempts
	}
	if d.Failures >= max {
		d.Status = DeliveryDead
		d.Owner = ""
		d.LeaseUntil = time.Time{}
		// 同一投递重试后可能再次进入死信：每轮一条记录，崩溃恢复时
		// 投递已处于 dead 状态不会重复进入，因此不会重复生成死信。
		episode := 1
		for _, ex := range s.data.DeadLetters {
			if ex.Key == key && ex.Episode >= episode {
				episode = ex.Episode + 1
			}
		}
		s.data.DeadLetters[deadLetterKey(key, episode)] = &DeadLetter{
			Episode:   episode,
			Key:       key,
			SourceID:  d.SourceID,
			Seq:       d.Seq,
			LastError: cause,
			Failures:  d.Failures,
			DeadAt:    now,
		}
		// 同批次中可能已领取了该死信序号之后的记录（dispatcher 按序发送，
		// 必然尚未发送它们）。在同一临界区把这些未发送的领取释放回 pending，
		// 使连续序列停在死信处等待处置；其尝试标记过期，迟到确认会被栅栏拒绝。
		for _, other := range s.data.Deliveries {
			if other.Key == key || other.SourceID != d.SourceID || other.Seq <= d.Seq {
				continue
			}
			if other.Status == DeliveryLeased {
				if a := s.data.Attempts[attemptID(other.Key, other.Attempts)]; a != nil {
					a.Outcome = AttemptExpired
				}
				other.Status = DeliveryPending
				other.Owner = ""
				other.LeaseUntil = time.Time{}
				other.UpdatedAt = now
			}
		}
	}
	// 未达上限：保持 leased，租约到期后由后续轮次接管重试。
	return s.persistLocked()
}

// RetryDeadLetter 操作员选择重试：生成新的领取尝试（投递回到 pending，
// 由下一轮领取递增栅栏号），但沿用原投递键，失败计数清零。
// 同一决定号重放幂等返回首次结果，不重复生成重试；决定号被其他内容
// 占用时返回 ErrDecisionConflict。
func (s *Store) RetryDeadLetter(sourceID string, seq int64, decisionID string) (Decision, error) {
	if decisionID == "" {
		return Decision{}, fmt.Errorf("%w: decision id 不能为空", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if dec, ok := s.data.Decisions[decisionID]; ok {
		if dec.SourceID == sourceID && dec.Seq == seq && dec.Kind == DecisionRetry {
			return *dec, nil
		}
		return Decision{}, fmt.Errorf("%w: decision %q already used for %s %s/%d",
			ErrDecisionConflict, decisionID, dec.Kind, dec.SourceID, dec.Seq)
	}
	dl, err := s.requireResolvableLocked(sourceID, seq)
	if err != nil {
		return Decision{}, err
	}
	d := s.data.Deliveries[dl.Key]
	now := s.now()
	d.Status = DeliveryPending
	d.Failures = 0
	d.LastFailure = ""
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.UpdatedAt = now
	dl.ResolvedAt = now
	dl.Resolution = ResolutionRetried
	dl.DecisionID = decisionID
	dec := Decision{
		ID:        decisionID,
		Kind:      DecisionRetry,
		SourceID:  sourceID,
		Seq:       seq,
		Key:       dl.Key,
		CreatedAt: now,
	}
	s.data.Decisions[decisionID] = &dec
	if err := s.persistLocked(); err != nil {
		return Decision{}, err
	}
	return dec, nil
}

// SkipDeadLetter 操作员明确跳过：需要跳过原因和唯一决定号。序号被记为
// 已处置（投递进入 skipped 终态），之后该来源的连续序列才允许继续释放。
// 一旦跳过生效，任何迟到的成功确认都不能把它恢复为已投递。
func (s *Store) SkipDeadLetter(sourceID string, seq int64, decisionID, reason string) (Decision, error) {
	if decisionID == "" || reason == "" {
		return Decision{}, fmt.Errorf("%w: decision id 和 reason 均不能为空", ErrInvalidArgument)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if dec, ok := s.data.Decisions[decisionID]; ok {
		if dec.SourceID == sourceID && dec.Seq == seq && dec.Kind == DecisionSkip && dec.Reason == reason {
			return *dec, nil
		}
		return Decision{}, fmt.Errorf("%w: decision %q already used for %s %s/%d",
			ErrDecisionConflict, decisionID, dec.Kind, dec.SourceID, dec.Seq)
	}
	dl, err := s.requireResolvableLocked(sourceID, seq)
	if err != nil {
		return Decision{}, err
	}
	d := s.data.Deliveries[dl.Key]
	now := s.now()
	d.Status = DeliverySkipped
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.UpdatedAt = now
	dl.ResolvedAt = now
	dl.Resolution = ResolutionSkipped
	dl.DecisionID = decisionID
	dec := Decision{
		ID:        decisionID,
		Kind:      DecisionSkip,
		SourceID:  sourceID,
		Seq:       seq,
		Key:       dl.Key,
		Reason:    reason,
		CreatedAt: now,
	}
	s.data.Decisions[decisionID] = &dec
	if err := s.persistLocked(); err != nil {
		return Decision{}, err
	}
	return dec, nil
}

// requireResolvableLocked 校验目标是来源内序号最小的未处置死信：
// 同一来源存在多个死信时必须按序号顺序处置。
func (s *Store) requireResolvableLocked(sourceID string, seq int64) (*DeadLetter, error) {
	var target *DeadLetter
	for _, dl := range s.data.DeadLetters {
		if dl.SourceID != sourceID || !dl.ResolvedAt.IsZero() {
			continue
		}
		if dl.Seq == seq {
			target = dl
		}
	}
	if target == nil {
		// 可能已被处置：返回已有的处置结果以便调用方区分。
		return nil, fmt.Errorf("%w: source %q seq %d", ErrNotDeadLetter, sourceID, seq)
	}
	if b := s.blockedSeqLocked(sourceID); b != 0 && b < seq {
		return nil, fmt.Errorf("%w: source %q seq %d blocked by earlier dead letter at seq %d",
			ErrDeadLetterOrder, sourceID, seq, b)
	}
	return target, nil
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
		case DeliverySkipped:
			b.Skipped++
		}
	}
	b.BlockedSeq = s.blockedSeqLocked(sourceID)
	return b, nil
}

// DeadLetters 返回死信记录副本。sourceID 为空时返回全部来源，
// 否则只返回该来源；unresolvedOnly 为真时只返回未处置记录。
// 结果按（来源，序号）升序。
func (s *Store) DeadLetters(sourceID string, unresolvedOnly bool) []DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DeadLetter
	for _, dl := range s.data.DeadLetters {
		if sourceID != "" && dl.SourceID != sourceID {
			continue
		}
		if unresolvedOnly && !dl.ResolvedAt.IsZero() {
			continue
		}
		out = append(out, *dl)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].Episode < out[j].Episode
	})
	return out
}

// Decisions 返回处置决定副本，按创建时间（决定号落库顺序）升序。
func (s *Store) Decisions() []Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Decision, 0, len(s.data.Decisions))
	for _, dec := range s.data.Decisions {
		out = append(out, *dec)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Attempts 返回领取尝试记录副本。sourceID 为空时返回全部来源，
// 否则只返回该来源；按（来源，序号，尝试号）升序。
func (s *Store) Attempts(sourceID string) []ClaimAttempt {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ClaimAttempt
	for _, a := range s.data.Attempts {
		if sourceID != "" && a.SourceID != sourceID {
			continue
		}
		out = append(out, *a)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SourceID != out[j].SourceID {
			return out[i].SourceID < out[j].SourceID
		}
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].Attempt < out[j].Attempt
	})
	return out
}
