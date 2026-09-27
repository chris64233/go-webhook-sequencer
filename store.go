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
}

func newStoreData() storeData {
	return storeData{
		Sources:    make(map[string]*sourceState),
		Deliveries: make(map[string]*Delivery),
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
	return s.persistLocked()
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
// 重复确认是幂等 no-op。
func (s *Store) Ack(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.data.Deliveries[key]
	if !ok {
		return fmt.Errorf("%w: %q", ErrDeliveryNotFound, key)
	}
	if d.Status == DeliveryAcked {
		return nil
	}
	d.Status = DeliveryAcked
	d.Owner = ""
	d.LeaseUntil = time.Time{}
	d.UpdatedAt = s.now()
	return s.persistLocked()
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
		}
	}
	return b, nil
}
