package webhooksequencer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingSender 记录每次投递的键与尝试次数，可配置前几次返回重试。
type recordingSender struct {
	mu        sync.Mutex
	got       []attempt
	failUntil map[string]int // key -> 第 attempt 次（含）之前都重试
	hang      chan struct{}  // 非 nil 时发送阻塞直到该通道关闭
	panicOn   map[string]bool
}

type attempt struct {
	key     string
	attempt int
}

func (r *recordingSender) Send(ctx context.Context, d *Delivery) SendResult {
	r.mu.Lock()
	r.got = append(r.got, attempt{d.Key, d.Attempt})
	if r.hang != nil {
		ch := r.hang
		r.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return SendRetry
		}
		r.mu.Lock()
	}
	panicSend := r.panicOn[d.Key]
	failUntil := r.failUntil[d.Key]
	r.mu.Unlock()

	if panicSend {
		panic("boom")
	}
	if d.Attempt <= failUntil {
		return SendRetry
	}
	return SendAck
}

func (r *recordingSender) attempts() []attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]attempt(nil), r.got...)
}

func TestDispatcherDeliversInOrderWithRetries(t *testing.T) {
	s := NewMemoryStore()
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if _, err := s.Receive(mkEvent("src", i, fmt.Sprintf("e%d", i), "d")); err != nil {
			t.Fatal(err)
		}
	}

	sender := &recordingSender{failUntil: map[string]int{"src:1": 1}}
	d := NewDispatcher(DispatcherConfig{
		Store:        s,
		Sender:       sender,
		PollInterval: 5 * time.Millisecond,
		Lease:        50 * time.Millisecond,
		BatchSize:    1,
		Concurrency:  1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go d.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		bl, _ := s.GetBacklog("src")
		if bl.LastDeliveredSeq == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	bl, _ := s.GetBacklog("src")
	if bl.LastDeliveredSeq != 3 || bl.PendingDelivery != 0 {
		t.Fatalf("all 3 should be delivered in order, got %+v", bl)
	}

	got := sender.attempts()
	// 首次成功事件的顺序必须严格为 1、2、3；src:1 尝试两次（同键重试）。
	var firstSuccess []string
	seen := map[string]int{}
	for _, a := range got {
		if seen[a.key] == 0 {
			firstSuccess = append(firstSuccess, a.key)
		}
		seen[a.key]++
	}
	want := []string{"src:1", "src:2", "src:3"}
	if len(firstSuccess) != len(want) {
		t.Fatalf("want %v, got %v (raw=%v)", want, firstSuccess, got)
	}
	for i := range want {
		if firstSuccess[i] != want[i] {
			t.Fatalf("delivery order violated: want %v, got %v (raw=%v)", want, firstSuccess, got)
		}
	}
	if seen["src:1"] != 2 {
		t.Fatalf("src:1 should be retried once with the same key, got %v", got)
	}
}

func TestDispatcherCrashBeforeAckRedeliversSameKey(t *testing.T) {
	s := NewMemoryStore()
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Receive(mkEvent("src", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}

	// 第一轮：发送方“崩溃”——拿到任务后一直挂住，永不 Ack。
	crash := make(chan struct{})
	sender := &recordingSender{hang: crash}
	d := NewDispatcher(DispatcherConfig{
		Store:        s,
		Sender:       sender,
		PollInterval: 5 * time.Millisecond,
		Lease:        40 * time.Millisecond,
		BatchSize:    4,
		Concurrency:  4,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go d.Run(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for len(sender.attempts()) == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if len(sender.attempts()) == 0 {
		t.Fatal("event was never claimed")
	}
	// 第一个发送协程仍挂死；租约到期后应出现同键、attempt=2 的重复在途投递。
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var retry bool
		for _, a := range sender.attempts() {
			if a.key == "src:1" && a.attempt == 2 {
				retry = true
			}
		}
		if retry {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	var sawRetry bool
	for _, a := range sender.attempts() {
		if a.key == "src:1" && a.attempt == 2 {
			sawRetry = true
		}
	}
	if !sawRetry {
		t.Fatalf("lease expiry should redeliver same key, got %v", sender.attempts())
	}

	// 挂死的两个发送方（attempt 1 与重领的 attempt 2）同时恢复并都回报成功：
	// fencing 保证只有 attempt 2 的确认生效，attempt 1 被 ErrLeaseLost 拒绝。
	close(crash)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if bl, _ := s.GetBacklog("src"); bl.LastDeliveredSeq == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	bl, _ := s.GetBacklog("src")
	if bl.LastDeliveredSeq != 1 {
		t.Fatalf("redelivery should eventually be acked by the newer lease, got %+v", bl)
	}
	cancel()
	// 终态：即使租约再过期很久也不会重投。
	if due := s.ClaimDue(time.Now().Add(time.Hour), time.Second, 10); len(due) != 0 {
		t.Fatalf("delivered event must never be claimed again, got %d", len(due))
	}
}

func TestDispatcherPanicInSenderRetries(t *testing.T) {
	s := NewMemoryStore()
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Receive(mkEvent("src", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}

	var calls int32
	sender := SenderFunc(func(ctx context.Context, d *Delivery) SendResult {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			panic("downstream blew up")
		}
		return SendAck
	})
	d := NewDispatcher(DispatcherConfig{
		Store:        s,
		Sender:       sender,
		PollInterval: 5 * time.Millisecond,
		Lease:        time.Hour, // 不依赖租约超时，Release 后应立即可重试
		BatchSize:    1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go d.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		bl, _ := s.GetBacklog("src")
		if bl.LastDeliveredSeq == 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("event was not delivered after sender panic")
}

func TestDispatcherParallelAcrossSources(t *testing.T) {
	s := NewMemoryStore()
	const n = 10
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("s%02d", i)
		if _, err := s.InitSource(id, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Receive(mkEvent(id, 1, id+"-e", "d")); err != nil {
			t.Fatal(err)
		}
	}

	var inFlight, maxInFlight int32
	sender := SenderFunc(func(ctx context.Context, d *Delivery) SendResult {
		cur := atomic.AddInt32(&inFlight, 1)
		for {
			old := atomic.LoadInt32(&maxInFlight)
			if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&inFlight, -1)
		return SendAck
	})
	d := NewDispatcher(DispatcherConfig{
		Store:        s,
		Sender:       sender,
		PollInterval: 5 * time.Millisecond,
		Lease:        5 * time.Second,
		BatchSize:    n,
		Concurrency:  n,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go d.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for i := 0; i < n; i++ {
			bl, err := s.GetBacklog(fmt.Sprintf("s%02d", i))
			if err != nil || bl.LastDeliveredSeq != 1 {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := 0; i < n; i++ {
		bl, err := s.GetBacklog(fmt.Sprintf("s%02d", i))
		if err != nil || bl.LastDeliveredSeq != 1 {
			t.Fatalf("source %d not delivered: %+v err=%v", i, bl, err)
		}
	}
	if maxInFlight < 2 {
		t.Fatalf("different sources should be delivered in parallel, max in-flight=%d", maxInFlight)
	}
}

func TestNewDispatcherRequiresDeps(t *testing.T) {
	if !panics(func() {
		NewDispatcher(DispatcherConfig{Sender: SenderFunc(func(context.Context, *Delivery) SendResult { return SendAck })})
	}) {
		t.Fatal("missing store should panic")
	}
	if !panics(func() { NewDispatcher(DispatcherConfig{Store: NewMemoryStore()}) }) {
		t.Fatal("missing sender should panic")
	}
}

func panics(f func()) (did bool) {
	defer func() {
		if r := recover(); r != nil {
			did = true
		}
	}()
	f()
	return false
}

func TestErrorSentinels(t *testing.T) {
	if !errors.Is(ErrInvalidArgument, ErrInvalidArgument) {
		t.Fatal("sentinel mismatch")
	}
}
