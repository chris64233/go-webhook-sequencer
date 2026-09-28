package webhooksequencer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type recordingTransport struct {
	mu   sync.Mutex
	sent []Delivery
	// failKeys 中的幂等键发送失败。
	failKeys map[string]bool
}

func (rt *recordingTransport) Send(_ context.Context, d Delivery) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.failKeys[d.Key] {
		return errors.New("transport error")
	}
	rt.sent = append(rt.sent, d)
	return nil
}

func (rt *recordingTransport) keys() []string {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var keys []string
	for _, d := range rt.sent {
		keys = append(keys, d.Key)
	}
	return keys
}

func setup(t *testing.T, seqs ...int64) (*Service, *Store) {
	t.Helper()
	store := NewMemoryStore()
	svc := NewService(store)
	if err := svc.InitSource("s", 1); err != nil {
		t.Fatal(err)
	}
	for _, seq := range seqs {
		if _, err := svc.Receive(ev("s", seq)); err != nil {
			t.Fatal(err)
		}
	}
	return svc, store
}

func TestDispatchDeliversInOrderAndAcks(t *testing.T) {
	svc, _ := setup(t, 1, 2, 3)
	tr := &recordingTransport{failKeys: map[string]bool{}}
	d := NewDispatcher(svc.store, tr)

	sent, err := d.DispatchOnce(context.Background())
	if err != nil || sent != 3 {
		t.Fatalf("sent=%d err=%v", sent, err)
	}
	want := []string{"s/s-e1", "s/s-e2", "s/s-e3"}
	if got := tr.keys(); !equalStrings(got, want) {
		t.Fatalf("sent keys %v, want %v", got, want)
	}
	b, _ := svc.Backlog("s")
	if b.Acked != 3 || b.Pending != 0 || b.Leased != 0 {
		t.Fatalf("backlog: %+v", b)
	}
}

func TestDispatchRetriesAfterLeaseExpiry(t *testing.T) {
	svc, store := setup(t, 1)
	key := deliveryKey("s", "s-e1")
	tr := &recordingTransport{failKeys: map[string]bool{key: true}}
	d := NewDispatcher(store, tr)
	d.Lease = time.Minute

	// 第一轮：发送失败，租约未过期时不会被重复领取。
	if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
		t.Fatalf("sent=%d", sent)
	}
	tr.failKeys = map[string]bool{}
	if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
		t.Fatalf("lease not expired, should not reclaim; sent=%d", sent)
	}

	// 租约过期后重试成功，幂等键不变，且只确认一次。
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if sent, _ := d.DispatchOnce(context.Background()); sent != 1 {
		t.Fatalf("retry sent=%d", sent)
	}
	if got := tr.keys(); !equalStrings(got, []string{key}) {
		t.Fatalf("retried keys %v", got)
	}
	dv, _ := store.Delivery(key)
	if dv.Status != DeliveryAcked || dv.Attempts != 2 {
		t.Fatalf("delivery: %+v", dv)
	}
	b, _ := svc.Backlog("s")
	if b.Acked != 1 || b.Pending != 0 {
		t.Fatalf("backlog: %+v", b)
	}
}

// 模拟发送成功但确认前进程崩溃：租约过期后以相同幂等键重投，
// 下游凭键去重；确认后不再回到待投递。
func TestCrashBetweenSendAndAckRedeliversSameKey(t *testing.T) {
	_, store := setup(t, 1)
	key := deliveryKey("s", "s-e1")

	// worker-1 领取并发送成功，但在 Ack 前崩溃（租约未确认）。
	first := store.Claim("worker-1", 10, time.Minute)
	if len(first) != 1 || first[0].Key != key {
		t.Fatalf("first claim: %+v", first)
	}

	// 进程重启，worker-2 在租约过期后重新领取：幂等键稳定不变。
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	second := store.Claim("worker-2", 10, time.Minute)
	if len(second) != 1 || second[0].Key != key {
		t.Fatalf("redelivery must reuse stable key: %+v", second)
	}

	if err := store.Ack(key, second[0].Attempts); err != nil {
		t.Fatal(err)
	}
	if got := store.Claim("worker-3", 10, time.Minute); len(got) != 0 {
		t.Fatalf("acked delivery resurfaced: %+v", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
