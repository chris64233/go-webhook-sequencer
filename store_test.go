package webhooksequencer

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestFileStorePersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")

	svc1 := NewService(mustOpen(t, path))
	if err := svc1.InitSource("s", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc1.Receive(ev("s", 1)); err != nil {
		t.Fatal(err)
	}
	// 缺口事件 3 已持久化但未释放，随后进程“崩溃”。
	if _, err := svc1.Receive(ev("s", 3)); err != nil {
		t.Fatal(err)
	}

	// 重启后恢复：缓冲事件仍在，缺口补齐后按序释放。
	svc2 := NewService(mustOpen(t, path))
	r, err := svc2.Receive(ev("s", 2))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"s-e2", "s-e3"}
	if fmt.Sprint(r.Released) != fmt.Sprint(want) {
		t.Fatalf("released after restart: %+v", r)
	}
	// 已释放的 1 不会因重启而重投。
	b, _ := svc2.Backlog("s")
	if b.Pending != 3 || b.NextSeq != 4 {
		t.Fatalf("backlog after restart: %+v", b)
	}
}

func TestFileStoreRecoversLeasesOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")

	s1 := mustOpen(t, path)
	svc := NewService(s1)
	svc.InitSource("s", 1)
	svc.Receive(ev("s", 1))
	claimed := s1.Claim("worker-1", 10, time.Hour)
	if len(claimed) != 1 {
		t.Fatalf("claimed %d", len(claimed))
	}
	// worker-1 持有租约时进程崩溃，未确认。

	s2 := mustOpen(t, path)
	d, ok := s2.Delivery(claimed[0].Key)
	if !ok || d.Status != DeliveryPending {
		t.Fatalf("lease should be recovered to pending, got %+v", d)
	}
}

func TestAckIsTerminalAndIdempotent(t *testing.T) {
	store := NewMemoryStore()
	svc := NewService(store)
	svc.InitSource("s", 1)
	svc.Receive(ev("s", 1))

	key := deliveryKey("s", "s-e1")
	if err := svc.AckDelivery(key); err != nil {
		t.Fatal(err)
	}
	if err := svc.AckDelivery(key); err != nil {
		t.Fatalf("double ack should be a no-op: %v", err)
	}
	if got := store.Claim("w", 10, time.Minute); len(got) != 0 {
		t.Fatalf("acked delivery must never return to pending: %+v", got)
	}
	if err := svc.AckDelivery("s/nope"); !errors.Is(err, ErrDeliveryNotFound) {
		t.Fatalf("unknown key: got %v", err)
	}
}

func mustOpen(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
