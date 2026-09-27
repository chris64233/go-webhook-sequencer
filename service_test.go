package webhooksequencer

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

func newService(t *testing.T) *Service {
	t.Helper()
	return NewService(NewMemoryStore())
}

func ev(source string, seq int64) Event {
	return Event{
		SourceID: source,
		Seq:      seq,
		EventID:  fmt.Sprintf("%s-e%d", source, seq),
		Digest:   fmt.Sprintf("digest-%d", seq),
	}
}

func TestInitSourceValidation(t *testing.T) {
	svc := newService(t)
	if err := svc.InitSource("", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: got %v", err)
	}
	if err := svc.InitSource("s", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("startSeq 0: got %v", err)
	}
	if err := svc.InitSource("s", 1); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := svc.InitSource("s", 1); !errors.Is(err, ErrSourceExists) {
		t.Fatalf("re-init: got %v", err)
	}
}

func TestReceiveValidation(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Receive(ev("ghost", 1)); !errors.Is(err, ErrSourceNotFound) {
		t.Fatalf("unknown source: got %v", err)
	}
	svc.InitSource("s", 1)
	for name, e := range map[string]Event{
		"empty event id": {SourceID: "s", Seq: 1, Digest: "d"},
		"empty digest":   {SourceID: "s", Seq: 1, EventID: "e"},
		"zero seq":       {SourceID: "s", EventID: "e", Digest: "d"},
		"negative seq":   {SourceID: "s", Seq: -1, EventID: "e", Digest: "d"},
	} {
		if _, err := svc.Receive(e); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: got %v", name, err)
		}
	}
}

func TestReceiveInOrderReleasesImmediately(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	for i := int64(1); i <= 3; i++ {
		r, err := svc.Receive(ev("s", i))
		if err != nil {
			t.Fatalf("receive %d: %v", i, err)
		}
		if r.Status != StatusReleased || r.NextSeq != i+1 {
			t.Fatalf("receive %d: got %+v", i, r)
		}
	}
	b, _ := svc.Backlog("s")
	if b.Pending != 3 || b.NextSeq != 4 || len(b.BufferedSeqs) != 0 {
		t.Fatalf("backlog: %+v", b)
	}
}

func TestGapBuffersThenReleasesInOrder(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)

	if _, err := svc.Receive(ev("s", 1)); err != nil {
		t.Fatal(err)
	}
	// 缺口：3、4 先到达，只能缓冲不能释放。
	for _, seq := range []int64{3, 4} {
		r, err := svc.Receive(ev("s", seq))
		if err != nil {
			t.Fatal(err)
		}
		if r.Status != StatusBuffered {
			t.Fatalf("seq %d should buffer, got %+v", seq, r)
		}
	}
	b, _ := svc.Backlog("s")
	if b.NextSeq != 2 || fmt.Sprint(b.BufferedSeqs) != "[3 4]" || b.Pending != 1 {
		t.Fatalf("backlog during gap: %+v", b)
	}

	// 补齐缺口后，2、3、4 按连续序号依次释放。
	r, err := svc.Receive(ev("s", 2))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"s-e2", "s-e3", "s-e4"}
	if r.Status != StatusReleased || fmt.Sprint(r.Released) != fmt.Sprint(want) || r.NextSeq != 5 {
		t.Fatalf("release after gap fill: %+v", r)
	}
	b, _ = svc.Backlog("s")
	if b.Pending != 4 || len(b.BufferedSeqs) != 0 {
		t.Fatalf("backlog after gap fill: %+v", b)
	}
}

func TestDuplicateReturnsOriginalReceipt(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	svc.Receive(ev("s", 1))

	first, err := svc.Receive(ev("s", 3)) // 缓冲
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Receive(ev("s", 3)) // 完全相同的重放
	if err != nil {
		t.Fatal(err)
	}
	if !again.Duplicate || again.Status != first.Status || again.NextSeq != first.NextSeq {
		t.Fatalf("duplicate should replay original receipt: first=%+v again=%+v", first, again)
	}
	// 重复事件不得产生额外投递。
	b, _ := svc.Backlog("s")
	if b.Pending != 1 || fmt.Sprint(b.BufferedSeqs) != "[3]" {
		t.Fatalf("backlog after duplicate: %+v", b)
	}
}

func TestPayloadConflict(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	svc.Receive(ev("s", 1))

	bad := ev("s", 2)
	bad.EventID = "s-e1" // 同一标识，不同摘要
	bad.Digest = "tampered"
	if _, err := svc.Receive(bad); err == nil {
		t.Fatal("expected payload conflict")
	} else {
		var pce *PayloadConflictError
		if !errors.As(err, &pce) {
			t.Fatalf("want PayloadConflictError, got %T: %v", err, err)
		}
	}
}

func TestSeqConflict(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	svc.Receive(ev("s", 1))
	svc.Receive(ev("s", 2))

	// 同一序号对应不同事件。
	other := ev("s", 2)
	other.EventID = "s-intruder"
	if _, err := svc.Receive(other); err == nil {
		t.Fatal("expected seq conflict for reused seq")
	} else {
		var sce *SeqConflictError
		if !errors.As(err, &sce) {
			t.Fatalf("want SeqConflictError, got %T: %v", err, err)
		}
	}

	// 同一标识出现在不同序号位置（摘要相同）。
	moved := ev("s", 1)
	moved.Seq = 5
	if _, err := svc.Receive(moved); err == nil {
		t.Fatal("expected seq conflict for moved event id")
	} else {
		var sce *SeqConflictError
		if !errors.As(err, &sce) {
			t.Fatalf("want SeqConflictError, got %T: %v", err, err)
		}
	}
}

// 并发接收同一来源的乱序相邻事件：不许漏投、重投或打乱顺序。
func TestConcurrentReceivesPreserveOrder(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	const n = 500

	order := rand.Perm(n)
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for _, i := range order {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			if _, err := svc.Receive(ev("s", seq)); err != nil {
				errs <- err
			}
		}(int64(i) + 1)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("receive: %v", err)
	}

	b, _ := svc.Backlog("s")
	if b.NextSeq != n+1 || b.Pending != n || len(b.BufferedSeqs) != 0 {
		t.Fatalf("backlog: %+v", b)
	}
	claimed := svc.store.Claim("tester", n, 0)
	if len(claimed) != n {
		t.Fatalf("claimed %d, want %d", len(claimed), n)
	}
	for i, d := range claimed {
		if d.Seq != int64(i)+1 || d.EventID != fmt.Sprintf("s-e%d", i+1) {
			t.Fatalf("outbox[%d] = seq %d event %s, out of order", i, d.Seq, d.EventID)
		}
	}
}

// 不同来源互不影响，可并行推进。
func TestSourcesAreIndependent(t *testing.T) {
	svc := newService(t)
	svc.InitSource("a", 1)
	svc.InitSource("b", 10)

	var wg sync.WaitGroup
	for _, src := range []string{"a", "b"} {
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(source string, seq int64) {
				defer wg.Done()
				if _, err := svc.Receive(ev(source, seq)); err != nil {
					t.Errorf("receive %s/%d: %v", source, seq, err)
				}
			}(src, map[string]int64{"a": 1, "b": 10}[src]+int64(i))
		}
	}
	wg.Wait()

	ba, _ := svc.Backlog("a")
	bb, _ := svc.Backlog("b")
	if ba.NextSeq != 51 || ba.Pending != 50 {
		t.Fatalf("backlog a: %+v", ba)
	}
	if bb.NextSeq != 60 || bb.Pending != 50 {
		t.Fatalf("backlog b: %+v", bb)
	}
}

func TestBacklogUnknownSource(t *testing.T) {
	svc := newService(t)
	if _, err := svc.Backlog("ghost"); !errors.Is(err, ErrSourceNotFound) {
		t.Fatalf("got %v", err)
	}
}
