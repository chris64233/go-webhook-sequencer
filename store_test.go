package webhooksequencer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *MemoryStore {
	t.Helper()
	return NewMemoryStore()
}

func mkEvent(source string, seq int64, id, digest string) Event {
	return Event{SourceID: source, Seq: seq, ID: id, Digest: digest, Payload: []byte("p-" + id)}
}

func TestInitSourceIdempotent(t *testing.T) {
	s := newTestStore(t)

	info, err := s.InitSource("src", 100)
	if err != nil {
		t.Fatalf("InitSource: %v", err)
	}
	if info.StartSeq != 100 || info.NextSeq != 100 {
		t.Fatalf("unexpected info: %+v", info)
	}

	// 相同 startSeq 重复初始化：幂等。
	info2, err := s.InitSource("src", 100)
	if err != nil {
		t.Fatalf("InitSource duplicate: %v", err)
	}
	if !info2.CreatedAt.Equal(info.CreatedAt) {
		t.Fatalf("idempotent InitSource must return the same source")
	}

	// 不同 startSeq：AlreadyExists。
	_, err = s.InitSource("src", 1)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}

	// 参数错误。
	if _, err = s.InitSource("  ", 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}

	if _, err = s.GetSource("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestReceiveGapBuffersAndReleasesInOrder(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}

	// seq=2 先到：持久化但缓存，不能提前交付。
	r2, err := s.Receive(mkEvent("src", 2, "e2", "d2"))
	if err != nil {
		t.Fatal(err)
	}
	if r2.Status != StatusBuffered || r2.IdempotencyKey != "" || r2.ReleasedCount != 0 {
		t.Fatalf("seq2 should be buffered, got %+v", r2)
	}

	// seq=3 也乱序到达，同样缓存。
	r3, err := s.Receive(mkEvent("src", 3, "e3", "d3"))
	if err != nil {
		t.Fatal(err)
	}
	if r3.Status != StatusBuffered {
		t.Fatalf("seq3 should be buffered, got %+v", r3)
	}

	bl, err := s.GetBacklog("src")
	if err != nil {
		t.Fatal(err)
	}
	if bl.Buffered != 2 || bl.PendingDelivery != 0 || bl.NextSeq != 1 || bl.LastDeliveredSeq != 0 {
		t.Fatalf("unexpected backlog before fill: %+v", bl)
	}

	// 缺口 seq=1 补齐：1、2、3 在同一次推进中连续释放。
	r1, err := s.Receive(mkEvent("src", 1, "e1", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	if r1.Status != StatusQueued || r1.ReleasedCount != 3 {
		t.Fatalf("gap fill should release 3 events, got %+v", r1)
	}
	if r1.IdempotencyKey != "src:1" {
		t.Fatalf("unexpected key: %q", r1.IdempotencyKey)
	}
	if r1.Watermark != 4 {
		t.Fatalf("watermark should be 4, got %d", r1.Watermark)
	}

	// outbox 顺序必须严格为 1、2、3：同源队头阻塞，每次只能领取队头，
	// 确认后下一个才释放。
	now := time.Now()
	wantKeys := []string{"src:1", "src:2", "src:3"}
	for i, wantKey := range wantKeys {
		got := s.ClaimDue(now.Add(time.Duration(i)*time.Minute), time.Minute, 10)
		if len(got) != 1 || got[0].Key != wantKey {
			t.Fatalf("position %d: want exactly %s, got %+v", i, wantKey, got)
		}
		if err := s.Ack(wantKey, got[0].Attempt); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceiveExactDuplicateReturnsOriginalReceipt(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}

	// seq=2 在缺口期接收：首次结果为 buffered。
	first, err := s.Receive(mkEvent("src", 2, "e2", "d2"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != StatusBuffered {
		t.Fatalf("want buffered, got %s", first.Status)
	}

	// 补齐缺口后，再重复提交 seq=2：必须返回首次的原始结果（buffered），
	// 且不产生第二条 outbox 记录。
	if _, err := s.Receive(mkEvent("src", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}
	dup, err := s.Receive(mkEvent("src", 2, "e2", "d2"))
	if err != nil {
		t.Fatal(err)
	}
	if !dup.Duplicate {
		t.Fatalf("expected Duplicate=true")
	}
	// 原始结果（buffered）原样返回，而不是被当前水位“改写”。
	if dup.Status != first.Status || dup.ReleasedCount != first.ReleasedCount ||
		dup.IdempotencyKey != first.IdempotencyKey || dup.Watermark != first.Watermark {
		t.Fatalf("duplicate receipt %+v differs from original %+v", dup, first)
	}

	// outbox 中 seq=1、seq=2 各只有一条（重复提交未重入队）。
	for i := 0; i < 2; i++ {
		due := s.ClaimDue(time.Now().Add(time.Duration(i)*time.Minute), time.Minute, 10)
		if len(due) != 1 {
			t.Fatalf("want exactly one head claim, got %d", len(due))
		}
		if err := s.Ack(due[0].Key, due[0].Attempt); err != nil {
			t.Fatal(err)
		}
	}
	if more := s.ClaimDue(time.Now().Add(time.Hour), time.Minute, 10); len(more) != 0 {
		t.Fatalf("duplicate event must not be requeued, got %d", len(more))
	}
}

func TestReceiveConflicts(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}

	// 同序号、不同标识 → 序号冲突。
	if _, err := s.Receive(mkEvent("src", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}
	_, err := s.Receive(mkEvent("src", 1, "other", "dx"))
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.Kind != ConflictSequence {
		t.Fatalf("want sequence conflict, got %v", err)
	}
	if !errors.Is(err, ErrSequenceConflict) || errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("conflict error classification wrong: %v", err)
	}

	// 同序号、同标识、不同负载摘要 → 负载冲突。
	_, err = s.Receive(mkEvent("src", 1, "e1", "tampered"))
	if !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("want payload conflict (same id/seq, different digest), got %v", err)
	}

	// 同标识、不同序号、相同摘要 → 负载冲突（标识对应了另一个序号）。
	_, err = s.Receive(mkEvent("src", 2, "e1", "d1"))
	if !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("want payload conflict (same id, different seq), got %v", err)
	}

	// 同标识、不同序号、不同摘要 → 负载冲突。
	_, err = s.Receive(mkEvent("src", 2, "e1", "different"))
	if !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf("want payload conflict (same id, different seq and digest), got %v", err)
	}

	// 冲突事件不得持久化、不得推进水位。seq=1 正常接收后已释放，
	// 基线为 NextSeq=2、PendingDelivery=1，冲突提交后该基线保持不变。
	bl, _ := s.GetBacklog("src")
	if bl.NextSeq != 2 || bl.Buffered != 0 || bl.PendingDelivery != 1 {
		t.Fatalf("conflicts must not mutate state, got %+v", bl)
	}
}

func TestReceiveValidationAndUnknownSource(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("src", 5); err != nil {
		t.Fatal(err)
	}

	cases := []Event{
		{SourceID: "src", Seq: 5, ID: "", Digest: "d"},
		{SourceID: "src", Seq: 5, ID: "e", Digest: ""},
	}
	for i, ev := range cases {
		if _, err := s.Receive(ev); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("case %d: want ErrInvalidArgument, got %v", i, err)
		}
	}
	// 序号早于起始序号 → 参数错误，而不是冲突。
	if _, err := s.Receive(mkEvent("src", 4, "old", "d")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument for stale seq, got %v", err)
	}
	// 未知来源。
	if _, err := s.Receive(mkEvent("nope", 5, "e", "d")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestClaimAndAckHeadOfLineBlocking(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("a", 1); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if _, err := s.Receive(mkEvent("a", i, fmt.Sprintf("e%d", i), fmt.Sprintf("d%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now()
	lease := time.Minute
	d1 := s.ClaimDue(now, lease, 10)
	if len(d1) != 1 || d1[0].Key != "a:1" || d1[0].Attempt != 1 {
		t.Fatalf("only head must be claimable, got %+v", d1)
	}
	// 队头租约未到期：什么都领不到，队头之后的事件被阻塞。
	if more := s.ClaimDue(now.Add(lease-time.Second), lease, 10); len(more) != 0 {
		t.Fatalf("head is leased, nothing should be due, got %d", len(more))
	}

	if err := s.Ack("a:1", d1[0].Attempt); err != nil {
		t.Fatal(err)
	}
	bl, _ := s.GetBacklog("a")
	if bl.LastDeliveredSeq != 1 || bl.PendingDelivery != 2 {
		t.Fatalf("unexpected backlog after ack: %+v", bl)
	}

	// 队头确认后，下一个可领取。
	d2 := s.ClaimDue(now.Add(lease), lease, 10)
	if len(d2) != 1 || d2[0].Key != "a:2" {
		t.Fatalf("want a:2, got %+v", d2)
	}
	if err := s.Ack("a:2", d2[0].Attempt); err != nil {
		t.Fatal(err)
	}

	// 重复 Ack（终态幂等）：任何 attempt 都无副作用返回 nil。
	if err := s.Ack("a:1", d1[0].Attempt); err != nil {
		t.Fatalf("duplicate ack should be a no-op, got %v", err)
	}

	if err := s.Ack("bogus", 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("malformed key should be invalid argument, got %v", err)
	}
	if err := s.Ack("missing:9", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown key should be not found, got %v", err)
	}
}

func TestAckFencingRejectsStaleAttempt(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Receive(mkEvent("a", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}
	t0 := time.Now()
	lease := 10 * time.Second
	first := s.ClaimDue(t0, lease, 10)
	// 租约到期被重新领取（attempt 2）。
	second := s.ClaimDue(t0.Add(lease+time.Millisecond), lease, 10)
	if len(second) != 1 || second[0].Attempt != 2 {
		t.Fatalf("want redelivery attempt 2, got %+v", second)
	}

	// 旧持有者（attempt 1）迟到的 Ack/Release 必须被 fencing 拒绝。
	if err := s.Ack("a:1", first[0].Attempt); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack should be ErrLeaseLost, got %v", err)
	}
	if err := s.Release("a:1", first[0].Attempt); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale release should be ErrLeaseLost, got %v", err)
	}
	// 当前租约持有者可正常确认。
	if err := s.Ack("a:1", second[0].Attempt); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseExpiryRedeliversSameKey(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.InitSource("a", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Receive(mkEvent("a", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}

	t0 := time.Now()
	lease := 10 * time.Second
	first := s.ClaimDue(t0, lease, 10)
	if len(first) != 1 || first[0].Attempt != 1 {
		t.Fatalf("unexpected first claim: %+v", first)
	}

	// 发送者在 Ack 前崩溃：租约内不可重复领取。
	if more := s.ClaimDue(t0.Add(lease-time.Millisecond), lease, 10); len(more) != 0 {
		t.Fatalf("lease still valid, got %d claims", len(more))
	}
	// 租约到期：以相同幂等键重新发放，attempt 递增；状态不回退到“已投递”。
	second := s.ClaimDue(t0.Add(lease+time.Millisecond), lease, 10)
	if len(second) != 1 || second[0].Key != "a:1" || second[0].Attempt != 2 {
		t.Fatalf("expired lease must redeliver same key on attempt 2, got %+v", second)
	}

	// 确认后进入终态：永不再投递。
	if err := s.Ack("a:1", second[0].Attempt); err != nil {
		t.Fatal(err)
	}
	if more := s.ClaimDue(t0.Add(time.Hour), lease, 10); len(more) != 0 {
		t.Fatalf("delivered must never become due again, got %d", len(more))
	}
	bl, _ := s.GetBacklog("a")
	if bl.LastDeliveredSeq != 1 || bl.PendingDelivery != 0 {
		t.Fatalf("unexpected backlog: %+v", bl)
	}
}

func TestDifferentSourcesClaimInParallel(t *testing.T) {
	s := newTestStore(t)
	for _, src := range []string{"a", "b", "c"} {
		if _, err := s.InitSource(src, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Receive(mkEvent(src, 1, src+"-e1", "d")); err != nil {
			t.Fatal(err)
		}
	}
	due := s.ClaimDue(time.Now(), time.Minute, 16)
	keys := make([]string, 0, len(due))
	for _, d := range due {
		keys = append(keys, d.Key)
	}
	sort.Strings(keys)
	if got, want := keys, []string{"a:1", "b:1", "c:1"}; len(got) != len(want) {
		t.Fatalf("want 3 parallel claims, got %v", got)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("want %v, got %v", want, got)
			}
		}
	}
}

// TestConcurrentReceiveAdjacentEvents 并发、乱序接收同一来源的相邻事件，
// 要求最终 outbox 无漏投、无重投且严格按序号排列。
func TestConcurrentReceiveAdjacentEvents(t *testing.T) {
	const n = 200
	s := newTestStore(t)
	if _, err := s.InitSource("src", 1); err != nil {
		t.Fatal(err)
	}

	seqs := make([]int64, n)
	for i := range seqs {
		seqs[i] = int64(i + 1)
	}
	shuffled := append([]int64(nil), seqs...)
	// 确定性乱序。
	perm := pseudoPerm(n)
	for i, p := range perm {
		shuffled[i] = int64(p + 1)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, seq := range shuffled {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("e%d", seq)
			_, err := s.Receive(mkEvent("src", seq, id, "digest-"+id))
			if err != nil {
				t.Errorf("receive seq %d: %v", seq, err)
			}
		}(seq)
	}
	close(start)
	wg.Wait()

	bl, err := s.GetBacklog("src")
	if err != nil {
		t.Fatal(err)
	}
	if bl.Buffered != 0 || bl.NextSeq != n+1 || bl.PendingDelivery != n {
		t.Fatalf("all events should be released, got %+v", bl)
	}

	// 全部事件应在 outbox 中按序排列，且每个幂等键恰好出现一次。
	seen := make(map[string]bool, n)
	prev := int64(0)
	claimed := 0
	for claimed < n {
		now := time.Now()
		due := s.ClaimDue(now, time.Minute, 1)
		if len(due) != 1 {
			t.Fatalf("expected exactly one head, got %d at %d", len(due), claimed)
		}
		d := due[0]
		if seen[d.Key] {
			t.Fatalf("duplicate delivery: %s", d.Key)
		}
		seen[d.Key] = true
		if d.Event.Seq != prev+1 {
			t.Fatalf("order broken: got seq %d after %d", d.Event.Seq, prev)
		}
		if d.Key != fmt.Sprintf("src:%d", d.Event.Seq) {
			t.Fatalf("unstable idempotency key: %s", d.Key)
		}
		prev = d.Event.Seq
		if err := s.Ack(d.Key, d.Attempt); err != nil {
			t.Fatal(err)
		}
		claimed++
	}
}

// TestConcurrentMixedOpsAcrossSources 让多个来源并发地接收、领取、确认、
// 查询积压与来源信息，压测跨来源共享结构（曾为全局 entries map）的数据竞争。
func TestConcurrentMixedOpsAcrossSources(t *testing.T) {
	const sources = 20
	const events = 50
	s := newTestStore(t)
	for i := 0; i < sources; i++ {
		if _, err := s.InitSource(fmt.Sprintf("src%02d", i), 1); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < sources; i++ {
		src := fmt.Sprintf("src%02d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// 接收方：按序快速灌入，制造 entries 写入。
			for seq := int64(1); seq <= events; seq++ {
				id := fmt.Sprintf("%s-e%d", src, seq)
				if _, err := s.Receive(mkEvent(src, seq, id, "digest-"+id)); err != nil {
					t.Errorf("receive %s/%d: %v", src, seq, err)
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// 查询方：并发读取来源信息与积压。
			for j := 0; j < 200; j++ {
				if _, err := s.GetSource(src); err != nil {
					t.Errorf("getsource: %v", err)
				}
				if _, err := s.GetBacklog(src); err != nil {
					t.Errorf("backlog: %v", err)
				}
			}
		}()
	}
	// 单个投递方不停跨来源领取并确认，与接收/查询并发，压测共享结构。
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		total := sources * events
		acked := 0
		deadline := time.Now().Add(5 * time.Second)
		for acked < total && time.Now().Before(deadline) {
			due := s.ClaimDue(time.Now(), time.Second, sources)
			if len(due) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			for _, d := range due {
				if err := s.Ack(d.Key, d.Attempt); err != nil {
					t.Errorf("ack %s: %v", d.Key, err)
				}
				acked++
			}
		}
		if acked != total {
			t.Errorf("dispatcher acked %d, want %d", acked, total)
		}
	}()
	close(start)
	wg.Wait()

	bl, err := s.GetBacklog("src00")
	if err != nil {
		t.Fatal(err)
	}
	if bl.LastDeliveredSeq != events || bl.PendingDelivery != 0 {
		t.Fatalf("src00 not fully delivered: %+v", bl)
	}
}

func pseudoPerm(n int) []int {
	perm := make([]int, n)
	for i := range perm {
		perm[i] = i
	}
	// LCG 洗牌（确定性）。
	state := uint64(0x12345678)
	for i := n - 1; i > 0; i-- {
		state = state*6364136223846793005 + 1442695040888963407
		j := int((state >> 33) % uint64(i+1))
		perm[i], perm[j] = perm[j], perm[i]
	}
	return perm
}
