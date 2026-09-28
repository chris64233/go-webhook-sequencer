package webhooksequencer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func deadSetup(t *testing.T, maxAttempts int, seqs ...int64) (*Service, *Store) {
	t.Helper()
	store := NewMemoryStore()
	store.MaxAttempts = maxAttempts
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

// 连续失败达到上限后进入死信：记录只含键、序号、最后失败，不含负载；
// 后续序号被释放回 pending 并被阻塞，既不能领取也不能确认，不会绕过死信。
func TestDeadLetterAfterMaxAttemptsBlocksFollowing(t *testing.T) {
	svc, store := deadSetup(t, 2, 1, 2, 3)
	key1 := deliveryKey("s", "s-e1")
	tr := &recordingTransport{failKeys: map[string]bool{key1: true}}
	d := NewDispatcher(store, tr)
	d.Lease = time.Hour

	// 第 1 次失败：未达上限，不进死信；同批次 seq2/3 本轮不再发送。
	if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
		t.Fatalf("sent=%d", sent)
	}
	dv1, _ := store.Delivery(key1)
	if dv1.Failures != 1 || dv1.Status != DeliveryLeased {
		t.Fatalf("seq1 after first failure: %+v", dv1)
	}
	if got := svc.DeadLetters("s", true); len(got) != 0 {
		t.Fatalf("should not be dead after 1 failure: %+v", got)
	}

	// 租约过期，第 2 次失败：seq1 进入死信，seq2/3 在此前批次被释放回 pending。
	store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
		t.Fatalf("sent=%d", sent)
	}

	dls := svc.DeadLetters("s", true)
	if len(dls) != 1 {
		t.Fatalf("dead letters: %+v", dls)
	}
	dl := dls[0]
	if dl.Key != key1 || dl.Seq != 1 || dl.Episode != 1 || dl.Failures != 2 {
		t.Fatalf("dead letter record: %+v", dl)
	}
	if dl.LastError != "transport error" {
		t.Fatalf("last failure not preserved: %q", dl.LastError)
	}

	// 死信之后序号 2、3 不得被领取（不会绕过死信继续确认）。
	store.now = func() time.Time { return time.Now().Add(3 * time.Hour) }
	if got := store.Claim("w", 10, time.Hour); len(got) != 0 {
		t.Fatalf("following seqs must not pass dead letter: %+v", got)
	}
	// 没有当前领取尝试的序号 2 不能被直接确认。
	if err := store.Ack(deliveryKey("s", "s-e2"), 1); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("ack without current lease: got %v", err)
	}

	b, _ := svc.Backlog("s")
	if b.Dead != 1 || b.BlockedSeq != 1 || b.Pending != 2 || b.Leased != 0 {
		t.Fatalf("backlog: %+v", b)
	}
}

// 重试生成新的领取尝试但沿用原投递键；成功后阻塞解除，后续序号继续投递。
func TestRetryReusesKeyWithNewAttemptAndUnblocks(t *testing.T) {
	svc, store := deadSetup(t, 1, 1, 2)
	key := deliveryKey("s", "s-e1")
	d := NewDispatcher(store, &recordingTransport{failKeys: map[string]bool{key: true}})
	d.Lease = time.Hour
	if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
		t.Fatalf("sent=%d", sent)
	}
	if got := svc.DeadLetters("s", true); len(got) != 1 {
		t.Fatal("expected dead letter with maxAttempts=1")
	}
	dead, _ := store.Delivery(key)

	dec, err := svc.RetryDeadLetter("s", 1, "decision-retry-1")
	if err != nil {
		t.Fatal(err)
	}
	if dec.Kind != DecisionRetry || dec.Key != key {
		t.Fatalf("decision: %+v", dec)
	}
	// 同一决定号重放幂等，不产生新决定。
	again, err := svc.RetryDeadLetter("s", 1, "decision-retry-1")
	if err != nil || again.ID != dec.ID {
		t.Fatalf("idempotent retry: %+v err=%v", again, err)
	}
	// 决定号被不同内容占用 → 冲突。
	if _, err := svc.RetryDeadLetter("s", 2, "decision-retry-1"); !errors.Is(err, ErrDecisionConflict) {
		t.Fatalf("decision conflict: got %v", err)
	}

	// 重试后投递回到 pending，失败计数清零，原键不变；旧尝试号不能确认。
	red, _ := store.Delivery(key)
	if red.Status != DeliveryPending || red.Failures != 0 || red.Key != key {
		t.Fatalf("retried delivery: %+v", red)
	}
	if err := store.Ack(key, dead.Attempts); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("old attempt ack after retry: got %v", err)
	}

	// 传输恢复：死信处置后阻塞解除，本轮按序领取 seq1（新尝试号、原键）
	// 与 seq2 并逐条确认。
	d.transport = &recordingTransport{failKeys: map[string]bool{}}
	if sent, _ := d.DispatchOnce(context.Background()); sent != 2 {
		t.Fatalf("retry round sent=%d, want 2 (retry + following)", sent)
	}
	sentKeys := d.transport.(*recordingTransport).keys()
	if len(sentKeys) != 2 || sentKeys[0] != key || sentKeys[1] != deliveryKey("s", "s-e2") {
		t.Fatalf("out-of-order delivery after retry: %v", sentKeys)
	}
	retry := d.transport.(*recordingTransport).sent[0]
	if retry.Key != key || retry.Attempts <= dead.Attempts {
		t.Fatalf("retry must reuse key with new attempt: old=%d new=%d", dead.Attempts, retry.Attempts)
	}
	b, _ := svc.Backlog("s")
	if b.Acked != 2 || b.BlockedSeq != 0 || b.Dead != 0 {
		t.Fatalf("backlog after retry success: %+v", b)
	}
}

// 跳过需要原因和唯一决定号；生效后该序号为终态，迟到成功不能恢复，
// 且连续序列继续释放。
func TestSkipDisposesSeqAndRejectsLateAck(t *testing.T) {
	svc, store := deadSetup(t, 1, 1, 2)
	key := deliveryKey("s", "s-e1")
	d := NewDispatcher(store, &recordingTransport{failKeys: map[string]bool{key: true}})
	d.Lease = time.Hour
	d.DispatchOnce(context.Background())

	if _, err := svc.SkipDeadLetter("s", 1, "dec-skip-1", ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("skip without reason: got %v", err)
	}
	if _, err := svc.SkipDeadLetter("s", 1, "", "give up"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("skip without decision id: got %v", err)
	}

	// worker 在跳过生效前持有的尝试号（此时投递已是 dead，记录尝试号）。
	dl, _ := store.Delivery(key)
	staleAttempt := dl.Attempts

	dec, err := svc.SkipDeadLetter("s", 1, "dec-skip-1", "下游明确不再需要")
	if err != nil {
		t.Fatal(err)
	}
	if dec.Kind != DecisionSkip || dec.Reason != "下游明确不再需要" {
		t.Fatalf("decision: %+v", dec)
	}
	// 同号同内容重放幂等；改原因即冲突。
	if _, err := svc.SkipDeadLetter("s", 1, "dec-skip-1", "下游明确不再需要"); err != nil {
		t.Fatalf("idempotent skip: %v", err)
	}
	if _, err := svc.SkipDeadLetter("s", 1, "dec-skip-1", "另一个原因"); !errors.Is(err, ErrDecisionConflict) {
		t.Fatalf("skip decision conflict: got %v", err)
	}

	got, _ := store.Delivery(key)
	if got.Status != DeliverySkipped {
		t.Fatalf("delivery should be skipped, got %+v", got)
	}
	// 旧发送者迟到的成功确认不能把 skipped 恢复为 acked。
	if err := store.Ack(key, staleAttempt); !errors.Is(err, ErrDeliveryDisposed) {
		t.Fatalf("late ack after skip: got %v", err)
	}
	got, _ = store.Delivery(key)
	if got.Status != DeliverySkipped {
		t.Fatalf("late success must not restore skipped delivery: %+v", got)
	}
	// 再次处置已处置序号 → 不是待处置死信。
	if _, err := svc.RetryDeadLetter("s", 1, "x"); !errors.Is(err, ErrNotDeadLetter) {
		t.Fatalf("retry resolved: got %v", err)
	}

	// 阻塞解除，序号 2 可领取并确认。
	tr2 := &recordingTransport{failKeys: map[string]bool{}}
	d2 := NewDispatcher(store, tr2)
	if sent, _ := d2.DispatchOnce(context.Background()); sent != 1 {
		t.Fatalf("following seq should release after skip: sent=%d", sent)
	}
	b, _ := svc.Backlog("s")
	if b.Skipped != 1 || b.Acked != 1 || b.BlockedSeq != 0 {
		t.Fatalf("backlog after skip: %+v", b)
	}
}

// 旧发送者在租约被接管后不得确认当前任务（栅栏）。
func TestStaleAttemptCannotAckAfterLeaseTakeover(t *testing.T) {
	_, store := deadSetup(t, 5, 1)
	key := deliveryKey("s", "s-e1")

	first := store.Claim("worker-1", 10, time.Minute)
	if len(first) != 1 {
		t.Fatalf("claim: %d", len(first))
	}
	oldAttempt := first[0].Attempts

	// 租约过期被 worker-2 接管，尝试号递增。
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	second := store.Claim("worker-2", 10, time.Minute)
	if len(second) != 1 || second[0].Attempts != oldAttempt+1 {
		t.Fatalf("takeover claim: %+v", second)
	}

	// worker-1 的迟到成功确认被拒绝；当前任务仍归 worker-2。
	if err := store.Ack(key, oldAttempt); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale ack: got %v", err)
	}
	dv, _ := store.Delivery(key)
	if dv.Status != DeliveryLeased || dv.Owner != "worker-2" {
		t.Fatalf("delivery after stale ack: %+v", dv)
	}
	// worker-2 的当前尝试可以确认；worker-1 的迟到失败上报也被拒绝。
	if err := store.ReportFailure(key, oldAttempt, "late failure"); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale failure report: got %v", err)
	}
	if err := store.Ack(key, second[0].Attempts); err != nil {
		t.Fatalf("current ack: %v", err)
	}
}

// 同一来源多个死信必须按序号处置，不能先重放后面的。
func TestDeadLettersMustResolveInSeqOrder(t *testing.T) {
	store := NewMemoryStore()
	store.MaxAttempts = 1
	svc := NewService(store)
	svc.InitSource("s", 1)
	svc.InitSource("o", 1)
	for i := int64(1); i <= 3; i++ {
		svc.Receive(ev("s", i))
	}
	svc.Receive(ev("o", 1))

	// seq1 投递失败进入死信（同批次 seq2/seq3 被释放回 pending 等待）。
	k1 := deliveryKey("s", "s-e1")
	c1 := store.Claim("w", 10, time.Hour)
	if err := store.ReportFailure(k1, c1[0].Attempts, "e"); err != nil {
		t.Fatal(err)
	}
	if got := store.Claim("w", 10, time.Hour); len(got) != 0 {
		t.Fatalf("seq2/3 must not be claimable behind dead seq1: %+v", got)
	}

	// 白盒构造 seq2、seq3 也处于死信：模拟其租约来自 seq1 死信之前的在途
	// 发送，独立完成了失败上报。此时同一来源同时存在三个未处置死信。
	for _, seq := range []int64{2, 3} {
		k := deliveryKey("s", fmt.Sprintf("s-e%d", seq))
		store.mu.Lock()
		dd := store.data.Deliveries[k]
		dd.Status = DeliveryDead
		dd.Failures = 1
		dd.LastFailure = "e"
		store.data.DeadLetters[deadLetterKey(k, 1)] = &DeadLetter{
			Episode: 1, Key: k, SourceID: "s", Seq: seq, LastError: "e", Failures: 1, DeadAt: store.now(),
		}
		store.persistLocked()
		store.mu.Unlock()
	}
	k2 := deliveryKey("s", "s-e2")
	_ = k2

	// 必须先处置 seq1：处置 seq2/seq3 被顺序拒绝。
	if _, err := svc.SkipDeadLetter("s", 2, "s2", "late"); !errors.Is(err, ErrDeadLetterOrder) {
		t.Fatalf("skip 2 before 1: got %v", err)
	}
	if _, err := svc.RetryDeadLetter("s", 3, "r3"); !errors.Is(err, ErrDeadLetterOrder) {
		t.Fatalf("retry 3 before 1: got %v", err)
	}
	if _, err := svc.SkipDeadLetter("s", 1, "s1", "give up on 1"); err != nil {
		t.Fatal(err)
	}
	// seq1 处置后 seq2 成为阻塞点，仍不能处置 seq3。
	if _, err := svc.SkipDeadLetter("s", 3, "s3", "late"); !errors.Is(err, ErrDeadLetterOrder) {
		t.Fatalf("skip 3 before 2: got %v", err)
	}
	if _, err := svc.SkipDeadLetter("s", 2, "s2", "give up on 2"); err != nil {
		t.Fatalf("skip 2 in order: %v", err)
	}

	// 另一来源 o 全程可并行领取，不受 s 阻塞（推进时间使其早先租约到期）。
	store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	co := store.Claim("w", 10, time.Hour)
	found := false
	for _, d := range co {
		if d.SourceID == "o" {
			found = true
		}
	}
	if !found {
		t.Fatalf("other source should be claimable in parallel: %+v", co)
	}
}

// 重试后再次失败：产生新一轮死信记录，处置后历史轮次都保留且可查。
func TestRepeatedFailureCreatesNewEpisode(t *testing.T) {
	svc, store := deadSetup(t, 1, 1)
	key := deliveryKey("s", "s-e1")
	tr := &recordingTransport{failKeys: map[string]bool{key: true}}
	d := NewDispatcher(store, tr)

	// 首次失败进入第 1 轮死信。
	if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
		t.Fatalf("initial send should fail")
	}
	for episode := 1; episode <= 2; episode++ {
		if dls := svc.DeadLetters("s", true); len(dls) != 1 || dls[0].Episode != episode {
			t.Fatalf("episode %d unresolved: %+v", episode, dls)
		}
		if _, err := svc.RetryDeadLetter("s", 1, fmt.Sprintf("retry-%d", episode)); err != nil {
			t.Fatal(err)
		}
		if sent, _ := d.DispatchOnce(context.Background()); sent != 0 {
			t.Fatalf("episode %d should fail again", episode)
		}
	}
	// 历史轮次都保留：第 1、2 轮已标重试，第 3 轮未处置。
	all := svc.DeadLetters("s", false)
	if len(all) != 3 {
		t.Fatalf("dead letter history: %+v", all)
	}
	if all[0].Episode != 1 || all[0].Resolution != ResolutionRetried || all[0].ResolvedAt.IsZero() {
		t.Fatalf("episode 1 history: %+v", all[0])
	}
	if all[1].Episode != 2 || all[1].Resolution != ResolutionRetried {
		t.Fatalf("episode 2 history: %+v", all[1])
	}
	if all[2].Episode != 3 || !all[2].ResolvedAt.IsZero() {
		t.Fatalf("episode 3 should be unresolved: %+v", all[2])
	}
}

// 进程重启后死信与决定持久化，不重复生成；可从持久化状态继续处置。
func TestDeadLetterStateSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")
	store1 := mustOpen(t, path)
	store1.MaxAttempts = 1
	svc1 := NewService(store1)
	svc1.InitSource("s", 1)
	svc1.Receive(ev("s", 1))
	svc1.Receive(ev("s", 2))
	key := deliveryKey("s", "s-e1")
	c := store1.Claim("w", 10, time.Hour)
	if err := store1.ReportFailure(key, c[0].Attempts, "persistent failure"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc1.SkipDeadLetter("s", 1, "dec-restart-1", "重启前跳过"); err != nil {
		t.Fatal(err)
	}

	store2 := mustOpen(t, path)
	store2.MaxAttempts = 1
	svc2 := NewService(store2)

	// 死信与决定都持久化，重启不重复生成。
	all := svc2.DeadLetters("s", false)
	if len(all) != 1 || all[0].Resolution != ResolutionSkipped || all[0].DecisionID != "dec-restart-1" {
		t.Fatalf("dead letters after restart: %+v", all)
	}
	decs := svc2.Decisions()
	if len(decs) != 1 || decs[0].ID != "dec-restart-1" {
		t.Fatalf("decisions after restart: %+v", decs)
	}
	// 同号重放仍幂等。
	if _, err := svc2.SkipDeadLetter("s", 1, "dec-restart-1", "重启前跳过"); err != nil {
		t.Fatalf("idempotent after restart: %v", err)
	}
	if len(svc2.DeadLetters("s", false)) != 1 || len(svc2.Decisions()) != 1 {
		t.Fatal("decision/dead letter must not be duplicated after restart")
	}

	// seq1 已是 skipped 终态；迟到确认仍被拒绝，seq2 可继续投递。
	if err := store2.Ack(key, c[0].Attempts); !errors.Is(err, ErrDeliveryDisposed) {
		t.Fatalf("late ack after restart: %v", err)
	}
	d := NewDispatcher(store2, &recordingTransport{failKeys: map[string]bool{}})
	if sent, _ := d.DispatchOnce(context.Background()); sent != 1 {
		t.Fatalf("resume after restart sent=%d", sent)
	}
	// 领取尝试记录随持久化保留。
	if attempts := svc2.Attempts("s"); len(attempts) < 2 {
		t.Fatalf("attempt history lost: %+v", attempts)
	}
}

// 查询：积压、死信、领取尝试、处置决定与当前阻塞序号。
func TestDeadLetterObservabilityQueries(t *testing.T) {
	svc, store := deadSetup(t, 1, 1, 2)
	k1 := deliveryKey("s", "s-e1")
	c := store.Claim("w", 10, time.Hour)
	if err := store.ReportFailure(k1, c[0].Attempts, "down"); err != nil {
		t.Fatal(err)
	}

	if dls := svc.DeadLetters("", true); len(dls) != 1 {
		t.Fatalf("dead letters: %+v", dls)
	}
	// seq1 的尝试结果为 failed；同批次 seq2 的尝试随死信产生被标过期。
	attempts := svc.Attempts("")
	var a1 *ClaimAttempt
	for i := range attempts {
		if attempts[i].Key == k1 {
			a1 = &attempts[i]
		}
	}
	if a1 == nil || a1.Outcome != AttemptFailed {
		t.Fatalf("seq1 attempt: %+v all=%+v", a1, attempts)
	}
	if decs := svc.Decisions(); len(decs) != 0 {
		t.Fatalf("decisions before resolution: %+v", decs)
	}
	b, _ := svc.Backlog("s")
	if b.BlockedSeq != 1 || b.Dead != 1 || b.Pending != 1 {
		t.Fatalf("backlog: %+v", b)
	}
	if _, err := svc.SkipDeadLetter("s", 1, "dec-q", "queries"); err != nil {
		t.Fatal(err)
	}
	decs := svc.Decisions()
	if len(decs) != 1 || decs[0].Kind != DecisionSkip || decs[0].Seq != 1 || decs[0].Reason != "queries" {
		t.Fatalf("decisions: %+v", decs)
	}
}

// 并发交错：重试、跳过和迟到确认同时发生时，先生效的处置决定结果，
// 另一个决定收到“已非死信”，跳过一旦生效即为终态。
func TestConcurrentRetrySkipAndLateAck(t *testing.T) {
	svc, store := deadSetup(t, 1, 1, 2)
	key := deliveryKey("s", "s-e1")
	c := store.Claim("w", 10, time.Hour)
	staleAttempt := c[0].Attempts
	if err := store.ReportFailure(key, staleAttempt, "fail"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	wg.Add(3)
	go func() { defer wg.Done(); _, err := svc.RetryDeadLetter("s", 1, "c-retry"); errs <- err }()
	go func() { defer wg.Done(); _, err := svc.SkipDeadLetter("s", 1, "c-skip", "concurrent"); errs <- err }()
	go func() {
		defer wg.Done()
		err := svc.AckDelivery(key, staleAttempt)
		// 死信/跳过/过期状态的迟到确认都必须失败，绝不能确认成功。
		if err == nil {
			errs <- fmt.Errorf("late ack unexpectedly succeeded")
			return
		}
		if !errors.Is(err, ErrDeliveryDead) && !errors.Is(err, ErrDeliveryDisposed) && !errors.Is(err, ErrStaleAttempt) {
			errs <- err
		}
	}()
	wg.Wait()
	close(errs)

	var nonBenign int
	for err := range errs {
		// 胜出的处置决定返回 nil、输掉竞争的决定返回 ErrNotDeadLetter，均属预期。
		if err == nil || errors.Is(err, ErrNotDeadLetter) {
			continue
		}
		nonBenign++
		t.Errorf("concurrent op: %v", err)
	}
	if nonBenign > 0 {
		t.FailNow()
	}

	dv, _ := store.Delivery(key)
	switch dv.Status {
	case DeliverySkipped:
		// 跳过先生效：重试已失败，任何迟到确认都不能恢复为已投递。
		if err := svc.AckDelivery(key, staleAttempt); !errors.Is(err, ErrDeliveryDisposed) {
			t.Fatalf("late ack after winning skip: %v", err)
		}
		dv2, _ := store.Delivery(key)
		if dv2.Status != DeliverySkipped {
			t.Fatalf("skip must remain terminal: %+v", dv2)
		}
	case DeliveryPending:
		// 重试先生效：跳过已失败，投递回到待投递，可正常领取。
		if got := store.DeadLetters("s", true); len(got) != 0 {
			t.Fatalf("retry won: no unresolved dead letter should remain: %+v", got)
		}
	default:
		t.Fatalf("unexpected status after race: %+v", dv)
	}
}
