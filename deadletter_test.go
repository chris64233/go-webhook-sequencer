package webhooksequencer

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// shiftTime 把存储的时钟向前拨，用于模拟租约过期。
func shiftTime(store *Store, d time.Duration) {
	now := store.now()
	store.now = func() time.Time { return now.Add(d) }
}

func setupDL(t *testing.T, maxAttempts int, seqs ...int64) (*Service, *Store) {
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

// claimAndFail 领取最多 limit 条可领取的投递，并对指定键上报发送失败。
// 返回本次领取结果。
func claimAndFail(store *Store, owner string, limit int, failKeys ...string) []Delivery {
	claimed := store.Claim(owner, limit, time.Minute)
	for _, dv := range claimed {
		for _, k := range failKeys {
			if dv.Key == k {
				store.Fail(dv.Key, dv.LeaseID, errors.New("send boom"))
			}
		}
	}
	return claimed
}

// 达到最大投递次数后进入死信：死信记录保存最后一次失败、稳定投递键和
// 来源序号；同来源后续序号不得绕过它被领取或确认。
func TestDeadLetterBlocksLaterSeqs(t *testing.T) {
	svc, store := setupDL(t, 2, 1, 2, 3)
	key1 := deliveryKey("s", "s-e1")
	key2 := deliveryKey("s", "s-e2")

	// 第一轮：1 被领取（短租约）并发送失败；2 被领取，租约全程有效。
	c1 := store.Claim("w1", 1, time.Minute)
	if len(c1) != 1 || c1[0].Key != key1 {
		t.Fatalf("round1 claimed %+v", c1)
	}
	store.Fail(key1, c1[0].LeaseID, errors.New("send boom"))
	c2 := store.Claim("w1", 1, time.Hour)
	if len(c2) != 1 || c2[0].Key != key2 {
		t.Fatalf("round1b claimed %+v", c2)
	}
	lease2 := c2[0].LeaseID // 序号 2 的当前租约

	// 第二轮（租约过期后）：1 再次被领取并失败，达到最大投递次数。
	shiftTime(store, 2*time.Minute)
	c3 := store.Claim("w1", 1, time.Minute)
	if len(c3) != 1 || c3[0].Key != key1 {
		t.Fatalf("round2 claimed %+v", c3)
	}
	store.Fail(key1, c3[0].LeaseID, errors.New("send boom"))

	// 第三轮：1 转入死信；2、3 被阻塞，不能被领取。
	shiftTime(store, 2*time.Minute)
	if got := store.Claim("w1", 100, time.Minute); len(got) != 0 {
		t.Fatalf("blocked source must not be claimed: %+v", got)
	}

	dls := svc.DeadLetters("s")
	if len(dls) != 1 {
		t.Fatalf("dead letters: %+v", dls)
	}
	dl := dls[0]
	if dl.Key != key1 || dl.Seq != 1 || dl.EventID != "s-e1" || dl.Status != DeadLetterOpen {
		t.Fatalf("dead letter: %+v", dl)
	}
	if dl.Attempts != 2 || dl.LastError != "send boom" || dl.LastFailedAt.IsZero() {
		t.Fatalf("dead letter must keep last failure: %+v", dl)
	}

	// 后续序号不得绕过死信确认：序号 2 持有有效租约，确认仍被拒绝。
	if err := svc.AckDelivery(key2, lease2); !errors.Is(err, ErrSourceBlocked) {
		t.Fatalf("ack later seq should be blocked, got %v", err)
	}
	if got := svc.BlockingSeq("s"); got != 1 {
		t.Fatalf("blocking seq = %d", got)
	}
	b, _ := svc.Backlog("s")
	if b.DeadLetters != 1 || b.BlockedSeq != 1 {
		t.Fatalf("backlog: %+v", b)
	}
}

// 重试死信：沿用原投递键生成新的领取尝试，确认成功后序列解除阻塞。
func TestRetryDeadLetterReusesKeyAndUnblocks(t *testing.T) {
	svc, store := setupDL(t, 1, 1, 2)
	key1 := deliveryKey("s", "s-e1")
	key2 := deliveryKey("s", "s-e2")

	claimAndFail(store, "w1", 1, key1) // 只领取序号 1
	shiftTime(store, 2*time.Minute)
	store.Claim("w1", 100, time.Minute) // 触发死信转移
	if got := svc.BlockingSeq("s"); got != 1 {
		t.Fatalf("blocking seq = %d", got)
	}

	dl, err := svc.RetryDeadLetter(key1)
	if err != nil {
		t.Fatal(err)
	}
	if dl.Status != DeadLetterRetried {
		t.Fatalf("dead letter after retry: %+v", dl)
	}
	// 已处置的死信不能重复处置。
	if _, err := svc.RetryDeadLetter(key1); !errors.Is(err, ErrDeadLetterNotOpen) {
		t.Fatalf("re-retry: got %v", err)
	}

	// 重新入队后沿用原投递键，Attempts 归零开始新一轮。
	claimed := store.Claim("w2", 1, time.Minute)
	if len(claimed) != 1 || claimed[0].Key != key1 || claimed[0].Attempts != 1 {
		t.Fatalf("replay claim: %+v", claimed)
	}
	if err := svc.AckDelivery(key1, claimed[0].LeaseID); err != nil {
		t.Fatal(err)
	}

	// 阻塞解除，后续序号可以继续确认。
	if got := svc.BlockingSeq("s"); got != 0 {
		t.Fatalf("still blocked at %d", got)
	}
	claimed = store.Claim("w2", 100, time.Minute)
	if len(claimed) != 1 || claimed[0].Key != key2 {
		t.Fatalf("after unblock: %+v", claimed)
	}
	if err := svc.AckDelivery(key2, claimed[0].LeaseID); err != nil {
		t.Fatal(err)
	}
}

// 同一来源存在多个死信时必须按序号处置，不能先重放或跳过后面的事件。
func TestDeadLettersDisposedInSeqOrder(t *testing.T) {
	svc, store := setupDL(t, 1, 1, 2, 3)
	key1 := deliveryKey("s", "s-e1")
	key2 := deliveryKey("s", "s-e2")
	key3 := deliveryKey("s", "s-e3")

	claimAndFail(store, "w1", 2, key1, key2)
	shiftTime(store, 2*time.Minute)
	store.Claim("w1", 100, time.Minute) // 1、2 进入死信，3 被阻塞

	if _, err := svc.RetryDeadLetter(key2); !errors.Is(err, ErrDeadLetterOrder) {
		t.Fatalf("retry later dead letter: got %v", err)
	}
	if _, err := svc.SkipDeadLetter(key2, "d-x", "reason"); !errors.Is(err, ErrDeadLetterOrder) {
		t.Fatalf("skip later dead letter: got %v", err)
	}

	// 按序号处置：先重试 1，再跳过 2，之后 3 可以继续。
	if _, err := svc.RetryDeadLetter(key1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SkipDeadLetter(key2, "d-2", "下游确认已处理"); err != nil {
		t.Fatal(err)
	}
	if got := svc.BlockingSeq("s"); got != 0 {
		t.Fatalf("still blocked at %d", got)
	}
	claimed := store.Claim("w2", 100, time.Minute)
	if len(claimed) != 2 || claimed[0].Key != key1 || claimed[1].Key != key3 {
		t.Fatalf("claim after disposal: %+v", claimed)
	}
}

// 跳过死信：需要原因和唯一决定号；跳过是终态，迟到确认不能恢复；
// 同一决定号重复提交幂等，决定号冲突报错。
func TestSkipDeadLetter(t *testing.T) {
	svc, store := setupDL(t, 1, 1, 2)
	key1 := deliveryKey("s", "s-e1")

	c1 := claimAndFail(store, "w1", 1, key1)
	staleLease := c1[0].LeaseID
	shiftTime(store, 2*time.Minute)
	store.Claim("w1", 100, time.Minute) // 1 进入死信

	// 参数校验：原因和决定号不能为空。
	if _, err := svc.SkipDeadLetter(key1, "", "reason"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty decision id: got %v", err)
	}
	if _, err := svc.SkipDeadLetter(key1, "d-1", ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty reason: got %v", err)
	}

	dec, err := svc.SkipDeadLetter(key1, "d-1", "下游确认重复事件，无需投递")
	if err != nil {
		t.Fatal(err)
	}
	if dec.Type != DecisionSkip || dec.Seq != 1 || dec.Key != key1 || dec.Reason == "" {
		t.Fatalf("decision: %+v", dec)
	}

	// 同一决定号重复提交：幂等返回首次决定，不重复生成。
	again, err := svc.SkipDeadLetter(key1, "d-1", "下游确认重复事件，无需投递")
	if err != nil || again.ID != dec.ID {
		t.Fatalf("idempotent replay: %+v %v", again, err)
	}
	// 决定号被用于其他死信：冲突。
	if _, err := svc.SkipDeadLetter("s/other", "d-1", "reason"); !errors.Is(err, ErrDecisionConflict) {
		t.Fatalf("decision conflict: got %v", err)
	}
	// 已跳过的死信不能再处置。
	if _, err := svc.RetryDeadLetter(key1); !errors.Is(err, ErrDeadLetterNotOpen) {
		t.Fatalf("retry after skip: got %v", err)
	}

	// 跳过是终态：迟到的确认不能把它恢复为已投递。
	if err := svc.AckDelivery(key1, staleLease); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("late ack after skip: got %v", err)
	}
	dv, _ := store.Delivery(key1)
	if dv.Status != DeliverySkipped {
		t.Fatalf("delivery after late ack: %+v", dv)
	}

	// 序列继续推进：序号 2 可以正常领取确认。
	if got := svc.BlockingSeq("s"); got != 0 {
		t.Fatalf("still blocked at %d", got)
	}
	claimed := store.Claim("w2", 100, time.Minute)
	if len(claimed) != 1 || claimed[0].Seq != 2 {
		t.Fatalf("claim after skip: %+v", claimed)
	}
	if err := svc.AckDelivery(claimed[0].Key, claimed[0].LeaseID); err != nil {
		t.Fatal(err)
	}

	// 处置决定可查询。
	decs := svc.Decisions("s")
	if len(decs) != 1 || decs[0].ID != "d-1" {
		t.Fatalf("decisions: %+v", decs)
	}
}

// 租约被接管后，旧发送者不得确认当前任务；只有当前领取尝试能确认成功。
func TestStaleLeaseCannotAck(t *testing.T) {
	svc, store := setupDL(t, 5, 1)
	key := deliveryKey("s", "s-e1")

	first := store.Claim("worker-1", 10, time.Minute)
	shiftTime(store, 2*time.Minute)
	second := store.Claim("worker-2", 10, time.Minute)
	if len(second) != 1 || second[0].LeaseID == first[0].LeaseID {
		t.Fatalf("takeover claim: %+v", second)
	}

	// 旧发送者的迟到确认被拒绝，且不影响当前租约。
	if err := svc.AckDelivery(key, first[0].LeaseID); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale ack: got %v", err)
	}
	dv, _ := store.Delivery(key)
	if dv.Status != DeliveryLeased || dv.LeaseID != second[0].LeaseID {
		t.Fatalf("delivery after stale ack: %+v", dv)
	}
	// 当前租约持有者确认成功。
	if err := svc.AckDelivery(key, second[0].LeaseID); err != nil {
		t.Fatal(err)
	}
}

// 领取尝试日志记录每次租约的结果：失败、过期、确认。
func TestAttemptLogRecordsOutcomes(t *testing.T) {
	svc, store := setupDL(t, 5, 1)
	key := deliveryKey("s", "s-e1")

	c1 := claimAndFail(store, "w1", 1, key) // 尝试 1：失败
	shiftTime(store, 2*time.Minute)
	c2 := store.Claim("w2", 10, time.Minute) // 尝试 2：领取时尝试 1 已过期
	if err := svc.AckDelivery(key, c2[0].LeaseID); err != nil {
		t.Fatal(err)
	}

	attempts := svc.DeliveryAttempts(key)
	if len(attempts) != 2 {
		t.Fatalf("attempts: %+v", attempts)
	}
	if attempts[0].ID != c1[0].LeaseID || attempts[0].Status != AttemptFailed {
		t.Fatalf("attempt 1: %+v", attempts[0])
	}
	if attempts[1].ID != c2[0].LeaseID || attempts[1].Status != AttemptAcked || attempts[1].Owner != "w2" {
		t.Fatalf("attempt 2: %+v", attempts[1])
	}
}

// 进程重启后从持久化状态继续：死信、处置决定、领取尝试不丢失、不重复生成。
func TestDeadLetterSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.json")

	s1 := mustOpen(t, path)
	s1.MaxAttempts = 1
	svc1 := NewService(s1)
	svc1.InitSource("s", 1)
	svc1.Receive(ev("s", 1))
	svc1.Receive(ev("s", 2))
	key1 := deliveryKey("s", "s-e1")

	claimAndFail(s1, "w1", 1, key1)
	shiftTime(s1, 2*time.Minute)
	s1.Claim("w1", 100, time.Minute) // 1 进入死信
	if _, err := svc1.SkipDeadLetter(key1, "d-1", "人工核对后跳过"); err != nil {
		t.Fatal(err)
	}

	// 重启：死信与决定原样恢复，不会重复生成。
	s2 := mustOpen(t, path)
	svc2 := NewService(s2)
	dls := svc2.DeadLetters("s")
	if len(dls) != 1 || dls[0].Status != DeadLetterSkipped || dls[0].LastError != "send boom" {
		t.Fatalf("dead letters after restart: %+v", dls)
	}
	decs := svc2.Decisions("s")
	if len(decs) != 1 || decs[0].ID != "d-1" {
		t.Fatalf("decisions after restart: %+v", decs)
	}
	if got := svc2.BlockingSeq("s"); got != 0 {
		t.Fatalf("blocked after restart: %d", got)
	}
	// 重复提交同一决定号：幂等，不重复生成决定。
	if _, err := svc2.SkipDeadLetter(key1, "d-1", "人工核对后跳过"); err != nil {
		t.Fatalf("idempotent skip after restart: %v", err)
	}
	if got := svc2.Decisions("s"); len(got) != 1 {
		t.Fatalf("duplicated decisions: %+v", got)
	}
	// 再跑一轮领取不会产生新的死信，序号 2 正常领取。
	claimed := s2.Claim("w2", 100, time.Minute)
	if len(claimed) != 1 || claimed[0].Seq != 2 {
		t.Fatalf("claim after restart: %+v", claimed)
	}
	if got := svc2.DeadLetters("s"); len(got) != 1 {
		t.Fatalf("duplicated dead letters: %+v", got)
	}
	if got := svc2.DeliveryAttempts(key1); len(got) != 1 {
		t.Fatalf("attempts after restart: %+v", got)
	}
}

// 死信重试后再次失败会重新进入死信（同一键，不复制事件）。
func TestRetriedDeliveryCanDeadLetterAgain(t *testing.T) {
	svc, store := setupDL(t, 1, 1)
	key := deliveryKey("s", "s-e1")

	claimAndFail(store, "w1", 1, key)
	shiftTime(store, 2*time.Minute)
	store.Claim("w1", 100, time.Minute)
	if _, err := svc.RetryDeadLetter(key); err != nil {
		t.Fatal(err)
	}

	// 新一轮仍然失败，再次进入死信，记录被更新而不是复制。
	claimAndFail(store, "w2", 1, key)
	shiftTime(store, 2*time.Minute)
	store.Claim("w2", 100, time.Minute)
	dls := svc.DeadLetters("s")
	if len(dls) != 1 || dls[0].Status != DeadLetterOpen || dls[0].Attempts != 1 {
		t.Fatalf("dead letters after second round: %+v", dls)
	}
}

// Dispatcher 集成：达到最大投递次数后进入死信并阻塞同来源后续事件；
// 重试后按序恢复投递。
func TestDispatcherDeadLetterAndReplay(t *testing.T) {
	svc, store := setupDL(t, 2, 1, 2)
	key1 := deliveryKey("s", "s-e1")
	tr := &recordingTransport{failKeys: map[string]bool{key1: true}}
	d := NewDispatcher(store, tr)
	d.Lease = time.Minute
	d.Batch = 1 // 每次只领一条，便于观察阻塞

	// 三轮投递：序号 1 两次失败进入死信；序号 2 被阻塞，不会被发送。
	for i := 0; i < 3; i++ {
		if _, err := d.DispatchOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		shiftTime(store, 2*time.Minute)
	}
	if got := svc.BlockingSeq("s"); got != 1 {
		t.Fatalf("blocking seq = %d", got)
	}
	// 传输层只记录成功发送：序号 1 从未成功，序号 2 被阻塞未发送。
	if got := tr.keys(); len(got) != 0 {
		t.Fatalf("blocked/dead events must not be sent: %v", got)
	}

	// 修复传输层并重试死信：按序恢复投递。
	tr.failKeys = map[string]bool{}
	if _, err := svc.RetryDeadLetter(key1); err != nil {
		t.Fatal(err)
	}
	if sent, _ := d.DispatchOnce(context.Background()); sent != 1 {
		t.Fatalf("replay round sent=%d", sent)
	}
	if sent, _ := d.DispatchOnce(context.Background()); sent != 1 {
		t.Fatalf("unblocked round sent=%d", sent)
	}
	want := []string{key1, deliveryKey("s", "s-e2")}
	if got := tr.keys(); !equalStrings(got, want) {
		t.Fatalf("send order %v, want %v", got, want)
	}
	b, _ := svc.Backlog("s")
	if b.Acked != 2 || b.DeadLetters != 0 {
		t.Fatalf("backlog: %+v", b)
	}
}

// 不同来源的死信互不影响：一个来源被阻塞时，另一来源仍可并行推进。
func TestDeadLetterDoesNotBlockOtherSources(t *testing.T) {
	svc, store := setupDL(t, 1, 1)
	keyS := deliveryKey("s", "s-e1")

	claimAndFail(store, "w1", 1, keyS)
	shiftTime(store, 2*time.Minute)
	store.Claim("w1", 100, time.Minute) // s-e1 进入死信
	if got := svc.BlockingSeq("s"); got != 1 {
		t.Fatalf("blocking seq s = %d", got)
	}

	// 来源 s 被阻塞期间，另一来源的事件照常接收、领取、确认。
	if err := svc.InitSource("other", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Receive(ev("other", 1)); err != nil {
		t.Fatal(err)
	}
	claimed := store.Claim("w1", 100, time.Minute)
	if len(claimed) != 1 || claimed[0].SourceID != "other" {
		t.Fatalf("other source should still be claimed: %+v", claimed)
	}
	if err := svc.AckDelivery(claimed[0].Key, claimed[0].LeaseID); err != nil {
		t.Fatal(err)
	}
}

// 查询接口：死信列表、领取尝试、处置决定、阻塞序号、积压统计。
func TestDeadLetterQueries(t *testing.T) {
	svc, store := setupDL(t, 1, 1, 2)
	key1 := deliveryKey("s", "s-e1")

	claimAndFail(store, "w1", 1, key1)
	shiftTime(store, 2*time.Minute)
	store.Claim("w1", 100, time.Minute)

	if _, err := svc.SkipDeadLetter(key1, "d-1", "人工核对"); err != nil {
		t.Fatal(err)
	}

	dls := svc.DeadLetters("")
	if len(dls) != 1 || dls[0].Key != key1 || dls[0].Status != DeadLetterSkipped {
		t.Fatalf("dead letters: %+v", dls)
	}
	attempts := svc.DeliveryAttempts(key1)
	if len(attempts) != 1 || attempts[0].Status != AttemptFailed {
		t.Fatalf("attempts: %+v", attempts)
	}
	decs := svc.Decisions("")
	if len(decs) != 1 || decs[0].Reason != "人工核对" {
		t.Fatalf("decisions: %+v", decs)
	}
	if got := svc.BlockingSeq("s"); got != 0 {
		t.Fatalf("blocking seq = %d", got)
	}
	b, err := svc.Backlog("s")
	if err != nil {
		t.Fatal(err)
	}
	if b.DeadLetters != 0 || b.BlockedSeq != 0 || b.Pending != 1 {
		t.Fatalf("backlog: %+v", b)
	}
	// 不存在的死信。
	if _, err := svc.RetryDeadLetter("s/ghost"); !errors.Is(err, ErrDeadLetterNotFound) {
		t.Fatalf("retry ghost: got %v", err)
	}
	if _, err := svc.SkipDeadLetter("s/ghost", "d-2", "r"); !errors.Is(err, ErrDeadLetterNotFound) {
		t.Fatalf("skip ghost: got %v", err)
	}
}
