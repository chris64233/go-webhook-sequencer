package webhooksequencer

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func planSeqs(entries []ManualEntry) []int64 {
	var seqs []int64
	for _, e := range entries {
		seqs = append(seqs, e.Seq)
	}
	return seqs
}

func TestPlanReplayValidation(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	if _, err := svc.PlanReplay("", 1, 2, time.Minute); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty source: got %v", err)
	}
	if _, err := svc.PlanReplay("s", 0, 2, time.Minute); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("fromSeq 0: got %v", err)
	}
	if _, err := svc.PlanReplay("s", 3, 2, time.Minute); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("toSeq < fromSeq: got %v", err)
	}
	if _, err := svc.PlanReplay("s", 1, 2, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero window: got %v", err)
	}
	if _, err := svc.PlanReplay("ghost", 1, 2, time.Minute); !errors.Is(err, ErrSourceNotFound) {
		t.Fatalf("unknown source: got %v", err)
	}
	if _, err := svc.ExpireReplay("s"); !errors.Is(err, ErrReplayNotFound) {
		t.Fatalf("expire without plan: got %v", err)
	}
}

// 相同重放请求返回原计划；范围改变返回冲突。
func TestPlanReplayIdempotentAndConflict(t *testing.T) {
	svc := newService(t)
	svc.InitSource("s", 1)
	svc.Receive(ev("s", 1))
	svc.Receive(ev("s", 2))

	p1, err := svc.PlanReplay("s", 1, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if p1.Status != ReplayActive || p1.Position != 1 || p1.Version != 1 {
		t.Fatalf("plan: %+v", p1)
	}
	// 相同范围：返回原计划，窗口参数被忽略，不产生副作用。
	p2, err := svc.PlanReplay("s", 1, 2, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !p2.Deadline.Equal(p1.Deadline) || p2.Version != p1.Version {
		t.Fatalf("same request must return original plan: %+v vs %+v", p1, p2)
	}
	// 范围改变：冲突。
	for _, r := range [][2]int64{{1, 3}, {2, 2}} {
		_, err := svc.PlanReplay("s", r[0], r[1], time.Minute)
		var rce *ReplayConflictError
		if !errors.As(err, &rce) {
			t.Fatalf("range %v: want ReplayConflictError, got %v", r, err)
		}
		if rce.Existing.FromSeq != 1 || rce.Existing.ToSeq != 2 {
			t.Fatalf("conflict detail: %+v", rce)
		}
	}
}

// 已成功投递的事件不得重新入队；确认位置直接跳过已确认序号。
func TestReplaySkipsAcked(t *testing.T) {
	svc, store := setup(t, 1, 2, 3)
	if err := svc.AckDelivery(deliveryKey("s", "s-e1")); err != nil {
		t.Fatal(err)
	}

	plan, err := svc.PlanReplay("s", 1, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Position != 2 {
		t.Fatalf("position should skip acked seq 1: %+v", plan)
	}
	d1, _ := store.Delivery(deliveryKey("s", "s-e1"))
	if d1.Status != DeliveryAcked {
		t.Fatalf("acked delivery must not be re-enqueued: %+v", d1)
	}
	claimed := store.Claim("w", 10, time.Minute)
	if len(claimed) != 2 || claimed[0].Seq != 2 || claimed[1].Seq != 3 {
		t.Fatalf("claimed: %+v", claimed)
	}
}

// 乱序交错：缺口未补齐时重放必须停住，后续事件不得提前入队；
// 新事件补齐缺口后，重放从确认位置继续。
func TestReplayStopsAtGap(t *testing.T) {
	svc, store := setup(t, 1)
	if _, err := svc.Receive(ev("s", 3)); err != nil { // 缺口 2，缓冲
		t.Fatal(err)
	}

	plan, err := svc.PlanReplay("s", 1, 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Position != 1 || plan.Status != ReplayActive {
		t.Fatalf("plan: %+v", plan)
	}
	// 只有 seq 1 入队；seq 3 因缺口 2 不得提前发送。
	claimed := store.Claim("w", 10, time.Minute)
	if len(claimed) != 1 || claimed[0].Seq != 1 {
		t.Fatalf("replay must stop at gap, claimed: %+v", claimed)
	}
	if _, ok := store.Delivery(deliveryKey("s", "s-e3")); ok {
		t.Fatal("seq 3 must not be enqueued while gap at 2")
	}

	// 新事件补齐缺口后，重放继续按序入队。
	if _, err := svc.Receive(ev("s", 2)); err != nil {
		t.Fatal(err)
	}
	claimed = store.Claim("w", 10, time.Minute)
	if len(claimed) != 2 || claimed[0].Seq != 2 || claimed[1].Seq != 3 {
		t.Fatalf("after gap fill, claimed: %+v", claimed)
	}
}

// 确认位置随 Ack 单调前进，全部确认后计划完成。
func TestReplayPositionAdvancesWithAcks(t *testing.T) {
	svc, _ := setup(t, 1, 2, 3)
	if _, err := svc.PlanReplay("s", 1, 3, time.Minute); err != nil {
		t.Fatal(err)
	}
	for i, want := range []int64{2, 3, 4} {
		if err := svc.AckDelivery(deliveryKey("s", fmt.Sprintf("s-e%d", i+1))); err != nil {
			t.Fatal(err)
		}
		plan, _, err := svc.ReplayStatus("s")
		if err != nil {
			t.Fatal(err)
		}
		if plan.Position != want {
			t.Fatalf("after ack %d: position=%d, want %d", i+1, plan.Position, want)
		}
	}
	plan, _, _ := svc.ReplayStatus("s")
	if plan.Status != ReplayCompleted {
		t.Fatalf("plan should complete: %+v", plan)
	}
}

// 窗口过期：未确认序号按原始顺序转入人工处理，事件不删除、不重新编号。
func TestReplayWindowExpiryMovesToManual(t *testing.T) {
	svc, store := setup(t, 1, 2, 3)
	if err := svc.AckDelivery(deliveryKey("s", "s-e1")); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.PlanReplay("s", 1, 3, time.Minute); err != nil {
		t.Fatal(err)
	}

	// 窗口未过期时结算是 no-op。
	plan, _ := svc.ExpireReplay("s")
	if plan.Status != ReplayActive {
		t.Fatalf("not yet expired: %+v", plan)
	}

	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	plan, err := svc.ExpireReplay("s")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != ReplayExpired || plan.Position != 2 {
		t.Fatalf("expired plan: %+v", plan)
	}
	_, entries, err := svc.ReplayStatus("s")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(planSeqs(entries)) != "[2 3]" {
		t.Fatalf("manual entries keep original order: %+v", entries)
	}
	for i, e := range entries {
		wantID := fmt.Sprintf("s-e%d", i+2)
		if e.Status != ManualPending || e.EventID != wantID {
			t.Fatalf("entry: %+v", e)
		}
	}
	// 未确认投递转为死信，不再被领取。
	if got := store.Claim("w", 10, time.Minute); len(got) != 0 {
		t.Fatalf("dead deliveries must not be claimed: %+v", got)
	}
	b, _ := svc.Backlog("s")
	if b.Dead != 2 || b.Acked != 1 {
		t.Fatalf("backlog: %+v", b)
	}
	// 事件保留原序号：重复接收返回原回执，不重新编号。
	r, err := svc.Receive(ev("s", 2))
	if err != nil || !r.Duplicate {
		t.Fatalf("event must survive expiry: %+v %v", r, err)
	}
	// 相同重放请求仍返回原（已过期）计划。
	again, err := svc.PlanReplay("s", 1, 3, time.Hour)
	if err != nil || again.Status != ReplayExpired || !again.Deadline.Equal(plan.Deadline) {
		t.Fatalf("same request returns original plan: %+v %v", again, err)
	}
}

// 乱序缺口 + 窗口过期：从未到达的序号也保留人工处理入口。
func TestReplayExpiryWithMissingEvent(t *testing.T) {
	svc, store := setup(t, 1)
	if _, err := svc.Receive(ev("s", 3)); err != nil { // 缺口 2
		t.Fatal(err)
	}
	if _, err := svc.PlanReplay("s", 1, 3, time.Minute); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := svc.ExpireReplay("s"); err != nil {
		t.Fatal(err)
	}
	_, entries, _ := svc.ReplayStatus("s")
	if fmt.Sprint(planSeqs(entries)) != "[1 2 3]" {
		t.Fatalf("entries: %+v", entries)
	}
	if entries[1].EventID != "" {
		t.Fatalf("missing event keeps seq with empty event id: %+v", entries[1])
	}
}

// 人工处理只能从缺口位置继续；已处理序号不可重复消费，迟到回执不覆盖。
func TestManualResolveFromGapOnly(t *testing.T) {
	svc, store := setup(t, 1, 2, 3)
	if _, err := svc.PlanReplay("s", 1, 3, time.Minute); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := svc.ExpireReplay("s"); err != nil {
		t.Fatal(err)
	}

	// 不能跳过缺口处理后面的序号。
	if _, err := svc.ResolveManual("s", 2); !errors.Is(err, ErrManualOrder) {
		t.Fatalf("skip gap: got %v", err)
	}
	plan, err := svc.ResolveManual("s", 1)
	if err != nil || plan.Position != 2 {
		t.Fatalf("resolve 1: %+v %v", plan, err)
	}
	// 已处理的条目不能重复处理。
	if _, err := svc.ResolveManual("s", 1); !errors.Is(err, ErrManualNotFound) {
		t.Fatalf("re-resolve: got %v", err)
	}
	if _, err := svc.ResolveManual("s", 2); err != nil {
		t.Fatal(err)
	}
	plan, err = svc.ResolveManual("s", 3)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Status != ReplayCompleted || plan.Position != 4 {
		t.Fatalf("plan completed: %+v", plan)
	}

	// 已处理序号不可重复消费：死信不被领取，迟到的确认是 no-op。
	if got := store.Claim("w", 10, time.Minute); len(got) != 0 {
		t.Fatalf("resolved seqs must not be consumable: %+v", got)
	}
	if err := svc.AckDelivery(deliveryKey("s", "s-e1")); err != nil {
		t.Fatalf("late ack on dead is a no-op: %v", err)
	}
	d, _ := store.Delivery(deliveryKey("s", "s-e1"))
	if d.Status != DeliveryDead {
		t.Fatalf("late ack must not overwrite manual handling: %+v", d)
	}
}

// 重复回执：已确认序号上的迟到确认与重复确认都是幂等 no-op。
func TestDuplicateAndLateAcksAreNoop(t *testing.T) {
	svc, store := setup(t, 1, 2)
	if _, err := svc.PlanReplay("s", 1, 2, time.Minute); err != nil {
		t.Fatal(err)
	}
	key := deliveryKey("s", "s-e1")
	if err := svc.AckDelivery(key); err != nil {
		t.Fatal(err)
	}
	if err := svc.AckDelivery(key); err != nil {
		t.Fatalf("duplicate ack: %v", err)
	}
	plan, _, _ := svc.ReplayStatus("s")
	if plan.Position != 2 {
		t.Fatalf("position must never move backward: %+v", plan)
	}
	d, _ := store.Delivery(key)
	if d.Status != DeliveryAcked {
		t.Fatalf("acked is terminal: %+v", d)
	}
}
