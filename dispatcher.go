package webhooksequencer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

// Transport 是下游投递的传输层。允许传输层产生重复
// （例如发送成功但确认丢失后重投），下游凭 Delivery.Key 幂等去重。
type Transport interface {
	Send(ctx context.Context, d Delivery) error
}

// TransportFunc 让普通函数适配 Transport。
type TransportFunc func(ctx context.Context, d Delivery) error

// Send 实现 Transport。
func (f TransportFunc) Send(ctx context.Context, d Delivery) error { return f(ctx, d) }

// Dispatcher 从 outbox 领取租约并投递，成功后确认。
// 崩溃安全：发送成功但确认前崩溃时，租约过期后会被重新领取，
// 以相同的幂等键再次投递（传输层重复，下游去重）。
// 每条投递连续失败 MaxAttempts 次后进入死信，阻塞该来源后续序号，
// 等待操作员通过 Service 重试或跳过。
type Dispatcher struct {
	store     *Store
	transport Transport
	// Owner 标识租约持有者，默认为 主机名/进程号。
	Owner string
	// Lease 是单次租约时长，默认 30s。
	Lease time.Duration
	// Batch 是单次领取的最大条数，默认 100。
	Batch int
}

// NewDispatcher 构造投递器。
func NewDispatcher(store *Store, t Transport) *Dispatcher {
	host, _ := os.Hostname()
	return &Dispatcher{
		store:     store,
		transport: t,
		Owner:     fmt.Sprintf("%s/%d", host, os.Getpid()),
		Lease:     30 * time.Second,
		Batch:     100,
	}
}

// DispatchOnce 领取一批租约并逐条投递，返回本次确认成功的条数。
// 发送失败的条目上报失败：未达上限时保留租约，到期后由后续轮次或
// 其他 worker 重试（尝试号递增，旧尝试的迟到确认会被拒绝）；达到
// 上限后进入死信。租约已被接管时，本次尝试的上报/确认都是 no-op。
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	claimed := d.store.Claim(d.Owner, d.Batch, d.Lease)
	sent := 0
	// 同一来源本轮一旦出现失败或死信，后续序号本轮不再发送，
	// 避免后续序号绕过未解决的前序事件；不同来源互不影响。
	halted := make(map[string]bool)
	for _, dv := range claimed {
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		if halted[dv.SourceID] {
			continue
		}
		attempt := dv.Attempts
		if err := d.transport.Send(ctx, dv); err != nil {
			// 租约可能已在发送期间过期并被接管：过期尝试的上报会被拒绝，忽略。
			_ = d.store.ReportFailure(dv.Key, attempt, err.Error())
			halted[dv.SourceID] = true
			continue
		}
		switch err := d.store.Ack(dv.Key, attempt); {
		case err == nil:
			sent++
		case isLateAttempt(err):
			// 租约已被接管、投递已被跳过/死信或被前方死信阻塞：
			// 本次迟到结果无效，且该来源本轮停止推进。
			halted[dv.SourceID] = true
		default:
			return sent, err
		}
	}
	return sent, nil
}

// isLateAttempt 判断错误是否来自不再有效的尝试（过期栅栏、已死信、
// 已跳过、被阻塞）：这类情况下当前发送结果应被忽略而不是中断投递。
func isLateAttempt(err error) bool {
	return errors.Is(err, ErrStaleAttempt) ||
		errors.Is(err, ErrDeliveryDead) ||
		errors.Is(err, ErrDeliveryDisposed) ||
		errors.Is(err, ErrSourceBlocked)
}

// Run 按固定间隔循环投递，直到 ctx 取消。
func (d *Dispatcher) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = d.DispatchOnce(ctx)
		}
	}
}
