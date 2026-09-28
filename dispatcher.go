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
// 发送失败的条目会记录最后一次失败详情并保留租约，到期后由后续轮次
// 或其他 worker 重试；达到最大投递次数后进入死信。确认时若租约已被
// 接管或来源被死信阻塞，该条跳过但不中断本轮。
func (d *Dispatcher) DispatchOnce(ctx context.Context) (int, error) {
	claimed := d.store.Claim(d.Owner, d.Batch, d.Lease)
	sent := 0
	for _, dv := range claimed {
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		if err := d.transport.Send(ctx, dv); err != nil {
			d.store.Fail(dv.Key, dv.LeaseID, err)
			continue
		}
		if err := d.store.Ack(dv.Key, dv.LeaseID); err != nil {
			if errors.Is(err, ErrStaleLease) || errors.Is(err, ErrSourceBlocked) {
				continue
			}
			return sent, err
		}
		sent++
	}
	return sent, nil
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
