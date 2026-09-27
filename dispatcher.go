package webhooksequencer

import (
	"context"
	"sync"
	"time"
)

// SendResult 是 Sender 对一次投递的处置结论。
type SendResult int

const (
	// SendAck 表示下游确认成功（2xx 或带幂等键的去重命中），记录进入终态 delivered。
	SendAck SendResult = iota
	// SendRetry 表示可重试失败（超时、5xx、网络错误），放弃租约等待下一轮重试。
	SendRetry
)

// Sender 负责把单个事件真正投递到下游。
//
// 实现方应使用 Delivery.Key 作为 HTTP 幂等头（如 Idempotency-Key）：
// 同一 Key 的重复请求是允许且预期的（崩溃恢复场景），下游需据此去重。
// 返回 SendRetry 或发送协程在返回前崩溃（建模为租约到期）时，
// 之后会携带同一个 Key 重新投递。
type Sender interface {
	Send(ctx context.Context, d *Delivery) SendResult
}

// SenderFunc 让普通函数满足 Sender。
type SenderFunc func(ctx context.Context, d *Delivery) SendResult

// Send 实现 Sender。
func (f SenderFunc) Send(ctx context.Context, d *Delivery) SendResult { return f(ctx, d) }

// DispatcherConfig 配置投递调度器。
type DispatcherConfig struct {
	// Store 为持久化 outbox。
	Store Store
	// Sender 为下游投递器。
	Sender Sender
	// PollInterval 为领取轮询间隔，默认 100ms。
	PollInterval time.Duration
	// Lease 为每次领取的租约时长；超过该时长未 Ack 的任务可被重新领取，
	// 用于覆盖“发送前后崩溃”场景。默认 10s。
	Lease time.Duration
	// BatchSize 为单轮最多领取（跨来源并行）的任务数，默认 16。
	BatchSize int
	// Concurrency 为跨来源并行发送的最大协程数，默认等于 BatchSize。
	Concurrency int
	// Now 注入时钟，主要用于测试；默认 time.Now。
	Now func() time.Time
}

// Dispatcher 是 outbox 租约重试泵。
//
// 领取循环与发送 worker 相互解耦：某个来源的发送方卡死（崩溃模型）只会占住
// 自己的租约，不阻塞其他来源投递，也不阻塞泵在租约到期后重新领取同一任务。
// 重新领取后 Attempt 递增并作为 fencing token：卡死的旧发送方若迟到地
// Ack/Release，会被 ErrLeaseLost 拒绝。
type Dispatcher struct {
	cfg  DispatcherConfig
	jobs chan *Delivery
	// sem 为容量 Concurrency 的信号量，每个在途/排队投递占一个槽。
	sem chan struct{}
	wg  sync.WaitGroup

	// sendCtx 与 Run 的生命周期解耦：进程“优雅停止”只停止领取，
	// 在途发送继续进行，结果通过租约/fencing 兜底，模拟真实崩溃恢复语义。
	sendCtx    context.Context
	cancelSend context.CancelFunc
}

// NewDispatcher 创建调度器，参数缺失会 panic（属于编程错误）。
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	if cfg.Store == nil || cfg.Sender == nil {
		panic("webhooksequencer: store and sender are required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 10 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 16
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = cfg.BatchSize
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	sendCtx, cancel := context.WithCancel(context.Background())
	return &Dispatcher{
		cfg:        cfg,
		jobs:       make(chan *Delivery, cfg.Concurrency),
		sem:        make(chan struct{}, cfg.Concurrency),
		sendCtx:    sendCtx,
		cancelSend: cancel,
	}
}

// Run 阻塞运行直到 ctx 被取消。停止时取消在途发送的 context 并等待 worker
// 结束；未确认的任务保留租约，到期后由下一个进程实例重新领取（同幂等键）。
// 真实进程崩溃（来不及走该流程）时等价于租约超时 + 下游幂等去重。
func (d *Dispatcher) Run(ctx context.Context) {
	for i := 0; i < d.cfg.Concurrency; i++ {
		d.wg.Add(1)
		go d.worker()
	}

	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()
	for {
		d.claimAndEnqueue()
		select {
		case <-ctx.Done():
			d.cancelSend() // 取消所有在途发送，保证关闭能在发送 ctx 的超时边界内完成
			close(d.jobs)  // 已排队任务仍会被 worker 取空
			d.wg.Wait()
			return
		case <-ticker.C:
		}
	}
}

// claimAndEnqueue 领取不超过空闲发送槽位的到期任务并入队。
// 先占槽再领取，因此不会出现“领了却没有 worker 可发、只能干等租约超时”：
// 卡死的来源只会占住自己的槽，其余来源照常并行；到期重领也必须先拿到空槽。
func (d *Dispatcher) claimAndEnqueue() {
	// 非阻塞地预占空闲发送槽位，至多 BatchSize 个；槽全满则本轮不领取。
	free := 0
	for free < d.cfg.BatchSize {
		select {
		case d.sem <- struct{}{}:
			free++
		default:
			goto claimed
		}
	}
claimed:
	if free == 0 {
		return
	}
	dues := d.cfg.Store.ClaimDue(d.cfg.Now(), d.cfg.Lease, free)
	// 没有那么多到期任务：归还多余的槽。
	for i := len(dues); i < free; i++ {
		<-d.sem
	}
	for _, del := range dues {
		// 槽已预先占住，通道容量等于 Concurrency，发送永不阻塞。
		d.jobs <- del
	}
}

func (d *Dispatcher) worker() {
	defer d.wg.Done()
	for del := range d.jobs {
		d.deliver(del)
		<-d.sem
	}
}

func (d *Dispatcher) deliver(del *Delivery) {
	result := safeSend(d.cfg.Sender, d.sendCtx, del)
	switch result {
	case SendAck:
		// 旧租约迟到的成功确认会被 fencing 拒绝：任务已由更新的租约在途，
		// 对方携带同一幂等键重投，由下游去重；此处忽略 ErrLeaseLost。
		_ = d.cfg.Store.Ack(del.Key, del.Attempt)
	case SendRetry:
		_ = d.cfg.Store.Release(del.Key, del.Attempt)
	}
}

func safeSend(s Sender, ctx context.Context, del *Delivery) (result SendResult) {
	defer func() {
		if r := recover(); r != nil {
			// 发送方 panic 视为可重试失败，显式释放租约以便更快重试。
			result = SendRetry
		}
	}()
	return s.Send(ctx, del)
}
