package webhooksequencer

import "time"

// Store 是顺序接收与投递所需的持久化抽象。MemoryStore 为其进程内参考实现；
// 生产环境可用同样的方法语义映射到数据库事务（events/outbox 两张表 + 来源级行锁）。
type Store interface {
	// InitSource 幂等初始化来源，指定起始序号。
	InitSource(id string, startSeq int64) (SourceInfo, error)
	// GetSource 查询来源信息，不存在返回 ErrNotFound。
	GetSource(id string) (SourceInfo, error)
	// Receive 接收事件，处理幂等、冲突与缺口释放。
	Receive(ev Event) (*Receipt, error)
	// ClaimDue 领取各来源队头的待投递任务；同来源至多一个租约在途。
	// 返回的每个 Delivery 都带单调递增的 Attempt，作为租约 fencing token。
	ClaimDue(now time.Time, lease time.Duration, max int) []*Delivery
	// Ack 确认投递成功，delivered 为终态。
	// attempt 必须等于当前租约的 Attempt：旧持有者迟到的确认会被
	// ErrLeaseLost 拒绝，避免“新租约正在重投时被旧租约误确认/回退”。
	Ack(key string, attempt int) error
	// Release 放弃当前租约，使任务立即重新可被领取；旧 Attempt 同样被 fencing。
	Release(key string, attempt int) error
	// GetBacklog 查询来源积压。
	GetBacklog(sourceID string) (*Backlog, error)
}
