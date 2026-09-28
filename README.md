# go-webhook-sequencer

Webhook 事件的接收与投递服务：**同一来源严格有序，不同来源并行**。

开发环境：Go 1.23.0，仅依赖标准库。

## 核心语义

### 1. 有序接收（`Service.Receive`）

每个事件携带来源标识、单调递增序号、事件标识和负载摘要：

- **幂等重放**：标识、序号、摘要完全相同 → 返回首次接收的结果（`Receipt.Duplicate=true`），不产生任何副作用。
- **负载冲突**：事件标识相同但摘要不同 → `PayloadConflictError`。
- **序号冲突**：同一序号对应不同事件，或同一标识出现在不同序号位置 → `SeqConflictError`。
- **缺口缓冲**：序号大于期望值时先持久化、不提前交付；缺口补齐后从期望序号起连续推进，按序释放进 outbox。
- 按来源加锁（`keyMutex`）：同一来源的并发接收被串行化，不漏投、不重投、不打乱顺序；不同来源互不阻塞。

### 2. 持久化 outbox + 租约重试（`Dispatcher`）

- 释放的事件写入持久化 outbox，携带**稳定幂等键**（`来源/事件标识`），崩溃重投时键不变，下游凭键去重。
- `Dispatcher` 通过租约（lease）领取待投递记录：发送成功才 `Ack`；发送失败会记录**最后一次失败**详情并保留租约，到期后可被重新领取。
- 每次领取分配单调递增的**租约编号**（`Delivery.LeaseID`）并写入领取尝试日志；**只有持有当前租约的发送者能确认成功**——租约被接管后，旧发送者的迟到确认返回 `ErrStaleLease`，不会改变任何状态。
- 发送成功但确认前崩溃 → 传输层会重复投递同一幂等键（at-least-once）；**确认是终态**，已确认的投递永远不会回到待投递。
- `OpenFileStore` 重启加载时回收所有未确认的租约为待投递，推进过程可恢复。

### 3. 死信与受控重放

投递次数达到 `Store.MaxAttempts`（默认 5）后，投递进入**死信**：

- 死信记录保存稳定投递键、来源序号和最后一次失败详情；只引用事件（标识 + 负载摘要），**不复制也不改写原事件负载**。
- **死信阻塞来源的连续序列**：同来源后续序号不得绕过它被领取或确认（`ErrSourceBlocked`）；不同来源互不影响，仍可并行推进。
- 操作员对死信有两种**处置**方式，同一来源存在多个死信时必须**按序号从小到大**处置（`ErrDeadLetterOrder`）：
  - **重试（`RetryDeadLetter`）**：业务含义是"问题已修复，这个事件仍然要送达"。沿用原投递键重新入队（下游幂等去重不受影响），生成全新的领取尝试；若新一轮仍达到上限，会再次进入死信。
  - **跳过（`SkipDeadLetter`）**：业务含义是"经人工核对，这个事件不需要再投递"（例如下游已线下补录、事件已过期失效）。必须提供**原因**和**全局唯一决定号**；决定号重复提交是幂等 no-op，被挪用到其他死信则报 `ErrDecisionConflict`。跳过生效后该序号记为**已处置**，连续序列继续推进；跳过是终态，**任何迟到的确认都不能把它恢复为已投递**。
- 死信记录、处置决定与领取尝试日志全部持久化，进程重启后原样恢复，不会重复生成。

### 4. API

| 方法 | 说明 |
|---|---|
| `Service.InitSource(id, startSeq)` | 初始化来源，序号从 `startSeq`（≥1）开始 |
| `Service.Receive(ev)` | 接收事件，返回 `Receipt`（`buffered`/`released`） |
| `Service.AckDelivery(key, leaseID)` | 确认一次领取尝试成功（仅当前租约持有者；幂等） |
| `Service.RetryDeadLetter(key)` | 重试死信：沿用原投递键重新入队 |
| `Service.SkipDeadLetter(key, decisionID, reason)` | 跳过死信：原因 + 唯一决定号，序号记为已处置 |
| `Service.Backlog(sourceID)` | 积压查询：期望序号、缓冲序号、outbox 各状态数量、待处置死信数、阻塞序号 |
| `Service.DeadLetters(sourceID)` | 死信查询（`sourceID` 为空返回全部来源） |
| `Service.DeliveryAttempts(key)` | 领取尝试日志查询 |
| `Service.Decisions(sourceID)` | 处置决定查询（`sourceID` 为空返回全部来源） |
| `Service.BlockingSeq(sourceID)` | 当前阻塞来源的最小死信号（0 表示未阻塞） |
| `Dispatcher.DispatchOnce(ctx)` / `Run(ctx, interval)` | 领取租约并投递一轮 / 循环投递 |

### 错误分类

- `ErrInvalidArgument`：参数不合法（空标识、非正序号等），`errors.Is` 判断。
- `ErrSourceExists` / `ErrSourceNotFound` / `ErrDeliveryNotFound` / `ErrDeadLetterNotFound`：状态前置条件不满足。
- `ErrStaleLease`：租约已被接管或目标已进入终态，迟到确认被拒绝。
- `ErrSourceBlocked`：来源被更早序号的死信阻塞，后续序号不得绕过确认。
- `ErrDeadLetterNotOpen` / `ErrDeadLetterOrder` / `ErrDecisionConflict`：死信处置约束（已处置、须按序号、决定号冲突）。
- `*SeqConflictError` / `*PayloadConflictError`：冲突详情，`errors.As` 提取。

## 存储

`Store` 有两种实现：`NewMemoryStore()`（纯内存）与 `OpenFileStore(path)`（JSON 快照，
临时文件 + rename 原子落盘 + fsync）。来源状态变更与新投递的入队在同一临界区内完成并一起落盘。

## 代码结构

- `types.go` — 事件、回执、投递、死信、处置决定、领取尝试、积压等类型
- `errors.go` — 错误分类
- `store.go` — 持久化存储（来源状态 + outbox + 租约 + 死信 + 决定 + 尝试日志）
- `service.go` — 接收、去重、冲突检测、缺口缓冲、按序释放、死信处置与查询
- `dispatcher.go` — outbox 租约投递、失败上报与确认

## 运行测试

    go test ./... -race

覆盖：参数/冲突分类、缺口缓冲与按序释放、幂等重放、并发乱序接收（500 事件 `-race`）、
多来源并行、崩溃恢复（文件持久化 + 租约回收）、发送后确认前崩溃的同键重投、确认终态；
死信（达到上限进入死信并阻塞后续序号、按序号处置、重试沿用原键、跳过终态与决定号幂等、
迟到确认/租约接管竞争、重启后不重复生成死信与决定、领取尝试日志、跨来源隔离）。
