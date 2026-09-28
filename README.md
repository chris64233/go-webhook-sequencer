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
- `Dispatcher` 通过租约（lease）领取待投递记录：每次领取把该投递的尝试号（`Delivery.Attempts`）加一作为**栅栏（fencing token）**；发送成功后必须凭当前尝试号确认。
- 发送失败上报 `ReportFailure(key, attempt, cause)`：未达上限时租约到期重试；连续失败达到 `Store.MaxAttempts`（默认 8）后进入死信。
- 发送成功但确认前崩溃 → 传输层会重复投递同一幂等键（at-least-once）；**确认是终态**，已确认的投递永远不会回到待投递。
- `OpenFileStore` 重启加载时回收所有未确认的租约为待投递，推进过程可恢复。

### 3. 死信与受控重放（`Service.RetryDeadLetter` / `SkipDeadLetter`）

**业务含义：** 同一来源的事件代表一条必须按序推进的业务流。当某序号连续投递失败时，
系统不能擅自跳过它（会让下游看到“跳号”的状态），也不能让它后面的事件先被确认，
于是该序号进入**死信**并把整个来源阻塞在这个序号上，等待操作员人工裁决：

- **重试（retry）**：操作员认为失败是暂时的（下游恢复、网络恢复），指示系统重新投递
  **原事件**。重试不复制、不改写任何负载：投递沿用同一个稳定幂等键（下游仍按同键去重），
  只是生成一次**新的领取尝试**（新的栅栏号）并把失败计数清零。
- **跳过（skip）**：操作员确认该事件永远不需要、也不应该再投递给下游（业务上作废）。
  跳过必须给出**原因**和**唯一决定号**；生效后该序号进入 `skipped` 终态，语义上视同
  “该序号已处置完毕”，来源的连续序列才允许继续向后释放。**跳过不可撤销**：即使旧发送者
  此时才带回成功响应，也不能把它恢复成已投递。

两种处置都需要**唯一决定号**（`decisionID`）：决定本身持久化，用同一决定号重复调用
幂等返回首次决定；决定号被内容不同的请求复用会得到 `ErrDecisionConflict`。
进程重启后从持久化状态继续，不会重复生成死信或处置决定。

**顺序约束：** 同一来源存在多个死信时，必须从序号最小的一条开始处置（`ErrDeadLetterOrder`），
不能先重放后面的事件；不同来源互不阻塞，仍然并行。

**并发安全（栅栏）：** 死信重试、跳过与原投递的迟到确认可能同时发生。

- 只有**当前领取尝试**能够确认成功：租约过期被新 worker 接管后，旧发送者的确认/失败上报
  返回 `ErrStaleAttempt`，不能影响当前任务。
- 跳过一旦生效，任何迟到成功返回 `ErrDeliveryDisposed` 且状态保持 `skipped`。

### 4. API

| 方法 | 说明 |
|---|---|
| `Service.InitSource(id, startSeq)` | 初始化来源，序号从 `startSeq`（≥1）开始 |
| `Service.Receive(ev)` | 接收事件，返回 `Receipt`（`buffered`/`released`） |
| `Service.AckDelivery(key, attempt)` | 凭当前尝试号确认投递成功；旧尝试返回 `ErrStaleAttempt` |
| `Service.RetryDeadLetter(source, seq, decisionID)` | 重试死信：新尝试号、原投递键 |
| `Service.SkipDeadLetter(source, seq, decisionID, reason)` | 跳过死信：需要原因与唯一决定号 |
| `Service.Backlog(sourceID)` | 积压查询：期望序号、缓冲序号、各状态数量、当前阻塞序号 `BlockedSeq` |
| `Service.DeadLetters(sourceID, unresolvedOnly)` | 死信查询（含历史轮次、最后失败、处置结果） |
| `Service.Attempts(sourceID)` | 领取尝试查询（栅栏号、持有者、结果） |
| `Service.Decisions()` | 处置决定查询（重试/跳过、原因、决定号） |
| `Dispatcher.DispatchOnce(ctx)` / `Run(ctx, interval)` | 领取租约并投递一轮 / 循环投递 |

### 错误分类

- `ErrInvalidArgument`：参数不合法（空标识、非正序号、跳过缺少原因/决定号等），`errors.Is` 判断。
- `ErrSourceExists` / `ErrSourceNotFound` / `ErrDeliveryNotFound`：状态前置条件不满足。
- `ErrStaleAttempt`：确认/失败上报来自过期尝试（租约已被接管，或重试后尚未重新领取）。
- `ErrSourceBlocked`：来源被更靠前的未处置死信阻塞，当前序号不能确认。
- `ErrDeliveryDead` / `ErrDeliveryDisposed`：投递已死信 / 已被跳过（终态）。
- `ErrNotDeadLetter` / `ErrDeadLetterOrder`：目标不是未处置死信 / 未按序号顺序处置。
- `ErrDecisionConflict`：决定号已被内容不同的决定占用。
- `*SeqConflictError` / `*PayloadConflictError`：冲突详情，`errors.As` 提取。

## 存储

`Store` 有两种实现：`NewMemoryStore()`（纯内存）与 `OpenFileStore(path)`（JSON 快照，
临时文件 + rename 原子落盘 + fsync）。来源状态变更与新投递的入队在同一临界区内完成并一起落盘。

## 代码结构

- `types.go` — 事件、回执、投递、死信、处置决定、领取尝试、积压等类型
- `errors.go` — 错误分类
- `store.go` — 持久化存储（来源状态 + outbox + 租约栅栏 + 死信 + 决定）
- `service.go` — 接收、去重、冲突检测、缺口缓冲、按序释放、死信处置与查询
- `dispatcher.go` — outbox 租约投递、栅栏确认、失败上报与死信转入

## 运行测试

    go test ./... -race

覆盖：参数/冲突分类、缺口缓冲与按序释放、幂等重放、并发乱序接收（500 事件 `-race`）、
多来源并行、崩溃恢复（文件持久化 + 租约回收）、发送后确认前崩溃的同键重投、确认终态；
以及死信转入与阻塞、失败记录只存键/序号/错误、重试沿用原键但产生新尝试号、
跳过终态拒绝迟到确认、租约接管栅栏、死信按序号处置、不同来源并行、
多轮死信、重启后状态与决定不重复生成、并发重试/跳过/迟到确认竞争、各类查询。
