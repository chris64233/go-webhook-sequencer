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
- `Dispatcher` 通过租约（lease）领取待投递记录：发送成功才 `Ack`；发送失败或进程崩溃时租约到期，记录可被重新领取。
- 发送成功但确认前崩溃 → 传输层会重复投递同一幂等键（at-least-once）；**确认是终态**，已确认的投递永远不会回到待投递。
- `OpenFileStore` 重启加载时回收所有未确认的租约为待投递，推进过程可恢复。

### 3. API

| 方法 | 说明 |
|---|---|
| `Service.InitSource(id, startSeq)` | 初始化来源，序号从 `startSeq`（≥1）开始 |
| `Service.Receive(ev)` | 接收事件，返回 `Receipt`（`buffered`/`released`） |
| `Service.AckDelivery(key)` | 确认投递成功（幂等） |
| `Service.Backlog(sourceID)` | 积压查询：期望序号、缺口后缓冲的序号、outbox 各状态数量 |
| `Dispatcher.DispatchOnce(ctx)` / `Run(ctx, interval)` | 领取租约并投递一轮 / 循环投递 |

### 4. 死信与重放窗口

超过 `Dispatcher.MaxAttempts`（默认 5）仍未确认的投递转为**死信**（`dead`），
不再被领取，只能由重放计划重新入队或转入人工处理。

`Service.PlanReplay(sourceID, fromSeq, toSeq, window)` 制定重放计划，计划固定
事件范围 `[fromSeq, toSeq]`、起始序号、截止时间和当前确认位置（`Position`）：

- **已确认不重复入队**：范围内已成功投递（acked）的事件直接跳过，`Position` 从第一个未确认序号开始。
- **缺口停住**：从确认位置起按原序号连续入队；正常投递、死信重放与新事件并发交错时，
  遇到序号缺口（事件未到达）必须停住，后续事件不会提前发送；缺口被新事件补齐后自动继续。
- **幂等与冲突**：相同范围的重复请求返回原计划（窗口参数被忽略）；范围或版本改变返回
  `ReplayConflictError`。确认位置只单调前进，迟到的旧重放/重复回执不能覆盖已确认的序号。
- **窗口过期**：`Service.ExpireReplay(sourceID)` 结算过期计划，未确认序号按**原始顺序**
  转入人工处理队列（`ManualEntry`），对应投递转为死信；事件保留原序号，不删除、不重新编号。
- **人工处理**：`Service.ResolveManual(sourceID, seq)` 只能处理缺口位置（`Position`）的条目，
  处理后从缺口位置继续，不能把后面的事件重排成另一条序列；已处理的序号不可重复消费
  （不再被领取，迟到的确认是 no-op）。全部确认或处理完毕后计划转为 `completed`。

| 方法 | 说明 |
|---|---|
| `Service.PlanReplay(sourceID, fromSeq, toSeq, window)` | 制定/查询重放计划（幂等） |
| `Service.ExpireReplay(sourceID)` | 窗口过期结算：未确认序号转人工 |
| `Service.ReplayStatus(sourceID)` | 查询计划与人工处理队列（按原序号升序） |
| `Service.ResolveManual(sourceID, seq)` | 人工处理缺口位置的条目 |

### 错误分类

- `ErrInvalidArgument`：参数不合法（空标识、非正序号等），`errors.Is` 判断。
- `ErrSourceExists` / `ErrSourceNotFound` / `ErrDeliveryNotFound`：状态前置条件不满足。
- `ErrReplayNotFound` / `ErrManualNotFound` / `ErrManualOrder`：重放与人工处理的前置条件。
- `*SeqConflictError` / `*PayloadConflictError`：冲突详情，`errors.As` 提取。
- `*ReplayConflictError`：重放范围/版本冲突，携带已存在的计划详情，`errors.As` 提取。

## 存储

`Store` 有两种实现：`NewMemoryStore()`（纯内存）与 `OpenFileStore(path)`（JSON 快照，
临时文件 + rename 原子落盘 + fsync）。来源状态变更与新投递的入队在同一临界区内完成并一起落盘。

## 代码结构

- `types.go` — 事件、回执、投递、积压等类型
- `errors.go` — 错误分类
- `store.go` — 持久化存储（来源状态 + outbox + 租约 + 重放计划/人工队列）
- `service.go` — 接收、去重、冲突检测、缺口缓冲、按序释放、重放计划 API
- `dispatcher.go` — outbox 租约投递与确认、死信转换

## 运行测试

    go test ./... -race

覆盖：参数/冲突分类、缺口缓冲与按序释放、幂等重放、并发乱序接收（500 事件 `-race`）、
多来源并行、崩溃恢复（文件持久化 + 租约回收）、发送后确认前崩溃的同键重投、确认终态。
重放窗口：计划幂等与范围冲突、已确认跳过、缺口停住与补齐继续、窗口过期转人工（含
从未到达的缺口序号）、人工按缺口顺序处理、迟到/重复回执不覆盖终态、死信重放入队。
