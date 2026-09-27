# go-webhook-sequencer

Webhook 接收与顺序投递服务：**同一来源严格有序，不同来源并行处理**。
支持事件重复/乱序到达、序号缺口暂存、基于持久化 outbox 与租约的至少一次投递，
以及崩溃恢复下的稳定幂等键。

开发环境：Go 1.23.0。

```bash
go test ./...        # 全部自动化测试（可加 -race）
go test -race ./...
```

## 核心语义

### 1. 事件接收：幂等、冲突与缺口

每个事件携带三元组：单调递增序号 `seq`、事件标识 `id`、负载摘要 `digest`。
接收时按来源在同一临界区内原子判定：

| 情况 | 判定 | 结果 |
| --- | --- | --- |
| `(seq, id, digest)` 与已存事件完全相同 | 重复 | 返回**首次接收的原始结果**（`duplicate=true`），不重复入队 |
| 同一 `seq` 对应不同 `id` | 序号冲突 | `409 sequence_conflict` / `ErrSequenceConflict` |
| 同一 `id`（同 seq）对应不同 `digest` | 负载冲突 | `422 payload_conflict` / `ErrPayloadConflict` |
| 同一 `id` 出现在另一个 `seq`（digest 无论是否相同） | 负载冲突 | `422 payload_conflict` / `ErrPayloadConflict` |
| 字段缺失、序号早于来源起始序号 | 参数错误 | `400 invalid_argument` / `ErrInvalidArgument` |

冲突事件不会被持久化，也不会推进任何水位。

**序号缺口**：事件先持久化到 `buffered`，但不进入投递队列。当缺口被补齐时，
接收操作在同一个临界区内从释放水位（watermark）开始**连续释放**所有无缺口事件
到 outbox（`status=queued`，回执带 `released_count`）。因此即使相邻事件被并发
接收，也不会漏投、重投或乱序。

### 2. 投递：outbox、租约与稳定幂等键

- 事件释放后写入 outbox，幂等键固定为 `<sourceID>:<seq>`，**重试与崩溃恢复永不改变**。
  下游应使用该键做 HTTP 幂等（如 `Idempotency-Key` 头）。
- **同源队头阻塞（head-of-line blocking）**：每个来源只有队头记录可被领取，
  前一个事件未确认成功前，后续事件绝不发出——保证同来源投递顺序与序号一致。
  不同来源的队头可在同一轮领取中并行发送。
- 领取即获得一份带到期时间的**租约**，并返回单调递增的 `attempt`（fencing token）。
  - 发送成功 → `Ack(key, attempt)`，记录进入终态 `delivered`，永不回到待投递。
  - 可重试失败 → `Release(key, attempt)`，立即等待下一轮。
  - 发送方在 Ack 前后崩溃 → 租约到期后以**同一个幂等键**、`attempt+1` 重新投递。
- 旧租约迟到的 Ack/Release 因 `attempt` 不匹配被 `ErrLeaseLost`（HTTP 412）
  拒绝，避免“新租约正在重投时被旧持有者误确认或回退”。
- 传输层允许重复（崩溃恢复的至少一次语义），去重边界在下游的幂等键；
  服务自身保证 `delivered` 为终态，确认成功后绝不重发。

### 3. 积压查询

`Backlog` 返回：释放水位 `next_seq`、已确认最大连续序号 `last_delivered_seq`、
缺口暂存数 `buffered`、已入 outbox 未确认数 `pending_delivery`。

## 代码结构

| 文件 | 职责 |
| --- | --- |
| `model.go` | `Event` / `Receipt` / `Delivery` / `SourceInfo` / `Backlog` 数据模型 |
| `errors.go` | 参数、序号冲突、负载冲突、租约失效等错误类型与哨兵 |
| `store.go` | `MemoryStore`：每来源独立锁的参考存储实现（接收判定、缺口释放、outbox 队头租约） |
| `store_iface.go` | `Store` 接口，可按相同语义映射到 SQL 事务（events/outbox 两表 + 来源级行锁） |
| `dispatcher.go` | 领取泵 + worker 池：跨来源并行、按来源有序、租约重试、fencing |
| `server.go` | HTTP 接口（`net/http`，Go 1.22+ 方法路由） |
| `*_test.go` | 19 个自动化测试，含并发乱序、崩溃恢复、fencing、HTTP 端到端 |

## HTTP 接口

```
PUT  /v1/sources/{id}              初始化来源（body: {"start_seq": 1}）
GET  /v1/sources/{id}              来源信息
POST /v1/sources/{id}/events       接收事件
GET  /v1/sources/{id}/backlog      积压查询
POST /v1/deliveries/{key}/ack      投递确认（body: {"attempt": n}）
POST /v1/deliveries/{key}/release  放弃租约（body: {"attempt": n}）
```

`{key}` 形如 `src-1:42`，以最后一个 `:` 之前为来源标识，来源标识本身可含 `:`。

示例：

```bash
curl -XPUT localhost:8080/v1/sources/shop-a -d '{"start_seq":1}'

# seq=2 先到：buffered（缺口暂存）
curl -XPOST localhost:8080/v1/sources/shop-a/events \
  -d '{"seq":2,"id":"e2","digest":"sha256:..."}'
# => {"status":"buffered","released_count":0,"watermark":1,...}

# 补齐 seq=1：1、2 在一次推进中连续释放
curl -XPOST localhost:8080/v1/sources/shop-a/events \
  -d '{"seq":1,"id":"e1","digest":"sha256:..."}'
# => {"status":"queued","idempotency_key":"shop-a:1","released_count":2,"watermark":3}

curl localhost:8080/v1/sources/shop-a/backlog
# => {"next_seq":3,"last_delivered_seq":0,"buffered":0,"pending_delivery":2}
```

错误响应统一为：

```json
{"error": "...", "kind": "sequence_conflict | payload_conflict | ...", "field": "digest"}
```

| HTTP | kind | 含义 |
| --- | --- | --- |
| 400 | `invalid_argument` | 参数错误（缺字段、序号早于起始序号、畸形 key/JSON） |
| 404 | `not_found` | 来源或投递记录不存在 |
| 409 | `already_exists` / `sequence_conflict` | 来源重复初始化 / 同一序号绑定不同事件 |
| 422 | `payload_conflict` | 同一事件标识对应不同内容 |
| 412 | `lease_lost` | 确认/释放携带的 attempt 已过期（fencing 拒绝） |

## 编程式使用

```go
store := webhooksequencer.NewMemoryStore()
store.InitSource("src", 1)

r, _ := store.Receive(webhooksequencer.Event{
    SourceID: "src", Seq: 1, ID: "e1", Digest: "sha256:...",
})
// r.Status == queued, r.IdempotencyKey == "src:1"

d := webhooksequencer.NewDispatcher(webhooksequencer.DispatcherConfig{
    Store:  store,
    Lease:  10 * time.Second,
    Sender: webhooksequencer.SenderFunc(func(ctx context.Context, d *webhooksequencer.Delivery) webhooksequencer.SendResult {
        // 用 d.Key 作为 Idempotency-Key 投递给下游；重复的 d.Key 由下游去重
        req, _ := http.NewRequestWithContext(ctx, "POST", downstream, bytes.NewReader(d.Event.Payload))
        req.Header.Set("Idempotency-Key", d.Key)
        resp, err := http.DefaultClient.Do(req)
        if err != nil || resp.StatusCode >= 500 {
            return webhooksequencer.SendRetry
        }
        return webhooksequencer.SendAck
    }),
})
go d.Run(ctx)
```

## 落地到真实持久化

`MemoryStore` 用每来源互斥锁实现完整语义；生产环境把同一组方法映射为数据库事务即可：

- `events(source_id, seq, event_id, digest, payload, queued)`：
  `(source_id, seq)` 与 `(source_id, event_id)` 两个唯一约束，由数据库直接拒绝
  序号冲突 / 负载冲突（可按唯一键区分错误类别）。
- `outbox(source_id, seq, status, attempts, lease_deadline)`：
  领取在 `BEGIN ... FOR UPDATE` 中读取各来源队头、按 `lease_deadline` 判断到期；
  `attempts` 自增并作为 fencing token；Ack 用
  `UPDATE ... SET status='delivered' WHERE key=? AND attempts=?` 条件更新，
  影响行数为 0 即租约失效。
- 接收事务内完成“写事件 + 从水位连续推进写 outbox”，与内存实现的临界区等价。

## 测试覆盖

- 来源初始化幂等、重复初始化冲突、未知来源。
- 缺口暂存与补齐后连续释放、释放水位与积压计数。
- 完全重复返回首次原始结果、重复不入队。
- 序号冲突（同 seq 不同 id）、负载冲突（同 id 不同 digest / 不同 seq），冲突不改状态。
- 参数错误分类（缺字段、序号早于起始、畸形 key/JSON）。
- 同源队头阻塞、Ack 终态幂等、租约到期同键重投、fencing 拒绝过期 attempt。
- 200 个事件乱序并发接收（`-race`）：无漏投/重投/乱序、幂等键稳定。
- 20 来源并发混合操作（接收 + 跨来源领取确认 + 积压查询，`-race`）：无共享状态竞争。
- 调度器：重试保持同键且有序、发送方崩溃后租约重投、panic 恢复、跨来源并行。
- HTTP 层端到端状态码与错误 `kind`。
