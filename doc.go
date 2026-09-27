// Package webhooksequencer 实现 Webhook 的接收与顺序投递：
// 同一来源严格按序号有序投递，不同来源并行处理。
//
// 主要组成：
//   - [MemoryStore] 实现 [Store]：事件幂等/冲突判定、序号缺口暂存与连续释放、
//     持久化 outbox 与队头租约；
//   - [Dispatcher]：周期性领取各来源队头任务并调用 [Sender]，按租约重试，
//     投递携带稳定幂等键 "<sourceID>:<seq>"；
//   - [Server]：把来源初始化、事件接收、投递确认与积压查询暴露为 HTTP 接口。
//
// 错误通过 errors.Is 区分：[ErrInvalidArgument]（参数错误）、
// [ErrSequenceConflict]（同一序号对应不同事件）、
// [ErrPayloadConflict]（同一事件标识对应不同内容）。
package webhooksequencer
