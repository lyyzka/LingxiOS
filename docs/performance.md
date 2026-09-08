# 性能优化实施与验证

本次实现对应《LingxiOS_v3.1.0_性能优化简案》，发布为 v3.2.1 / schema 10 / protocol 8。结果是可配置的框架优化，不代表所有模型、任务和部署环境都达到同一延迟。

## 已落地

| 计划 | 实现及边界 |
| --- | --- |
| 埋点 | 队列等待、Host 调用、SQL 语句与参数字节、模型首内容/首可公开正文/总时间、运行耗时、最终投递延迟；浏览器 TTFT 与 commit→UI 由真实消费链路基准测量。 |
| PR 1：正文实时输出 | 单次 worker HTTP NDJSON 上传 → 宿主有界快照 → 认证 SSE → UI reducer。首段立即发送，后续约 50ms 合并，不逐 token 写库；重试、修订、失效 fence、序列缺口和崩溃清除草稿。只有宿主明确允许且驱动提供独立正文或可解析 candidate JSON 时展示草稿。 |
| PR 1：完成语义 | 用量结算、授权、原子提交和验收保持原流程；最终持久事件带实际 candidateHash。已提交消息替换草稿，旧 fence 和完成后的预览被拒绝。 |
| PR 2：唤醒与投递 | PostgreSQL 空 payload 的 LISTEN/NOTIFY；首次监听和重连后扫描，保留 750ms 周期兜底。当前进程完成入队/事件/提交后也发本地提示。Outbox 默认 4 个独立领取通道，同一 work 的事件保序，慢接收方不占据所有通道；保留重试、claim 和 ack。 |
| PR 3：重复读取与写入 | 初次会话、有效请求版本和步骤来自一个 MVCC 查询；后续上下文不再传完整 session。相同已确认 checkpoint 去重，并发 checkpoint 串行保持 CAS；请求快照触发器仅在内容改变时更新。动态授权仍每 hop 获取、动作和提交仍重新验证。 |
| PR 3：上下文与记忆 | 有界 prompt 编译缓存包含租户、principal、授权、内容和 schema；复用已有 embedding Promise 缓存。Memory 默认每批 2 个 scope，必要读取有总截止时间；仅显式 optionalRecall 可超时降级，返回前复核权限。 |
| PR 3：附件与摘要 | 附件原文和 revisions 留在持久请求；工具可用时只传 512 字符预览，可用 task.read_attachment 按 id/sourceVersion/范围读取。无工具的 chat 或旧宿主保留原行为。摘要候选可在工具执行期间计算，只在历史前缀、摘要代次及完整请求不变时安装，并保留未完成的工具调用与后续追加结果。 |
| PR 4：长任务 | 运行槽、模型配额、Python 环境数量独立；多槽 worker 默认保留 1 槽给 interactive/approval lane。保留已有只读工具每批 4 个、写工具串行、持久挂起后释放运行槽的流程。抓取支持外部取消、总截止时间、限频真实字节进度和独立有界配额。 |
| PR 4：artifact | 二进制上传，保留 16MiB 上限、大小、哈希、租约和回读检查。旧服务 404 时回退原 base64 接口。 |
| PR 5 | 增加可重复的 SQL / 延迟基准和 claim 的 EXPLAIN (ANALYZE, BUFFERS)。没有凭静态分析添加索引，没有降低模型推理预算。HTTP 重试复用序列化请求，运行时继续统一预占与结算每个 provider attempt，避免 provider 内层重复重试。 |

## 配置与接入

默认仅关闭公开草稿。其余优化可逐项关闭，便于同版本灰度比较：

```ts
const control = await createLingxiOS({
  database: pool,
  // 必须由产品确认此请求允许在验收前公开正文；默认不允许。
  realtime: { allowDraft: (work, requestVersion) => permitsDraft(work, requestVersion) },
  performance: { notifications: true, contextSnapshot: true, outboxConcurrency: 4 },
})

const worker = createWorker({
  controlPlane: control,
  model,
  worker: { concurrency: 2, reservedInteractiveRuns: 1 },
  resources: { model: 2, python: 1 },
  performance: {
    checkpointDedup: true, promptCache: true,
    asyncCompaction: true, onDemandAttachments: true,
  },
})
```

`reservedInteractiveRuns` 必须小于总运行槽数；单槽 worker 默认不保留。保留容量按 lane 筛选，适用于匹配版本的宿主和 worker。自定义 `kernelFactory` 自行负责 Python 资源限制。模型配额包含执行、审查及摘要调用；已取消但底层尚未结束的调用仍占有资源槽，避免实际并发失控。

Memory 可配置 `contextBudget: { concurrency: 2, timeoutMs: 10000, optionalRecall: true, recallTimeoutMs: 3000 }`。`optionalRecall` 默认 false；必要 core 读取失败或权限撤销始终失败，不会被超时降级掩盖。

产品的 HTTP 路由应从现有认证上下文取得完整身份，再返回 `control.streamRun(identity, { signal: request.signal, lastEventId: request.headers.get('last-event-id') })`。identity 包含 runId、tenantId、agentId、sessionId、principalId，以及存在时的 threadId；不能直接信任客户端提交的 principalId。返回的 Response 已包含 `text/event-stream`、`no-cache, no-transform` 和 `X-Accel-Buffering: no`，部署代理仍需允许持续转发。

前端使用 `@lyyzka/lingxios/ui` 的 `createRunView` 与 `consumeRunStreamEvent` 消费 `state`、`event`、`preview`、`reset` 四种 SSE 事件。`view.draft` 明确显示为草稿；仅 `view.message` 是持久提交结果。终态后关闭 EventSource，避免无意义重连。临时草稿不持久化，断线重连读取当前快照；若外层丢帧导致序列缺口，重连取得完整快照。浏览器输出须按文本转义，不能直接插入 HTML。

当前仓库是 SDK，没有具体产品页面；公开 HTTP 身份认证、页面渲染和部署代理由消费端接入。`test/realtime.test.ts` 已通过真实 worker HTTP 和浏览器 HTTP/SSE 验证该链路。

## 数据库升级与回退

新安装的 `schema` 已包含通知与请求快照去重。已有 schema-10 部署在产品迁移锁下应用 `packageResources().migration011`；此迁移不删除业务数据，不提升 schema 版本，不在应用启动时自动执行。先升级宿主，再升级 worker。

LISTEN 使用传入 PostgreSQL pool 的一个专用连接，断线后丢弃连接而非归还普通查询。连接池需要为它和前台事务预留容量；总上限仍由产品传入的 pool 控制，不创建第二个无上限连接池。事务代理不支持持久 LISTEN、或者使用单连接适配器时，设置 `performance.notifications: false` 并使用周期扫描。外部产品事务内的入队依赖提交后的数据库通知，丢失提示时仍由周期扫描恢复。

回退可先将 notifications/contextSnapshot/checkpointDedup/promptCache/asyncCompaction/onDemandAttachments 设为 false，outboxConcurrency 设为 1，reservedInteractiveRuns 设为 0，并取消 allowDraft。保留新增触发器对旧运行时兼容；若需停掉数据库提示，可删除四个 `agent_*_wakeup` 触发器后删除 `lingxios.notify_runtime()`。请求快照的内容相等检查可以保留。

## 验证与基准

运行 `npm run bench:performance` 重建并生成 [原始基准数据](performance-benchmark.json)。默认每种独立场景每配置 20 次，另有 8 请求并发积压；可用 `LINGXIOS_BENCH_REPETITIONS` 调整。两组使用同一个受控 OpenAI-compatible SSE stub、相同请求和预算，通过真实 worker HTTP、浏览器 HTTP 和 UI reducer 比较功能开关，不是与历史版本的对照。

2026-09-08 的 20 次本机样本如下；时间为 p50，输入为每次场景全部模型请求的序列化字节总和：

| 场景 | 正文 TTFT（关闭 → 开启） | 完成（关闭 → 开启） | 模型输入（关闭 → 开启） |
| --- | ---: | ---: | ---: |
| 短对话 | 772ms → 110ms | 772ms → 288ms | 3.2KB → 3.2KB |
| 多 hop | 1533ms → 355ms | 1533ms → 542ms | 19.1KB → 19.1KB |
| 附件 | 773ms → 380ms | 773ms → 554ms | 115.3KB → 25.3KB |
| 长历史 | 1530ms → 137ms | 1533ms → 300ms | 109.0KB → 109.0KB |
| 两次写操作 | 775ms → 451ms | 775ms → 675ms | 27.9KB → 27.9KB |

短对话、多 hop、附件、长历史分别报告 queue、provider→UI、端到端正文 TTFT、commit→UI、总耗时和 HTTP/SQL/输入体积 p50/p95。预热样本排除，初始化时间单独记录。所有客户端计时来自同一进程的单调时钟；COMMIT 返回处记录提交完成，不能将 PostgreSQL 事务开始时间当成提交完成。

SQL 写入量为发送的写语句数，包含空写和重试，不等于行更新数、WAL 或磁盘 I/O。模型输入字节是实际序列化请求体，stub 的 token 用量不能用于真实费用结论。并发积压场景的全局 HTTP/SQL 计数相互重叠，不作每请求资源比较。内存仅是进程采样，包含 PGlite/WASM，不是生产峰值或租户归因。

**外部验收尚需真实环境：** 当前没有模型凭据或专用 PostgreSQL 测试连接，Docker 引擎不可用。PGlite 验证了事务通知、SQL 语义和本机链路，但没有验证真实 PostgreSQL 跨进程监听、生产数据分布、代理缓冲、跨机时钟或真实模型质量。未部署、未做真实模型 A/B，也未宣称生产 SLO 已通过。

按方案保留的后续条件项：只有实际 JSONB/WAL 写放大需要时才引入增量日志；只有内存预算和冷启动收益实测成立才预热隔离 Python 环境；只有生产 EXPLAIN/profiling 指出热点才添加索引、向量索引或减少推理预算。现有隔离、模型质量和验收门槛均未降低。
