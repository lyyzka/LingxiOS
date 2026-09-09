# 性能优化实施与验证

对应《LingxiOS_v3.1.0_性能优化简案》和《LingxiOS_async_blocking_audit》；当前版本 **3.2.3 / schema 10 / control-plane protocol 9**。框架回归、受控基准和产品端到端验收是不同的证据，不能互相替代。

## 实现与边界

| 报告项 | 实现 |
| --- | --- |
| 性能 PR 1：正文与完成 | 独立、有界 worker NDJSON preview → 宿主快照 → 认证 SSE → UI reducer；首段立即发送，后续约 50ms 合并。宿主明确允许且驱动能够区分公开正文才展示草稿。重试、修订、失效 fence、缺口和崩溃清除草稿；实际 candidateHash 的已提交结果替换草稿。预算、审计、用量结算、审批与验收不绕过。 |
| 性能 PR 2：通知与投递 | PostgreSQL 空 payload 的 LISTEN/NOTIFY；监听建立/重连后扫描，保留周期兜底。Outbox 默认 4 个独立通道，同一 work 事件保序，保留 claim、重试、幂等 ack，慢接收方不占据所有通道。 |
| 性能 PR 3：重复工作 | 初次会话、有效请求和步骤来自 MVCC 快照；checkpoint 去重并保留恢复边界；请求快照仅在内容改变时写入。有界 prompt 缓存包含租户、principal、授权、内容和 schema；复用 embedding Promise 缓存。原始附件/revisions 保留，工具可按 id/sourceVersion 读取片段。动态授权仍在上下文、动作和提交处检查。 |
| 审计 P0-1：CPU | HTML 改为线性扫描；research HTML/PDF/文本、artifact JSON/文本和文档提取进入独立 Node 进程。最多 2 个解析进程、16 个等待任务、累计输入字节有界、单输入 16MiB、10 秒 deadline、128MiB V8 heap；输出有界。V8 heap **不是 RSS 限额**，生产仍须容器/OS 内存限制。 |
| 审计 P0-2：领取 | claim 只领取，Worker 先注册 attempt、启动心跳，再调用 fenced recover。reconcile、已决审批、等待恢复由该 attempt 承担，单飞并有总 deadline/取消；不堵塞下一次领取。后续副作用仍须等待恢复核对。 |
| 审计 P0-3：准入 | 受信任 executionClass 区分 conversation / operation，不再只看来源 lane。运行槽和模型配额分别预留前台容量；有界前后台队列，余量允许后台取得进展。单模型槽的前台请求取消后台摘要，但仅在底层真正结束后归还配额。 |
| 审计 P0-4：修订 | 独立通知监听在所有运行槽占满时仍读取控制信号。steer 中止旧 generation 与摘要、重置草稿并按新版本继续；心跳只是兜底。副作用仍使用 attempt 生命周期，未知效果不视为未执行。 |
| 审计 P1-1：摘要 | 软阈值只启动单飞低优先级候选，不阻塞下一次生成或提交；真正到硬预算才等待必要整理。仅历史前缀、完整请求和摘要代次匹配时安装；保留工具配对。取消后的摘要用量仍结算，Worker shutdown 跟踪清理。 |
| 审计 P1-2：记忆 | 必要授权/core 保留 deadline 与权限复核；显式 optionalRecall 不启动补充检索，按需用 memory.search。结果提交只插入 memory-capture 的 result_id 引用，不复制未经隐私过滤的正文。后台在事务外解析授权/执行写策略，短事务内复核源状态、请求指纹、claim 和 forgetting epoch 后幂等写 evidence。最多 5 次重试，不在队列保存 policy 错误或原文。 |
| 审计 P1-3：实际取消 | 取消贯穿解析、上传、校验、模型及宿主调用。解析进程 SIGKILL 后等待 close 才释放名额；忽略 signal 的自定义 provider/工具仍占额至真实结束。资源释放与调用者返回分开计量，不能假释放。 |
| 审计 P1-4：artifact | 流式写隔离临时文件、64KiB 增量哈希，完整大小/哈希验证后原子发布，失败清临时文件。全局/租户在途字节准入覆盖上传、staging、解析；旧自定义 stager 只做有界缓冲回退。二进制入口先验证租约与 metadata，普通/兼容 JSON 请求也有总缓冲字节限制与传输 deadline。单文件仍限 16MiB，产物权威回读保留。 |
| 审计 P1-5：父任务 | 子结果提交、work settle、父任务 park 和通知触发幂等依赖检查；定时扫描只作修复。完成先于 park 的竞态同样处理。SSE 不因 waiting 关闭，子任务内容仍经原受众/身份边界读取。 |
| 性能 PR 4/5 | 模型、Python、抓取分别有界；只读工具按批并行，写工具默认串行。复用持久子任务、幂等账本、版本与等待，不自动并发写同一 session。HTTP 重试复用请求与幂等身份，provider attempts 统一预算/结算。没有无依据加索引、降低推理预算或预热带用户状态的 Python。 |

## 配置与接入

```ts
const control = await createLingxiOS({
  database: pool,
  // 产品确认允许在验收前公开正文；默认不允许。
  realtime: { allowDraft: (work, version) => permitsDraft(work, version) },
  performance: { notifications: true, contextSnapshot: true, outboxConcurrency: 4 },
})
const worker = createWorker({
  controlPlane: control, model,
  worker: { concurrency: 2, reservedInteractiveRuns: 1 },
  resources: { model: 2, python: 1 },
  performance: { checkpointDedup: true, promptCache: true, asyncCompaction: true, onDemandAttachments: true },
})
```

- `reservedInteractiveRuns` 保留旧名称，但筛选 **conversation 执行类别**；必须小于总运行槽，单运行槽不能保证长任务与对话并行。多模型槽也保留 1 个前台槽；后台在余下容量内 FIFO 进展，前台持续超载时不承诺无限容量或无饥饿。
- `enqueue/enqueueJob` 的顶层 `executionClass` 是宿主策略，不接受用户 meta 伪造。默认 chat 是 conversation，其他 mode 是 operation。IM 产品可调用 `conversations.ingest(message, { mode: 'chat', executionClass: 'conversation' })`，第二参数同样只取可信配置。旧记录未设置类别时按 lane 回退。
- conversation 禁止 Python，并隐藏声明为 operation 的工具。普通 read/execute 请求默认保留原工具权限，但归入 operation。关键子任务默认继承父类别；宿主 `ActionContext.enqueueChild({ ..., executionClass: 'operation' })` 可将长工作下放到独立子 session，不能将 operation 子任务提升到预留前台池。省略子 sessionId 即自动隔离；不得让同 Agent 的长子任务复用父 session/thread。
- **同会话锁未删除。** 长任务须先持久 enqueueChild（与动作回执同事务），再返回既有 defer/taskRef 并让父 run 持久 waiting；这样父会话和前台槽才释放。直接执行中的 operation 仍持有自己的会话/执行环境，不能宣称只加一个执行类别就使任意旧长工具自动后台化。产品原生工具需采用该持久接管契约；新的独立对话才能与子 operation 并行。
- 原生长工具可声明 `execution: { class: 'operation', timeoutMs: 30000, maxConcurrency: 2, cancellation: 'signal' }`（无法撤销外部效果时用 `reconcile`）。合约绑定审批指纹，运行时限制单次 timeout ≤30 秒、并发 ≤1024（应按实际服务容量配置，示例为 2）；transaction 工具不允许伪装成长执行。更久的操作应返回持久任务身份/进展并等待续跑，不在一个 native 回调里无限等待。CPU 密集的自定义回调仍由产品隔离；Promise 不是隔离。
- Memory 配置 `contextBudget: { concurrency: 2, timeoutMs: 10000, optionalRecall: true }` 可将补充召回移出首轮关键路径，状态为 `optional_deferred`；默认 false 保留必需召回。旧 `recallTimeoutMs` 在 deferred 模式不再启动计时检索。必要 core/授权失败不降级。scope resolver 和 writePolicy 接收可选 signal，调用有 10 秒 deadline，产品适配器应实际中止底层 I/O。
- `readOperations().failedMemoryCaptures` 与 `agentos_memory_capture_failed` 暴露重试耗尽的引用。可信运维在修复策略后，可针对指定 result_id 将 `agent_memory_capture` 的 attempts 置 0、available_at 置 NOW()、claim_token 置 NULL（仅 completed_at IS NULL）；没有不经复核重放原文的快捷通道。

HTTP 路由须从认证上下文构造完整 identity，再返回 `control.streamRun(identity, { signal: request.signal, lastEventId: request.headers.get('last-event-id') })`。不能信任客户端 principalId。Response 含 `text/event-stream`、`no-cache, no-transform`、`X-Accel-Buffering: no`；代理仍须持续转发。

前端用 `@lyyzka/lingxios/ui` 的 `createRunView/consumeRunStreamEvent` 消费 `state/event/preview/reset`；`draft` 明确标草稿，只有 `message` 是提交结果。waiting 不是终态；真正终态再关闭 EventSource。重连读取持久事件/当前快照，临时草稿不逐 token 持久化。输出按文本转义。

**消费端升级不是 SDK 提交能替代的工作。** 本地 `E:/lyyzka/LingxiLoop` 根目录和 server 仍声明 `lingxios@2.1.0`、导入旧 `lingxios`，delivery 仍走 `model.delta → assistant-stream/Redis`。该独立仓库未在本 SDK 提交中修改或部署。产品须更换为精确版本 `@lyyzka/lingxios@3.2.3`、升级 Worker/迁移/接入 API，配置 allowDraft 与独立 preview，再验收实际 Redis/IM/UI、代理与身份边界；不能把内部推理 delta 接成公开正文。

## 数据库升级与回退

新安装只应用 `packageResources().schema`。已有 schema 10 必须在产品迁移锁下按序应用 **migration011 → migration012**；schema 9 先应用 migration010。012 添加 durable memory-capture 表并扩展 cancel/preempt 通知，保持 schema marker 10；启动额外检查新表，因此不能只看旧的 schema marker。迁移不自动执行、不删除业务/记忆数据。

先暂停 ingress、排空并停止旧 Worker、保留数据库及 artifact 备份；迁移后同时部署匹配的 3.2.3 host/Worker，readiness 后恢复。**protocol 9 是领取/恢复分阶段合约**；新宿主以 409/protocol_mismatch 拒绝旧 Worker，不能依赖混版滚动运行。自定义 HostPort 应按 `claim → 注册/心跳 → recoverWork → 执行` 接入，不要重新把恢复放回 claim。

LISTEN 占传入 pool 的一个专用连接，失联后丢弃该连接；总上限仍由同一个产品 pool 控制。为 listener、前台事务及后台查询预留容量，依据连接等待指标调 outbox 并发。事务代理/单连接适配器设置 notifications:false。外部事务通知提交后才生效，丢失时由周期扫描修复。

可关闭 notifications/contextSnapshot/checkpointDedup/promptCache/asyncCompaction/onDemandAttachments、将 outboxConcurrency 设 1、reservedInteractiveRuns 设 0 并禁用 allowDraft 来做同版本对照；这不取消身份、资源、fence 或协议检查。跨版本回退仍需停 ingress/Worker，协调恢复匹配代码、数据库和 artifact 备份并核对外部效果，不能混用旧领取语义。没有自动 down migration。

## 回归与观测

`npm run typecheck && npm test`；专项检查：

- `async-audit.test.ts`：异常 HTML、真实解析取消、先心跳后恢复、不阻塞下一领取、conversation 预留、满槽通知修订、慢摘要不阻塞前台/提交、模型准入与用量、artifact 清理和字节配额。
- `memory-capture.test.ts`：慢隐私 hook 不持事务、队列只含引用、过滤后幂等写入、forget/cancel 并发禁止恢复旧证据。
- `performance-migration.test.ts`、`wakeup.test.ts`：迁移可重复、提交才通知、cancel/preempt 提示、类别过滤和周期兜底。
- `jobs/collaboration/realtime/reliability/host-http`：持久子任务/恢复、受众/重连/草稿边界、实际用量、旧协议拒绝及 HTTP 租约检查。

新增 `agentos_event_loop_delay_p95_seconds`、`agentos_resource_queue_wait_seconds`、`agentos_cancel_resource_release_seconds`（模型配额）、领取恢复耗时、数据库连接等待/事务耗时。事务耗时是锁持有的观测上界，不冒充逐锁测量。原有队列、SQL 语句/参数字节、模型首公开正文、commit/投递指标保留；实际绘制时间只能由产品浏览器计量。

## 基准证据与发布门

`npm run bench:performance` 使用同一 OpenAI-compatible SSE stub、真实 worker HTTP/浏览器 HTTP 和 UI reducer，对比同版本功能开关；默认每场景每配置 20 次和 8 请求积压。不是历史版本或真实模型 A/B。

[performance-benchmark.json](performance-benchmark.json) 是 **修复前 v3.2.1 的历史基准**，不是 3.2.3 的复测结果。其 2026-09-08 本机 p50（关闭 → 开启）：短对话正文 TTFT 772→110ms，多 hop 1533→355ms，附件 773→380ms，长历史 1530→137ms；只说明当时受控环境，不可当本提交或生产 SLO 的证明。

跨进程计时须校准；SQL 发送次数不等于 WAL/磁盘写入；stub token 不等于真实费用；PGlite/WASM 进程内存不等于生产峰值。真实环境必须补测 PostgreSQL 跨进程通知/重连、后台满载、父任务接续、慢收方、取消实际释放、代理缓冲、浏览器绘制、生产 EXPLAIN 和相同模型/预算的质量 A/B。候选 provider 已公开正文→UI p95 ≤100/200ms、入队→领取 p95 ≤100ms 均仍是验收目标，不是保证。

只在实测 JSONB/WAL 写放大需要时加增量日志；冷启动收益和内存预算成立才预热空隔离 Python；生产 EXPLAIN/profiling 证明热点才补索引或调整推理预算。当前 SDK 改动不宣称消费端、真实模型或生产容量验收已完成。
