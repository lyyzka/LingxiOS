# Harness v3：通用提示词与验收层

本版本在现有持久执行内核上补齐执行模式、完成义务、技能按需加载、可信展示、记忆治理和行为版本固定。所有生成调用统一使用主模型；授权、审批、动作回执、Worker fencing 与 outbox 仍由同一控制面执行。

## 来源与取舍

设计参考固定提交 [`a6e1cb39ebebb818ad7ecef4ec1c23e4249668c2`](https://github.com/asgeirtj/system_prompts_leaks/tree/a6e1cb39ebebb818ad7ecef4ec1c23e4249668c2) 中的以下资料。该仓库是第三方收集材料，不能证明其真实性、生产配置或效果；它提供待验证的设计原则。

| 参考资料 | 采用的原则 | 落实位置 |
| --- | --- | --- |
| [Codex](https://github.com/asgeirtj/system_prompts_leaks/blob/a6e1cb39ebebb818ad7ecef4ec1c23e4249668c2/Codex/gpt-6-astra.md) | 持续执行已授权任务，交付前检查真实结果，区分进度和完成 | `context/compiler.ts`、`runtime/runtime.ts` |
| [Claude Code](https://github.com/asgeirtj/system_prompts_leaks/blob/a6e1cb39ebebb818ad7ecef4ec1c23e4249668c2/Anthropic/claude-code/claude-code-headless-fable-5.1.md) | 系统约束、用户任务与观察数据分层；限制未经授权的操作 | `context/compiler.ts`、现有控制面授权 |
| [Cursor](https://github.com/asgeirtj/system_prompts_leaks/blob/a6e1cb39ebebb818ad7ecef4ec1c23e4249668c2/Cursor/cursor.md) | 使用当前上下文与工具观察，完成任务后提供可核对结果 | `context/request.ts`、`outcome/content-check.ts` |

没有复制整段产品提示词或依赖其中的角色标签建立权限。权限、租约、动作幂等、审批与原子交付继续由运行时代码和持久存储执行。

## 指令职责

`buildPromptContext()` 每轮从实时配置收集产品规则、授权和偏好，`compileContext()` 负责纯函数编译。主执行和辅助调用共用该编译器；`compileAuxiliaryPrompt()` 声明独立目的，现有 `auxiliaryInstructions()` 保留兼容，不继承用户 persona。装配、缓存与审计契约见 [Prompt runtime](prompt-runtime.md)。

| 层 | 来源 | 发送方式与边界 |
| --- | --- | --- |
| 平台规则 | 运行时固定规则 | 始终存在于 system 消息 |
| 产品与执行角色 | 可信 `RuntimePolicy.productRules()` / `context.productRules` | system 消息；产品负责规则内容和角色对应授权 |
| 当前请求与修订 | 持久请求快照 | 完整原文与有序修订；每轮各注入一次 |
| 默认偏好、派生计划 | persona、任务清单、委派说明 | 普通数据消息，不能替换原始请求或授予权限 |
| 观察 | 工具结果、附件、记忆、历史摘要 | 普通数据消息；历史里的 system 角色会降为观察 |

`ContextBlock` 保存 `source`、`version`、`trust`、`content`、`truncated` 与可选 `cache`。`PromptContext.version` 保持 3，默认提示词契约为 `prompt-v3.2`，包含不含正文的 `manifest`。指纹包括平台/产品内容、persona、授权方法、工具 schema 和产品来源版本，每轮重新编译。稳定指令必须位于动态指令之前；重复来源、截断的可信指令、数据声明指令缓存前缀均拒绝编译。编译器不把标签当作安全边界；可信产品回调必须由应用代码配置，不能接收未经校验的模型输出作为规则。

当前原始请求及修订独立于可压缩历史。旧任务进入历史时保留来源。上下文预算使用保守 UTF-8 字节估算，包括实际业务工具定义以及正文、思考输出预留。记忆先按模型窗口的 8%（最多 8,000 估算 token）装载完整核心、目录和召回摘要；整体压力下先压缩旧历史，再削减参考召回、目录，最后明确记录无法容纳的核心遗漏。必要内容超限时停止，不截断当前要求。压缩输出固定为观察结果、决定、剩余工作和不确定性四个字段，逐字段限长并标记截断，保持工具调用/结果配对。

## 完成与继续

简单文本直接回答，可以原样返回用户要求的 JSON，不强制生成自评对象。显式结构化驱动仍可提供可选自评，但其结论不能代替验收。

合同、修订、多个执行步骤、文件、写入、委派和明显多项要求触发独立内容复核。显式文件请求即使被模型遗漏执行也触发复核。自然语言复杂度检测是保守启发式；产品已知复杂任务应提供明确合同和执行记录，不能把词语匹配当作完整需求解析器。

复核输入包括完整请求、修订、附件文本、候选正文、持久步骤、子任务结果和独立文件/资源读回。结果绑定 `workId + requestVersion + candidateHash`，通过步骤存储持久化。控制面提交重新检查匹配的复核记录、未决动作、资源核对和流内容，再使用现有事务提交正文、结果、工作状态与 outbox。

内容复核是可能误判的模型调用，不是形式证明。`satisfied` 表示当前完成门禁未发现缺口；`verification` 保留 `not_run` 或 `inconclusive`，worker 不能自报 `passed`。复核不可用、发现遗漏、未知效果或当前资源验证失败会阻止满意结论。失败动作观察保留错误原因，不能仅因确认“没有副作用”就推断原任务已完成。

格式修正最多两次。同一失败连续三次且没有有效进展时停止该路径；进展只考虑请求、结构化动作状态、资源版本和产物哈希等事实，时间戳与措辞变化不重置计数。有效成果与缺口保留后交付，避免为了修正格式重做已执行动作。

## 模型分工

统一使用 `model` 配置的主模型（默认 `deepseek-ai/DeepSeek-V4-Flash`）处理主对话、完成复核、审批续跑、记忆整理与历史压缩。审批的授权、批准/拒绝、版本核对和动作执行继续由用户及控制面决定，模型只消费持久结果。

调用用途保持隔离，但不再决定模型身份。所有生成调用使用同一模型窗口、输出和思考配置，并共用根任务预算、调用序号和重试计数；输入超限会明确失败，不截断原始要求。

本地只配置 `model`。远程 Worker 只读取 `AGENT_OS_MODEL`、`AGENT_OS_MODEL_BASE_URL` 和 `AGENT_OS_MODEL_API_KEY`；移除旧 `smallModel` 参数与 `AGENT_OS_SMALL_MODEL*` 设置。独立的向量检索仍使用可选 embedding 接口，不承担生成任务。

三模型对比执行器、专用样本与发布依赖已移除。保留通用确定性验收和原有单模型评测入口；评测结果目录由 Git 忽略，不进入本次代码提交。

## 与架构重组集成

`RuntimePolicy.assembleSystemPrompt()` 已改为 `productRules()`。消费端应返回可信产品/角色规则，把 persona 和委派说明留在数据层，并使用新编译输出。业务工具迁移时保留实际 schema、授权方法与来源版本进入指纹，同时保留持久步骤、资源读回和复核绑定。

包版本为 3.1.0，schema 为 9，控制面协议为 7。协议升级会拒绝缺少认知记忆快照与独立复核传输的旧 Worker；Kernel 协议和已提交消息版本不变。升级和回滚步骤见 [packaged runtime](packaged-runtime.md)。旧记忆由宿主显式重置，不回灌；本包启动只读检查，不自动改生产数据库或流量。

## 可信完成义务

宿主通过 `RequestInput.mode` 选择 `chat / read / execute`，通过 `obligations` 指定 `external-delivery / artifact / resource-postcondition / answer-content / delegation`。这些都是可信入口参数，模型清单不能修改它们。外部交付匹配当前请求版本的具体动作和完整参数；无关成功写操作不能替代所需动作。文件必须有真实内容检查，领域及回答检查由 `verifyRun({ candidate, ... })` 返回指定的 `product:*` 记录，委派必须具有匹配父任务、版本和主体的已提交满意结果。执行模式没有显式义务时，需要真实成功动作或已核验产物才能完成。未声明模式的旧调用保留兼容行为，宿主应为新产品流程明确选择模式。

义务与不可变的原始请求绑定；修订会使旧版本验收失效，但不会把自然语言修订自动变成削弱义务的授权。需要替换义务时，通过可信入口创建新请求并取消旧请求。开放式自然语言复核仍可能误判，不是确定性的语义证明。

## 业务工具、技能与展示

`HarnessProfile` 是规则、原生工具、技能与展示的轻量配置组装层。`dependsOn` 用于验证配置依赖；不增加执行循环。每轮只求值一次执行快照；下一轮看到撤权，原生动作在实际执行时再次授权。配置的行为哈希固定到新任务；恢复时发现不匹配会明确阻断，需恢复对应部署或处置旧任务。

作者维护的 `SkillDefinition` 提供名称、版本、描述、正文和所需动作。上下文只放索引，正文经 `skills.load` 按哈希加载并留下动作回执。技能不能授予权限，与进化型策略来源分开。将大目录中的工具标记 `deferred: true` 后，通过 `catalog.discover` 授权搜索并加载 schema。大结果的完整值保留在动作账本中，模型预览附带引用与哈希，可用 `observations.read` 按范围补读；过期、换主体、撤权和哈希不匹配都会拒绝。

工具可声明 `observation` 和 `preconditions`，由原生 `observe` 从实际读结果提取对象与版本，再由 `observationRequirement` 提供当前对象版本。受保护写操作要求同任务、同主体、同请求版本的完整读取回执。外部服务仍须使用自身的条件更新。`semanticVersion` 必须在解析、授权、预览、执行或核验语义变化时递增；哈希同时覆盖 schema、效果类型、审批、前置观察与版本。旧审批不会因为同名工具部署而自动获得新语义的授权，unknown 仍必须对账。

`PresentationDefinition` 接受模型选择的类型、引用和注释；原生授权及 `resolve` 补齐字段、来源版本和数据时间。任意数值字段输入被拒绝。组件在同一次结果/outbox 事务中提交，绑定现有 resultId、requestVersion 和 fence，重连读取原快照。注释是模型解释，不能当作权威字段；引用支持状态仍是 `not_assessed`。

## 记忆内容与遗忘

宿主统一使用 `control.memory` 的类型化文档接口与 `initialize`。原生工具提供 list/read/search/apply/history/restore/forget/reflect/doctor；更新、移动、合并与恢复核对预期版本。正文上限 16 KiB，路径形成目录，中文和英文经原生分词进入 PostgreSQL 全文索引；可选向量不可用时显式降级。核心记忆不依赖查询命中，历史证据独立于会话压缩，同主体、同 Agent 搜索并重新检查原来源权限。

`memory.writePolicy` 对文档正文、元数据、历史证据、冲突、恢复、自动合成和进化候选共享生效，可允许、脱敏或拒绝；最终写入还拒绝明显凭证。产品负责配置领域隐私规则。原生修改经过主模型独立复核；明确保存或锁定的内容需要当前人类请求支持，模型参数不能自行声明授权。可信管理入口及宿主自定义 `ActionContext.writeMemory` 工具负责验证操作意图。后台按作用域累计 5 次已提交交互或空闲 10 分钟触发，每批最多 20 次交互与 12 项变更，执行提案和独立复核；技能与策略仍走原独立评估流程。

`control.memory.forget` 在授权作用域内提升持久 epoch、清除记忆、历史版本、索引和相关可检索证据。单条删除保守地使该作用域全部旧来源失效，其他记忆保留；旧任务即使重试、换 Worker 或尝试重新采集来源，也不能回写。作用域锁、epoch 核验和提交处于同一数据库事务。恢复产生新版本，不能找回已遗忘内容。新请求可以形成新记忆。该接口处理长期记忆及合成证据；不会替产品删除会话、法定审计记录或历史模型 trace，后者仍按宿主保留/删除策略管理。

## 发布证据

`npm run check:release` 运行单测、安装包、真实 PostgreSQL、跨进程 Worker 恢复、容量及 Docker/Linux 隔离检查。`test/harness-regressions.test.ts` 与记忆/原生工具测试包含模式、撤权、伪造展示、尾部补读、遗忘回写和审批语义变化负例。真实模型质量、延迟、成本和 LingxiLoop 消费端集成需在已授权的隔离环境单独验收；这些结果不能用 mock、文档或本地硬门替代。
