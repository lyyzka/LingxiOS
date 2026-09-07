# Harness v3：通用提示词与验收层

本次改动负责指令分层、上下文编译、复杂任务复核、有界修正和主辅模型分工。业务工具定义、原生工具迁移、审批/恢复公共接口、UI 和数据库版本重组由另一工作树负责。

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

`ContextBlock` 保存 `source`、`version`、`trust`、`content`、`truncated` 与可选 `cache`。`PromptContext.version` 保持 3，默认提示词契约升级到 `prompt-v3.1`，新增不含正文的 `manifest`。指纹包括平台/产品内容、persona、授权方法、工具 schema 和产品来源版本，每轮重新编译。稳定指令必须位于动态指令之前；重复来源、截断的可信指令、数据声明指令缓存前缀均拒绝编译。编译器不把标签当作安全边界；可信产品回调必须由应用代码配置，不能接收未经校验的模型输出作为规则。

当前原始请求及修订独立于可压缩历史。旧任务进入历史时保留来源。上下文预算使用保守 UTF-8 字节估算，包括实际业务工具定义以及正文、思考输出预留；先削减可选记忆，再压缩旧历史。必要内容超限时停止，不截断当前要求。压缩输出固定为观察结果、决定、剩余工作和不确定性四个字段，逐字段限长并标记截断，保持工具调用/结果配对。

## 完成与继续

简单文本直接回答，可以原样返回用户要求的 JSON，不强制生成自评对象。显式结构化驱动仍可提供可选自评，但其结论不能代替验收。

合同、修订、多个执行步骤、文件、写入、委派和明显多项要求触发独立内容复核。显式文件请求即使被模型遗漏执行也触发复核。自然语言复杂度检测是保守启发式；产品已知复杂任务应提供明确合同和执行记录，不能把词语匹配当作完整需求解析器。

复核输入包括完整请求、修订、附件文本、候选正文、持久步骤、子任务结果和独立文件/资源读回。结果绑定 `workId + requestVersion + candidateHash`，通过步骤存储持久化。控制面提交重新检查匹配的复核记录、未决动作、资源核对和流内容，再使用现有事务提交正文、结果、工作状态与 outbox。

内容复核是可能误判的模型调用，不是形式证明。`satisfied` 表示当前完成门禁未发现缺口；`verification` 保留 `not_run` 或 `inconclusive`，worker 不能自报 `passed`。复核不可用、发现遗漏、未知效果或当前资源验证失败会阻止满意结论。失败动作观察保留错误原因，不能仅因确认“没有副作用”就推断原任务已完成。

格式修正最多两次。同一失败连续三次且没有有效进展时停止该路径；进展只考虑请求、结构化动作状态、资源版本和产物哈希等事实，时间戳与措辞变化不重置计数。有效成果与缺口保留后交付，避免为了修正格式重做已执行动作。

## 模型分工

默认主模型为 `deepseek-ai/DeepSeek-V4-Flash`，处理主对话、复杂任务和独立完成复核。`Qwen/Qwen3.5-4B` 处理审批通道中的续跑说明、记忆整理及历史压缩，默认关闭思考、正文上限 2,048 tokens。审批的授权、批准/拒绝、版本核对和动作执行继续由用户及控制面决定，模型只消费持久结果。

分工依据可信工作类型与调用目的，不通过关键词猜测任务权限。两种模型共用一个根任务预算、调用序号和重试计数；每次按实际调用模型的输出与思考上限预占，并记录实际模型和用量。复杂任务复核仍使用主模型，包括审批续跑后的复核。

本地工厂可用 `smallModel` 覆盖辅助模型配置；默认复用主模型端点与密钥。远程 Worker 对应设置为 `AGENT_OS_SMALL_MODEL`、`AGENT_OS_SMALL_MODEL_BASE_URL` 和 `AGENT_OS_SMALL_MODEL_API_KEY`。部署时的统一价格上界应覆盖两种模型；产品账本可以按调用记录中的模型分别结算。

三模型对比执行器、专用样本与发布依赖已移除。保留通用确定性验收和原有单模型评测入口；评测结果目录由 Git 忽略，不进入本次代码提交。

## 与架构重组集成

`RuntimePolicy.assembleSystemPrompt()` 已改为 `productRules()`。消费端应返回可信产品/角色规则，把 persona 和委派说明留在数据层，并使用新编译输出。业务工具迁移时保留实际 schema、授权方法与来源版本进入指纹，同时保留持久步骤、资源读回和复核绑定。

本分支没有改变数据库 schema 和包发布版本；最终控制面/worker/产品版本由架构重组统一。旧数据库、旧任务领取和生产路由未操作。合并新公共接口后，应重新执行安装包和原生消费门禁，再单独安排发布及首发切换。
