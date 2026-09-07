# 首发 Harness / AgentOS 开发状态

以下内容保留为 v2 历史记录。当前通用提示词、纯文本完成和独立复核规则以 [Harness v3](harness-v3.md) 为准；产品架构迁移在另一工作树进行。

以 2026-09-06 用户最新要求为准：继续对照当前 LingxiLoop 补齐全部产品能力。当前不存在生产数据，交付单一首发架构；仅提供初始 schema version 1，不提供历史数据导入、schema 升级、旧协议兼容或双写路径。

邮件原生能力已重新接入：`whoami/contacts/inbox/show` 使用 Agent 邮箱身份并先校验持久化人类的当前会话读取权限；`send/reply` 绑定收件人、主题、正文及当前会话已提交附件到版本化人工审批，批准时重新授权和复核预览，以动作键调用原生幂等投递，执行回执落库后才恢复任务。演示文稿 `approve_outline` 已接通原生 schema，并把标题、完整 outline 与 expected revision 绑定到版本化人工审批；批准时重新读取并拒绝陈旧 outline，原生幂等执行回执落库后才恢复任务。邮件真实 PostgreSQL/provider 执行检查仍待补齐。

日历 Agent Task 原生唤醒入口已补齐：仅接受已提交的 `calendar` 系统消息，核对产品调度 nonce、原生派发记录、事件指派和目标会话，并从仍活跃的事件创建者恢复人类授权后入队；伪造系统消息不能直接获得 Agent 权限。文档 mention 已由产品写成 mentioner 身份的已提交消息，可复用普通入口。外部邮件发件人不能充当产品内人类授权主体，因此未伪造自动邮件执行权限。

通用 Agent handoff 已接入当前 `agents/coworker.ts` 领域记录和结构化消息，创建前校验原人类会话权限与已提交上下文消息。创建时唤醒目标 Agent，完成或阻塞时唤醒来源 Agent；`receiveHandoff` 均通过原生 handoff 的幂等键回查 namespaced action intent/work，恢复原人类主体后写入唯一 LingxiOS 队列。list/update 继续使用原生领域状态；领域记录本身不被当作任务完成证据。

已实现的主干包括：单包入口、PostgreSQL 存储、租约与 fencing、不可变请求快照及修订、动作意图与回执、交付 outbox、Python 执行、响应 envelope、部分 LingxiLoop 业务能力和知识审批。

已补齐核心闭环：原始请求与修订自检、候选资源刷新、未决动作核对、真实等待和委派引用校验、完整响应持久提交与跨进程恢复。Canvas assignment、依赖、报告、handoff 和 reporter 队列已使用原生领域资源与唯一 namespaced 工作队列。原生文档创建/正文编辑/删除审批已接入，原生 Yjs 更新、动作回执和通知同事务，PostgreSQL 原生执行与失败恢复检查通过。核心最终发布验证已完成；完整业务能力矩阵已移出本次发布范围。下方为历史开发记录，不能据旧记录判断当前实现范围。

默认模型已统一为 `deepseek-ai/DeepSeek-V4-Flash`，硅基流动端点，显式 `reasoning_effort=high`；主运行时、worker、压缩和评测共用默认设置。`eval/results/2026-09-06/deepseek-high2` 两轮 8 个样本全部通过确定性检查及未校准语义评审；这是四个 agent 编写用例的重复执行，不是人工校准质量基准。更早模型的报告保留为历史证据。最终自检格式修正耗尽时保留可交付正文和实际文件、标为 partial，不因自检格式问题丢掉已有成果。

模型的最终 JSON 对象携带正文、原始要求引用、自检依据与缺口。`satisfied` 是有记录的模型自检结论，与独立验证分开；`verification` 仍为 `not_run / inconclusive`，不接受 worker 自报 `passed`。控制面核验持久资源观察、未决动作、真实子任务、当前请求版本及已提交响应。已知资源失败或未知执行阻止满意结论；尚有可执行工作时，运行时提供一次有界继续机会，避免停在执行承诺。

`host.task.check_receipt` 已能核对当前请求版本、身份范围内的指定业务动作和完整结果，返回 `pass / fail / not_observed` 并持久保存核对回执。它不把读取成功当成写入成功，不接受旧请求版本或未知执行状态；匹配历史回执仍不能证明当前资源状态或整个目标达成。

`host.knowledge.check_source` 复用原生授权读取，检查指定知识源的 enabled/status/title 字段并记录观察时间。没有暴露的资源版本不会被编造，不可见或缺少字段返回 `not_observed`；这仍只是指定字段的检查，不是完整用户目标验收。

本次核心开发与发布验证完成，版本为 1.0.0，交付目标是 origin/main 的一次提交推送与可安装 tarball；不代表已向 npm 注册表发布。

最终 `npm run check:release` 成功退出：143 项包测试全部通过，包含仓库外 tarball 安装；五个新建的 PostgreSQL 17 测试库完成存储、跨进程 worker 恢复和现有日历/文档检查；其余已实现的原生契约与领域回归也通过。`lingxios-worker:1.0.0` 镜像通过非 root、公开入口、Python、缺配置拒绝、鉴权轮询、健康检查与 SIGTERM 测试。最终 `core-release` 真实模型复测四例全部通过确定性检查及未校准语义评审。纯文本最终答复的格式修正耗尽时也保留经文本/引用检查的正文与实际文件，状态保持 partial。

本轮后续修复：通用等待恢复现在同时处理 `task.ask` 与已持久化的 PENDING 审批回执，不重跑 Python 或业务动作。读取全部 intent（包括缺失 receipt 的 intent），任何遗漏都拒绝恢复；恢复前检查当前请求修订。日历公开入口的审批回执后崩溃模拟及输入等待回归通过。真实评测的文件场景原先仅把答复交给语义评审，导致实际已下载的文件仍被判为未观测；现在评测把独立 `app.readArtifact` 的版本、哈希和有界 JSON 内容作为观测传给评审，未观测的外部交付继续是 unknown。最新构建及 eval/input/lingxiloop 共 14 项定向检查通过，未把模型评审当作权威目标验收。

在上述等待/评测增量之前，完整 `check:release` 已以四个独立 PostgreSQL 17 测试库实际通过（135 项包测试、原生契约、存储进程恢复、真实 worker 崩溃恢复、全部现有原生业务检查）；Linux worker 镜像构建及非 root/入口/Python/鉴权轮询/健康/SIGTERM 检查通过。日历还通过原生权限、并发批准和授权依赖的第二连接撤权锁检查。此记录不代表完整能力矩阵或首发质量通过。

真实模型 Qwen/Qwen3.5-4B 与 PostgreSQL 的四例结果保存在 `eval/results/2026-09-06/first-release-native`：四例提交、聊天限制和文件下载内容检查通过；完整推导/聊天审查符合，提示审查超时、文件审查因缺观测不确定，命令非零。用例仍未经人工审阅/校准。带文件观测的后续定向评测单独记录，不覆盖原报告。

日历创建/删除已接入版本绑定审批与公开 `approveCalendar`：批准前重新核对人类权限、agent 能力/会话成员、当前请求版本、过期与取消以及完整事件快照。原生业务变更、审批结果、动作回执、通知 outbox 同事务；回执失败回滚业务变更，续跑失败重试不重复创建/删除。日历更新也持有当前 action intent 与工作锁。原生应用/SQL、真实个人项目权限、Python→Host→审批→续跑测试均已通过；消息通知故障使用持久队列、租约及有界退避，单池最多一个未结束的原生发布，Redis 挂起不会卡住领取任务。完整教育席位策略、跨进程 PostgreSQL 并发和真实调度仍需独立验证。

2026-09-06 本轮复核：LingxiLoop 提交 `d1dc951` 已修复 Canvas 终态与 Mission SQL 参数类型问题，`node scripts/test-native-learning.mjs` 和 `node scripts/test-native-canvas.mjs` 均实际通过；删除 `docs/patches`。首发仅使用原生架构及初始 schema，不提供旧数据或旧架构兼容。下方旧记录保留历史验证范围，不代表仍存在这两项阻塞。

文档重命名已接通额外原始写资源：同一事务客户端调用带依赖锁的原生权限服务，校验当前标题后执行原生 renameDocument SQL，提交后发布 Agent 归属的文档事件。冲突拒绝、写入回滚、通知失败与 Python→Host 路径共7项定向测试通过，真实源码签名检查通过；独立原生文档检查现已加载真实权限工厂、策略及查询，在个人项目最小表结构上验证成功授权及成员、会话、权益、角色、账号状态撤权后拒绝写入；已补锁文档关联会话，真实 PostgreSQL 17 双连接检查证明授权后、写入前的关联会话撤权会等待，提交后撤权可完成且后续写入被拒绝；教育席位策略尚未验证。创建、正文编辑和删除审批仍缺。本次重新读取原生列表 SQL 确认其 LIMIT 200，已纠正先前“读取所有行”的限制说明；正文读取的底层内存限制仍未解决。

文档读能力已接通原始 `listAgentDocuments/readAgentDocument` 引用：按原始用户与会话项目授权，拒绝模型指定范围，读取后复核权限，限制返回条数/正文并标明截断。Python→Host、缺少主体/项目、范围注入及读取中权限撤销检查通过，原生签名检查通过。文档写入与删除审批尚未接通；原生协作正文读取的完整执行验证及底层内存上限仍缺，不能以这些测试代替完整文档能力验收。

历史记录（两项领域缺陷已由下述 d1dc951 修复）：此前完整 `check:release` 已实际执行：134项包测试、原生契约、PostgreSQL存储、真实worker恢复及Canvas之前的原生检查通过；门禁在Canvas终态assignment被frame创建重置为working的断言处失败。串行链未运行到学习检查，随后单独运行 `test:native-learning` 仍报42P08参数类型错误。worker恢复已加入发布门禁，需独立的 `LINGXIOS_WORKER_TEST_DATABASE_URL` 空测试库，避免复用存储检查的数据。两处原生失败未绕过，测试数据库容器已清理，发布仍未就绪。

新增 `test:worker-recovery` 并实际通过：真实 worker CLI 子进程通过公开控制面、PostgreSQL 与 Python 生成并提交文件，测试代理拦住随后的完成请求后杀死 worker；过期测试租约并启动替代 worker 后，原消息与文件保持一致，没有新增模型调用，记录一次 `response.recovered`。这是提交后崩溃恢复证据，不涵盖未知业务副作用执行中的崩溃。Windows 子进程强制终止与 Linux SIGTERM 优雅退出分别验证，未混为一谈。

`test:postgres-stores` 现使用独立 Node 子进程从 PostgreSQL 接管过期任务，检查 fence/home epoch 递增并读取原有 intent、unknown receipt 和会话版本；原进程重新连接后验证旧租约不能续期/完成，新租约可取消和处理后续任务。真实 PostgreSQL 执行通过，测试容器已清理。此证据强于同进程换连接池，但尚不代表完整 worker 崩溃/启动及数据库服务重启链路通过。

评测可通过 `LINGXIOS_EVAL_REVIEW_MAX_OUTPUT_TOKENS` 单独设置审查输出预算（128–16384，默认4096），复用现有整数配置校验；报告记录运行时/审查输出预算和上下文上限。端到端测试验证真实 HTTP 请求中运行时仍为4096、审查为8192，6项评测测试通过。实际8192审查重跑答复已提交，但触发60秒超时，见 `eval/results/2026-09-06/hint-review8k`；提高token上限不足以证明审查可靠，仍未通过质量门禁。

默认提示词新增明确数量/详略约束：用户只要一个提示时不附加替代方案或追问，要求完整解答时不替换成提示；prompt-v3 使旧派生提示快照失效而保留历史。最新全量 `npm test` 134 项通过。真实模型重跑中，3 个提示答复均提交，但审查均因 `length` 截断未完成；1 个完整推导答复提交且本次审查符合。报告在 `eval/results/2026-09-06/{hint-v3,full-v3}`，不足以证明质量问题已彻底解决或发布就绪。

真实模型评测现支持 `LINGXIOS_TEST_DATABASE_URL` 指向独立空 PostgreSQL 数据库，复用原生 `pg` 连接池；不同样本使用独立租户/会话，报告记录数据库引擎和服务端版本。已实际使用 PostgreSQL 17.11 与 Qwen/Qwen3.5-4B 运行 4 个样本：全部提交，聊天约束和下载 JSON 内容检查通过；未校准语义审查为 2 个符合、1 个不确定、1 个未正常完成，命令因此返回非零。报告位于 `eval/results/2026-09-06/postgresql`；不代表产品集成、人工校准或发布质量通过。另已验证非空数据库被拒绝且原表未改动，测试容器已清理。审查失败报告尚缺 finish reason 等详细诊断，不能倒推其具体失败原因。

生产 Dockerfile 已实际构建，并通过 `npm run test:worker-image -- lingxios-worker:validation-20260906`：隔离容器不挂载主机目录、不开放端口、禁用外部网络，验证非 root、全部公开入口导入、缺配置启动拒绝、真实 Python kernel、带鉴权的空任务轮询、健康接口和 SIGTERM 正常退出。停止 worker 会取消未完成的领取请求和轮询等待，正在执行的任务仍保留退出宽限期；镜像检查刻意挂起第二次 HTTP 领取请求以验证取消。控制面为测试 HTTP 服务，未调用模型；此检查不证明生产部署或端到端业务质量。

修复前原生学习发布检查失败：`node scripts/test-native-learning.mjs` 直接执行消费项目源码时，`updateLearningMissionStepRecord` 报 PostgreSQL `42P08`（参数 `$8` 类型无法推断）。检查脚本已停止临时改写 SQL 后再报成功；消费项目源码保持不变。完整学习链路不能宣称通过。

`npm run check:release` 串行运行包测试、原生契约及现有业务检查，`prepublishOnly` 使用同一命令。它保留上述领域回归检查；即使这些检查通过，仍需完成前述业务覆盖、真实模型评测及首发验证，不能据此单独宣称发布就绪。

Canvas 已接通 `host.canvas.current()`：绑定原生会话读取服务，由包先校验持久化用户的会话读取权限；不接受模型指定 Canvas 或身份。真实源码签名和包内 Python→Host 回归已通过，原生 Canvas SQL、写入、报告和 assignment 完成链路仍未验证或实现。

Canvas `create_frame` 已接通原生 frame schema、用户写权限与稳定动作键。原生 frame 应用及 SQL 在 PGlite 中验证了重试保留同一 frame 和 1 MiB 写入边界；重试仍会重新发布事件，不能声称通知 exactly-once。报告与 assignment 的原生接口仍依赖旧 work 表，尚未接通。

Canvas `update_frame` 已接通：只允许当前会话 snapshot 中的 frame，使用原生严格 patch schema 和 frame 级写权限；所有更新要求 baseRevision。原生 SQL 已验证成功递增版本、旧版本拒绝、权限撤销不写入和跨会话拒绝。

Canvas `append_content` 已接通原生原子追加，校验当前会话 frame、用户写权限及 64 KiB UTF-8 字节上限；原生 SQL 和拒绝路径已验证。无原生追加幂等能力，失败不能自动重发；沿用动作意图/未知回执边界。

Canvas `delete_frame` 已接通原生删除，复用当前会话 frame 范围与写权限；原生 SQL 已验证权限撤销保留资源、授权删除后行消失及外部 frame 拒绝。完整产品外键、报告与 assignment 流程仍待验证。

Canvas 原生测试已载入实际基线中的主键及四项 frame 外键，验证删除 frame 后 assignment.active_frame_id、activity/comment/presence.frame_id 由数据库置空且记录保留。仍未覆盖完整产品 schema 或报告/assignment 执行流程。

Canvas 可用代理查询已通过原生应用/SQL 与实际 participants 基线验证：仅返回本租户、未离职且含 canvas 能力的 agent；排除 human，能力撤销后的重新读取不再返回该代理。授权服务仍使用测试资源，真实产品权限解析未因此获得验证。

已通过原生 SQL 确认：setCanvasStatus(status="completed") 会直接把 canvas_agent_assignments.status 改为 completed，不要求报告。包不暴露该接口；后续需将 presence 与任务状态转换分离，不能把这一原生行为当作完成验收。此反例已加入 native-canvas 检查。

原生 Canvas 创建的历史缺陷为 markAssignmentFrame 把终态 assignment 改回 working；LingxiLoop 提交 d1dc951 已修复，原生回归现已通过。临时补丁目录已删除。

PDF/DOCX 文本提取已接入独立 Node 子进程，保留来源字节哈希；PDF 提供页码并标记无可提取文本的页面，未做 OCR/图片理解。PDF.js 6.3.289 要求 Node >=22.13.0，包 engines 与 doctor 已同步。当前全量包测试 111 项通过（含 tarball 安装）；原生 Canvas/学习发布检查的已知缺陷仍未解决。

research.read 的成功回执现可追加到已有证据快照：动作键、原始字节版本与实际返回文本绑定，控制面核对当前请求版本和身份下的 intent/receipt；旧证据不可改写。回归覆盖下一次模型输入、最终引用和伪造拒绝，全量包测试 112 项通过。语义支持仍为 not_assessed，完整刷新策略与目标验收尚未完成。

Teacher 已从旧 Agent 编排入口拆出：包内直接绑定原生范围、报表、审批目标和领域命令。上下文、查询、普通写入、审批预览与已批准动作均使用持久化身份，不再调用 loadTeacherTurnContext/describeTeacherAction/executeTeacherAction，也不再回查最近 80 条消息。目标/活动发布、关闭、归档、评价审阅、教师成员管理和课程生命周期保留原生领域校验、事务与后置检查。完整原生应用授权仍是单独验收项。

Teacher 已接入 update_course、draft_objectives、draft_activity、set_room_binding 和 set_learner_membership；参数严格校验，原生执行与返回结果检查放在同一事务。课程元数据、目标草案、活动草案已通过 Python 到 native-shaped 服务、数据库与持久化回执的链路测试；房间绑定及学员成员变更已通过包内事务检查。未修改的原生 SQL 已验证草案关联范围、房间类型限制、解绑范围、公司成员状态及教师/所有者/创建者保护。原生应用权限链、绑定与成员操作的完整 Python 链路、发布审批和摘要调度仍待完成；上述检查不构成整个用户目标的验收。

目标发布/归档、活动发布/关闭和评价采纳/退回已接入审批：原生预览绑定执行意图，批准时锁定目标、重查版本和身份，再把原生数据库变更与执行回执一起提交。Python 发布审批到任务恢复的链路通过；过期、取消、回执缺失、陈旧预览和恢复失败的回归已覆盖。完整原生授权与评价应用投影仍未验收。

教师身份变更仍有跨系统提交缺口：消费项目 `teacher-agent-application.ts` 的 `set_teacher_membership` 在成员更新后调用 `syncTeacherRoomMembers`，后者在持久化房间成员之后直接 `wukongClient().upsertChannel(profile)`；`im/wukong.ts` 会执行外部 HTTP POST。传入包事务客户端不能把这个 HTTP 副作用纳入数据库回滚。包已改用原生 requireLearningCourseRole、setLearningCourseMembershipRecord 和 enqueueLearningEffect，在同一审批事务里更新成员并入队 teacher_room.sync，避开原生直接 HTTP 路径。相应绑定配置齐全时开放此审批；回执只确认 channelSync=queued。原生队列表结构和 SQL 已验证回滚、重复入队、处理中更新、失败重试与过期租约替换；实际 IM 同步结果仍待验证。

复杂请求在记录 task.contract 后，候选答复会用独立辅助调用核对原始文本、修订和附件，派生清单不替代原始要求。检查输入不截断，超预算或无效输出记为检查不可用；遗漏最多修正一次，仍未解决则提交明确的部分结果。新修订使检查失效，普通无清单问答不增加 grader 调用。该检查是模型辅助内容审查，不能验证文件内容或外部资源状态，不能授予 verification=passed；完整目标验收仍待完成。

资源读回新增 host.task.check_resource(action=..., args=..., expected={...})：控制面核对目标能力与当前请求版本，包内只读白名单复用 polls.show、learning.get_mission/get_activity、canvas.current 的原生授权路径，比较 1–16 个完整顶层字段并持久化观察时间。新动作读取当前状态，同一动作键只重放历史观察；能力撤销后不返回旧观察。缺失资源或字段是 not_observed，不能推断已删除。检查结果仍是局部资源证据，尚未与完整原始要求形成最终验收闭环，也不能授予 verification=passed。

资源观察现纳入不可改写的请求快照及最终 Envelope，最多保存 64 条。控制面逐条核对当前工作、身份、请求版本、task.check_resource intent 与真实回执，拒绝伪造、覆盖和冒用旧版本的新观察；期望字段与实际字段一并留存。模型恢复上下文和复杂候选内容审查均读取此快照，旧版本明确标记为历史观察。此链路保存局部检查证据，尚未实现候选提交前自动刷新所有资源后置条件，也不证明全目标完成。

候选交付前现会刷新当前请求版本已经记录的资源字段检查：相同 action/args/expected 去重，使用本次 fence/hop 的新动作键重新授权读回，保存新观察并检查取消/修订。最新观察及刷新缺口进入内容核对；先前通过、后来不符的字段保留失败缺口。旧修订不自动重新执行，达到 64 条观察上限时明确报告无法刷新。有当前资源检查的候选也会进行内容核对，无相关检查的简单问答不新增调用。该刷新覆盖已明确记录的检查，完整原始要求的覆盖性与权威完成判定仍未完成。

包内初始 agent_memories 表及 memory.list/recall/note/verify 已接通。普通代理可见该能力，teacher 管理代理仍保留原有专属范围；每次操作走 agent_memory 原生权限，learner 目标必须是当前会话的活跃人类成员。记忆按租户与 learner/course/agent_role 范围隔离，note 绑定当前租约、请求版本及原始输入哈希，verify 要求版本匹配并保留来源；过期记忆不召回，过期验证必须提供新的有效期限。最初接入的是字面匹配和时间排序；后续自动召回、综合与语义索引进展见下文。全量包测试 122 项通过，原生签名检查通过；没有宣称完整记忆功能或全目标完成。

自动记忆上下文已接通：当前发起人的 learner 记忆（须为会话活跃成员）、course 和 agent_role 记忆共同使用约 12 KiB 上限，保留完整条目、来源及版本并记录遗漏数。召回授权失败或读取故障时返回 unavailable，仍继续原始请求；模型调用事件保存实际使用的快照。若记忆使完整输入超过预算，优先移除可选记忆，保留原始请求并记录 memoryOmittedForBudget。原生形状 Python 链路验证了首次召回故障仍完成请求、写入后下一轮取得记忆及快照留痕。全量 123 项测试通过；随后预算优先级修正的 9 项运行时测试通过。后续语义排序与后台综合进展见下文。

记忆管理补齐 memory.pin/delete：固定与取消固定会递增版本并记录当前请求来源，召回按固定状态优先；删除按租户、范围和 expectedVersion 删除指定记录。两者沿用活跃租约、取消检查和 agent_memory 写权限。实际 SQL 验证了排序、陈旧版本、错误范围和权限撤销；Python 到持久化回执的固定/删除链路已通过，临时记忆删除后不再召回。构建与 7 项记忆/产品集成定向测试通过；未重复全量测试。后续语义召回与综合流程进展见下文。

已提交对话的记忆证据采集和后台入队已接入最终消息事务：仅对具备 memory 能力的工作采集，source_run_id 外键指向真实已提交消息；保存身份、请求版本、来源引用和含附件的完整输入哈希。输入和答复摘录各最多 16000 字符，截断单独标记且不拆 Unicode 代理对。消息事务回滚时证据与综合工作一同回滚，重复提交不重复入队。每份来源对应一个 namespaced memory_synthesis 后台工作，利用当前三层记忆合并跨轮信息，本地与 HTTP worker 均已注册处理器。提议与独立验证共用 90 秒模型调用期限，输入预算检查保留输出额度；来源摘要各限 4000 字符且标记截断，现有记忆快照不超过 12KB，每次最多 12 项变更。最终领域权限和成员身份、工作租约、来源版本、实际读取的记忆版本均在写入前重查；create/update/expire 与证据处理状态同事务提交，置信度不足 0.6 或验证拒绝不写记忆。显式或置顶记录不能被综合覆盖或过期。失败工作指数退避，最多三次执行，第三次失联租约过期也会终止，避免热循环。

工作取消或修订版本变化通过数据库触发器在同一事务中将旧版 pending/processed 证据标为 superseded，并将依赖该来源的普通 synthesized 记忆过期、增加版本；显式和置顶记录保持保护。普通完成与后续新请求不撤销历史来源。启动检查拒绝缺失或禁用该触发器的 schema。新请求提交后，综合会将各范围的有效与过期记录交错纳入同一个 12KB 预算，过期记录标记 needsReverification。续期须通过独立验证、提供新的未来 validUntil，且证据观察时间晚于该记录过期时间；观察时间取请求创建时间或不晚于证据提交的服务端最新修订时间；没有新依据时仍不召回过期记录。显式或置顶记录继续禁止自动续期。较早请求内的新修订也可提供复核依据，无效或晚于证据提交的修订时间不会提高证据新鲜度。纯定时巡检和真实模型复核质量仍未验证或实现。

记忆版本变化会在同一事务中将被替换记录的完整快照写入 agent_memory_versions，保留正文、来源、置顶、过期时间和状态；版本回退被拒绝。显式删除记忆通过外键同时删除该记忆的历史快照，避免历史表保留已删除的记忆正文。启动检查同时验证版本历史表与触发器。

语义索引的包内 embedding HTTP 客户端已实现，按官方 embeddings 协议发送批量 float 请求：模型必须显式指定，每次最多 32 段、每段 8000 UTF-8 字节，响应最多 4 MiB；校验索引唯一且完整、维度一致、向量数值有限且非零，并按输入顺序归一化。支持调用取消和请求超时，拒绝重定向，不把服务端错误正文写进异常。

createLingxiLoop 的可选 embeddings 配置已接入实际索引与召回。授权读取每次为该范围最多 32 条缺失索引创建持久 memory_index 工作，后续读取继续回填，查询不限制在最近 32 条。后台索引前后重验原生权限，写回时锁定工作租约与目标记忆版本，删除级联清理向量。模型配置和实际返回模型均参与匹配，旧版本或旧模型向量不参与排序。前台查询向量缓存 60 秒、最多 64 项，外部调用最多等待 3 秒；SQL 按完整授权范围精确计算余弦相似度，语句同样限时 3 秒。无索引、服务故障和查询降级均带 retrieval 标记，不妨碍原始请求。未配置 embedding 时保持字面召回。包测试已验证旧语义匹配、跨范围隔离、版本变更、权限撤销、模型切换、故障降级，以及 HTTP worker 到下一轮真实模型输入的快照链路；全量 127 项通过。真实向量语义质量和大范围扫描性能仍未验证。

Worker Docker 镜像已实际构建并启动验证：Node 22.23.2、Python 3.11.2、UID 1000，使用只读根文件系统、受限 tmpfs、移除 capabilities 与 no-new-privileges。包内 schema/runner 可读取，Python runner 可解析，homes 可写，实际 worker 能向测试控制面领取空队列、返回 /healthz 并在 SIGTERM 后正常退出。此检查不证明真实模型、产品数据库连接或多租户 OS 隔离。独立 tarball 安装测试已通过，运行说明见 packaged-runtime.md。

Teacher draft_objectives 返回值现校验课程范围、有效且唯一的 ID，以及至少容纳请求批次的条目数。原生接口返回整个课程目标列表，因此保留已有已发布目标，不错误要求所有返回值为 DRAFT。校验失败与写入在同一事务中回滚；这只检查返回结果的结构与范围，不能单独证明新增目标的全部内容或完整用户目标达成。构建及 7 项 Teacher/产品集成定向测试通过。

Teacher transition_course 已接入版本绑定审批与事务后置条件：锁定 project/course/active room，执行原生生命周期，核对项目和房间状态，再持久回执。END 批准后正常恢复；只读/归档关闭房间后，审批返回 continuation_unavailable 与执行结果，工作记录 blocked/inconclusive，保留会话回执，并原子写入有序公开停止事件和恢复标记。重试不再执行变更；审批查询现在也返回摘要与结果。Python END/只读链路、事务回滚、失联恢复与重复调用通过；原生生命周期策略和房间关闭/效果入队 SQL 通过。原生 Teacher 要求活跃房间，而只读投影会关闭房间，因此其后从关闭房间发起 ARCHIVE 仍不可用，完整归档产品流程和真实应用权限链尚未完成，不能以此宣称完整 Teacher 能力已交付。

新增开发用真实模型评测命令 eval:live：直接调用公开 app/eval 入口、真实 HTTP 模型驱动和 Python runner，每个样本使用独立 PGlite 数据库及 homes。初始 4 个候选题覆盖完整推导、只给提示、只聊天且禁止代码/文件、真实 JSON 文件交付；明确尚未经人工审阅。独立模型审查现在可直接接收正常模型配置，不需自定义 driver。报告保留原题、答复、下载字节、模型用量/finish reason、失败信息及实现哈希，确定性检查与未校准语义审查分开。已使用现有 Qwen/Qwen3.5-4B 配置实际运行 12 个样本：11 个本地提交答复，3 次文件内容和 3 次仅聊天约束检查通过；语义审查 6 次符合、1 次不确定、5 次无评估（其中一次无答复，四次审查不可用）。首轮失败原因因旧版报告缺诊断仍未定位，不能把后续通过当作已修复。原始两轮报告保存在 eval/results/2026-09-06。构建及 6 项 eval/独立安装测试通过；真实模型命令因未交付或审查不确定返回非零，未宣称发布质量通过。

Teacher context now binds native findTeacherScopeBinding/findTeacherTurnCounts/requireLearningCourseRole directly and uses the persisted principal. Context no longer reads legacy agent_routines or recent messages. Native action execution and approval preview still resolve trigger messages; their history limit and the package-owned digest scheduler remain unfinished. Build, nine targeted Teacher/approval/integration tests and native source contract checks passed.

Teacher 摘要已使用初始命名空间 schema 的 agent_routines/agent_routine_runs，自带 30 秒 claim 调度、每批 8 项锁定、漏跑合并、版本取消、同计划单待处理任务和权限撤销暂停。local/remote worker 注册对话执行流程；计划任务只授予摘要读能力，并禁止问题、写入与个体详情。Python 端到端已验证入队、只读能力、消息交付与失败后的撤权/暂停抑制。下次执行时间由包内 PostgreSQL 查询计算，已移除宿主 calculateTeacherDigestRun 绑定；DST 缺失/重复时刻、回拨期间和日/周边界检查通过。完整原生授权、多连接 PostgreSQL 并发与真实模型摘要质量仍未验收；发送中的 IM 调用无法撤回。

本轮验证：完整 npm test 128 项通过（使用 D:/Temp/LingxiOS-npm-cache 完成离线独立包安装），包括真实 Python 与远程 worker 摘要执行。随后保留原生指标计数并收紧指标名称契约，Teacher/摘要定向 3 项与原生签名契约再次通过。git diff --check 无空白错误。完整原生业务发布门禁与全计划验收仍未完成，未推送。


通用 routines 已接入 list/pause/create/activate、原生 agent_run:control 权限、包内审批事务和调度。初始 schema 支持多个通用计划及独立教师摘要唯一约束；创建后暂停，激活单独审批并核对原计划版本。计划保存创建者和回复线程，调度/执行/交付重验当前绑定、能力和授权；暂停取消未完成工作与未交付结果。定向 PGlite 检查覆盖事务回滚、旧预览、跨线程隔离、撤权、DST；本地与远程 Python 到消息交付及撤权/暂停后的重试抑制通过。完整原生授权、真实 PostgreSQL 并发和真实模型定时任务质量尚未验收。


learning.start_mission 已迁入包内编排：绑定原生 Mission repository 和 inc，保留原协调者选择/去重，产品 Mission 与 lingxios 协调工作同事务提交，禁止替换原授权用户。新协调任务保存触发人原文和回复线程，使用会话处理器；原生卡片在事务后发送。新增 test:native-missions 使用未修改原生 SQL 验证来源范围、回滚、去重、卡片和 Python 协调者交付；暂停、取消及重新分配后拒绝旧协调工作。完整原生权限、其他学习写操作和真实模型质量仍未验收。


学习草稿 draft_knowledge_units/draft_activity 已绑定原生 runtime 导出，当前上下文确定 project，原用户经过 conversation/project learning:submit 检查，原生作者保留 Agent。批量、文本、枚举、等级、ID 与时间参数验证已补齐；不发布，不添加虚假幂等保证。构建、5 项集成/定向测试、原生契约与 Mission SQL/Python 回归通过；这两项草稿写入的完整原生应用权限执行仍未单独验证。


record_attempt 已接入原生 recordLearningAttempt 与证据查询导出；消息排除 Agent 代发标记，文档/Canvas 原生证据作者必须等于持久化原用户。身份和版本检查与原生写入共享 repeatable-read 事务，返回学习者不符会回滚，指标延后至提交。构建、6 项定向检查、原生签名契约、Mission 回归通过；新增测试中的原生记录写入为 mock，完整原生证据应用执行仍未验证。
# Native evidence application check

`npm run test:native-evidence` builds the package and executes its `recordAttempt` adapter plus the sibling's unmodified `recordLearningAttempt`, evidence application, and repository SQL in PGlite using minimal relational fixtures. It checks published activity submission and evidence links; rollback for draft, foreign-company, and missing activities; success metrics; exact replay; conflicting record content; scoped learning-attempt links; rollback after a later invalid link; size/link bounds; and scoped product evidence reads. The adapter check verifies the conversation permission request, real native writer, outer transaction rollback, and post-commit metric. It is included in `check:release`. Permission service and unrelated lifecycle methods are explicit test seams. This does not prove real learning authorization, the full product schema, or HTTP/Python action dispatch.
# Learning evaluation adapter

The learning native check now supports an explicit disposable PostgreSQL URL as well as PGlite. It passed against a temporary local PostgreSQL 17 container, including actual native permissions, submission, evaluation and state SQL. A multi-connection test observed the revocation session in `pg_blocking_pids` while the authorized transaction held its dependency locks, then verified successful commit followed by denial of later writes. This proves that tested authorization ordering; it does not cover every concurrent state-projection scenario or the full production schema. The temporary container was removed after validation.

Attempt list/detail reads now reauthorize inside the same repeatable-read, read-only transaction as their resource queries. A native-policy regression first reproduced a read succeeding after membership was revoked between precheck and transaction; both list and detail now reject that case. The resource-check path reuses this reader. This establishes snapshot-consistent authorization, not cancellation of reads already authorized in an earlier snapshot.

Submission and evaluation now perform a final native permission check inside the write transaction, using the raw access factory with dependency locks. The native evidence script loads the actual access resolver, policy, repositories and domain functions, plus the full native submission/evaluation module graph. It verifies a valid personal teaching-project learner and rejects inactive company/project membership, observer writes, suspended users, disabled learning entitlement and cross-company project scope. Revoking membership after a successful precheck also prevents both writes. These checks use minimal relational tables; multi-connection concurrency and full production-schema coverage remain separate.

Attempt detail is also wired into `task.check_resource`. The packaged integration test now executes the check from Python through the Host/control plane, refreshes it before delivery, and verifies the observed `EVALUATED` status in the persisted response envelope. This uses a relational fixture and a scripted model; it does not establish semantic answer quality or complete goal acceptance.

Learner-scoped `list_attempts` and `get_attempt` now expose persisted submission metadata, evidence references, and evaluation results. Reads authorize the original principal, require an active human, constrain company/project/learner, and report truncation after 100 results. The native evidence check exercises these reads against the records created by the actual native submission/evaluation functions, plus cross-scope rejection and bounds. This supplies an inspection path, not automatic resolution of an unknown action outcome or permission to replay a write.

`learning.propose_evaluation` binds the raw native proposal function and score schema. It validates arguments, authorizes the persisted principal, requires an active human-owned attempt in the current project, validates optional evidence scope, and preserves native evaluation/status logic within a serializable transaction. Metrics are delayed until commit. The targeted adapter test verifies scope rejection and rollback. The native evidence check now executes the actual proposal, score schema, learning-state policy, and state repository through the package adapter: accepted evaluation updates the learner state and attempt status; low-confidence evaluation remains pending; a real state-scope failure rolls back the evaluation insert and suppresses metrics. The source contract check verifies function/schema compatibility. Full native authorization, concurrent state projection across database connections, and uncertain-result reconciliation remain unverified.
# Canvas comment capability

`canvas.add_comment` now binds the native comment writer/schema with original-human authorization, package-supplied canvas/agent identity, and optional frame membership validation. Adapter and native schema checks pass. The Canvas script also executes the real collaboration application and comment SQL, verifies agent/frame attribution and the published event, and demonstrates that a publication failure leaves the comment stored. Through the actual control-plane action path with in-memory stores, this failure yields an unknown receipt and replaying the same action does not insert another comment. Scope resolution, activity logging, and publication use fixture callbacks; full native authorization and crash recovery with PostgreSQL control stores are separate checks. The terminal-assignment regression now passes against LingxiLoop d1dc951. Assignment/report orchestration still depends on native legacy work tables and remains incomplete.
# Real PostgreSQL control stores

`test:postgres-stores` passed on a temporary PostgreSQL 17 container. Separate pool connections tested competing work claims, same-session exclusion, independent thread execution, a single successful action reservation among eight contenders, and a single successful session revision among concurrent writers. Expired leases recovered through a new pool/store instance with increased fence/home epoch; old credentials failed, unknown receipts persisted, cancellation released the session, and a product-table sentinel remained unchanged. This check is now required by `check:release`. It does not prove full worker-process or database-server crash recovery. The temporary container was removed.

日历读取已接通原生 calendarApplication.list/get 与原生日期查询 schema，按持久化人类身份和当前会话项目授权；教师能力不扩张。公共工厂→Python→Host 5 项集成检查、真实源码签名检查和原生日历应用/SQL检查通过，覆盖私有事件、跨租户/项目、重复规则、范围与100条截断。原生检查中的权限服务仍为替身；日历写入、派发和提醒尚未接通。

日历 update 已接通额外原始资源，完整事件快照比较、锁定人类权限、原生校验与应用更新共用事务客户端，提交后通知归属于 Agent。原生日历应用/SQL检查和5项公共入口集成检查通过；创建/删除审批与主动派发仍缺。

本轮全量 npm test 134/135通过，唯一失败为离线安装测试误用默认空 npm 缓存导致 ENOTCACHED；设置此前准备的 D:/Temp/LingxiOS-npm-cache 后，独立安装测试单独重跑通过。随后补日历目标会话写权限检查，构建和原生日历检查再次通过，未重复运行全量。
