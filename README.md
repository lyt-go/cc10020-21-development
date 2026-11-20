# API集成与事件编排平台

面向第三方API接入、Webhook事件接收、消息队列消费与服务编排的全链路API数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| API端点注册 | `api_endpoint` | 端点配置 | 端点编号、端点名称、请求方法 |
| Webhook通道 | `webhook_channel` | 通道配置 | 通道编号、通道名称、接收路径 |
| 事件结构定义 | `event_schema` | 事件结构 | 结构编号、事件类型、字段名称 |
| 消息队列消费 | `mq_consumer` | 消费配置 | 配置编号、队列名称、消费组名 |
| 死信队列管理 | `dlq_handler` | 死信记录 | 记录编号、原始队列、失败原因 |
| 认证凭证管理 | `api_auth_store` | 认证凭证 | 凭证编号、关联端点、凭证类型 |
| 流量控制 | `rate_limit_config` | 限流规则 | 规则编号、关联端点、请求间隔 |
| 数据转换编排 | `data_transform` | 转换规则 | 规则编号、源Schema、目标Schema |
| 事件分发路由 | `dispatch_rule` | 分发规则 | 规则编号、事件类型、目标系统 |
| 事件去重 | `dedup_filter` | 去重规则 | 规则编号、事件类型、去重键列 |
| 批量接入调度 | `batch_ingest` | 接入任务 | 任务编号、关联端点、Cron表达式 |
| 数据落湖 | `data_lake_sink` | 落湖配置 | 配置编号、数据来源、目标路径 |
| 接入质量校验 | `data_quality_rule` | 质量规则 | 规则编号、数据来源、校验字段 |
| 调用链路追踪 | `call_trace_log` | 调用记录 | 记录编号、关联端点、请求参数 |
| 增量水位管理 | `incre_cursor` | 水位标记 | 标记编号、关联端点、增量字段 |
| 历史回补任务 | `backfill_job` | 回补任务 | 任务编号、关联端点、起始日期 |
| 事件归档清理 | `event_archive` | 归档策略 | 策略编号、事件类型、归档条件 |
| 集成监控看板 | `integration_monitor` | 监控指标 | 指标编号、监控对象、成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## Webhook 接入补验：已确认范围与未知字段处置

Webhook 事件接入不再“结构校验失败就整条丢弃”。处理顺序是
**先留存原文 → 再做结构校验 → 只把已确认字段送正式分区**：

1. **原始接入留存**（`webhook_ingest_raw`）
   每笔事件在签名校验后立即落一条来源行，保存原始 Payload、原文摘要、
   接收时间和结论修订记录。即使结构校验失败或中断，`raw_event_id` 仍然可查。
2. **结构校验分级**（`event_schema.validate_event`）
   - 已确认结构范围：结构定义里存在且类型匹配的字段，进入 `usable_fields`；
   - 未知字段：Payload 里有、结构定义里没有（或该事件类型根本没有已生效定义）
     的字段，记入未知字段处置明细并说明来源；
   - 类型不符 / 缺少必填字段：标记“待更正 / 待补验”，不会当作通过。
3. **数据落湖**（`data_lake_sink.write_formal_event`）
   只有已确认字段写入正式分区 `formal/<事件类型>/<日期>`，记录带 `raw_event_id`
   回指来源行。任何未经确认的字段都不会进入正式取数。
4. **待处理清单**（`webhook_pending`）
   一条来源行一条清单，写明本次异常原因、已确认结构范围、可正式取数字段
   （用于下游当日核对）和待补验字段。清单关闭时保留最初的异常原因。
5. **补验**（`ingest_validation.revalidate`）
   补齐结构定义后自动重新校验同通道/同事件类型的待补验来源行，也可手动补验。
   补验只更新**同一条来源行**和它名下的未知字段明细，不新增来源行；
   最初原文与新旧结构结论都保留在 `结论记录` 里，正式分区中旧版本标记为“已取代”，
   同一条订单只保留一条生效记录。

相关接口（`backend/app/routers/pipeline.py`）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/pipeline/webhook-to-lake?channel_id=…` | 接收事件，返回 `raw_event_id`、已确认范围、未知字段与待处理清单 |
| GET | `/api/pipeline/webhook-ingest/raw` | 查询原始接入来源行（可按通道/事件类型/结论过滤） |
| GET | `/api/pipeline/webhook-ingest/raw/{id}` | 追溯单笔：原文、新旧结构结论、未知字段明细、待处理清单 |
| GET | `/api/pipeline/webhook-ingest/unknown-fields` | 未知字段处置明细 |
| GET | `/api/pipeline/webhook-ingest/pending` | 待处理清单 |
| POST | `/api/pipeline/webhook-ingest/revalidate` | 补验（`raw_event_id` 或 `raw_event_ids`） |
| GET | `/api/pipeline/webhook-ingest/formal?event_type=…` | 只查正式分区，用于当日核对 |

前端入口：侧边栏「接入补验工作台」（`frontend/src/views/IngestReviewView.vue`），
可查待处理清单、看原文与新旧结论、执行补验。

领域：数据工程
子方向：数据采集、爬虫与RPA
