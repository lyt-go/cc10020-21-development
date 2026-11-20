"""跨模块数据流管道API：暴露完整业务链路端点。

将领域服务的能力串联为可测试的数据流管道，每条管道对应
一个跨模块数据流链路。
"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/pipeline", tags=["跨模块数据流"])


class WebhookPayload(BaseModel):
    event_type: str = "default_event"
    data: dict[str, Any] = {}
    headers: dict[str, str] | None = None
    signature: str | None = None


class BatchExecuteRequest(BaseModel):
    task_id: int
    max_pages: int = 5


class DlqReplayRequest(BaseModel):
    dlq_ids: list[int]


class BackfillExecuteRequest(BaseModel):
    job_id: int


class EndpointTestRequest(BaseModel):
    endpoint_id: int


class TransformRequest(BaseModel):
    rule_id: int
    source_data: list[dict[str, Any]]


class ValidationRequest(BaseModel):
    records: list[dict[str, Any]]
    source_module: str = ""


class RawEventQuery(BaseModel):
    channel_id: int | None = None
    event_type: str | None = None
    outcome: str | None = None
    page: int = 1
    size: int = 50


class RevalidateRequest(BaseModel):
    raw_event_id: int | None = None
    raw_event_ids: list[int] = []


@router.post("/webhook-to-lake")
def pipeline_webhook_to_lake(channel_id: int, payload: WebhookPayload) -> dict[str, Any]:
    """链路1: Webhook接收 → 原始接入留存 → 结构校验 → 去重 → 分发 → 正式分区落湖。

    完整覆盖：webhook_channel → webhook_ingest_raw → event_schema →
    dedup_filter → dispatch_rule → data_lake_sink（formal）。
    结构校验失败/中断时返回 raw_event_id，原始数据仍可查询和补验。
    """
    from app.services.webhook_channel import WebhookChannelService
    svc = WebhookChannelService()
    result = svc.receive_webhook(
        channel_id=channel_id,
        payload={"type": payload.event_type, **payload.data},
        signature=payload.signature,
        headers=payload.headers,
    )
    return result


@router.get("/webhook-ingest/raw")
def pipeline_list_raw_events(
    channel_id: int | None = None,
    event_type: str | None = None,
    outcome: str | None = None,
    page: int = 1,
    size: int = 50,
) -> dict[str, Any]:
    """查询原始接入数据（来源行），结构校验失败或中断后仍可查到。"""
    from app.persistence import json_store
    from app.services.ingest_validation import RAW_MODULE
    rows = json_store.get_all(RAW_MODULE)
    if channel_id is not None:
        rows = [row for row in rows if int(row.get("通道ID", 0)) == channel_id]
    if event_type:
        rows = [row for row in rows if row.get("事件类型") == event_type]
    if outcome:
        rows = [row for row in rows if row.get("处理结论") == outcome]
    rows.sort(key=lambda row: str(row.get("接收时间", "")), reverse=True)
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start:start + size], "total": total, "page": page, "size": size}


@router.get("/webhook-ingest/raw/{raw_event_id}")
def pipeline_get_raw_event(raw_event_id: int) -> dict[str, Any]:
    """追溯单笔数据：最初原文、新旧结构结论、未知字段明细与待处理清单。"""
    from app.services.ingest_validation import IngestValidationService
    result = IngestValidationService().trace(raw_event_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("message", "原始接入记录不存在"))
    return result


@router.get("/webhook-ingest/unknown-fields")
def pipeline_list_unknown_fields(
    raw_event_id: int | None = None,
    status: str | None = None,
) -> dict[str, Any]:
    """未知字段处置明细：标出来源行、字段和待补验原因。"""
    from app.services.ingest_validation import IngestValidationService
    rows = IngestValidationService().list_unknown_fields(
        raw_event_id=raw_event_id, status=status)
    return {"items": rows, "total": len(rows)}


@router.get("/webhook-ingest/pending")
def pipeline_list_pending(status: str | None = None, page: int = 1, size: int = 50) -> dict[str, Any]:
    """待处理清单：每条来源行一条，说明本次异常原因以供补验。"""
    from app.services.ingest_validation import IngestValidationService
    rows, total = IngestValidationService().list_pending(status=status, page=page, size=size)
    return {"items": rows, "total": total, "page": page, "size": size}


@router.post("/webhook-ingest/revalidate")
def pipeline_revalidate(request: RevalidateRequest) -> dict[str, Any]:
    """补验：补齐结构定义后，对同一来源行重新校验并补写正式分区。"""
    from app.services.ingest_validation import IngestValidationService
    svc = IngestValidationService()
    ids = list(request.raw_event_ids)
    if request.raw_event_id is not None and request.raw_event_id not in ids:
        ids.insert(0, request.raw_event_id)
    if not ids:
        raise HTTPException(status_code=400, detail="需要提供 raw_event_id 或 raw_event_ids")
    results = [svc.revalidate(raw_event_id) for raw_event_id in ids]
    return {
        "ok": all(item.get("ok") for item in results),
        "message": f"已补验 {len(results)} 条来源行",
        "results": results,
    }


@router.get("/webhook-ingest/formal")
def pipeline_query_formal(event_type: str, date_str: str | None = None) -> dict[str, Any]:
    """仅查询正式分区（只含已确认结构字段），用于当日核对。"""
    from app.services.data_lake_sink import DataLakeSinkService
    return DataLakeSinkService().query_formal_data(event_type, date_str)


@router.post("/batch-ingest-to-lake")
def pipeline_batch_ingest_to_lake(request: BatchExecuteRequest) -> dict[str, Any]:
    """链路2: 批量拉取 → 数据转换 → 质量校验 → 数据落湖。

    完整覆盖：batch_ingest → data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.batch_ingest import BatchIngestService
    svc = BatchIngestService()
    result = svc.execute_batch(task_id=request.task_id, max_pages=request.max_pages)
    return result


@router.post("/dlq-replay")
def pipeline_dlq_replay(request: DlqReplayRequest) -> dict[str, Any]:
    """链路3: 死信队列 → 分析 → 重放 → 追踪。

    完整覆盖：dlq_handler → mq_consumer → call_trace_log。
    """
    from app.services.dlq_handler import DlqHandlerService
    svc = DlqHandlerService()

    if len(request.dlq_ids) == 1 and request.dlq_ids[0] == -1:
        # 全部待处置记录重放
        analysis = svc.analyze_failure_reasons()
        if analysis["total"] == 0:
            return {"ok": False, "message": "无死信记录", "replayed": 0}
        from app.persistence import json_store
        all_dlq = json_store.get_all("dlq_handler")
        pending_ids = [int(r["id"]) for r in all_dlq if r.get("status") in ("草稿", "待处置")]
        return svc.batch_replay(pending_ids)

    return svc.batch_replay(request.dlq_ids)


@router.post("/backfill-execute")
def pipeline_backfill_execute(request: BackfillExecuteRequest) -> dict[str, Any]:
    """链路4: 历史回补 → 水位查询 → 批量拉取 → 数据落湖。

    完整覆盖：backfill_job → incre_cursor → batch_ingest → data_lake_sink。
    """
    from app.services.backfill_job import BackfillJobService
    svc = BackfillJobService()
    result = svc.execute_backfill(job_id=request.job_id)
    return result


@router.post("/endpoint-test")
def pipeline_endpoint_test(request: EndpointTestRequest) -> dict[str, Any]:
    """链路5: 端点连接测试 → 认证验证 → 调用追踪。

    完整覆盖：api_endpoint → api_auth_store → call_trace_log。
    """
    from app.services.api_endpoint import ApiEndpointService
    svc = ApiEndpointService()
    result = svc.test_connection(endpoint_id=request.endpoint_id)
    return result


@router.post("/transform-validate")
def pipeline_transform_validate(request: TransformRequest) -> dict[str, Any]:
    """链路6: 数据转换 → 质量校验 → 结果追踪。

    完整覆盖：data_transform → data_quality_rule → data_lake_sink。
    """
    from app.services.data_transform import DataTransformService
    transform_svc = DataTransformService()
    batch_result = transform_svc.batch_transform(rule_id=request.rule_id, source_records=request.source_data)
    return batch_result


@router.post("/batch-validate")
def pipeline_validate_batch(request: ValidationRequest) -> dict[str, Any]:
    """链路7: 质量批量校验 → 统计报告。

    完整覆盖：data_quality_rule → integration_monitor。
    """
    from app.services.data_quality_rule import DataQualityRuleService
    svc = DataQualityRuleService()
    result = svc.validate_batch(records=request.records, source_module=request.source_module)
    return result


@router.get("/health-summary")
def pipeline_health_summary() -> dict[str, Any]:
    """链路8: 全局监控指标聚合 → 健康度评分。

    完整覆盖：integration_monitor → call_trace_log → dlq_handler → data_lake_sink。
    """
    from app.services.integration_monitor import IntegrationMonitorService
    svc = IntegrationMonitorService()
    return svc.collect_global_metrics()


@router.get("/dedup-stats")
def pipeline_dedup_stats() -> dict[str, Any]:
    """链路9: 去重统计查询。"""
    from app.services.dedup_filter import DedupFilterService
    svc = DedupFilterService()
    return svc.get_dedup_stats()


@router.post("/archive-execute")
def pipeline_archive_execute(policy_id: int) -> dict[str, Any]:
    """链路10: 归档策略执行 → 压缩 → 过期清理。"""
    from app.services.event_archive import EventArchiveService
    svc = EventArchiveService()
    return svc.execute_archive(policy_id=policy_id)