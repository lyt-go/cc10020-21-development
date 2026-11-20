"""Webhook通道领域服务：事件接收、签名验证、Payload解析与分发编排。

提供完整的Webhook接收流程：签名校验→Payload验证→事件解析→去重→分发路由→落湖。
关联模块：event_schema（结构校验）、dedup_filter（幂等去重）、dispatch_rule（条件路由）、data_lake_sink（落湖存储）。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.ingest_validation import RAW_MODULE, IngestValidationService
from app.store import store


class WebhookChannelService(BaseModuleService):
    MODULE = "webhook_channel"
    ENTITY = "通道配置"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["通道编号", "通道名称", "接收路径", "签名密钥", "验证方式", "并发上限", "回调地址", "通道版本"]
    REQUIRED_FIELDS = ["通道编号", "通道名称", "接收路径"]
    STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启用通道": "已启用", "停用通道": "已停用"}
    NEGATIVE_ACTIONS = ["停用通道"]

    def __init__(self) -> None:
        super().__init__()
        self._received_events: list[dict[str, Any]] = []

    def receive_webhook(self, channel_id: int, payload: dict[str, Any],
                        signature: str | None = None,
                        headers: dict[str, str] | None = None) -> dict[str, Any]:
        """接收Webhook事件的核心方法。

        完整流程：
        1. 通道状态校验
        2. 签名验证（HMAC-SHA256或RAW）
        3. 原始接入留存（先落原文，再做结构校验）
        4. 事件结构校验：区分已确认结构范围和未知字段，生成未知字段处置明细
        5. 事件去重判定
        6. 分发规则匹配
        7. 已确认字段写入正式分区；待补验内容同步到待处理清单

        结构校验未通过或中断时不再直接丢弃事件：原始数据已留存，
        结果里带 raw_event_id，可通过补验接口继续处理。
        """
        channel = self._resolve_channel(channel_id)
        if channel is None:
            return {"ok": False, "message": f"通道 {channel_id} 不存在", "event_id": None, "stage": "通道校验"}

        if channel.get("status") != "已启用":
            return {"ok": False, "message": f"通道 {channel_id} 未启用（当前状态: {channel['status']}）", "event_id": None, "stage": "状态校验"}

        event_type = str(payload.get("type") or payload.get("event_type", "unknown"))
        event_id = str(uuid.uuid4())
        trace: list[dict[str, Any]] = []
        ingest_svc = IngestValidationService()

        # 阶段1：签名验证
        secret = str(channel.get("签名密钥", ""))
        verify_method = str(channel.get("验证方式", "HMAC-SHA256"))
        sign_ok = self._verify_signature(payload, signature, secret, verify_method)
        trace.append({"stage": "签名验证", "passed": sign_ok, "method": verify_method})

        # 阶段2：原始接入留存（在任何校验结论产生之前）
        raw = ingest_svc.record_raw_event(
            channel_id=channel_id, event_type=event_type, payload=payload, event_id=event_id)
        raw_event_id = int(raw["id"])
        trace.append({"stage": "原始接入留存", "passed": True,
                      "detail": raw.get("原始接入编号"), "raw_event_id": raw_event_id})

        if not sign_ok and verify_method != "NONE":
            ingest_svc.apply_validation(
                raw_event_id,
                {"ok": False, "schema_found": False, "message": "签名验证失败，未进入结构校验",
                 "pending_reason": "签名验证失败"},
                conclusion_kind="签名验证失败", interrupted=True, outcome="signature_failed")
            self._record_receive_event(channel_id, payload, "signature_failed", trace)
            return {"ok": False, "message": "签名验证失败", "event_id": None,
                    "raw_event_id": raw_event_id, "stage": "签名验证", "trace": trace}

        # 阶段3：事件结构校验（跨模块：event_schema）
        schema_validation, interrupted = self._validate_event_schema(
            channel_id, event_type, payload)
        if interrupted:
            trace.append({"stage": "结构校验", "passed": False, "interrupted": True,
                          "detail": schema_validation.get("message")})
            updated = ingest_svc.apply_validation(
                raw_event_id, schema_validation, conclusion_kind="结构校验中断",
                interrupted=True, outcome="schema_interrupted")
            self._record_receive_event(channel_id, payload, "schema_invalid", trace)
            return {
                "ok": False,
                "message": f"结构校验中断，原始接入数据已留存可补验: {schema_validation.get('message')}",
                "event_id": event_id,
                "raw_event_id": raw_event_id,
                "stage": "结构校验",
                "trace": trace,
                "pending": ingest_svc.pending_for(raw_event_id),
                "unknown_fields": ingest_svc.list_unknown_fields(raw_event_id=raw_event_id),
                "raw_event": updated,
            }

        updated = ingest_svc.apply_validation(
            raw_event_id, schema_validation, conclusion_kind="初次校验",
            interrupted=False, outcome=None) or raw
        trace.append({
            "stage": "结构校验",
            "passed": schema_validation["ok"],
            "detail": schema_validation.get("message"),
            "confirmed_fields": schema_validation.get("confirmed_fields", []),
            "unknown_fields": [u["field"] for u in schema_validation.get("unknown_fields", [])],
            "pending_reason": schema_validation.get("pending_reason", ""),
        })

        if not schema_validation["ok"]:
            self._record_receive_event(channel_id, payload, "schema_invalid", trace)
            return {
                "ok": False,
                "message": f"事件结构校验失败，原始接入数据已留存可补验: {schema_validation['message']}",
                "event_id": event_id,
                "raw_event_id": raw_event_id,
                "stage": "结构校验",
                "trace": trace,
                "pending": ingest_svc.pending_for(raw_event_id),
                "unknown_fields": ingest_svc.list_unknown_fields(raw_event_id=raw_event_id),
                "raw_event": updated,
            }

        # 阶段4：去重判定（跨模块：dedup_filter）
        dedup_result = self._check_dedup(channel_id, event_type, payload)
        trace.append({"stage": "去重判定", "passed": dedup_result["ok"], "is_duplicate": dedup_result.get("is_duplicate")})

        if dedup_result.get("is_duplicate"):
            json_store.update(RAW_MODULE, raw_event_id, {"处理结论": "duplicate"})
            self._record_receive_event(channel_id, payload, "duplicate", trace)
            return {"ok": True, "message": "事件已去重（重复事件）", "event_id": event_id,
                    "raw_event_id": raw_event_id, "stage": "去重判定", "trace": trace, "skipped": True}

        # 阶段5：分发路由（跨模块：dispatch_rule）
        dispatch_targets = self._match_dispatch_rules(event_type, payload)
        trace.append({"stage": "分发路由", "targets": len(dispatch_targets), "routes": [t.get("目标系统") for t in dispatch_targets]})

        # 阶段6：已确认字段写入正式分区（跨模块：data_lake_sink）
        usable_fields = dict(schema_validation.get("usable_fields") or {})
        sink_result = self._sink_event(
            event_id, event_type, usable_fields, channel_id,
            raw_event_id=raw_event_id,
            confirmed_scope=schema_validation.get("confirmed_scope") or {},
            pending_fields=schema_validation.get("pending_fields") or [],
            schema_versions=schema_validation.get("schema_versions") or [],
        )
        if sink_result.get("written_fields"):
            ingest_svc.mark_formal_write(
                raw_event_id,
                fields=list(sink_result["written_fields"]),
                partition=str(sink_result.get("partition", "")),
                file_path=str(sink_result.get("file_path", "")),
            )
        # 落湖后刷新来源行结论（补全已入湖字段），不再重复追加校验结论
        ingest_svc.refresh_pending(raw_event_id)
        trace.append({"stage": "数据落湖", "passed": sink_result["ok"],
                      "detail": sink_result.get("message"),
                      "written_fields": sink_result.get("written_fields", [])})

        # 记录成功接收
        self._record_receive_event(channel_id, payload, "success", trace)
        json_store.update(self.MODULE, channel_id, {
            "last_event_at": datetime.now(timezone.utc).isoformat(),
            "total_received": int(channel.get("total_received", 0)) + 1,
        })

        unknown_fields = schema_validation.get("unknown_fields") or []
        if unknown_fields:
            message = (
                f"事件已接收：{len(usable_fields)} 个已确认字段进入正式分区，"
                f"{len(unknown_fields)} 个未知字段待补验"
            )
        else:
            message = f"事件接收成功，已路由至 {len(dispatch_targets)} 个目标系统"

        final_raw = json_store.get_by_id(RAW_MODULE, raw_event_id) or updated
        return {
            "ok": True,
            "message": message,
            "event_id": event_id,
            "raw_event_id": raw_event_id,
            "原始接入编号": final_raw.get("原始接入编号"),
            "stage": "完成",
            "trace": trace,
            "dispatch_targets": [t.get("目标系统") for t in dispatch_targets],
            "confirmed_scope": schema_validation.get("confirmed_scope"),
            "confirmed_fields": schema_validation.get("confirmed_fields", []),
            "unknown_fields": unknown_fields,
            "pending_reason": schema_validation.get("pending_reason", ""),
            "pending": ingest_svc.pending_for(raw_event_id),
        }

    def _resolve_channel(self, channel_id: int) -> dict[str, Any] | None:
        """通道可能在内存仓库（资源接口登记）或持久化分区（管道用例）里，两处都查。"""
        channel = json_store.get_by_id(self.MODULE, channel_id)
        if channel is not None:
            return channel
        return store.find(self.MODULE, channel_id)

    def _verify_signature(self, payload: dict[str, Any], signature: str | None,
                          secret: str, method: str) -> bool:
        """验证Webhook签名。

        支持HMAC-SHA256和SHA256两种验证方式。
        """
        if method == "NONE":
            return True
        if not signature:
            return False
        body = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        if method == "HMAC-SHA256":
            expected = hmac.new(secret.encode(), body.encode(), hashlib.sha256).hexdigest()
            return hmac.compare_digest(expected, signature)
        elif method == "SHA256":
            expected = hashlib.sha256(body.encode()).hexdigest()
            return hmac.compare_digest(expected, signature)
        return False

    def _validate_event_schema(self, channel_id: int, event_type: str,
                               payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        """跨模块调用event_schema进行事件结构校验。

        返回 (校验结果, 是否中断)。Schema服务不可用属于“校验中断”，
        不能当作校验通过，否则未知字段会绕过补验直接进入取数。
        """
        try:
            from app.services.event_schema import EventSchemaService
            schema_svc = EventSchemaService()
            return schema_svc.validate_event(channel_id, event_type, payload), False
        except Exception as e:
            return {
                "ok": False,
                "schema_found": False,
                "message": f"Schema服务不可用: {e}",
                "confirmed_fields": [],
                "unknown_fields": [],
                "usable_fields": {},
                "pending_fields": [],
                "confirmed_scope": {},
                "pending_reason": "结构校验中断（Schema服务不可用）",
            }, True

    def _check_dedup(self, channel_id: int, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        """跨模块调用dedup_filter进行事件去重判定。"""
        try:
            from app.services.dedup_filter import DedupFilterService
            dedup_svc = DedupFilterService()
            return dedup_svc.check_duplicate(event_type, payload)
        except Exception:
            return {"ok": True, "is_duplicate": False}

    def _match_dispatch_rules(self, event_type: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """跨模块调用dispatch_rule进行分发目标匹配。"""
        try:
            from app.services.dispatch_rule import DispatchRuleService
            dispatch_svc = DispatchRuleService()
            return dispatch_svc.match_targets(event_type, payload)
        except Exception:
            return []

    def _sink_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    channel_id: int, *, raw_event_id: int = 0,
                    confirmed_scope: dict[str, Any] | None = None,
                    pending_fields: list[str] | None = None,
                    schema_versions: list[str] | None = None) -> dict[str, Any]:
        """跨模块调用data_lake_sink，只把已确认结构字段写入正式分区。"""
        try:
            from app.services.data_lake_sink import DataLakeSinkService
            sink_svc = DataLakeSinkService()
            return sink_svc.write_formal_event(
                event_id, event_type, payload,
                source_channel_id=channel_id,
                raw_event_id=raw_event_id,
                confirmed_scope=confirmed_scope or {},
                pending_fields=pending_fields or [],
                schema_versions=schema_versions or [],
            )
        except Exception as e:
            return {"ok": False, "message": f"落湖失败: {e}", "written_fields": []}

    def _record_receive_event(self, channel_id: int, payload: dict[str, Any],
                              outcome: str, trace: list[dict[str, Any]]) -> None:
        """记录Webhook接收事件的完整轨迹。"""
        record = {
            "channel_id": channel_id,
            "event_type": payload.get("type") or payload.get("event_type", "unknown"),
            "payload_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
            "outcome": outcome,
            "trace": trace,
            "received_at": datetime.now(timezone.utc).isoformat(),
        }
        self._received_events.append(record)

    def get_channel_stats(self, channel_id: int) -> dict[str, Any]:
        """获取通道的接收统计信息。"""
        channel = self._resolve_channel(channel_id)
        if channel is None:
            return {"ok": False, "message": f"通道 {channel_id} 不存在"}

        channel_events = [e for e in self._received_events if e["channel_id"] == channel_id]
        return {
            "ok": True,
            "channel": channel.get("通道名称"),
            "total_received": len(channel_events),
            "success_count": sum(1 for e in channel_events if e["outcome"] == "success"),
            "signature_failures": sum(1 for e in channel_events if e["outcome"] == "signature_failed"),
            "schema_failures": sum(1 for e in channel_events if e["outcome"] == "schema_invalid"),
            "duplicates": sum(1 for e in channel_events if e["outcome"] == "duplicate"),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出Webhook通道的数据质量快照，包含跨模块链路健康信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "Webhook通道数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_events_received": len(self._received_events),
            "active_channels": sum(1 for r in rows if r.get("status") == "已启用"),
            "operation": self.OPERATION_TERM,
        }