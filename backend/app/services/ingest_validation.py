"""Webhook接入补验服务：原始接入留存、未知字段处置明细与待处理清单同步。

解决的问题：
- 事件数据缺少结构定义（或结构校验失败/中断）时，原始接入数据必须有落点，
  否则没有可供补验的记录。
- 符合已确认结构的字段可以进入正式取数；没有结构定义的字段只登记处置明细，
  任何未经确认的值都不进入正式分区。
- 后续补齐结构定义后的更正只更新同一条来源行（raw_event_id）及其明细，
  不新增来源行，并保留最初收到的原文与新旧结构结论以便追溯。

存储分区：
- webhook_ingest_raw       原始接入数据（来源行），保存原文与结构结论修订记录
- webhook_unknown_field    未知字段处置明细，按（来源行, 字段名称）唯一
- webhook_pending          待处理清单，按来源行唯一
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store

RAW_MODULE = "webhook_ingest_raw"
UNKNOWN_MODULE = "webhook_unknown_field"
PENDING_MODULE = "webhook_pending"

RECEIVED = "已接收"
AWAITING_REVIEW = "待补验"
REVIEWED = "已补验"
NEEDS_FIX = "待更正"
CLOSED = "已关闭"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()[:16]


class IngestValidationService:
    """Webhook原始接入与结构补验的持久化服务。"""

    ENTITY = "原始接入记录"
    OPERATION_TERM = "补验"

    # ── 原始接入 ──────────────────────────────────────────
    def record_raw_event(
        self,
        *,
        channel_id: int,
        event_type: str,
        payload: dict[str, Any],
        event_id: str | None = None,
        outcome: str = RECEIVED,
        validation: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """留存一条原始接入数据（来源行），返回持久化后的记录。

        必须在结构校验结论产生之前调用：即使后续校验中断，原文也已经落盘。
        """
        snapshot = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
        entry = {
            "原始接入编号": "",
            "来源": "webhook_channel",
            "通道ID": channel_id,
            "事件类型": event_type,
            "事件ID": event_id or "",
            "接收时间": _now(),
            "原始Payload": snapshot,
            "原文摘要": _payload_hash(snapshot),
            "处理结论": outcome,
            "结构校验状态": AWAITING_REVIEW,
            "结构校验中断": False,
            "当前结构结论": {},
            "已确认结构范围": {},
            "可用字段": [],
            "待补验字段": [],
            "已入湖字段": [],
            "入湖分区": "",
            "入湖文件": [],
            "待处理清单编号": "",
            "补验次数": 0,
            "结论记录": [],
            "最近校验时间": _now(),
            "最近更新时间": _now(),
        }
        if validation is not None:
            self._apply_conclusion_fields(entry, validation)
        record = json_store.insert(RAW_MODULE, entry)
        record["原始接入编号"] = f"RAW-{int(record['id']):06d}"
        json_store.update(RAW_MODULE, int(record["id"]), {
            "原始接入编号": record["原始接入编号"],
        })
        return record

    def apply_validation(
        self,
        raw_event_id: int,
        validation: dict[str, Any],
        *,
        conclusion_kind: str = "初次校验",
        interrupted: bool = False,
        outcome: str | None = None,
    ) -> dict[str, Any] | None:
        """把一次结构校验结论落到同一条来源行，并同步明细与待处理清单。"""
        raw = json_store.get_by_id(RAW_MODULE, raw_event_id)
        if raw is None:
            return None

        revision = {
            "时间": _now(),
            "类型": conclusion_kind,
            "结论": self._compact(validation),
            "中断": interrupted,
        }
        updates: dict[str, Any] = {
            "当前结构结论": self._compact(validation),
            "结论记录": list(raw.get("结论记录") or []) + [revision],
            "最近校验时间": _now(),
            "最近更新时间": _now(),
            "结构校验中断": interrupted,
            "结构校验状态": self._validation_state(validation, interrupted),
        }
        if outcome is not None:
            updates["处理结论"] = outcome

        # 已确认结构范围与可用字段同步到来源行
        normalized = dict(raw)
        normalized.update(updates)
        self._apply_conclusion_fields(normalized, validation)
        for key in ("可用字段", "待补验字段", "已确认结构范围"):
            updates[key] = normalized[key]

        updated = json_store.update(RAW_MODULE, raw_event_id, updates)
        if updated is None:
            return None

        self._sync_unknown_fields(updated, validation)
        self._sync_pending(updated, validation, interrupted=interrupted)
        return json_store.get_by_id(RAW_MODULE, raw_event_id)

    def refresh_pending(self, raw_event_id: int) -> dict[str, Any] | None:
        """来源行字段已入湖后刷新待处理清单里的“已入湖字段”，不新增结论记录。"""
        raw = json_store.get_by_id(RAW_MODULE, raw_event_id)
        if raw is None:
            return None
        pending = self.pending_for(raw_event_id)
        if pending is not None:
            json_store.update(PENDING_MODULE, int(pending["id"]), {
                "已入湖字段": list(raw.get("已入湖字段") or []),
                "可正式取数字段": list(raw.get("可用字段") or []),
                "更新时间": _now(),
            })
        return self.pending_for(raw_event_id)

    def mark_formal_write(
        self,
        raw_event_id: int,
        *,
        fields: list[str],
        partition: str,
        file_path: str,
    ) -> dict[str, Any] | None:
        """记录哪些字段已经进入正式分区，补验时只补写尚未入湖的字段。"""
        raw = json_store.get_by_id(RAW_MODULE, raw_event_id)
        if raw is None:
            return None
        written = list(raw.get("已入湖字段") or [])
        for field in fields:
            if field not in written:
                written.append(field)
        files = list(raw.get("入湖文件") or [])
        if file_path and file_path not in files:
            files.append(file_path)
        return json_store.update(RAW_MODULE, raw_event_id, {
            "已入湖字段": written,
            "入湖分区": partition or raw.get("入湖分区", ""),
            "入湖文件": files,
            "最近更新时间": _now(),
        })

    # ── 补验 ─────────────────────────────────────────────
    def revalidate(self, raw_event_id: int) -> dict[str, Any]:
        """对同一来源行重新执行结构校验。

        - 只更新这条来源行和它名下的未知字段明细，不新增来源行；
        - 结构已补齐并通过的字段标记为已补验，并补写正式分区；
        - 仍无定义的字段保持待补验，原因刷新。
        """
        return self._revalidate(raw_event_id, conclusion_kind="补验")

    def revalidate_for_schema(self, schema: dict[str, Any]) -> list[int]:
        """结构定义标记生效后，重新校验同通道/事件类型的待补验来源行。

        返回本次真正重新校验的来源行 ID 列表。
        """
        schema_channel = str(schema.get("所属通道", ""))
        schema_event_type = str(schema.get("事件类型", ""))
        revalidated: list[int] = []
        for raw in json_store.get_all(RAW_MODULE):
            if raw.get("结构校验状态") == REVIEWED:
                continue
            raw_channel = str(raw.get("通道ID", ""))
            raw_event_type = str(raw.get("事件类型", ""))
            channel_match = schema_channel in (raw_channel, "", "全通道")
            type_match = schema_event_type in (raw_event_type, "通用")
            if not (channel_match and type_match):
                continue
            self._revalidate(int(raw["id"]), conclusion_kind="结构定义补齐后重新校验")
            revalidated.append(int(raw["id"]))
        return revalidated

    def _revalidate(self, raw_event_id: int, *, conclusion_kind: str) -> dict[str, Any]:
        from app.services.data_lake_sink import DataLakeSinkService
        from app.services.event_schema import EventSchemaService

        raw = json_store.get_by_id(RAW_MODULE, raw_event_id)
        if raw is None:
            return {"ok": False, "message": f"原始接入记录 {raw_event_id} 不存在"}

        before = dict(raw.get("当前结构结论") or {})
        before_written = list(raw.get("已入湖字段") or [])

        try:
            validation = EventSchemaService().validate_event(
                int(raw.get("通道ID", 0)), str(raw.get("事件类型", "")),
                dict(raw.get("原始Payload") or {}))
        except Exception as exc:  # 校验服务异常同样要留痕，不能丢掉来源行
            self.apply_validation(
                raw_event_id,
                {"ok": False, "schema_found": False, "message": f"结构校验中断: {exc}"},
                conclusion_kind=conclusion_kind, interrupted=True)
            return {"ok": False, "message": "结构校验中断，原始接入数据仍可查询",
                    "raw_event_id": raw_event_id}

        updated = self.apply_validation(
            raw_event_id, validation, conclusion_kind=conclusion_kind,
            interrupted=False, outcome=None)
        if updated is None:
            return {"ok": False, "message": f"原始接入记录 {raw_event_id} 更新失败"}

        updated = json_store.update(RAW_MODULE, raw_event_id, {
            "补验次数": int(raw.get("补验次数", 0)) + 1,
        }) or updated

        lake_result: dict[str, Any] = {"ok": True, "message": "无新增可正式取数字段", "written_fields": []}
        usable = dict(validation.get("usable_fields") or {})
        newly = [field for field in usable if field not in before_written]
        if newly:
            # 补验写正式分区时重建整条记录并取代旧版本：同一个来源行只保留一条生效数据，
            # 下游核对不会因为新旧版本并存而重复计数。
            replacement = {field: usable[field] for field in usable}
            lake_result = DataLakeSinkService().write_formal_event(
                str(updated.get("事件ID") or f"raw-{raw_event_id}"),
                str(updated.get("事件类型", "")),
                replacement,
                source_channel_id=int(updated.get("通道ID", 0)),
                raw_event_id=raw_event_id,
                confirmed_scope=validation.get("confirmed_scope") or {},
                pending_fields=list(validation.get("pending_fields") or []),
                schema_versions=list(validation.get("schema_versions") or []),
                supplemental=True,
                replace_previous=True,
            )
            if lake_result.get("ok"):
                updated = self.mark_formal_write(
                    raw_event_id,
                    fields=list(replacement.keys()),
                    partition=str(lake_result.get("partition", "")),
                    file_path=str(lake_result.get("file_path", "")),
                ) or updated
                lake_result["written_fields"] = sorted(replacement.keys())

        self.refresh_pending(raw_event_id)
        updated = json_store.get_by_id(RAW_MODULE, raw_event_id) or updated
        details = self.list_unknown_fields(raw_event_id=raw_event_id)
        pending = self.pending_for(raw_event_id)
        return {
            "ok": True,
            "message": (
                "补验完成，字段已确认并补写正式分区"
                if lake_result.get("written_fields")
                else ("补验后结构校验通过" if validation.get("ok") else "补验后仍有字段待补验")
            ),
            "raw_event_id": raw_event_id,
            "原始接入编号": updated.get("原始接入编号"),
            "old_conclusion": before,
            "new_conclusion": self._compact(validation),
            "conclusion_history": list(updated.get("结论记录") or []),
            "unknown_fields": details,
            "pending": pending,
            "lake": lake_result,
        }

    # ── 查询 ─────────────────────────────────────────────
    def list_pending(self, status: str | None = None,
                     page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
        rows = json_store.get_all(PENDING_MODULE)
        if status:
            rows = [row for row in rows if row.get("状态") == status]
        rows.sort(key=lambda row: str(row.get("更新时间", "")), reverse=True)
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def pending_for(self, raw_event_id: int) -> dict[str, Any] | None:
        for row in json_store.get_all(PENDING_MODULE):
            if int(row.get("来源行ID", 0)) == raw_event_id:
                return row
        return None

    def list_unknown_fields(self, raw_event_id: int | None = None,
                            status: str | None = None) -> list[dict[str, Any]]:
        rows = json_store.get_all(UNKNOWN_MODULE)
        if raw_event_id is not None:
            rows = [row for row in rows if int(row.get("来源行ID", 0)) == raw_event_id]
        if status:
            rows = [row for row in rows if row.get("校验状态") == status]
        return rows

    def trace(self, raw_event_id: int) -> dict[str, Any]:
        """追溯一笔数据：最初原文、新旧结构结论、未知字段明细与待处理清单。"""
        raw = json_store.get_by_id(RAW_MODULE, raw_event_id)
        if raw is None:
            return {"ok": False, "message": f"原始接入记录 {raw_event_id} 不存在"}
        revisions = list(raw.get("结论记录") or [])
        return {
            "ok": True,
            "raw_event_id": raw_event_id,
            "原始接入编号": raw.get("原始接入编号"),
            "来源行": raw.get("原始接入编号"),
            "通道ID": raw.get("通道ID"),
            "事件类型": raw.get("事件类型"),
            "原始Payload": raw.get("原始Payload"),
            "原文摘要": raw.get("原文摘要"),
            "接收时间": raw.get("接收时间"),
            "处理结论": raw.get("处理结论"),
            "结构校验状态": raw.get("结构校验状态"),
            "已确认结构范围": raw.get("已确认结构范围"),
            "可用字段": raw.get("可用字段"),
            "待补验字段": raw.get("待补验字段"),
            "已入湖字段": raw.get("已入湖字段"),
            "入湖分区": raw.get("入湖分区"),
            "入湖文件": raw.get("入湖文件"),
            "初始结构结论": revisions[0]["结论"] if revisions else {},
            "当前结构结论": raw.get("当前结构结论"),
            "结论记录": revisions,
            "补验次数": raw.get("补验次数", 0),
            "未知字段明细": self.list_unknown_fields(raw_event_id=raw_event_id),
            "待处理清单": self.pending_for(raw_event_id),
        }

    # ── 内部 ─────────────────────────────────────────────
    def _apply_conclusion_fields(self, entry: dict[str, Any], validation: dict[str, Any]) -> None:
        entry["可用字段"] = sorted((validation.get("usable_fields") or {}).keys())
        entry["待补验字段"] = list(validation.get("pending_fields") or [])
        entry["已确认结构范围"] = dict(validation.get("confirmed_scope") or {})
        if validation.get("schema_found"):
            entry["结构校验状态"] = self._validation_state(validation, bool(entry.get("结构校验中断")))
        elif entry.get("结构校验状态") in (None, "", AWAITING_REVIEW):
            entry["结构校验状态"] = AWAITING_REVIEW

    @staticmethod
    def _validation_state(validation: dict[str, Any], interrupted: bool) -> str:
        if interrupted:
            return AWAITING_REVIEW
        if not validation.get("ok"):
            return NEEDS_FIX if validation.get("type_errors") else AWAITING_REVIEW
        if not validation.get("schema_found"):
            return AWAITING_REVIEW
        if validation.get("unknown_fields"):
            return AWAITING_REVIEW
        return REVIEWED

    @staticmethod
    def _compact(validation: dict[str, Any]) -> dict[str, Any]:
        scope = dict(validation.get("confirmed_scope") or {})
        return {
            "schema_found": bool(validation.get("schema_found")),
            "ok": bool(validation.get("ok")),
            "message": validation.get("message", ""),
            "confirmed_fields": list(validation.get("confirmed_fields") or []),
            "unknown_fields": [
                item.get("field") if isinstance(item, dict) else str(item)
                for item in (validation.get("unknown_fields") or [])
            ],
            "pending_fields": list(validation.get("pending_fields") or []),
            "missing_fields": list(validation.get("missing_fields") or []),
            "type_errors": list(validation.get("type_errors") or []),
            "confirmed_scope": scope,
            "schema_versions": list(validation.get("schema_versions") or []),
            "schema_ids": list(validation.get("schema_ids") or []),
            "pending_reason": validation.get("pending_reason", ""),
        }

    def _sync_unknown_fields(self, raw: dict[str, Any], validation: dict[str, Any]) -> None:
        """按（来源行, 字段名称）更新未知字段处置明细，不新增重复行。"""
        raw_id = int(raw["id"])
        source_line = str(raw.get("原始接入编号", ""))
        channel_id = int(raw.get("通道ID", 0))
        event_type = str(raw.get("事件类型", ""))
        schema_versions = list(validation.get("schema_versions") or [])
        unknown_by_field = {
            str(item.get("field")): item
            for item in (validation.get("unknown_fields") or [])
            if isinstance(item, dict)
        }
        type_errors = {
            str(item.get("field")): item
            for item in (validation.get("type_errors") or [])
            if isinstance(item, dict)
        }
        confirmed = set(validation.get("confirmed_fields") or [])
        existing = {
            str(row.get("字段名称")): row
            for row in json_store.get_all(UNKNOWN_MODULE)
            if int(row.get("来源行ID", 0)) == raw_id
        }

        # 1) 仍未定义的字段：保持/新建明细
        for field, item in unknown_by_field.items():
            revision = {
                "时间": _now(),
                "校验状态": AWAITING_REVIEW,
                "处置结论": "结构定义中尚无该字段，待补齐后补验",
                "Schema版本": schema_versions,
            }
            row = existing.get(field)
            if row is None:
                record = json_store.insert(UNKNOWN_MODULE, {
                    "明细编号": "",
                    "来源行": source_line,
                    "来源行ID": raw_id,
                    "来源通道": channel_id,
                    "事件类型": event_type,
                    "字段名称": field,
                    "字段值": item.get("value"),
                    "未知原因": item.get("reason", ""),
                    "校验状态": AWAITING_REVIEW,
                    "处置结论": "结构定义中尚无该字段，待补齐后补验",
                    "可正式取数": False,
                    "发现时Schema版本": schema_versions,
                    "当前Schema版本": schema_versions,
                    "首次发现时间": _now(),
                    "最近补验时间": _now(),
                    "修订记录": [revision],
                })
                record["明细编号"] = f"UNK-{int(record['id']):06d}"
                json_store.update(UNKNOWN_MODULE, int(record["id"]), {
                    "明细编号": record["明细编号"],
                })
            else:
                # 同一状态重复校验（如落湖后刷新结论）不重复追加修订记录
                if row.get("校验状态") == AWAITING_REVIEW:
                    revisions = list(row.get("修订记录") or [])
                else:
                    revisions = list(row.get("修订记录") or []) + [revision]
                json_store.update(UNKNOWN_MODULE, int(row["id"]), {
                    "来源行": source_line,
                    "来源通道": channel_id,
                    "事件类型": event_type,
                    "字段值": item.get("value"),
                    "未知原因": item.get("reason", ""),
                    "校验状态": AWAITING_REVIEW,
                    "处置结论": revision["处置结论"],
                    "可正式取数": False,
                    "当前Schema版本": schema_versions,
                    "最近补验时间": _now(),
                    "修订记录": revisions,
                })

        # 2) 已补齐定义的字段：在同一明细行上改为已补验/待更正
        for field, row in existing.items():
            if field in unknown_by_field:
                continue
            if field in type_errors:
                status = NEEDS_FIX
                conclusion = (
                    f"结构定义已补齐，但字段类型不符"
                    f"（期望{type_errors[field].get('expected')}，实际{type_errors[field].get('actual')}）"
                )
            elif field in confirmed:
                status = REVIEWED
                conclusion = "结构定义已补齐，字段通过校验并纳入正式取数"
            else:
                status = NEEDS_FIX
                conclusion = "结构定义已补齐，但该字段未通过校验"
            revision = {
                "时间": _now(),
                "校验状态": status,
                "处置结论": conclusion,
                "Schema版本": schema_versions,
            }
            if row.get("校验状态") == status:
                revisions = list(row.get("修订记录") or [])
            else:
                revisions = list(row.get("修订记录") or []) + [revision]
            json_store.update(UNKNOWN_MODULE, int(row["id"]), {
                "校验状态": status,
                "处置结论": conclusion,
                "可正式取数": status == REVIEWED,
                "当前Schema版本": schema_versions,
                "最近补验时间": _now(),
                "修订记录": revisions,
            })

        # 3) 曾经缺失、这次补齐的字段不在未知明细里，无需处理；缺失信息由来源行记录。

    def _sync_pending(self, raw: dict[str, Any], validation: dict[str, Any],
                      *, interrupted: bool = False) -> dict[str, Any]:
        """把校验结论同步到待处理清单，一个来源行只保留一条。"""
        raw_id = int(raw["id"])
        pending = self.pending_for(raw_id)
        state = self._validation_state(validation, interrupted)
        if state == REVIEWED:
            state = CLOSED

        reason = self._pending_reason(validation, interrupted)
        # 关闭时保留最初的异常原因，另记关闭结论，补验后仍能说明当时为什么挂起。
        if state == CLOSED and pending is not None:
            reason = str(pending.get("异常原因") or reason)
            close_note = "结构校验通过，待补验字段已关闭"
        else:
            close_note = ""
        updates = {
            "来源行": raw.get("原始接入编号", ""),
            "来源行ID": raw_id,
            "来源通道": raw.get("通道ID", 0),
            "事件类型": raw.get("事件类型", ""),
            "异常原因": reason,
            "关闭结论": close_note,
            "待补验字段": list(validation.get("pending_fields") or []),
            "已确认结构范围": dict(validation.get("confirmed_scope") or {}),
            "可正式取数字段": sorted((validation.get("usable_fields") or {}).keys()),
            "已入湖字段": list(raw.get("已入湖字段") or []),
            "状态": state,
            "最近结论": self._compact(validation),
            "更新时间": _now(),
        }
        if pending is None:
            if state == CLOSED:
                # 正常通过的记录不占待处理清单，清单只收需要人工补验的异常。
                return {"状态": state, "清单编号": "", "异常原因": reason,
                        "来源行": raw.get("原始接入编号", ""), "来源行ID": raw_id}
            record = json_store.insert(PENDING_MODULE, {
                "清单编号": "",
                **updates,
                "首次异常原因": reason,
                "创建时间": _now(),
            })
            record["清单编号"] = f"PEND-{int(record['id']):06d}"
            json_store.update(PENDING_MODULE, int(record["id"]), {
                "清单编号": record["清单编号"],
            })
            json_store.update(RAW_MODULE, raw_id, {
                "待处理清单编号": record["清单编号"],
            })
            return record
        return json_store.update(PENDING_MODULE, int(pending["id"]), updates) or pending

    @staticmethod
    def _pending_reason(validation: dict[str, Any], interrupted: bool) -> str:
        if interrupted:
            return "结构校验中断（校验服务异常），原始接入数据已留存，待重新补验"
        if not validation.get("schema_found"):
            return "未找到该通道/事件类型的已生效结构定义，全部字段待补验"
        parts = []
        if validation.get("unknown_fields"):
            parts.append("未知字段待补验: " + ", ".join(
                str(item.get("field")) for item in validation["unknown_fields"]))
        if validation.get("type_errors"):
            parts.append("字段类型不符待更正: " + ", ".join(
                f"{item.get('field')}(期望{item.get('expected')},实际{item.get('actual')})"
                for item in validation["type_errors"]))
        if validation.get("missing_fields"):
            parts.append("缺少必填字段: " + ", ".join(validation["missing_fields"]))
        return "；".join(parts) or "结构校验通过"
