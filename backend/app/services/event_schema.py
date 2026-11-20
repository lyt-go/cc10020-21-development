"""事件结构定义领域服务：Schema定义、字段校验、版本演进与兼容性检查。

提供事件结构版本管理、字段级Schema校验和跨版本兼容性比对能力。
关联模块：webhook_channel（通道关联校验）、data_quality_rule（质量规则校验）。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class EventSchemaService(BaseModuleService):
    MODULE = "event_schema"
    ENTITY = "事件结构"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["结构编号", "事件类型", "字段名称", "字段类型", "必填标识", "默认值", "Schema版本", "所属通道"]
    REQUIRED_FIELDS = ["结构编号", "事件类型", "字段名称"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已废弃"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "标记废弃": "已废弃"}
    NEGATIVE_ACTIONS = []

    _TYPE_MAP = {
        "string": str, "integer": int, "float": float, "boolean": bool,
        "array": list, "object": dict, "number": (int, float),
        "datetime": str, "json": (dict, list), "any": object,
    }

    # 事件信封字段由接入层解释，不参与业务字段的结构匹配。
    ENVELOPE_FIELDS = {"type", "event_type"}

    def validate_event(self, channel_id: int, event_type: str,
                       payload: dict[str, Any]) -> dict[str, Any]:
        """对接收的事件Payload执行Schema校验，并区分“已确认结构范围”和“未知字段”。

        校验流程：
        1. 查找该通道/事件类型下所有已生效的Schema字段定义
        2. 逐一检查必填字段是否存在、类型是否匹配，通过者进入已确认结构范围
        3. Payload 中没有结构定义的字段记为未知字段，交给接入层生成处置明细

        未知字段不会让整条事件直接失败（否则永远没有可供补验的记录），
        但也不会进入 usable_fields，因此不会被送往正式分区。

        Args:
            channel_id: Webhook通道ID
            event_type: 事件类型名称
            payload: 事件数据体

        Returns:
            校验结果:{ ok, message, schema_found, confirmed_fields, unknown_fields,
                     usable_fields, missing_fields, type_errors, confirmed_scope, pending_reason }
        """
        # 查找关联通道的已生效schema字段
        schemas = json_store.get_all(self.MODULE)
        matching = [
            s for s in schemas
            if s.get("status") == "已生效"
            and (str(s.get("事件类型", "")) == event_type or s.get("事件类型", "") == "通用")
            and str(s.get("所属通道", "")) in (str(channel_id), "", "全通道")
        ]

        schema_ids = sorted({int(s.get("id", 0)) for s in matching if s.get("id") is not None})
        schema_versions = sorted({str(s.get("Schema版本", "")) for s in matching if s.get("Schema版本")})
        result: dict[str, Any] = {
            "ok": True,
            "message": "结构校验通过",
            "schema_found": bool(matching),
            "schema_ids": schema_ids,
            "schema_versions": schema_versions,
            "missing_fields": [],
            "type_errors": [],
            "checked_fields": len(matching),
            "confirmed_fields": [],
            "unknown_fields": [],
            "usable_fields": {},
            "pending_fields": [],
            "confirmed_scope": {
                "channel_id": channel_id,
                "event_type": event_type,
                "fields": [],
                "schema_ids": schema_ids,
                "schema_versions": schema_versions,
            },
            "pending_reason": "",
        }

        defined_fields = {
            str(s.get("字段名称", "")): s for s in matching
            if str(s.get("字段名称", "")).strip()
        }

        for field_name, schema_field in defined_fields.items():
            field_type = str(schema_field.get("字段类型", "any"))
            is_required = str(schema_field.get("必填标识", "否")) in ("是", "true", "True")

            if field_name not in payload:
                if is_required:
                    result["ok"] = False
                    result["missing_fields"].append(field_name)
                    result["pending_fields"].append(field_name)
                continue

            expected_type = self._TYPE_MAP.get(field_type, object)
            actual_value = payload[field_name]

            # 类型检查
            if isinstance(expected_type, tuple):
                type_ok = isinstance(actual_value, expected_type)
            elif expected_type is object:
                type_ok = True
            else:
                type_ok = isinstance(actual_value, expected_type)

            if not type_ok:
                result["ok"] = False
                result["type_errors"].append({
                    "field": field_name,
                    "expected": field_type,
                    "actual": type(actual_value).__name__,
                })
                result["pending_fields"].append(field_name)
                continue

            result["confirmed_fields"].append(field_name)
            result["usable_fields"][field_name] = actual_value

        # 未在结构定义中登记的字段：单独标记来源和原因，等待补验，不阻断事件留存。
        unknown_reason = (
            "事件结构定义中未登记该字段" if matching
            else "未找到该通道/事件类型的已生效结构定义"
        )
        for field_name, value in payload.items():
            if field_name in self.ENVELOPE_FIELDS:
                continue
            if field_name in defined_fields:
                continue
            result["unknown_fields"].append({
                "field": field_name,
                "value": value,
                "reason": unknown_reason,
            })
            result["pending_fields"].append(field_name)

        result["confirmed_scope"]["fields"] = list(result["confirmed_fields"])

        if not result["ok"]:
            messages = []
            if result["missing_fields"]:
                messages.append(f"缺少必填字段: {', '.join(result['missing_fields'])}")
            if result["type_errors"]:
                type_msgs = [f"{te['field']}(期望{te['expected']},实际{te['actual']})" for te in result["type_errors"]]
                messages.append(f"字段类型不匹配: {'; '.join(type_msgs)}")
            result["message"] = " | ".join(messages)

        pending_bits = []
        if not matching:
            pending_bits.append("未找到已生效结构定义")
        elif result["unknown_fields"]:
            pending_bits.append(
                "未知字段待补验: " + ", ".join(u["field"] for u in result["unknown_fields"]))
        if result["type_errors"]:
            pending_bits.append("字段类型不符待更正")
        if result["missing_fields"]:
            pending_bits.append("必填字段缺失")
        result["pending_reason"] = "；".join(pending_bits)

        if not matching:
            result["message"] = "结构校验中断：未找到已生效结构定义，原始数据已留存待补验"
        elif not result["ok"]:
            result["message"] = result["message"] + "；原始数据已留存待补验"
        elif result["unknown_fields"]:
            result["message"] = "结构校验部分通过：" + result["pending_reason"]

        return result

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """结构状态流转；标记生效后自动对同一通道/事件类型的待补验记录重新校验。

        结构定义登记在持久化分区里（create_entry 写 json_store），所以这里优先
        操作持久化记录，避免更新到内存仓库里同 ID 的示例行。
        """
        target = self.ACTION_RULES.get(action)
        if target is None:
            return super().run_action(entry_id, action)

        stored = json_store.get_by_id(self.MODULE, entry_id)
        if stored is not None:
            updated = json_store.update(self.MODULE, entry_id, {
                "status": target,
                "pending": target != self.STATUS_ORDER[-1],
            })
            if updated is None:
                return None, f"{self.ENTITY} {entry_id} 更新失败"
            message = f"{self.ENTITY}已{action}"
            if action == "标记生效":
                from app.services.ingest_validation import IngestValidationService
                revalidated = IngestValidationService().revalidate_for_schema(updated)
                if revalidated:
                    message = f"{message}，已重新校验 {len(revalidated)} 条待补验记录"
            return updated, message

        return super().run_action(entry_id, action)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建Schema结构定义，附带版本管理和字段去重检查。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        # 检查是否已存在同事件类型+同字段名称的生效结构
        existing = json_store.get_all(self.MODULE)
        duplicate = [
            e for e in existing
            if e.get("事件类型") == values.get("事件类型")
            and e.get("字段名称") == values.get("字段名称")
            and e.get("status") in ("已生效", "待校验")
        ]
        if duplicate:
            return None, ["同事件类型下已存在同名字段定义，请先废弃旧版本或修改字段名称"]

        max_id = max((int(e.get("id", 0)) for e in existing), default=0)
        entry = {"id": max_id + 1}
        entry.update({field: values.get(field, "") for field in self.LIST_FIELDS})
        entry["status"] = self.STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["created_at"] = datetime.now(timezone.utc).isoformat()
        entry["parent_schema_id"] = values.get("parent_schema_id")

        # Schema版本自动递增
        schema_version = str(values.get("Schema版本", "v1"))
        entry["Schema版本"] = schema_version

        json_store.insert(self.MODULE, entry)
        return entry, []

    def get_schema_for_channel(self, channel_id: int, event_type: str | None = None) -> dict[str, Any]:
        """获取通道的完整Schema定义（聚合所有已生效的字段定义）。"""
        schemas = json_store.get_all(self.MODULE)
        channel_schemas = [
            s for s in schemas
            if s.get("status") == "已生效"
            and str(s.get("所属通道", "")) in (str(channel_id), "", "全通道")
        ]
        if event_type:
            channel_schemas = [
                s for s in channel_schemas
                if s.get("事件类型") == event_type or s.get("事件类型") == "通用"
            ]

        fields_by_type: dict[str, list[dict[str, Any]]] = {}
        for s in channel_schemas:
            et = str(s.get("事件类型", "通用"))
            if et not in fields_by_type:
                fields_by_type[et] = []
            fields_by_type[et].append({
                "field_name": s.get("字段名称"),
                "field_type": s.get("字段类型"),
                "required": str(s.get("必填标识", "否")) in ("是", "true", "True"),
                "default": s.get("默认值"),
                "version": s.get("Schema版本"),
            })

        return {
            "channel_id": channel_id,
            "event_types": list(fields_by_type.keys()),
            "schemas": fields_by_type,
            "total_fields": len(channel_schemas),
        }

    def check_version_compatibility(self, schema_id: int, new_fields: dict[str, Any]) -> dict[str, Any]:
        """检查Schema版本演进的兼容性（新增可选字段兼容，删除必填字段不兼容）。"""
        existing = json_store.get_by_id(self.MODULE, schema_id)
        if existing is None:
            return {"compatible": False, "reason": "Schema不存在"}

        issues = []
        # 检查是否删除了已有的必填字段
        existing_required = str(existing.get("必填标识", "否")) in ("是", "true", "True")
        new_required = str(new_fields.get("必填标识", "否")) in ("是", "true", "True")

        if existing_required and not new_required:
            issues.append("警告：将必填字段改为非必填可能影响下游消费者")

        type_changed = str(existing.get("字段类型", "")) != str(new_fields.get("字段类型", ""))
        if type_changed:
            issues.append(f"警告：字段类型从 {existing.get('字段类型')} 变更为 {new_fields.get('字段类型')}，历史数据需要迁移")

        compatible = len([i for i in issues if i.startswith("错误")]) == 0
        return {
            "compatible": compatible,
            "issues": issues,
            "recommendation": "建议创建新Schema版本而非修改已有定义" if not compatible else "变更兼容，可安全升级",
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件结构定义的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        event_types = set(r.get("事件类型") for r in rows if r.get("事件类型"))
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件结构定义数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "unique_event_types": len(event_types),
            "required_fields_count": sum(1 for r in rows if str(r.get("必填标识")) in ("是", "true", "True")),
            "operation": self.OPERATION_TERM,
        }