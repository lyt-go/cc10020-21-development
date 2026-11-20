"""Webhook未知字段补验链路测试。

覆盖需求中的关键结论：
1. 缺少结构定义时原始接入数据仍可查询；
2. 结构部分匹配时，只有已确认字段进入正式分区，未知字段生成处置明细；
3. 待处理清单同步本次异常原因，可解释“哪些字段可用、哪些待补验”；
4. 补齐结构定义后在同一来源行上补验，原文和新旧结论都可追溯；
5. 校验中断不会丢数据，补验后可恢复。
"""
from __future__ import annotations

import importlib
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore
import app.persistence

RAW = "webhook_ingest_raw"
UNKNOWN = "webhook_unknown_field"
PENDING = "webhook_pending"


@pytest.fixture()
def services():
    """临时存储 + 重新绑定 json_store 的领域服务。"""
    import app.routers.pipeline
    import app.services.data_lake_sink as sink_mod
    import app.services.event_schema as schema_mod
    import app.services.ingest_validation as ingest_mod
    import app.services.webhook_channel as webhook_mod

    module_names = [
        "api_endpoint", "webhook_channel", "event_schema", "mq_consumer",
        "dlq_handler", "data_lake_sink", "data_transform", "data_quality_rule",
        "dispatch_rule", "dedup_filter", "batch_ingest", "incre_cursor",
        "backfill_job", "call_trace_log", "rate_limit_config", "api_auth_store",
        "event_archive", "integration_monitor", "ingest_validation",
    ]
    modules = [importlib.import_module(f"app.services.{name}") for name in module_names]
    modules.append(app.routers.pipeline)

    with tempfile.TemporaryDirectory() as tmp:
        previous = app.persistence.json_store
        app.persistence.json_store = JsonStore(base_dir=tmp)
        for mod in modules:
            importlib.reload(mod)
        try:
            yield {
                "webhook": app.services.webhook_channel.WebhookChannelService(),
                "schema": app.services.event_schema.EventSchemaService(),
                "sink": app.services.data_lake_sink.DataLakeSinkService(),
                "ingest": app.services.ingest_validation.IngestValidationService(),
            }
        finally:
            app.persistence.json_store = previous
            for mod in modules:
                importlib.reload(mod)


def _enabled_order_channel(webhook, channel_no="WH-001"):
    channel, missing = webhook.create_entry({
        "通道编号": channel_no,
        "通道名称": "订单事件通道",
        "接收路径": "/webhook/order",
        "签名密钥": "secret123",
        "验证方式": "NONE",
        "并发上限": "100",
        "回调地址": "https://callback.example.com",
        "通道版本": "v1",
    })
    assert channel is not None, missing
    webhook.run_action(channel["id"], "启用通道")
    return channel["id"]


def _order_schema(schema, channel_id, fields=(("order_id", "string", "是"),
                                              ("amount", "number", "是")),
                  version="v1"):
    created = []
    for index, (name, field_type, required) in enumerate(fields, start=1):
        entry, missing = schema.create_entry({
            "结构编号": f"SC-{version}-{index}",
            "事件类型": "order_created",
            "字段名称": name,
            "字段类型": field_type,
            "必填标识": required,
            "默认值": "",
            "Schema版本": version,
            "所属通道": str(channel_id),
        })
        assert entry is not None, missing
        schema.run_action(entry["id"], "标记生效")
        created.append(entry)
    return created


def _order_payload(**extra):
    payload = {"type": "order_created", "order_id": "ORD-2026-0001", "amount": 299.99}
    payload.update(extra)
    return payload


# ── 结构部分匹配：已确认范围 + 未知字段来源 ──────────────────
def test_confirmed_scope_and_unknown_field_are_separated(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    result = services["webhook"].receive_webhook(
        channel_id, _order_payload(channel_remark="加急"))

    assert result["ok"] is True
    # 已确认结构范围只包含结构定义里有的字段
    assert result["confirmed_scope"]["fields"] == ["order_id", "amount"]
    assert result["confirmed_scope"]["schema_versions"] == ["v1"]
    assert result["confirmed_fields"] == ["order_id", "amount"]
    # 未知字段带来源行和原因
    [unknown] = result["unknown_fields"]
    assert unknown["field"] == "channel_remark"
    assert unknown["value"] == "加急"
    assert unknown["reason"]
    assert "channel_remark" in result["pending_reason"]


def test_only_confirmed_fields_reach_formal_partition(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    services["webhook"].receive_webhook(channel_id, _order_payload(channel_remark="加急"))

    formal = services["sink"].query_formal_data("order_created")
    assert formal["total"] == 1
    record = formal["records"][0]
    # 下游当日核对只能看到订单号和金额，备注不在正式取数范围内
    assert record["payload"] == {"order_id": "ORD-2026-0001", "amount": 299.99}
    assert record["pending_fields"] == ["channel_remark"]
    assert record["raw_event_id"] > 0


def test_unknown_field_detail_points_back_to_source_row(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    result = services["webhook"].receive_webhook(
        channel_id, _order_payload(channel_remark="加急"))

    details = services["ingest"].list_unknown_fields(raw_event_id=result["raw_event_id"])
    assert len(details) == 1
    detail = details[0]
    assert detail["字段名称"] == "channel_remark"
    assert detail["来源行"] == result["原始接入编号"]
    assert detail["来源行ID"] == result["raw_event_id"]
    assert detail["来源通道"] == channel_id
    assert detail["校验状态"] == "待补验"
    assert detail["可正式取数"] is False
    assert detail["发现时Schema版本"] == ["v1"]


def test_pending_list_explains_why_review_is_needed(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    result = services["webhook"].receive_webhook(
        channel_id, _order_payload(channel_remark="加急"))

    pending = result["pending"]
    assert pending["状态"] == "待补验"
    assert "channel_remark" in pending["异常原因"]
    assert pending["可正式取数字段"] == ["amount", "order_id"]
    assert pending["已入湖字段"] == ["amount", "order_id"]
    assert pending["待补验字段"] == ["channel_remark"]
    items, total = services["ingest"].list_pending()
    assert total == 1 and items[0]["清单编号"] == pending["清单编号"]


def test_schema_failure_keeps_raw_event_queryable(services):
    """必填字段缺失导致校验失败时，原始数据仍可查到，且不进入正式分区。"""
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    result = services["webhook"].receive_webhook(
        channel_id, {"type": "order_created", "order_id": "ORD-X", "channel_remark": "加急"})

    assert result["ok"] is False
    raw_event_id = result["raw_event_id"]
    trace = services["ingest"].trace(raw_event_id)
    assert trace["ok"] is True
    assert trace["原始Payload"]["channel_remark"] == "加急"
    assert trace["当前结构结论"]["missing_fields"] == ["amount"]
    assert trace["初始结构结论"]["ok"] is False
    assert services["sink"].query_formal_data("order_created")["total"] == 0
    assert trace["待处理清单"]["状态"] in ("待补验", "待更正")


def test_no_schema_defined_still_keeps_raw_event(services):
    """通道/事件类型完全没有结构定义时，同样留下可供补验的来源行。"""
    channel_id = _enabled_order_channel(services["webhook"])

    result = services["webhook"].receive_webhook(
        channel_id, {"type": "order_created", "order_id": "ORD-X", "amount": 10})

    assert result["ok"] is True
    assert result["confirmed_fields"] == []
    assert {item["field"] for item in result["unknown_fields"]} == {"order_id", "amount"}
    assert result["pending"]["状态"] == "待补验"
    assert "未找到" in result["pending"]["异常原因"]
    assert services["sink"].query_formal_data("order_created")["total"] == 0
    assert services["ingest"].trace(result["raw_event_id"])["原始Payload"]["order_id"] == "ORD-X"


# ── 补齐定义后的补验：同一来源行生效、可追溯 ──────────────────
def test_revalidate_reuses_same_source_row_and_keeps_history(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)
    result = services["webhook"].receive_webhook(
        channel_id, _order_payload(channel_remark="加急"))
    raw_event_id = result["raw_event_id"]

    # 补齐渠道备注的定义
    remark_schema, _ = services["schema"].create_entry({
        "结构编号": "SC-remark-v2",
        "事件类型": "order_created",
        "字段名称": "channel_remark",
        "字段类型": "string",
        "必填标识": "否",
        "默认值": "",
        "Schema版本": "v2",
        "所属通道": str(channel_id),
    })
    _, message = services["schema"].run_action(remark_schema["id"], "标记生效")
    assert "重新校验" in message

    # 来源行还是同一条，没有新增行
    rows = app.persistence.json_store.get_all(RAW)
    assert len(rows) == 1
    assert rows[0]["id"] == raw_event_id

    trace = services["ingest"].trace(raw_event_id)
    # 最初原文与新旧结构结论同时保留
    assert trace["原始Payload"]["channel_remark"] == "加急"
    assert trace["初始结构结论"]["unknown_fields"] == ["channel_remark"]
    assert trace["当前结构结论"]["unknown_fields"] == []
    assert [item["类型"] for item in trace["结论记录"]] == ["初次校验", "结构定义补齐后重新校验"]
    assert trace["补验次数"] == 1
    # 未知字段明细在同一行改为已补验
    [detail] = trace["未知字段明细"]
    assert detail["校验状态"] == "已补验"
    assert detail["可正式取数"] is True
    assert detail["来源行"] == trace["原始接入编号"]
    assert [rev["校验状态"] for rev in detail["修订记录"]] == ["待补验", "已补验"]
    # 待处理清单关闭，但保留了本次异常原因
    assert trace["待处理清单"]["状态"] == "已关闭"
    assert "channel_remark" in trace["待处理清单"]["异常原因"]
    assert trace["待处理清单"]["关闭结论"]


def test_revalidate_replaces_previous_formal_record_for_same_row(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)
    result = services["webhook"].receive_webhook(
        channel_id, _order_payload(channel_remark="加急"))
    raw_event_id = result["raw_event_id"]

    remark_schema, _ = services["schema"].create_entry({
        "结构编号": "SC-remark-v2",
        "事件类型": "order_created",
        "字段名称": "channel_remark",
        "字段类型": "string",
        "必填标识": "否",
        "默认值": "",
        "Schema版本": "v2",
        "所属通道": str(channel_id),
    })
    services["schema"].run_action(remark_schema["id"], "标记生效")

    formal = services["sink"].query_formal_data("order_created")
    # 同一条订单在正式分区只留一条生效记录，避免下游重复计数
    assert formal["total"] == 1
    assert formal["records"][0]["payload"]["channel_remark"] == "加急"
    assert formal["records"][0]["raw_event_id"] == raw_event_id
    assert formal["records"][0]["supplemental"] is True

    history = services["sink"].query_formal_data("order_created", include_superseded=True)
    assert history["total"] == 2
    statuses = sorted(record["status"] for record in history["records"])
    assert statuses == ["已取代", "生效"]


def test_manual_revalidate_endpoint_works_without_schema_change(services):
    """没有新增结构时手动补验：结论刷新但不重复写入正式分区。"""
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)
    result = services["webhook"].receive_webhook(
        channel_id, _order_payload(channel_remark="加急"))

    revalidated = services["ingest"].revalidate(result["raw_event_id"])
    assert revalidated["ok"] is True
    assert revalidated["lake"]["written_fields"] == []
    assert services["sink"].query_formal_data("order_created")["total"] == 1
    assert "channel_remark" in [item["字段名称"] for item in revalidated["unknown_fields"]]


def test_unknown_field_never_enters_formal_partition_before_definition(services):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)
    for remark in ("加急", "普通"):
        services["webhook"].receive_webhook(
            channel_id, _order_payload(channel_remark=remark))

    formal = services["sink"].query_formal_data("order_created")
    assert formal["total"] == 2
    for record in formal["records"]:
        assert "channel_remark" not in record["payload"]
    pending, total = services["ingest"].list_pending()
    assert total == 2
    assert all(row["状态"] == "待补验" for row in pending)


# ── 校验中断 ─────────────────────────────────────────────
def test_interrupted_validation_keeps_raw_event_available(services, monkeypatch):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    def _boom(*args, **kwargs):
        raise RuntimeError("schema service down")

    monkeypatch.setattr(
        "app.services.event_schema.EventSchemaService.validate_event", _boom)

    result = services["webhook"].receive_webhook(channel_id, _order_payload())

    assert result["ok"] is False
    assert result["stage"] == "结构校验"
    trace = services["ingest"].trace(result["raw_event_id"])
    assert trace["原始Payload"]["order_id"] == "ORD-2026-0001"
    assert trace["结构校验状态"] == "待补验"
    assert trace["待处理清单"]["状态"] == "待补验"
    assert "中断" in trace["待处理清单"]["异常原因"]
    assert services["sink"].query_formal_data("order_created")["total"] == 0


def test_revalidation_after_interruption_succeeds(services, monkeypatch):
    channel_id = _enabled_order_channel(services["webhook"])
    _order_schema(services["schema"], channel_id)

    original = None
    from app.services.event_schema import EventSchemaService
    original = EventSchemaService.validate_event

    def _boom(*args, **kwargs):
        raise RuntimeError("schema service down")

    monkeypatch.setattr(EventSchemaService, "validate_event", _boom)
    failed = services["webhook"].receive_webhook(channel_id, _order_payload())
    assert failed["ok"] is False

    monkeypatch.setattr(EventSchemaService, "validate_event", original)
    revalidated = services["ingest"].revalidate(failed["raw_event_id"])

    assert revalidated["ok"] is True
    assert revalidated["lake"]["written_fields"] == ["amount", "order_id"]
    assert services["sink"].query_formal_data("order_created")["total"] == 1
    trace = services["ingest"].trace(failed["raw_event_id"])
    assert trace["当前结构结论"]["ok"] is True
    assert trace["待处理清单"]["状态"] == "已关闭"


def test_signature_failure_keeps_raw_event_queryable(services):
    channel_id = _enabled_order_channel(services["webhook"])
    from app.store import store
    # 资源接口登记的通道在内存仓库里，改成需要验签
    store.find("webhook_channel", channel_id)["验证方式"] = "HMAC-SHA256"

    result = services["webhook"].receive_webhook(channel_id, _order_payload(), signature="bad")

    assert result["ok"] is False
    trace = services["ingest"].trace(result["raw_event_id"])
    assert trace["原始Payload"]["order_id"] == "ORD-2026-0001"
    assert trace["处理结论"] == "signature_failed"
