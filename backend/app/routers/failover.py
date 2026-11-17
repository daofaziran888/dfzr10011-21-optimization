"""数据源批量故障切接口。

承接采集数据源池到采集代理路由之间的批量故障切换：批量提交、逐条结果、整组重试、
待处理队列、代理应用、原主源恢复与统一重算都在这里暴露。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas import FailoverBatchPayload
from app.services.failover import POLICIES, failover_service

router = APIRouter(prefix="/api/failover", tags=["数据源批量故障切"])


@router.get("/policies")
def policies() -> dict[str, object]:
    """同优先级备源健康采样冲突时可选的两种处理策略。"""
    return {"policies": list(POLICIES)}


@router.get("/groups")
def group_overview() -> dict[str, object]:
    """采集数据源池 → 代理路由的分组差异：每组主源唯一性与可恢复原主源位置。"""
    from app.services.datasource import DatasourceService

    return {"items": DatasourceService().group_overview()}


@router.post("/batches")
def submit_batch(payload: FailoverBatchPayload) -> dict[str, object]:
    """一次提交多条数据源条目做故障切换，逐条给出成功/失败/跳过结果。"""
    source = payload.source or "数据源池批量故障切"
    batch, message = failover_service.submit_batch(
        payload.entry_ids, payload.policy,
        batch_no=payload.batch_no, source=source,
    )
    if batch is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "batch": batch}


@router.get("/batches")
def list_batches() -> dict[str, object]:
    """故障切换批次与逐条结果（含切换前后快照），供字段复核。"""
    return {"items": failover_service.list_batches(), "total": len(failover_service.list_batches())}


@router.get("/batches/{batch_no}")
def get_batch(batch_no: str) -> dict[str, object]:
    batch = failover_service.get_batch(batch_no)
    if batch is None:
        raise HTTPException(status_code=404, detail=f"故障切换批次 {batch_no} 不存在")
    return batch


@router.post("/batches/{batch_no}/retry")
def retry_batch(batch_no: str) -> dict[str, object]:
    """整组重试：只继续失败与未切换条目，已切换条目不会产生第二个主源。"""
    batch, message = failover_service.retry_batch(batch_no)
    if batch is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, "batch": batch}


@router.get("/queue")
def list_queue(status: str | None = None) -> dict[str, object]:
    """代理模块的数据源批量故障切待处理队列。"""
    items = failover_service.list_queue(status)
    return {"items": items, "total": len(items)}


@router.post("/queue/{queue_id}/apply")
def apply_queue_item(queue_id: int) -> dict[str, object]:
    """代理路由读取一条故障切换结果，更新唯一主源与备用恢复位置。"""
    from app.services.proxy import ProxyService

    queue_row, message = ProxyService().apply_failover(queue_id)
    if queue_row is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "queue": queue_row}


@router.post("/queue/apply-all")
def apply_all_queue() -> dict[str, object]:
    """一次性应用全部待处理队列项，逐条返回应用结果。"""
    from app.services.proxy import ProxyService

    return {"ok": True, **ProxyService().apply_all_pending()}


@router.post("/groups/{group}/restore")
def restore_primary(group: str) -> dict[str, object]:
    """原主源恢复：回到保留位置，配额按恢复后的唯一主源重算。"""
    from app.services.proxy import ProxyService

    result, message = ProxyService().restore_primary(group)
    if result is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, "result": result}


@router.post("/recompute")
def recompute(source: str = "其他业务入口重算") -> dict[str, object]:
    """别的业务入口跟着重算代理模块的故障切队列与故障切换结果数据（幂等）。"""
    return {"ok": True, "result": failover_service.recompute(source)}
