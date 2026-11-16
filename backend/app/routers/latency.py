"""延迟追踪管理接口：维护延迟记录，并作为消费侧关联窗口触发结果数据集。

静态路径（``/sync``、``/export``）必须排在 ``/{entry_id}`` 之前，否则会被
路径参数抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.latency import LatencyService
from app.services.window_dataset import DatasetError

router = APIRouter(prefix="/api/latency", tags=["延迟追踪管理"])

service = LatencyService()

LIST_FIELDS = ["记录编号", "流作业名", "端到端延迟", "处理延迟", "网络延迟", "延迟阈值", "超标次数", "记录时间"]
STATUSES = ["采集中", "已超标", "已恢复", "已归档"]


@router.get("/sync", response_model=dict)
def sync_all() -> dict[str, Any]:
    """消费侧批量关联：按记录编号校验同一份窗口触发结果数据集。"""
    return service.sync_all()


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出延迟追踪管理清单：带窗口关联字段，便于核对同一份数据集。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "latency", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="采集中、已超标、已恢复、已归档"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤延迟追踪管理列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条延迟记录明细（含与窗口触发结果数据集的关联字段）。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"延迟记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条延迟记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="延迟记录已登记", entry=entry)


@router.post("/{entry_id}/sync", response_model=ActionResult)
def sync_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """单条消费记录关联窗口触发结果数据集；批次不一致或重复归集时拒绝写入。"""
    batch_code = str(payload.values.get("批次编号") or "") or None
    try:
        result = service.sync_entry(entry_id, batch_code=batch_code)
    except DatasetError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    return ActionResult(ok=True, message=result["结论"], entry=result)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条延迟记录执行标记超标、优化处理、归档记录；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
