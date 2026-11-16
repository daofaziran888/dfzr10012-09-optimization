"""检查点恢复接口：维护检查点记录，并校验恢复时所用窗口触发结果数据集版本。

静态路径（``/version/consistency``、``/export``）必须排在 ``/{entry_id}`` 之前，
否则会被路径参数抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import window_dataset
from app.services.checkpoint import CheckpointService
from app.services.window_dataset import DatasetError

router = APIRouter(prefix="/api/checkpoint", tags=["检查点恢复"])

service = CheckpointService()

LIST_FIELDS = ["记录编号", "流作业名", "检查点ID", "状态大小", "保存路径", "创建时间", "恢复耗时", "记录状态"]
STATUSES = ["已创建", "已恢复", "已过期", "已清理"]


@router.get("/version/consistency", response_model=dict)
def version_consistency() -> dict[str, Any]:
    """检查点与窗口触发结果数据集的版本一致性总览。"""
    return window_dataset.run_upgrade("依赖兼容", note="检查点版本一致性巡检")


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出检查点恢复清单：带恢复批次与阈值版本，便于离线核对。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "checkpoint", "total": total, "items": items}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按记录编号检索"),
    status: str | None = Query(default=None, description="已创建、已恢复、已过期、已清理"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按记录编号与状态过滤检查点恢复列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条检查点记录明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"检查点记录 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条检查点记录，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="检查点记录已登记", entry=entry)


@router.get("/{entry_id}/restore", response_model=dict)
def restore_preview(
    entry_id: int,
    version: str | None = Query(default=None, description="必须与检查点声明的阈值版本一致"),
    batch_code: str | None = Query(default=None, description="指定来源批次，需能解析为唯一批次"),
) -> dict[str, Any]:
    """恢复预检：只读取检查点声明的那个阈值版本，冲突时明确拒绝。"""
    try:
        return service.restore_view(entry_id, version=version, batch_code=batch_code)
    except DatasetError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条检查点记录执行恢复状态、清理记录、归档记录；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
