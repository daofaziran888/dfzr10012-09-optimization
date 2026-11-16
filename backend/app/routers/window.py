"""窗口计算配置接口：维护窗口定义、调整延迟容忍，并产出可核对的窗口触发结果数据集。

路由层只做参数搬运与 HTTP 语义转换；受影响窗口判定、唯一归属、版本发布、
检查点与消费侧口径全部收在 ``app.services`` 里。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import window_dataset
from app.services.window import WindowService
from app.services.window_dataset import DatasetError
from app.services.window_engine import SpecError

router = APIRouter(prefix="/api/window", tags=["窗口计算配置"])

service = WindowService()

LIST_FIELDS = ["窗口编号", "数据流名称", "窗口类型", "窗口长度", "滑动步长", "延迟容忍", "触发策略", "窗口状态"]
STATUSES = ["草稿", "生效中", "已暂停", "已关闭"]

THRESHOLD_KEYS = ["窗口类型", "窗口长度", "滑动步长", "延迟容忍", "触发策略", "水位线"]


def _dataset_error(error: Exception) -> HTTPException:
    if isinstance(error, (DatasetError, SpecError)):
        return HTTPException(status_code=409, detail=str(error))
    return HTTPException(status_code=400, detail=str(error))


def _decorate(entry: dict[str, Any]) -> dict[str, Any]:
    """列表与详情共用同一份生效数据集口径，避免两处读到不同阈值版本。"""
    row = dict(entry)
    window_id = str(entry.get("窗口编号") or "")
    try:
        batch = window_dataset.resolve_dataset(window_id, str(entry.get("阈值版本") or "") or None, require_active=True)
    except DatasetError as error:
        row["数据集状态"] = "缺失"
        row["数据集说明"] = str(error)
        row["生效批次编号"] = None
        row["生效批次阈值版本"] = None
        return row
    row["数据集状态"] = batch.get("状态")
    row["数据集说明"] = batch.get("重算说明") or batch.get("数据集来源")
    row["生效批次编号"] = batch.get("批次编号")
    row["生效批次阈值版本"] = batch.get("阈值版本")
    return row


# -- 升级流程（静态路径必须排在 /{entry_id} 之前） -------------------------

@router.get("/upgrade/plan", response_model=dict)
def upgrade_plan() -> dict[str, Any]:
    """升级流程总览：旧数据库、新环境、部署回滚、依赖兼容四个阶段及其记录。"""
    return window_dataset.upgrade_plan()


@router.post("/upgrade/actions", response_model=ActionResult)
def run_upgrade(payload: EntryPayload) -> ActionResult:
    """执行一个升级阶段；未知阶段会被拦下并说明允许范围。"""
    stage = str(payload.values.get("stage") or payload.values.get("阶段") or "").strip()
    try:
        result = window_dataset.run_upgrade(stage, note=payload.remark)
    except DatasetError as error:
        raise _dataset_error(error) from error
    return ActionResult(ok=bool(result.get("ok", True)), message=result.get("message") or stage, entry=result)


@router.get("/datasets", response_model=PageResult[dict])
def list_datasets(
    window_id: str | None = Query(default=None, description="按窗口编号过滤"),
    version: str | None = Query(default=None, description="按阈值版本过滤"),
    status: str | None = Query(default=None, description="生效、历史、失败、已回滚"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """窗口触发结果数据集批次列表；每行保留所用阈值版本与数据集来源。"""
    rows = window_dataset.list_batches(window_id=window_id, version=version, status=status)
    total = len(rows)
    start = max(page - 1, 0) * size
    return PageResult(items=rows[start:start + size], total=total, page=page, size=size)


@router.get("/datasets/{batch_code}", response_model=dict)
def dataset_detail(batch_code: str) -> dict[str, Any]:
    """批次明细：返回窗口边界、每行所用阈值版本以及沿用快照/重算来源。"""
    try:
        return window_dataset.batch_detail(batch_code)
    except DatasetError as error:
        raise _dataset_error(error) from error


@router.get("/datasets/{batch_code}/verify", response_model=dict)
def verify_dataset(batch_code: str) -> dict[str, Any]:
    """校验窗口类型、触发策略、窗口边界、阈值版本与唯一归属；异常版本会标记到窗口定义。"""
    try:
        result = window_dataset.verify_batch(batch_code)
    except DatasetError as error:
        raise _dataset_error(error) from error
    return result


@router.post("/datasets/{batch_code}/recompute", response_model=ActionResult)
def recompute_from_batch(batch_code: str, payload: EntryPayload) -> ActionResult:
    """以指定批次为基线重算；并发请求只落成一个终态。"""
    try:
        batch = window_dataset.get_batch(batch_code)
    except DatasetError as error:
        raise _dataset_error(error) from error
    window_id = str(batch.get("窗口编号") or "")
    entry = next((row for row in service.list_entries(size=200)[0] if row.get("窗口编号") == window_id), None)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"窗口 {window_id} 定义不存在或已归档")
    values = {key: payload.values[key] for key in THRESHOLD_KEYS if key in payload.values}
    try:
        result = service.adjust_threshold(
            int(entry["id"]),
            values,
            batch_code=batch_code,
            idempotency_key=str(payload.values.get("幂等键") or "") or None,
        )
    except (DatasetError, SpecError) as error:
        raise _dataset_error(error) from error
    return ActionResult(ok=True, message=result["message"], entry=result)


# -- 列表与单条窗口定义 ---------------------------------------------------

@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按窗口编号检索"),
    status: str | None = Query(default=None, description="草稿、生效中、已暂停、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按窗口编号与状态过滤窗口计算配置列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=[_decorate(item) for item in items], total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出窗口计算配置清单：带上生效批次与阈值版本，便于离线核对。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "window", "total": total, "items": [_decorate(item) for item in items]}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条窗口定义明细；不存在时给出可读的错误说明。"""
    try:
        entry = service.threshold_view(entry_id)["entry"]
    except DatasetError as error:
        raise _dataset_error(error) from error
    return _decorate(entry)


@router.get("/{entry_id}/threshold", response_model=dict)
def threshold_detail(entry_id: int) -> dict[str, Any]:
    """窗口长度、滑动步长、延迟容忍、触发策略与阈值版本历史。"""
    try:
        return service.threshold_view(entry_id)
    except (DatasetError, SpecError) as error:
        raise _dataset_error(error) from error


@router.post("/{entry_id}/threshold/preview", response_model=ActionResult)
def preview_threshold(entry_id: int, payload: EntryPayload) -> ActionResult:
    """调整前预演受影响窗口与事件，不写任何结果。"""
    try:
        result = service.preview(entry_id, payload.values)
    except (DatasetError, SpecError) as error:
        raise _dataset_error(error) from error
    return ActionResult(ok=True, message=result["结论"], entry=result)


@router.post("/{entry_id}/threshold/actions", response_model=ActionResult)
def adjust_threshold(entry_id: int, payload: EntryPayload) -> ActionResult:
    """调整延迟容忍等阈值：只重算受影响窗口，校验通过后写入窗口触发结果数据集。"""
    values = {key: payload.values[key] for key in THRESHOLD_KEYS if key in payload.values}
    if not values:
        return ActionResult(ok=False, message="没有提交需要调整的窗口参数")
    try:
        result = service.adjust_threshold(
            entry_id,
            values,
            batch_code=str(payload.values.get("基线批次编号") or "") or None,
            idempotency_key=str(payload.values.get("幂等键") or "") or None,
        )
    except (DatasetError, SpecError) as error:
        raise _dataset_error(error) from error
    return ActionResult(ok=bool(result.get("校验", {}).get("通过", True)), message=result["message"], entry=result)


@router.post("/{entry_id}/repair", response_model=ActionResult)
def repair_entry(entry_id: int) -> ActionResult:
    """顺着当前取数链路改正异常版本：收敛重复来源、修正版本标注、沿用历史快照。"""
    try:
        result = service.repair(entry_id)
    except (DatasetError, SpecError) as error:
        raise _dataset_error(error) from error
    return ActionResult(ok=bool(result.get("校验", {}).get("通过", True)), message=result["message"], entry=result)


@router.get("/{entry_id}/results", response_model=dict)
def window_results(entry_id: int, version: str | None = Query(default=None, description="阈值版本")) -> dict[str, Any]:
    """窗口触发结果数据集：详情页与列表页读取同一阈值版本。"""
    try:
        entry = service.get_entry(entry_id)
        if entry is None:
            raise DatasetError(f"窗口定义 {entry_id} 不存在或已归档")
        window_id = str(entry.get("窗口编号") or "")
        batch = window_dataset.resolve_dataset(window_id, version or str(entry.get("阈值版本") or "") or None)
    except (DatasetError, SpecError) as error:
        raise _dataset_error(error) from error
    rows = window_dataset.batch_detail(str(batch["批次编号"]))["rows"]
    return {"entry": _decorate(entry), "batch": batch, "rows": rows}


@router.get("/{entry_id}/datasets", response_model=PageResult[dict])
def entry_datasets(entry_id: int) -> PageResult[dict]:
    """某个窗口的全部数据集批次（生效、历史、失败、已回滚）。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"窗口定义 {entry_id} 不存在或已归档")
    rows = window_dataset.list_batches(window_id=str(entry.get("窗口编号") or ""))
    return PageResult(items=rows, total=len(rows), page=1, size=len(rows) or 20)


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条窗口定义，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="窗口定义已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条窗口定义执行启用窗口、暂停窗口、关闭窗口；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
