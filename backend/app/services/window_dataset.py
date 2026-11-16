"""窗口触发结果数据集：版本发布、受影响窗口重算、检查点与消费侧的唯一取数口径。

数据表（内存仓库，字段风格与既有模块一致）：

* ``window_version``        每个窗口的阈值版本历史（窗口类型/长度/步长/延迟容忍/触发策略）
* ``window_dataset``        数据集批次；一个版本同时最多一个「生效」批次
* ``window_dataset_row``    批次内的窗口结果行；保留窗口边界与所用阈值版本
* ``window_recompute``      重算任务流水，用于幂等命中与失败留痕
* ``window_migration``      升级流程的四个阶段执行记录
* ``window_legacy_dataset`` 旧数据库里尚未带阈值版本/窗口边界的批次
* ``window_dataset_meta``   数据集结构版本等元信息

并发约定：同一窗口的重算串行执行；相同幂等键或相同目标参数的并发请求只会
落成一个批次，调用方拿到的都是同一个「批次编号」。
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Any, Iterable

from app.services.window_engine import (
    Assignment,
    SpecError,
    WindowSpec,
    affected_windows,
    assign_events,
    dataset_rows,
    duplicate_ownership,
    event_roster,
    parse_spec,
)
from app.store import store

DATASET = "window_dataset"
DATASET_ROW = "window_dataset_row"
VERSION = "window_version"
JOB = "window_recompute"
MIGRATION = "window_migration"
LEGACY = "window_legacy_dataset"
META = "window_dataset_meta"

CURRENT_SCHEMA_VERSION = "2.0"
ACTIVE = "生效"
HISTORY = "历史"
FAILED = "失败"
ROLLED_BACK = "已回滚"
MIGRATED = "已迁移"

UPGRADE_STAGES = ["旧数据库", "新环境", "部署回滚", "依赖兼容"]

_lock = threading.RLock()
_window_locks: dict[str, threading.Lock] = {}
_bootstrapped = False


class DatasetError(ValueError):
    """数据集口径下的可读错误；路由层原样转成 4xx 说明。"""


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _next_code(prefix: str, rows: list[dict[str, Any]], field: str) -> str:
    numbers: list[int] = []
    for row in rows:
        text = str(row.get(field) or "")
        if not text.startswith(prefix + "-"):
            continue
        digit_tail = text[len(prefix) + 1:]
        if digit_tail.isdigit():
            numbers.append(int(digit_tail))
    return f"{prefix}-{max(numbers, default=0) + 1:04d}"


def _window_lock(window_id: str) -> threading.Lock:
    with _lock:
        return _window_locks.setdefault(window_id, threading.Lock())


def _meta() -> dict[str, Any]:
    rows = store.rows(META)
    if not rows:
        rows.append(
            {
                "id": 1,
                "键": "schema_version",
                "值": CURRENT_SCHEMA_VERSION,
                "说明": "新建环境直接使用当前结构",
            }
        )
    return rows[0]


def _meta_value(key: str, default: str = "") -> str:
    value = _meta().get("值")
    return str(value) if value is not None else default


# ---------------------------------------------------------------------------
# 启动与基线
# ---------------------------------------------------------------------------

def bootstrap() -> None:
    """补齐阈值版本与基线数据集；旧数据库里的批次留给升级流程处理。"""
    global _bootstrapped
    with _lock:
        if _bootstrapped:
            return
        legacy_windows = {str(row.get("窗口编号")) for row in store.rows(LEGACY)}
        for definition in store.rows("window"):
            window_id = str(definition.get("窗口编号") or "")
            if not window_id:
                continue
            try:
                spec = parse_spec({}, definition)
            except SpecError:
                definition["abnormal"] = True
                continue
            version = _ensure_version(definition, spec)
            if window_id in legacy_windows:
                continue
            if _active_batch(window_id) is None:
                _publish_baseline(definition, spec, version)
        _bootstrapped = True


def reset_for_tests() -> None:
    """把仓库恢复成种子状态并重建基线，供测试逐例隔离。"""
    global _bootstrapped
    with _lock:
        store.reset()
        _bootstrapped = False
    bootstrap()


def _ensure_version(definition: dict[str, Any], spec: WindowSpec) -> dict[str, Any]:
    window_id = str(definition.get("窗口编号") or "")
    rows = store.rows(VERSION)
    for row in rows:
        if row.get("窗口编号") == window_id and row.get("阈值版本") == definition.get("阈值版本"):
            return row
    number = max((int(row.get("阈值版本号", 0)) for row in rows if row.get("窗口编号") == window_id), default=0) + 1
    version_code = f"v{number}"
    version = {
        "id": _next_id(rows),
        "窗口编号": window_id,
        "阈值版本": version_code,
        "阈值版本号": number,
        "窗口类型": spec.窗口类型,
        "窗口长度": spec.窗口长度,
        "滑动步长": spec.滑动步长,
        "延迟容忍": spec.延迟容忍,
        "触发策略": spec.触发策略,
        "水位线": spec.水位线,
        "状态": ACTIVE,
        "创建时间": _now(),
        "说明": "基线版本",
    }
    rows.append(version)
    definition.setdefault("阈值版本", version_code)
    definition.setdefault("阈值版本号", number)
    return version


def _publish_baseline(definition: dict[str, Any], spec: WindowSpec, version: dict[str, Any]) -> dict[str, Any]:
    window_id = str(definition.get("窗口编号") or "")
    events = _events_for(window_id)
    assignment = assign_events(events, spec)
    batches = store.rows(DATASET)
    batch_code = _next_code("DSET", batches, "批次编号")
    batch = {
        "id": _next_id(batches),
        "批次编号": batch_code,
        "窗口编号": window_id,
        "数据流名称": definition.get("数据流名称"),
        "阈值版本": version.get("阈值版本"),
        "基线批次编号": None,
        "窗口类型": spec.窗口类型,
        "触发策略": spec.触发策略,
        "窗口长度": spec.窗口长度,
        "滑动步长": spec.滑动步长,
        "延迟容忍": spec.延迟容忍,
        "水位线": spec.水位线,
        "状态": ACTIVE,
        "完整性": "完整",
        "数据集来源": "初始构建",
        "窗口数": len(assignment.windows),
        "沿用快照窗口数": 0,
        "记录数": sum(len(bucket) for bucket in assignment.windows.values()),
        "幂等键": None,
        "创建时间": _now(),
        "失败原因": None,
    }
    rows = dataset_rows(
        batch_id=batch_code,
        definition=definition,
        spec=spec,
        assignment=assignment,
        default_source="初始构建",
    )
    _save_rows(batch_code, rows)
    batches.append(batch)
    return batch


def _save_rows(batch_code: str, rows: Iterable[dict[str, Any]]) -> None:
    table = store.rows(DATASET_ROW)
    for row in rows:
        entry = {"id": _next_id(table), "批次编号": batch_code}
        entry.update(row)
        table.append(entry)


def _events_for(window_id: str) -> list[dict[str, Any]]:
    return [row for row in store.rows("window_event") if row.get("窗口编号") == window_id]


def _rows_of(batch_code: str) -> list[dict[str, Any]]:
    return [row for row in store.rows(DATASET_ROW) if row.get("批次编号") == batch_code]


def _batch_of(batch_code: str) -> dict[str, Any] | None:
    for row in store.rows(DATASET):
        if row.get("批次编号") == batch_code:
            return row
    return None


def _active_batch(window_id: str) -> dict[str, Any] | None:
    for batch in store.rows(DATASET):
        if batch.get("窗口编号") == window_id and batch.get("状态") == ACTIVE:
            return batch
    return None


def get_batch(batch_code: str) -> dict[str, Any]:
    batch = _batch_of(batch_code)
    if batch is None:
        raise DatasetError(f"窗口触发结果数据集 {batch_code} 不存在")
    return batch


def list_batches(
    *,
    window_id: str | None = None,
    version: str | None = None,
    status: str | None = None,
) -> list[dict[str, Any]]:
    bootstrap()
    rows = list(store.rows(DATASET))
    if window_id:
        rows = [row for row in rows if row.get("窗口编号") == window_id]
    if version:
        rows = [row for row in rows if row.get("阈值版本") == version]
    if status:
        rows = [row for row in rows if row.get("状态") == status]
    return sorted(rows, key=lambda row: int(row.get("id", 0)), reverse=True)


def versions(window_id: str) -> list[dict[str, Any]]:
    bootstrap()
    rows = [row for row in store.rows(VERSION) if row.get("窗口编号") == window_id]
    return sorted(rows, key=lambda row: int(row.get("阈值版本号", 0)), reverse=True)


# ---------------------------------------------------------------------------
# 版本解析：列表、详情、检查点、消费侧共用同一口径
# ---------------------------------------------------------------------------

def resolve_dataset(
    window_id: str,
    version: str | None = None,
    *,
    require_active: bool = False,
) -> dict[str, Any]:
    """解析唯一数据集批次；来源不唯一时直接拒绝，避免同一事件两个结论。"""
    bootstrap()
    candidates = [
        batch
        for batch in store.rows(DATASET)
        if batch.get("窗口编号") == window_id
        and batch.get("完整性") == "完整"
        and batch.get("状态") != FAILED
    ]
    if version:
        candidates = [batch for batch in candidates if batch.get("阈值版本") == version]
    if require_active:
        candidates = [batch for batch in candidates if batch.get("状态") == ACTIVE]
    if not candidates:
        scope = f"阈值版本 {version}" if version else "可用窗口触发结果数据集"
        raise DatasetError(f"窗口 {window_id} 没有{scope}，请先完成重算或升级迁移")
    if len(candidates) > 1:
        codes = "、".join(str(batch.get("批次编号")) for batch in candidates)
        raise DatasetError(
            f"窗口 {window_id} 的{'阈值版本 ' + version if version else '数据集'}存在多个来源（{codes}），"
            "无法确定唯一归属，请先执行升级流程收敛版本"
        )
    return candidates[0]


def batch_detail(batch_code: str) -> dict[str, Any]:
    batch = get_batch(batch_code)
    rows = sorted(
        _rows_of(batch_code),
        key=lambda row: (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))),
    )
    return {"batch": batch, "rows": rows}


# ---------------------------------------------------------------------------
# 重算
# ---------------------------------------------------------------------------

def recompute(
    definition: dict[str, Any],
    values: dict[str, Any],
    *,
    baseline_batch: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    """按新阈值重算；只重算归属变化的窗口，未变化的窗口沿用上一份快照。"""
    bootstrap()
    window_id = str(definition.get("窗口编号") or "")
    if not window_id:
        raise DatasetError("窗口定义缺少窗口编号，无法重算")

    with _window_lock(window_id):
        if idempotency_key:
            for job in store.rows(JOB):
                if (
                    job.get("窗口编号") == window_id
                    and job.get("幂等键") == idempotency_key
                    and job.get("状态") == "成功"
                ):
                    batch = get_batch(str(job.get("目标批次编号")))
                    return {
                        "batch": batch,
                        "rows": sorted(_rows_of(batch["批次编号"]), key=lambda row: int(row.get("窗口开始", 0))),
                        "affected": job.get("受影响窗口") or [],
                        "merged": True,
                        "message": f"幂等键 {idempotency_key} 已命中批次 {batch['批次编号']}，不重复生成",
                    }

        active = _active_batch(window_id)
        if baseline_batch and active and baseline_batch != active.get("批次编号"):
            raise DatasetError(
                f"基线批次 {baseline_batch} 已不是当前生效数据集（当前为 {active.get('批次编号')}），"
                "并发重算只接受一个终态，请刷新后重试"
            )

        current_spec = parse_spec({}, definition)
        target_spec = parse_spec(values, definition)
        if target_spec.signature == current_spec.signature and active is not None:
            return {
                "batch": active,
                "rows": sorted(_rows_of(active["批次编号"]), key=lambda row: int(row.get("窗口开始", 0))),
                "affected": [],
                "merged": True,
                "message": f"阈值参数未变化，沿用生效批次 {active['批次编号']}",
            }

        job = _new_job(window_id, definition, active, target_spec, idempotency_key)
        try:
            result = _recompute_locked(definition, window_id, active, target_spec, job)
        except Exception as error:  # noqa: BLE001 - 失败必须留痕并保住上一份数据集
            job["状态"] = "失败"
            job["说明"] = str(error)
            job["结束时间"] = _now()
            raise
        return result


def _new_job(
    window_id: str,
    definition: dict[str, Any],
    active: dict[str, Any] | None,
    target_spec: WindowSpec,
    idempotency_key: str | None,
) -> dict[str, Any]:
    rows = store.rows(JOB)
    job = {
        "id": _next_id(rows),
        "任务编号": _next_code("RCNT", rows, "任务编号"),
        "窗口编号": window_id,
        "基线批次编号": active.get("批次编号") if active else None,
        "目标批次编号": None,
        "旧阈值版本": active.get("阈值版本") if active else None,
        "新阈值版本": None,
        "目标延迟容忍": target_spec.延迟容忍,
        "幂等键": idempotency_key,
        "状态": "进行中",
        "受影响窗口": [],
        "说明": None,
        "创建时间": _now(),
        "结束时间": None,
    }
    rows.append(job)
    return job


def _recompute_locked(
    definition: dict[str, Any],
    window_id: str,
    active: dict[str, Any] | None,
    target_spec: WindowSpec,
    job: dict[str, Any],
) -> dict[str, Any]:
    if active is None:
        raise DatasetError(f"窗口 {window_id} 缺少完整基线数据集，无法做增量重算")

    baseline_rows = _rows_of(str(active["批次编号"]))
    baseline_duplicates = duplicate_ownership(baseline_rows)
    if baseline_duplicates:
        raise DatasetError(
            f"基线批次 {active['批次编号']} 存在同一记录编号的重复归集（"
            + "、".join(baseline_duplicates)
            + "），不能沿用其中有冲突的窗口快照"
        )
    previous_ids = {
        (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))): set(row.get("事件记录编号") or [])
        for row in baseline_rows
    }
    events = _events_for(window_id)
    assignment = assign_events(events, target_spec)
    changes = affected_windows(
        Assignment(windows={key: [{"记录编号": record} for record in value] for key, value in previous_ids.items()}, dropped=[]),
        assignment,
    )
    affected_boundaries = {(int(item["窗口开始"]), int(item["窗口结束"])) for item in changes}
    affected_events = sorted(
        {
            record
            for item in changes
            for record in list(item.get("新增记录") or []) + list(item.get("移除记录") or [])
        }
    )

    versions_table = store.rows(VERSION)
    number = max(
        (int(row.get("阈值版本号", 0)) for row in versions_table if row.get("窗口编号") == window_id),
        default=0,
    ) + 1
    new_version_code = f"v{number}"

    batches = store.rows(DATASET)
    batch_code = _next_code("DSET", batches, "批次编号")

    reused: list[dict[str, Any]] = []
    recomputed: list[dict[str, Any]] = []
    previous_by_boundary = {
        (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))): row for row in baseline_rows
    }
    new_rows: list[dict[str, Any]] = []
    for boundary in sorted(assignment.windows, key=lambda item: item[0]):
        ids = assignment.ids(boundary)
        snapshot = previous_by_boundary.get(boundary)
        if boundary not in affected_boundaries and snapshot and set(snapshot.get("事件记录编号") or []) == set(ids):
            carried = {
                key: value
                for key, value in snapshot.items()
                if key not in {"id", "批次编号", "状态", "来源"}
            }
            carried["批次编号"] = batch_code
            carried["来源"] = "沿用快照"
            carried["状态"] = ACTIVE
            new_rows.append(carried)
            reused.append(
                {
                    "窗口开始": boundary[0],
                    "窗口结束": boundary[1],
                    "所用阈值版本": carried.get("阈值版本"),
                    "来源": "沿用快照",
                }
            )
        else:
            recomputed.append(
                {
                    "窗口开始": boundary[0],
                    "窗口结束": boundary[1],
                    "所用阈值版本": new_version_code,
                    "来源": "重算",
                }
            )

    recomputed_rows = dataset_rows(
        batch_id=batch_code,
        definition={**definition, "阈值版本": new_version_code},
        spec=target_spec,
        assignment=assignment,
        previous_rows=[],
    )
    recomputed_by_boundary = {
        (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))): row for row in recomputed_rows
    }
    for item in recomputed:
        boundary = (int(item["窗口开始"]), int(item["窗口结束"]))
        new_rows.append(recomputed_by_boundary[boundary])

    duplicates = duplicate_ownership(new_rows)
    if duplicates:
        raise DatasetError(
            "重算结果出现同一记录编号的重复归集："
            + "；".join(f"{record} -> {hits}" for record, hits in duplicates.items())
        )

    removed = [
        {
            "窗口开始": item["窗口开始"],
            "窗口结束": item["窗口结束"],
            "移除记录": item.get("移除记录") or [],
        }
        for item in changes
        if item.get("变化类型") == "移除窗口"
    ]
    dropped = [event.get("记录编号") for event in assignment.dropped]

    batch = {
        "id": _next_id(batches),
        "批次编号": batch_code,
        "窗口编号": window_id,
        "数据流名称": definition.get("数据流名称"),
        "阈值版本": new_version_code,
        "基线批次编号": active.get("批次编号"),
        "窗口类型": target_spec.窗口类型,
        "触发策略": target_spec.触发策略,
        "窗口长度": target_spec.窗口长度,
        "滑动步长": target_spec.滑动步长,
        "延迟容忍": target_spec.延迟容忍,
        "水位线": target_spec.水位线,
        "状态": ACTIVE,
        "完整性": "完整",
        "数据集来源": "阈值调整重算",
        "窗口数": len(new_rows),
        "沿用快照窗口数": len(reused),
        "记录数": sum(int(row.get("记录数", 0)) for row in new_rows),
        "幂等键": job.get("幂等键"),
        "创建时间": _now(),
        "失败原因": None,
        "重算说明": f"仅重算 {len(affected_boundaries)} 个受影响窗口，沿用 {len(reused)} 个历史快照窗口",
    }

    # 发布顺序：先写行，再切换批次与版本状态。所有校验都在此之前完成，
    # 任何异常都不会留下半份数据集，上一份完整数据集保持「生效」。
    _save_rows(batch_code, new_rows)
    batches.append(batch)
    active["状态"] = HISTORY

    for row in versions_table:
        if row.get("窗口编号") == window_id and row.get("状态") == ACTIVE:
            row["状态"] = HISTORY
    versions_table.append(
        {
            "id": _next_id(versions_table),
            "窗口编号": window_id,
            "阈值版本": new_version_code,
            "阈值版本号": number,
            "窗口类型": target_spec.窗口类型,
            "窗口长度": target_spec.窗口长度,
            "滑动步长": target_spec.滑动步长,
            "延迟容忍": target_spec.延迟容忍,
            "触发策略": target_spec.触发策略,
            "水位线": target_spec.水位线,
            "状态": ACTIVE,
            "创建时间": _now(),
            "说明": f"由 {active.get('阈值版本')} 重算发布",
        }
    )

    definition["阈值版本"] = new_version_code
    definition["阈值版本号"] = number
    definition["配置修订"] = int(definition.get("配置修订", 0) or 0) + 1

    definition.update(
        {
            "窗口类型": target_spec.窗口类型,
            "窗口长度": target_spec.窗口长度,
            "滑动步长": target_spec.滑动步长,
            "延迟容忍": target_spec.延迟容忍,
            "触发策略": target_spec.触发策略,
            "异常版本": False,
            "abnormal": False,
        }
    )

    job.update(
        {
            "目标批次编号": batch_code,
            "新阈值版本": new_version_code,
            "状态": "成功",
            "受影响窗口": changes,
            "受影响事件": affected_events,
            "说明": batch["重算说明"],
            "结束时间": _now(),
        }
    )

    return {
        "batch": batch,
        "rows": sorted(new_rows, key=lambda row: int(row.get("窗口开始", 0))),
        "affected": changes,
        "affected_events": affected_events,
        "reused": reused,
        "removed": removed,
        "dropped": dropped,
        "duplicates": duplicates,
        "merged": False,
        "message": batch["重算说明"],
    }


# ---------------------------------------------------------------------------
# 校验：窗口类型、触发策略、窗口边界、阈值版本与唯一归属
# ---------------------------------------------------------------------------

def verify_batch(batch_code: str) -> dict[str, Any]:
    detail = batch_detail(batch_code)
    batch = detail["batch"]
    rows = detail["rows"]
    checks: list[dict[str, Any]] = []

    def add(name: str, ok: bool, detail_text: str) -> None:
        checks.append({"检查项": name, "通过": ok, "说明": detail_text})

    version_rows = {
        str(row.get("阈值版本")): row
        for row in store.rows(VERSION)
        if row.get("窗口编号") == batch.get("窗口编号")
    }
    header_version = str(batch.get("阈值版本"))
    add("阈值版本已标注", bool(header_version), f"批次版本 {header_version or '缺失'}")

    missing_boundary = [
        row.get("批次编号")
        for row in rows
        if row.get("窗口开始") is None or row.get("窗口结束") is None
    ]
    add("窗口边界完整", not missing_boundary, f"{len(rows)} 行均带窗口开始/结束")

    bad_geometry = []
    bad_version = []
    for row in rows:
        start = int(row.get("窗口开始", 0))
        end = int(row.get("窗口结束", 0))
        length = int(row.get("窗口长度", 0))
        slide = int(row.get("滑动步长", 0))
        if end - start != length or start % max(slide, 1) != 0:
            bad_geometry.append(f"{start}-{end}")
        row_version = str(row.get("阈值版本") or "")
        known = version_rows.get(row_version)
        if not known:
            bad_version.append(f"{start}-{end}:{row_version or '缺失'}")
            continue
        if (
            row.get("窗口类型") != known.get("窗口类型")
            or row.get("触发策略") != known.get("触发策略")
            or int(row.get("延迟容忍", -1)) != int(known.get("延迟容忍", -2))
        ):
            bad_version.append(f"{start}-{end}:{row_version} 参数与版本记录不一致")
    add("窗口边界符合窗口类型与步长", not bad_geometry, "、".join(bad_geometry) or "边界连续且对齐滑动步长")
    add("所用阈值版本可追溯", not bad_version, "、".join(bad_version) or "每行都能在版本历史里找到")

    strategy_bad = [
        row.get("批次编号")
        for row in rows
        if row.get("触发策略") not in ("水位线触发", "处理时间触发", "计数触发")
    ]
    add("触发策略在允许范围", not strategy_bad, "触发策略来自窗口定义")

    duplicates = duplicate_ownership(rows)
    add(
        "同一记录编号只进入一个窗口",
        not duplicates,
        "、".join(f"{record} 命中 {len(hits)} 个窗口" for record, hits in duplicates.items()) or "无重复归集",
    )

    rosters = event_roster(rows)
    batch_spec = parse_spec(
        {
            "窗口类型": batch.get("窗口类型"),
            "窗口长度": batch.get("窗口长度"),
            "滑动步长": batch.get("滑动步长"),
            "延迟容忍": batch.get("延迟容忍"),
            "触发策略": batch.get("触发策略"),
            "水位线": batch.get("水位线"),
        }
    )
    expected = assign_events(_events_for(str(batch.get("窗口编号"))), batch_spec)
    expected_ids = {
        str(event.get("记录编号"))
        for bucket in expected.windows.values()
        for event in bucket
    }
    unowned = sorted(expected_ids - set(rosters))
    add(
        "受影响事件均有归属",
        not unowned,
        "、".join(unowned) or f"应归集事件 {len(expected_ids)} 条全部落在窗口结果行",
    )

    abnormal_versions = sorted({item for item in bad_version})
    definition = _definition_for(str(batch.get("窗口编号")))
    active_version = definition.get("阈值版本") if definition else None
    if batch.get("状态") == ACTIVE:
        add(
            "生效批次与窗口定义版本一致",
            str(active_version) == header_version,
            f"窗口定义 {active_version or '缺失'}，批次 {header_version}",
        )

    passed = all(item["通过"] for item in checks)
    if not passed:
        if definition is not None:
            definition["异常版本"] = True
            definition["abnormal"] = True
    return {
        "批次编号": batch_code,
        "通过": passed,
        "检查项": checks,
        "重复归集": duplicates,
        "异常版本": abnormal_versions,
    }


def repair_window(window_id: str, *, note: str | None = None) -> dict[str, Any]:
    """顺着当前取数链路改正异常版本：收敛重复来源、修正版本标注、沿用历史快照。

    与阈值调整不同，这里不改参数，只让「窗口定义 - 版本历史 - 生效批次」三者回到
    同一条链路上；能沿用的窗口行原样沿用，只有标注异常的行才按当前参数重算。
    """
    bootstrap()
    definition = _definition_for(window_id)
    if definition is None:
        raise DatasetError(f"窗口 {window_id} 定义不存在")
    with _window_lock(window_id):
        candidates = [
            batch
            for batch in store.rows(DATASET)
            if batch.get("窗口编号") == window_id
            and batch.get("完整性") == "完整"
            and batch.get("状态") != FAILED
        ]
        if not candidates:
            raise DatasetError(f"窗口 {window_id} 没有可比对的完整批次，请先执行升级流程")
        active = next((batch for batch in candidates if batch.get("状态") == ACTIVE), None)
        if active is None:
            active = max(candidates, key=lambda item: int(item.get("id", 0)))
        # 只有同时处于「生效」的批次才算重复来源；历史批次是正常的版本留档。
        conflicts = [
            batch
            for batch in candidates
            if batch.get("状态") == ACTIVE and batch.get("批次编号") != active.get("批次编号")
        ]
        for batch in conflicts:
            batch["状态"] = ROLLED_BACK
            batch["重算说明"] = f"重复来源收敛：保留 {active.get('批次编号')}"
        if active.get("状态") != ACTIVE:
            active["状态"] = ACTIVE

        spec = parse_spec(
            {
                "窗口类型": active.get("窗口类型"),
                "窗口长度": active.get("窗口长度"),
                "滑动步长": active.get("滑动步长"),
                "延迟容忍": active.get("延迟容忍"),
                "触发策略": active.get("触发策略"),
                "水位线": active.get("水位线"),
            }
        )
        version_code = str(active.get("阈值版本") or "").strip()
        known_versions = {str(row.get("阈值版本")): row for row in store.rows(VERSION) if row.get("窗口编号") == window_id}
        if version_code not in known_versions:
            version_code = f"v{max((int(row.get('阈值版本号', 0)) for row in known_versions.values()), default=0) + 1}"
            new_version = {
                "id": _next_id(store.rows(VERSION)),
                "窗口编号": window_id,
                "阈值版本": version_code,
                "阈值版本号": int(version_code.lstrip("v") or 0),
                "窗口类型": spec.窗口类型,
                "窗口长度": spec.窗口长度,
                "滑动步长": spec.滑动步长,
                "延迟容忍": spec.延迟容忍,
                "触发策略": spec.触发策略,
                "水位线": spec.水位线,
                "状态": ACTIVE,
                "创建时间": _now(),
                "说明": note or "异常版本改正：补齐缺失的阈值版本记录",
            }
            store.rows(VERSION).append(new_version)
            known_versions[version_code] = new_version
            active["阈值版本"] = version_code

        active_version = known_versions.get(version_code)
        assert active_version is not None
        version_params = (
            active_version.get("窗口类型"),
            int(active_version.get("窗口长度", -1)),
            int(active_version.get("滑动步长", -1)),
            int(active_version.get("延迟容忍", -2)),
            active_version.get("触发策略"),
        )

        rows = _rows_of(str(active["批次编号"]))
        assignment = assign_events(_events_for(window_id), spec)
        kept: list[dict[str, Any]] = []
        kept_boundaries: set[tuple[int, int]] = set()
        repaired: list[dict[str, Any]] = []
        reused: list[dict[str, Any]] = []

        def version_params(row: dict[str, Any]) -> tuple[Any, ...]:
            return (
                row.get("窗口类型"),
                int(row.get("窗口长度", -1)),
                int(row.get("滑动步长", -1)),
                int(row.get("延迟容忍", -2)),
                row.get("触发策略"),
            )

        for row in rows:
            boundary = (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0)))
            declared = str(row.get("阈值版本") or "")
            known = known_versions.get(declared)
            # 一行是否可信：声明的版本在历史里存在、参数与该版本一致、窗口成员与归属一致。
            params_ok = known is not None and version_params(row) == version_params(known)
            events_ok = boundary in assignment.windows and set(row.get("事件记录编号") or []) == set(
                assignment.ids(boundary)
            )
            if params_ok and events_ok and boundary not in kept_boundaries:
                carried = {key: value for key, value in row.items() if key != "id"}
                kept.append(carried)
                kept_boundaries.add(boundary)
                reused.append(
                    {
                        "窗口开始": boundary[0],
                        "窗口结束": boundary[1],
                        "所用阈值版本": declared,
                        "来源": "沿用历史快照",
                    }
                )
            else:
                repaired.append(
                    {
                        "窗口开始": boundary[0],
                        "窗口结束": boundary[1],
                        "原阈值版本": declared or "缺失",
                        "原延迟容忍": row.get("延迟容忍"),
                        "改为": version_code,
                    }
                )

        for boundary in sorted(assignment.windows, key=lambda item: item[0]):
            if boundary in kept_boundaries:
                continue
            if any((item["窗口开始"], item["窗口结束"]) == boundary for item in repaired):
                continue
            repaired.append(
                {
                    "窗口开始": boundary[0],
                    "窗口结束": boundary[1],
                    "原阈值版本": "缺失",
                    "原延迟容忍": None,
                    "改为": version_code,
                }
            )

        if not repaired and not conflicts:
            definition["异常版本"] = False
            definition["abnormal"] = False
            return {
                "batch": active,
                "rows": sorted(rows, key=lambda row: int(row.get("窗口开始", 0))),
                "repaired": [],
                "reused": reused,
                "message": f"批次 {active.get('批次编号')} 版本标注一致，无需改正",
                "merged": True,
            }

        # 只重建标注异常的窗口行，其余行原样沿用历史快照
        rebuilt = dataset_rows(
            batch_id=str(active["批次编号"]),
            definition={**definition, "阈值版本": version_code},
            spec=spec,
            assignment=assignment,
            previous_rows=[],
        )
        rebuilt_by_boundary = {
            (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))): row for row in rebuilt
        }
        repaired_boundaries = {(int(item["窗口开始"]), int(item["窗口结束"])) for item in repaired}
        final_rows: list[dict[str, Any]] = [row for row in kept if (
            int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))
        ) not in repaired_boundaries]
        for boundary in sorted(assignment.windows, key=lambda item: item[0]):
            if boundary in repaired_boundaries and boundary in rebuilt_by_boundary:
                final_rows.append(rebuilt_by_boundary[boundary])
        final_rows.sort(key=lambda row: (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))))

        duplicates = duplicate_ownership(final_rows)
        if duplicates:
            raise DatasetError(
                "改正后仍存在同一记录编号的重复归集："
                + "；".join(f"{record} -> {hits}" for record, hits in duplicates.items())
            )

        # 原地改写生效批次的行，保证「改正」不引入第二份生效数据集
        table = store.rows(DATASET_ROW)
        table[:] = [row for row in table if row.get("批次编号") != active.get("批次编号")]
        for row in final_rows:
            entry = {"id": _next_id(table), "批次编号": active.get("批次编号")}
            entry.update(row)
            table.append(entry)
        active.update(
            {
                "阈值版本": version_code,
                "窗口数": len(final_rows),
                "记录数": sum(int(row.get("记录数", 0)) for row in final_rows),
                "沿用快照窗口数": sum(1 for row in final_rows if row.get("来源") == "沿用快照"),
                "数据集来源": "异常版本改正",
                "重算说明": f"改正 {len(repaired)} 个窗口行，沿用 {len(reused)} 个历史快照窗口",
            }
        )
        definition.update(
            {
                "阈值版本": version_code,
                "窗口类型": spec.窗口类型,
                "窗口长度": spec.窗口长度,
                "滑动步长": spec.滑动步长,
                "延迟容忍": spec.延迟容忍,
                "触发策略": spec.触发策略,
                "异常版本": False,
                "abnormal": False,
            }
        )
        return {
            "batch": active,
            "rows": sorted(final_rows, key=lambda row: int(row.get("窗口开始", 0))),
            "repaired": repaired,
            "reused": reused,
            "converged": [batch.get("批次编号") for batch in conflicts],
            "message": f"已改正 {len(repaired)} 个窗口行并收敛 {len(conflicts)} 个重复来源，沿用 {len(reused)} 个历史快照窗口",
            "merged": False,
        }


def _definition_for(window_id: str) -> dict[str, Any] | None:
    for row in store.rows("window"):
        if row.get("窗口编号") == window_id:
            return row
    return None


# ---------------------------------------------------------------------------
# 检查点恢复：只认一个阈值版本、一个来源明确的批次
# ---------------------------------------------------------------------------

def checkpoint_restore(
    checkpoint: dict[str, Any],
    *,
    version: str | None = None,
    batch_code: str | None = None,
) -> dict[str, Any]:
    bootstrap()
    window_id = str(checkpoint.get("窗口编号") or "")
    if not window_id:
        raise DatasetError(f"检查点 {checkpoint.get('记录编号')} 未关联窗口编号，无法恢复窗口触发结果")
    declared = str(checkpoint.get("阈值版本") or "").strip() or None
    requested = str(version or "").strip() or declared
    if declared and version and str(version).strip() != declared:
        raise DatasetError(
            f"检查点声明的阈值版本为 {declared}，本次请求 {version}，检查点恢复必须读取同一阈值版本"
        )
    if not requested:
        raise DatasetError("检查点没有阈值版本，请先执行升级迁移补齐版本后再恢复")

    if batch_code:
        batch = get_batch(batch_code)
        if batch.get("窗口编号") != window_id:
            raise DatasetError(f"批次 {batch_code} 属于窗口 {batch.get('窗口编号')}，与检查点窗口 {window_id} 不一致")
        if str(batch.get("阈值版本")) != requested:
            raise DatasetError(f"批次 {batch_code} 使用阈值版本 {batch.get('阈值版本')}，与检查点 {requested} 不一致")
        if batch.get("完整性") != "完整":
            raise DatasetError(f"批次 {batch_code} 不完整，不能用于恢复")
        unique = resolve_dataset(window_id, requested)
        if unique.get("批次编号") != batch_code:
            raise DatasetError(
                f"阈值版本 {requested} 下存在批次 {unique.get('批次编号')} 与 {batch_code} 两个来源，无法确定唯一归属"
            )
    else:
        batch = resolve_dataset(window_id, requested)

    rows = _rows_of(str(batch["批次编号"]))
    duplicates = duplicate_ownership(rows)
    if duplicates:
        raise DatasetError(
            f"批次 {batch['批次编号']} 存在同一记录编号的重复归集，检查点恢复已终止"
        )
    active = _active_batch(window_id)
    conflict_note = None
    if active and active.get("批次编号") != batch.get("批次编号"):
        conflict_note = (
            f"当前生效批次 {active.get('批次编号')}（{active.get('阈值版本')}）与恢复批次 "
            f"{batch.get('批次编号')}（{batch.get('阈值版本')}）并存；检查点只以 {batch.get('阈值版本')} "
            "的窗口归属为准，不会把同一记录编号并入当前生效批次"
        )

    checkpoint.update(
        {
            "恢复批次编号": batch.get("批次编号"),
            "恢复阈值版本": batch.get("阈值版本"),
            "恢复数据集来源": batch.get("数据集来源"),
            "恢复归属说明": conflict_note or f"按阈值版本 {batch.get('阈值版本')} 唯一批次恢复",
            "status": "已恢复",
            "记录状态": "已恢复",
            "pending": False,
            "abnormal": False,
        }
    )
    return {
        "batch": batch,
        "rows": rows,
        "conflict": conflict_note,
        "duplicates": duplicates,
        "message": f"检查点已按阈值版本 {batch.get('阈值版本')} 读取批次 {batch.get('批次编号')}",
    }


def active_window_result(window_id: str) -> dict[str, Any]:
    batch = resolve_dataset(window_id, require_active=True)
    return {"batch": batch, "rows": _rows_of(str(batch["批次编号"]))}


# ---------------------------------------------------------------------------
# 消费侧：按记录编号关联同一份数据集
# ---------------------------------------------------------------------------

def consumer_sync(record: dict[str, Any], *, batch_code: str | None = None) -> dict[str, Any]:
    bootstrap()
    window_id = str(record.get("窗口编号") or "")
    event_id = str(record.get("事件记录编号") or "").strip()
    if not window_id or not event_id:
        raise DatasetError("消费记录缺少窗口编号或事件记录编号，无法关联窗口触发结果")
    active = resolve_dataset(window_id, require_active=True)
    if batch_code and batch_code != active.get("批次编号"):
        raise DatasetError(
            f"请求批次 {batch_code} 与窗口 {window_id} 当前生效批次 {active.get('批次编号')} 不一致，"
            "列表页与消费侧必须读取同一份窗口触发结果数据集"
        )
    hits = [
        row
        for row in _rows_of(str(active["批次编号"]))
        if event_id in (row.get("事件记录编号") or [])
    ]
    if not hits:
        raise DatasetError(
            f"记录编号 {event_id} 未归集到批次 {active.get('批次编号')}，可能被延迟容忍判定为超期丢弃"
        )
    if len(hits) > 1:
        boundaries = "、".join(f"{row.get('窗口开始')}-{row.get('窗口结束')}" for row in hits)
        raise DatasetError(
            f"记录编号 {event_id} 同时出现在 {boundaries} 多个窗口，拒绝写入消费结果"
        )
    row = hits[0]
    record.update(
        {
            "关联批次编号": active.get("批次编号"),
            "关联阈值版本": row.get("阈值版本"),
            "关联窗口开始": row.get("窗口开始"),
            "关联窗口结束": row.get("窗口结束"),
            "归集状态": "已归集",
            "归集说明": f"记录 {event_id} 落在窗口 {row.get('窗口开始')}-{row.get('窗口结束')}（所用阈值版本 {row.get('阈值版本')}）",
        }
    )
    return {
        "batch": active,
        "row": row,
        "message": record["归集说明"],
    }


# ---------------------------------------------------------------------------
# 升级流程：旧数据库、新环境、部署回滚、依赖兼容
# ---------------------------------------------------------------------------

def upgrade_plan() -> dict[str, Any]:
    bootstrap()
    legacy_pending = [row for row in store.rows(LEGACY) if row.get("状态") != MIGRATED]
    return {
        "当前结构版本": _meta_value("schema_version", "未知"),
        "目标结构版本": CURRENT_SCHEMA_VERSION,
        "阶段": UPGRADE_STAGES,
        "旧数据库待迁移批次": len(legacy_pending),
        "迁移记录": list(store.rows(MIGRATION)),
        "说明": {
            "旧数据库": "为旧批次补齐阈值版本、窗口边界与完整性标记",
            "新环境": "补齐阈值版本与基线数据集，重复执行不会产生第二份生效批次",
            "部署回滚": "把最近的阈值调整重算回退到上一份完整批次，并同步窗口定义版本",
            "依赖兼容": "校验检查点与消费侧都能解析到唯一阈值版本的数据集",
        },
    }


def run_upgrade(stage: str, *, note: str | None = None) -> dict[str, Any]:
    bootstrap()
    if stage not in UPGRADE_STAGES:
        raise DatasetError(f"升级阶段「{stage}」不在允许范围：{'、'.join(UPGRADE_STAGES)}")
    with _lock:
        if stage == "旧数据库":
            result = _upgrade_legacy()
        elif stage == "新环境":
            result = _upgrade_new_env()
        elif stage == "部署回滚":
            result = _upgrade_rollback()
        else:
            result = _upgrade_compat()
        record = {
            "id": _next_id(store.rows(MIGRATION)),
            "迁移编号": _next_code("MIGR", store.rows(MIGRATION), "迁移编号"),
            "阶段": stage,
            "结构版本": _meta_value("schema_version", "未知"),
            "状态": "成功" if result.get("ok", True) else "需处理",
            "说明": result.get("message") or "",
            "备注": note,
            "创建时间": _now(),
        }
        store.rows(MIGRATION).append(record)
    result["迁移记录"] = record
    return result


def _upgrade_legacy() -> dict[str, Any]:
    migrated: list[dict[str, Any]] = []
    for legacy in store.rows(LEGACY):
        if legacy.get("状态") == MIGRATED:
            continue
        window_id = str(legacy.get("窗口编号") or "")
        definition = _definition_for(window_id)
        if definition is None:
            legacy["状态"] = "失败"
            legacy["迁移说明"] = f"窗口 {window_id} 不在窗口定义中"
            continue
        try:
            spec = parse_spec({}, definition)
        except SpecError as error:
            legacy["状态"] = "失败"
            legacy["迁移说明"] = str(error)
            continue
        version = _ensure_version(definition, spec)
        event_ids = [str(item) for item in legacy.get("事件记录编号") or []]
        valid_ids = {
            str(event.get("记录编号"))
            for event in _events_for(window_id)
            if str(event.get("记录编号")) in event_ids
        }
        if not valid_ids:
            legacy["状态"] = "失败"
            legacy["迁移说明"] = "旧批次引用的事件在当前窗口事件表里找不到"
            continue
        batch_code = str(legacy.get("批次编号") or _next_code("DSET", store.rows(DATASET), "批次编号"))
        start = int(legacy.get("窗口开始", 0))
        end = int(legacy.get("窗口结束", start + spec.窗口长度))
        events = [event for event in _events_for(window_id) if str(event.get("记录编号")) in valid_ids]
        has_active = _active_batch(window_id) is not None
        batch = {
            "id": _next_id(store.rows(DATASET)),
            "批次编号": batch_code,
            "窗口编号": window_id,
            "数据流名称": definition.get("数据流名称"),
            "阈值版本": version.get("阈值版本"),
            "基线批次编号": None,
            "窗口类型": spec.窗口类型,
            "触发策略": spec.触发策略,
            "窗口长度": spec.窗口长度,
            "滑动步长": spec.滑动步长,
            "延迟容忍": spec.延迟容忍,
            "水位线": spec.水位线,
            "状态": HISTORY if has_active else ACTIVE,
            "完整性": "完整",
            "数据集来源": "升级迁移",
            "窗口数": 1,
            "沿用快照窗口数": 0,
            "记录数": len(events),
            "幂等键": None,
            "创建时间": _now(),
            "失败原因": None,
            "重算说明": f"由旧数据库批次 {legacy.get('批次编号')} 迁移",
        }
        row = {
            "批次编号": batch_code,
            "窗口编号": window_id,
            "数据流名称": definition.get("数据流名称"),
            "窗口类型": spec.窗口类型,
            "触发策略": spec.触发策略,
            "阈值版本": version.get("阈值版本"),
            "窗口开始": start,
            "窗口结束": end,
            "窗口长度": spec.窗口长度,
            "滑动步长": spec.滑动步长,
            "延迟容忍": spec.延迟容忍,
            "记录数": len(events),
            "聚合值": sum(int(event.get("事件值") or 0) for event in events),
            "事件记录编号": sorted(valid_ids),
            "来源": "升级迁移",
            "状态": ACTIVE,
        }
        _save_rows(batch_code, [row])
        store.rows(DATASET).append(batch)
        legacy.update(
            {
                "状态": MIGRATED,
                "迁移批次编号": batch_code,
                "迁移阈值版本": version.get("阈值版本"),
                "迁移说明": "已补齐阈值版本与窗口边界",
            }
        )
        migrated.append({"窗口编号": window_id, "批次编号": batch_code, "阈值版本": version.get("阈值版本")})
    if migrated and _meta_value("schema_version") != CURRENT_SCHEMA_VERSION:
        _meta()["值"] = CURRENT_SCHEMA_VERSION
        _meta()["说明"] = "旧数据库已完成结构升级"
    return {
        "ok": True,
        "阶段": "旧数据库",
        "已迁移": migrated,
        "当前结构版本": _meta_value("schema_version"),
        "message": f"旧数据库迁移 {len(migrated)} 个批次，已补齐阈值版本与窗口边界",
    }


def _upgrade_new_env() -> dict[str, Any]:
    created: list[dict[str, Any]] = []
    legacy_windows = {str(row.get("窗口编号")) for row in store.rows(LEGACY) if row.get("状态") != MIGRATED}
    for definition in store.rows("window"):
        window_id = str(definition.get("窗口编号") or "")
        if not window_id or window_id in legacy_windows:
            continue
        if _active_batch(window_id) is not None:
            continue
        try:
            spec = parse_spec({}, definition)
        except SpecError:
            definition["abnormal"] = True
            continue
        version = _ensure_version(definition, spec)
        batch = _publish_baseline(definition, spec, version)
        created.append({"窗口编号": window_id, "批次编号": batch["批次编号"], "阈值版本": version.get("阈值版本")})
    if _meta_value("schema_version") != CURRENT_SCHEMA_VERSION:
        _meta()["值"] = CURRENT_SCHEMA_VERSION
        _meta()["说明"] = "新环境已初始化到当前结构"
    return {
        "ok": True,
        "阶段": "新环境",
        "已创建": created,
        "当前结构版本": _meta_value("schema_version"),
        "message": f"新环境补齐 {len(created)} 份基线数据集，重复执行不会产生第二份生效批次",
    }


def _upgrade_rollback() -> dict[str, Any]:
    recomputed = [
        batch
        for batch in store.rows(DATASET)
        if batch.get("数据集来源") == "阈值调整重算" and batch.get("状态") == ACTIVE
    ]
    if not recomputed:
        return {"ok": True, "阶段": "部署回滚", "回滚": None, "message": "没有待回滚的阈值调整批次"}
    batch = max(recomputed, key=lambda item: int(item.get("id", 0)))
    window_id = str(batch.get("窗口编号"))
    baseline_code = batch.get("基线批次编号")
    baseline = _batch_of(str(baseline_code)) if baseline_code else None
    if baseline is None or baseline.get("完整性") != "完整":
        raise DatasetError(f"批次 {batch.get('批次编号')} 缺少完整的上一份数据集，不能回滚")
    definition = _definition_for(window_id)
    if definition is None:
        raise DatasetError(f"窗口 {window_id} 定义不存在，不能回滚")
    for version in store.rows(VERSION):
        if version.get("窗口编号") != window_id:
            continue
        if version.get("阈值版本") == baseline.get("阈值版本"):
            version["状态"] = ACTIVE
        elif version.get("阈值版本") == batch.get("阈值版本"):
            version["状态"] = ROLLED_BACK
    batch["状态"] = ROLLED_BACK
    baseline["状态"] = ACTIVE
    definition.update(
        {
            "阈值版本": baseline.get("阈值版本"),
            "阈值版本号": next(
                (
                    int(version.get("阈值版本号", 0))
                    for version in store.rows(VERSION)
                    if version.get("窗口编号") == window_id
                    and version.get("阈值版本") == baseline.get("阈值版本")
                ),
                1,
            ),
            "窗口类型": baseline.get("窗口类型"),
            "窗口长度": baseline.get("窗口长度"),
            "滑动步长": baseline.get("滑动步长"),
            "延迟容忍": baseline.get("延迟容忍"),
            "触发策略": baseline.get("触发策略"),
            "异常版本": False,
            "abnormal": False,
        }
    )
    _meta()["最近回滚批次"] = batch.get("批次编号")
    _meta()["最近回滚到"] = baseline.get("批次编号")
    return {
        "ok": True,
        "阶段": "部署回滚",
        "回滚": {
            "窗口编号": window_id,
            "回滚批次编号": batch.get("批次编号"),
            "回到批次编号": baseline.get("批次编号"),
            "回到阈值版本": baseline.get("阈值版本"),
        },
        "message": f"已将窗口 {window_id} 回滚到批次 {baseline.get('批次编号')}（阈值版本 {baseline.get('阈值版本')}）",
    }


def _upgrade_compat() -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    checkpoints: list[dict[str, Any]] = []
    for checkpoint in store.rows("checkpoint"):
        window_id = str(checkpoint.get("窗口编号") or "")
        version = str(checkpoint.get("阈值版本") or "")
        if not window_id:
            continue
        try:
            batch = resolve_dataset(window_id, version or None)
            duplicates = duplicate_ownership(_rows_of(str(batch["批次编号"])))
            if duplicates:
                raise DatasetError(f"批次 {batch['批次编号']} 存在重复归集")
            checkpoints.append(
                {
                    "记录编号": checkpoint.get("记录编号"),
                    "窗口编号": window_id,
                    "阈值版本": batch.get("阈值版本"),
                    "批次编号": batch.get("批次编号"),
                }
            )
        except DatasetError as error:
            issues.append({"对象": f"检查点 {checkpoint.get('记录编号')}", "原因": str(error)})

    consumers: list[dict[str, Any]] = []
    for record in store.rows("latency"):
        window_id = str(record.get("窗口编号") or "")
        event_id = str(record.get("事件记录编号") or "")
        if not window_id or not event_id:
            continue
        try:
            active = resolve_dataset(window_id, require_active=True)
            hits = [
                row
                for row in _rows_of(str(active["批次编号"]))
                if event_id in (row.get("事件记录编号") or [])
            ]
            if len(hits) != 1:
                raise DatasetError(
                    f"记录编号 {event_id} 在批次 {active.get('批次编号')} 命中 {len(hits)} 个窗口"
                )
            consumers.append(
                {
                    "记录编号": record.get("记录编号"),
                    "事件记录编号": event_id,
                    "批次编号": active.get("批次编号"),
                    "窗口": f"{hits[0].get('窗口开始')}-{hits[0].get('窗口结束')}",
                }
            )
        except DatasetError as error:
            issues.append({"对象": f"消费记录 {record.get('记录编号')}", "原因": str(error)})

    for definition in store.rows("window"):
        window_id = str(definition.get("窗口编号") or "")
        if not window_id:
            continue
        try:
            active = resolve_dataset(window_id, str(definition.get("阈值版本") or "") or None, require_active=True)
            if str(active.get("阈值版本")) != str(definition.get("阈值版本")):
                raise DatasetError("窗口定义版本与生效批次版本不一致")
        except DatasetError as error:
            issues.append({"对象": f"窗口 {window_id}", "原因": str(error)})

    return {
        "ok": not issues,
        "阶段": "依赖兼容",
        "检查点": checkpoints,
        "消费侧": consumers,
        "问题": issues,
        "message": "检查点与消费侧均可解析到唯一阈值版本" if not issues else f"发现 {len(issues)} 个依赖兼容问题",
    }
