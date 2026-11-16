"""窗口计算配置业务规则：状态流转、阈值调整、受影响事件校验与重算入口。

阈值调整只改参数，不直接写结果；真正的数据集发布、受影响窗口判定和历史快照
沿用都交给 ``window_dataset``，保证列表页、详情页、检查点与消费侧读同一份
窗口触发结果数据集。状态流转仍然只在这里改，路由层不做业务判断。
"""
from __future__ import annotations

from typing import Any

from app.services import window_dataset
from app.services.window_dataset import DatasetError
from app.services.window_engine import SpecError, affected_windows, assign_events, parse_spec
from app.store import store

MODULE = "window"
REQUIRED_FIELDS = ["窗口编号", "数据流名称", "窗口类型"]
STATUS_ORDER = ["草稿", "生效中", "已暂停", "已关闭"]
ACTION_RULES = {"启用窗口": "生效中", "暂停窗口": "已暂停", "关闭窗口": "已关闭"}
NEGATIVE_ACTIONS = []

THRESHOLD_FIELDS = ["窗口类型", "窗口长度", "滑动步长", "延迟容忍", "触发策略", "水位线"]


class WindowService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("窗口编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"窗口定义 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于窗口计算配置可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["窗口状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"窗口定义已{action}"

    # -- 阈值查看 ---------------------------------------------------------

    def threshold_view(self, entry_id: int) -> dict[str, Any]:
        entry = self._require(entry_id)
        window_id = str(entry.get("窗口编号"))
        current = parse_spec({}, entry)
        dataset_note = "生效批次与窗口定义版本一致"
        try:
            active_batch: dict[str, Any] | None = window_dataset.resolve_dataset(
                window_id, str(entry.get("阈值版本")), require_active=True
            )
        except DatasetError as error:
            active_batch = None
            dataset_note = str(error)
        events = self._events(window_id)
        assignment = assign_events(events, current)
        return {
            "entry": entry,
            "当前参数": {
                "窗口类型": current.窗口类型,
                "窗口长度": current.窗口长度,
                "滑动步长": current.滑动步长,
                "延迟容忍": current.延迟容忍,
                "触发策略": current.触发策略,
                "水位线": current.水位线,
            },
            "当前阈值版本": entry.get("阈值版本"),
            "生效批次": active_batch,
            "数据集说明": dataset_note,
            "阈值版本历史": window_dataset.versions(window_id),
            "事件数": len(events),
            "归集事件数": sum(len(bucket) for bucket in assignment.windows.values()),
            "超期丢弃记录": [event.get("记录编号") for event in assignment.dropped],
        }

    def preview(self, entry_id: int, values: dict[str, Any]) -> dict[str, Any]:
        """调整前预演：用同一批事件比较新旧阈值，只列出受影响的事件与窗口。"""
        entry = self._require(entry_id)
        window_id = str(entry.get("窗口编号"))
        current = parse_spec({}, entry)
        target = parse_spec(values, entry)
        events = self._events(window_id)
        previous = assign_events(events, current)
        upcoming = assign_events(events, target)
        changes = affected_windows(previous, upcoming)
        return {
            "窗口编号": window_id,
            "当前阈值版本": entry.get("阈值版本"),
            "旧参数": {
                "延迟容忍": current.延迟容忍,
                "窗口长度": current.窗口长度,
                "滑动步长": current.滑动步长,
                "窗口类型": current.窗口类型,
                "触发策略": current.触发策略,
            },
            "新参数": {
                "延迟容忍": target.延迟容忍,
                "窗口长度": target.窗口长度,
                "滑动步长": target.滑动步长,
                "窗口类型": target.窗口类型,
                "触发策略": target.触发策略,
            },
            "参数变化": target.signature != current.signature,
            "受影响事件": sorted(
                {
                    record
                    for item in changes
                    for record in list(item.get("新增记录") or []) + list(item.get("移除记录") or [])
                }
            ),
            "受影响窗口": changes,
            "需重算窗口数": len(changes),
            "超期丢弃记录": [event.get("记录编号") for event in upcoming.dropped],
            "结论": (
                f"只重算 {len(changes)} 个受影响窗口，其余窗口沿用历史快照"
                if changes
                else "阈值调整不改变任何窗口归属，可直接沿用当前完整数据集"
            ),
        }

    # -- 调整阈值并写入触发结果数据集 --------------------------------------

    def adjust_threshold(
        self,
        entry_id: int,
        values: dict[str, Any],
        *,
        batch_code: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        entry = self._require(entry_id)
        try:
            target = parse_spec(values, entry)
        except SpecError as error:
            raise DatasetError(str(error)) from error
        result = window_dataset.recompute(
            entry,
            values,
            baseline_batch=batch_code,
            idempotency_key=idempotency_key,
        )
        if not result.get("merged"):
            self._apply_actions(entry, values)
            verification = window_dataset.verify_batch(str(result["batch"]["批次编号"]))
            result["校验"] = verification
            if not verification["通过"]:
                raise DatasetError(
                    f"批次 {result['batch']['批次编号']} 校验未通过："
                    + "；".join(
                        f"{item['检查项']}（{item['说明']}）"
                        for item in verification["检查项"]
                        if not item["通过"]
                    )
                )
        return result

    def repair(self, entry_id: int) -> dict[str, Any]:
        """顺着当前取数链路改正异常版本，不新开生效批次。"""
        entry = self._require(entry_id)
        result = window_dataset.repair_window(str(entry.get("窗口编号")), note=f"手工触发：窗口定义 {entry_id}")
        result["校验"] = window_dataset.verify_batch(str(result["batch"]["批次编号"]))
        return result

    def _apply_actions(self, entry: dict[str, Any], values: dict[str, Any]) -> None:
        """把阈值调整沿用到关联对象：延迟触发策略同步到水位线配置。"""
        if "触发策略" not in values and "延迟容忍" not in values:
            return
        for config in store.rows("watermark"):
            if config.get("流名称") == entry.get("数据流名称"):
                config["延迟容忍"] = entry.get("延迟容忍")
                config["对齐策略"] = entry.get("触发策略")
                config["同步窗口编号"] = entry.get("窗口编号")
                config["同步阈值版本"] = entry.get("阈值版本")

    def _require(self, entry_id: int) -> dict[str, Any]:
        window_dataset.bootstrap()
        entry = store.find(MODULE, entry_id)
        if entry is None:
            raise DatasetError(f"窗口定义 {entry_id} 不存在或已归档")
        return entry

    @staticmethod
    def _events(window_id: str) -> list[dict[str, Any]]:
        return [row for row in store.rows("window_event") if row.get("窗口编号") == window_id]
