"""检查点恢复业务规则：状态流转与窗口触发结果数据集的版本一致性校验。

恢复动作不自己拼结果：窗口归属一律从检查点声明的阈值版本对应的唯一完整批次
读取，避免新旧阈值并存时同一迟到事件同时落进两个窗口。
"""
from __future__ import annotations

from typing import Any

from app.services import window_dataset
from app.services.window_dataset import DatasetError
from app.store import store

MODULE = "checkpoint"
REQUIRED_FIELDS = ["记录编号", "流作业名", "检查点ID"]
STATUS_ORDER = ["已创建", "已恢复", "已过期", "已清理"]
ACTION_RULES = {"恢复状态": "已恢复", "清理记录": "已过期", "归档记录": "已清理"}
NEGATIVE_ACTIONS = []


class CheckpointService:
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
            rows = [row for row in rows if keyword in str(row.get("记录编号", ""))]
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
        """状态流转；「恢复状态」额外校验窗口触发结果数据集的阈值版本。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"检查点记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于检查点恢复可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "恢复状态" and entry.get("窗口编号"):
            try:
                window_dataset.checkpoint_restore(entry)
            except DatasetError as error:
                return None, str(error)
            entry["pending"] = False
            entry["abnormal"] = False
            return entry, f"检查点记录已{action}，{entry.get('恢复归属说明')}"
        entry["status"] = target
        entry["记录状态"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"检查点记录已{action}"

    def restore_view(self, entry_id: int, *, version: str | None = None, batch_code: str | None = None) -> dict[str, Any]:
        """恢复前预检：检查点声明版本、指定批次与唯一归属是否一致。"""
        window_dataset.bootstrap()
        entry = store.find(MODULE, entry_id)
        if entry is None:
            raise DatasetError(f"检查点记录 {entry_id} 不存在或已归档")
        result = window_dataset.checkpoint_restore(
            {key: value for key, value in entry.items()},
            version=version,
            batch_code=batch_code,
        )
        return {
            "记录编号": entry.get("记录编号"),
            "窗口编号": entry.get("窗口编号"),
            "检查点阈值版本": entry.get("阈值版本"),
            "批次": result["batch"],
            "窗口结果行": result["rows"],
            "版本冲突说明": result["conflict"],
            "结论": result["message"],
        }
