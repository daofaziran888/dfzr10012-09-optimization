"""延迟追踪管理业务规则：状态流转与窗口触发结果数据集的消费侧关联。

消费侧不自己算窗口，只按「记录编号」到当前生效批次里查归属；命中多个窗口
时拒绝写入，保证同一个迟到事件不会产出两个互相冲突的结论。
"""
from __future__ import annotations

from typing import Any

from app.services import window_dataset
from app.services.window_dataset import DatasetError
from app.store import store

MODULE = "latency"
REQUIRED_FIELDS = ["记录编号", "流作业名", "端到端延迟"]
STATUS_ORDER = ["采集中", "已超标", "已恢复", "已归档"]
ACTION_RULES = {"标记超标": "已超标", "优化处理": "已恢复", "归档记录": "已归档"}
NEGATIVE_ACTIONS = ["标记超标"]


class LatencyService:
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
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"延迟记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于延迟追踪管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        if action == "标记超标" and entry.get("窗口编号"):
            try:
                result = self.sync_entry(entry_id)
            except DatasetError as error:
                return None, str(error)
            entry.update(
                {
                    "status": target,
                    "pending": False,
                    "abnormal": action in NEGATIVE_ACTIONS,
                }
            )
            return entry, f"延迟记录已{action}；{result['message']}"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"延迟记录已{action}"

    def sync_entry(self, entry_id: int, *, batch_code: str | None = None) -> dict[str, Any]:
        """按记录编号关联当前生效数据集；这是另一个业务入口的同一份口径校验。"""
        window_dataset.bootstrap()
        record = store.find(MODULE, entry_id)
        if record is None:
            raise DatasetError(f"延迟记录 {entry_id} 不存在或已归档")
        result = window_dataset.consumer_sync(record, batch_code=batch_code)
        return {
            "记录编号": record.get("记录编号"),
            "事件记录编号": record.get("事件记录编号"),
            "批次": result["batch"],
            "窗口结果行": result["row"],
            "结论": result["message"],
        }

    def sync_all(self) -> dict[str, Any]:
        """批量巡检：返回每个消费记录关联到的批次、窗口边界与结论。"""
        window_dataset.bootstrap()
        synced: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        for record in store.rows(MODULE):
            if not record.get("窗口编号"):
                continue
            try:
                synced.append(self.sync_entry(int(record["id"])))
            except DatasetError as error:
                issues.append({"记录编号": record.get("记录编号"), "原因": str(error)})
        return {
            "已关联": synced,
            "问题": issues,
            "结论": (
                f"消费侧 {len(synced)} 条记录均与列表页读取同一份窗口触发结果数据集"
                if not issues
                else f"{len(issues)} 条消费记录无法唯一关联，需先处理版本冲突"
            ),
        }
