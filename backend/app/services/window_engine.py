"""窗口触发结果数据集引擎：窗口归属计算、受影响窗口判定与数据集行构造。

唯一归属规则（同一记录编号只能进入一个窗口）：

1. 事件只进入包含其事件时间、且已随水位线闭合的窗口实例；
2. 到达时间必须落在「窗口结束时间 + 延迟容忍」之内（含边界）；
3. 多个候选窗口同时可接纳时，取结束时间最早的那个，保证迟到事件不会因为
   阈值放宽而被后来的窗口重复归集。

延迟容忍调整后，用同一批事件分别按旧阈值和新阈值计算归属；只有归属发生
变化的窗口才进入重算范围，其余窗口沿用上一份完整数据集里的快照行。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

TRIGGER_STRATEGIES = ("水位线触发", "处理时间触发", "计数触发")
TUMBLING = "滚动窗口"
SLIDING = "滑动窗口"


class SpecError(ValueError):
    """窗口参数不合法：调用方应把说明原样带回给前端。"""


@dataclass(frozen=True)
class WindowSpec:
    窗口类型: str
    窗口长度: int
    滑动步长: int
    延迟容忍: int
    触发策略: str
    水位线: int

    @property
    def signature(self) -> tuple[int | str, ...]:
        return (
            self.窗口类型,
            self.窗口长度,
            self.滑动步长,
            self.延迟容忍,
            self.触发策略,
        )


def as_int(value: Any, field: str) -> int:
    """把前端传来的数字或数字字符串收敛成 int，避免 "60" 与 60 混用。"""
    if isinstance(value, bool):
        raise SpecError(f"{field}必须是整数")
    if isinstance(value, int):
        return value
    text = str(value if value is not None else "").strip()
    if not text:
        raise SpecError(f"{field}不能为空")
    try:
        number = float(text)
    except ValueError as error:
        raise SpecError(f"{field}必须是整数，当前为「{text}」") from error
    if number != int(number):
        raise SpecError(f"{field}必须是整数，当前为「{text}」")
    return int(number)


def parse_spec(values: dict[str, Any], base: dict[str, Any] | None = None) -> WindowSpec:
    """从提交字段或已有定义读取参数；缺字段时回落到 base（当前定义）。"""
    source: dict[str, Any] = {}
    if base:
        source.update(base)
    source.update({key: value for key, value in values.items() if value is not None})

    窗口类型 = str(source.get("窗口类型") or "").strip()
    触发策略 = str(source.get("触发策略") or "").strip()
    if 窗口类型 not in (TUMBLING, SLIDING):
        raise SpecError(f"窗口类型只支持{TUMBLING}、{SLIDING}")
    if 触发策略 not in TRIGGER_STRATEGIES:
        raise SpecError(f"触发策略只支持{'、'.join(TRIGGER_STRATEGIES)}")

    length = as_int(source.get("窗口长度"), "窗口长度")
    slide = as_int(source.get("滑动步长"), "滑动步长")
    lateness = as_int(source.get("延迟容忍"), "延迟容忍")
    watermark = as_int(source.get("水位线", 0), "水位线")

    if length <= 0:
        raise SpecError("窗口长度必须大于 0")
    if slide <= 0:
        raise SpecError("滑动步长必须大于 0")
    if slide > length:
        raise SpecError("滑动步长不能大于窗口长度")
    if 窗口类型 == TUMBLING and slide != length:
        raise SpecError(f"{TUMBLING}要求滑动步长等于窗口长度")
    if 窗口类型 == SLIDING and slide >= length:
        raise SpecError(f"{SLIDING}要求滑动步长小于窗口长度")
    if lateness < 0:
        raise SpecError("延迟容忍不能为负数")
    if lateness > length:
        raise SpecError("延迟容忍不能超过窗口长度，否则同一事件会落进过多窗口")
    return WindowSpec(窗口类型, length, slide, lateness, 触发策略, watermark)


@dataclass
class Assignment:
    """一次归属计算的结果：窗口边界 -> 事件列表，以及超期丢弃的事件。"""

    windows: dict[tuple[int, int], list[dict[str, Any]]]
    dropped: list[dict[str, Any]]

    def ids(self, boundary: tuple[int, int]) -> list[str]:
        return sorted({str(event.get("记录编号") or "") for event in self.windows.get(boundary, [])})


def assign_events(events: Iterable[dict[str, Any]], spec: WindowSpec) -> Assignment:
    """按唯一归属规则把事件分到窗口实例。

    窗口实例只统计已闭合（窗口结束 <= 水位线）且至少落进一个事件的时间段；
    空窗口不会出现在触发结果数据集里。
    """
    windows: dict[tuple[int, int], list[dict[str, Any]]] = {}
    dropped: list[dict[str, Any]] = []
    for event in events:
        event_time = as_int(event.get("事件时间"), "事件时间")
        arrival = as_int(event.get("到达时间"), "到达时间")
        candidates: list[tuple[int, int]] = []
        last_start = (event_time // spec.滑动步长) * spec.滑动步长
        for start in range(0, last_start + 1, spec.滑动步长):
            end = start + spec.窗口长度
            if event_time < start or event_time >= end:
                continue
            if end > spec.水位线:
                continue  # 窗口还没闭合，不能写入触发结果
            if arrival <= end + spec.延迟容忍:
                candidates.append((start, end))
        if not candidates:
            dropped.append(dict(event))
            continue
        boundary = min(candidates, key=lambda item: item[1])
        windows.setdefault(boundary, []).append(dict(event))
    for bucket in windows.values():
        bucket.sort(key=lambda item: (int(item.get("事件时间") or 0), str(item.get("记录编号") or "")))
    return Assignment(windows=windows, dropped=dropped)


def affected_windows(previous: Assignment, current: Assignment) -> list[dict[str, Any]]:
    """比较新旧归属，返回归属发生变化的窗口（新增、移除、成员变化）。"""
    changes: list[dict[str, Any]] = []
    for boundary in sorted(set(previous.windows) | set(current.windows), key=lambda item: item[0]):
        before = previous.ids(boundary)
        after = current.ids(boundary)
        if before == after:
            continue
        changes.append(
            {
                "窗口开始": boundary[0],
                "窗口结束": boundary[1],
                "新增记录": [item for item in after if item not in before],
                "移除记录": [item for item in before if item not in after],
                "变化类型": "新增窗口" if boundary not in previous.windows else (
                    "移除窗口" if boundary not in current.windows else "成员变化"
                ),
            }
        )
    return changes


def dataset_rows(
    *,
    batch_id: str,
    definition: dict[str, Any],
    spec: WindowSpec,
    assignment: Assignment,
    previous_rows: Iterable[dict[str, Any]] = (),
    default_source: str = "重算",
) -> list[dict[str, Any]]:
    """构造一批触发结果行，并给每一行标注窗口边界、阈值版本与结果来源。

    未受阈值调整影响的窗口优先复用上一份完整数据集的快照行；只有成员与参数
    都一致时才标记为「沿用快照」，否则按重算结果写入。
    """
    previous = {
        (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0))): row
        for row in previous_rows
    }
    rows: list[dict[str, Any]] = []
    for boundary in sorted(assignment.windows, key=lambda item: item[0]):
        events = assignment.windows[boundary]
        ids = assignment.ids(boundary)
        snapshot = previous.get(boundary)
        reused = bool(
            snapshot
            and snapshot.get("事件记录编号") == ids
            and snapshot.get("阈值版本") == definition.get("阈值版本")
            and snapshot.get("窗口类型") == spec.窗口类型
            and snapshot.get("触发策略") == spec.触发策略
            and int(snapshot.get("窗口长度", -1)) == spec.窗口长度
            and int(snapshot.get("滑动步长", -1)) == spec.滑动步长
            and int(snapshot.get("延迟容忍", -1)) == spec.延迟容忍
        )
        rows.append(
            {
                "批次编号": batch_id,
                "窗口编号": definition.get("窗口编号"),
                "数据流名称": definition.get("数据流名称"),
                "窗口类型": spec.窗口类型,
                "触发策略": spec.触发策略,
                "阈值版本": definition.get("阈值版本"),
                "窗口开始": boundary[0],
                "窗口结束": boundary[1],
                "窗口长度": spec.窗口长度,
                "滑动步长": spec.滑动步长,
                "延迟容忍": spec.延迟容忍,
                "记录数": len(events),
                "聚合值": sum(int(event.get("事件值") or 0) for event in events),
                "事件记录编号": ids,
                "来源": "沿用快照" if reused else default_source,
                "状态": "生效",
            }
        )
    return rows


def duplicate_ownership(rows: Iterable[dict[str, Any]]) -> dict[str, list[dict[str, int]]]:
    """检查同一记录编号是否落入多个窗口；返回记录编号 -> 命中的窗口边界列表。"""
    ownership: dict[str, list[dict[str, int]]] = {}
    for row in rows:
        for record_id in row.get("事件记录编号") or []:
            ownership.setdefault(str(record_id), []).append(
                {"窗口开始": int(row.get("窗口开始", 0)), "窗口结束": int(row.get("窗口结束", 0))}
            )
    return {record_id: hits for record_id, hits in ownership.items() if len(hits) > 1}


def event_roster(rows: Iterable[dict[str, Any]]) -> dict[str, tuple[int, int]]:
    """记录编号 -> 所在窗口边界，用于跨版本比较迟到事件迁移。"""
    roster: dict[str, tuple[int, int]] = {}
    for row in rows:
        boundary = (int(row.get("窗口开始", 0)), int(row.get("窗口结束", 0)))
        for record_id in row.get("事件记录编号") or []:
            roster.setdefault(str(record_id), boundary)
    return roster
