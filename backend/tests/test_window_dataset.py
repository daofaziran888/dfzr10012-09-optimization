"""窗口触发结果数据集回归测试：不需要 pytest，直接用标准库 unittest 跑。

运行：``cd backend && .venv/bin/python -I -m unittest discover -s tests -v``
"""
from __future__ import annotations

import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services import window_dataset  # noqa: E402
from app.services.checkpoint import CheckpointService  # noqa: E402
from app.services.latency import LatencyService  # noqa: E402
from app.services.window import WindowService  # noqa: E402
from app.services.window_dataset import DatasetError  # noqa: E402
from app.services.window_engine import SpecError  # noqa: E402
from app.store import store  # noqa: E402


def row_of(rows, start: int, end: int):
    for row in rows:
        if int(row["窗口开始"]) == start and int(row["窗口结束"]) == end:
            return row
    return None


class DatasetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        window_dataset.reset_for_tests()
        self.windows = WindowService()
        self.checkpoints = CheckpointService()
        self.latency = LatencyService()

    def adjust_wind_0002(self, lateness: int = 30, **kwargs):
        return self.windows.adjust_threshold(2, {"延迟容忍": lateness}, **kwargs)


class AcceptanceSampleTests(DatasetTestCase):
    """验收样例：WIND-0002 滑窗 60s + 延迟容忍 15 -> 30，只重算受影响窗口；WIND-0003 沿用旧规则。"""

    def test_only_affected_windows_recomputed(self) -> None:
        before_wind3 = dict(self.windows.get_entry(3))
        before_wind3_batch = window_dataset.resolve_dataset("WIND-0003", "v1")["批次编号"]
        result = self.adjust_wind_0002()
        rows = result["rows"]

        affected = {(item["窗口开始"], item["窗口结束"]) for item in result["affected"]}
        self.assertEqual(affected, {(30, 90), (60, 120), (90, 150)})

        # 新增/成员变化的两个窗口按新版本重算
        for start, end in ((30, 90), (60, 120)):
            row = row_of(rows, start, end)
            self.assertIsNotNone(row)
            self.assertEqual(row["阈值版本"], "v2")

        # 成员被移出的窗口不再出现在结果里，事件没有留下重复归属
        self.assertIsNone(row_of(rows, 90, 150))

        # 不受影响的窗口沿用上一份完整数据集的快照
        reused = row_of(rows, 0, 60)
        self.assertEqual(reused["来源"], "沿用快照")
        self.assertEqual(reused["阈值版本"], "v1")
        self.assertEqual(reused["事件记录编号"], ["EVT-2002"])

        # WIND-0003 完全不受影响：版本、参数、批次都不变
        after_wind3 = self.windows.get_entry(3)
        self.assertEqual(after_wind3["阈值版本"], "v1")
        self.assertEqual(after_wind3["延迟容忍"], before_wind3["延迟容忍"])
        batches = window_dataset.list_batches(window_id="WIND-0003")
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["状态"], "生效")
        self.assertEqual(batches[0]["批次编号"], before_wind3_batch)

    def test_late_event_has_single_window(self) -> None:
        result = self.adjust_wind_0002()
        ownership = {}
        for row in result["rows"]:
            for record in row["事件记录编号"]:
                ownership.setdefault(record, []).append((row["窗口开始"], row["窗口结束"]))
        self.assertEqual(ownership["EVT-2001"], [(30, 90)])
        self.assertEqual(ownership["EVT-2003"], [(60, 120)])
        self.assertEqual(ownership["EVT-2002"], [(0, 60)])
        self.assertFalse(window_dataset.duplicate_ownership(result["rows"]))

    def test_boundaries_and_version_retained(self) -> None:
        result = self.adjust_wind_0002()
        for row in result["rows"]:
            self.assertEqual(row["窗口结束"] - row["窗口开始"], 60)
            self.assertEqual(row["窗口开始"] % 30, 0)
            self.assertIn(row["阈值版本"], {"v1", "v2"})
            self.assertEqual(row["窗口类型"], "滑动窗口")
            self.assertEqual(row["触发策略"], "水位线触发")
        self.assertEqual(result["batch"]["窗口长度"], 60)
        self.assertEqual(result["batch"]["滑动步长"], 30)
        self.assertEqual(result["batch"]["延迟容忍"], 30)
        self.assertEqual(result["batch"]["阈值版本"], "v2")
        passed = window_dataset.verify_batch(result["batch"]["批次编号"])
        self.assertTrue(passed["通过"], passed)


class ThresholdBehaviourTests(DatasetTestCase):
    def test_preview_only_reports_affected_events(self) -> None:
        preview = self.windows.preview(2, {"延迟容忍": 30})
        self.assertEqual(preview["受影响事件"], ["EVT-2001", "EVT-2003"])
        self.assertEqual(preview["需重算窗口数"], 3)
        self.assertFalse(set(preview["受影响事件"]) & {"EVT-2002", "EVT-2004"})

    def test_no_change_keeps_active_batch(self) -> None:
        active = window_dataset.resolve_dataset("WIND-0002", "v1")
        result = self.windows.adjust_threshold(2, {"延迟容忍": 15}, idempotency_key="same")
        self.assertTrue(result["merged"])
        self.assertEqual(result["batch"]["批次编号"], active["批次编号"])
        self.assertEqual(len(window_dataset.list_batches(window_id="WIND-0002")), 1)

    def test_idempotent_requests_share_one_batch(self) -> None:
        first = self.adjust_wind_0002(idempotency_key="same-key")
        second = self.adjust_wind_0002(30, idempotency_key="same-key")
        self.assertTrue(second["merged"])
        self.assertEqual(first["batch"]["批次编号"], second["batch"]["批次编号"])
        window_batches = window_dataset.list_batches(window_id="WIND-0002")
        active = [batch for batch in window_batches if batch["状态"] == "生效"]
        self.assertEqual(len(active), 1)

    def test_concurrent_recompute_single_terminal_state(self) -> None:
        results = []
        errors = []
        lock = threading.Lock()

        def worker() -> None:
            try:
                result = self.adjust_wind_0002(idempotency_key="concurrent")
                with lock:
                    results.append(result)
            except Exception as error:  # noqa: BLE001 - 测试要把失败原因收集出来
                with lock:
                    errors.append(error)

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        batch_codes = {result["batch"]["批次编号"] for result in results}
        self.assertEqual(len(batch_codes), 1, batch_codes)
        active = [batch for batch in window_dataset.list_batches(window_id="WIND-0002") if batch["状态"] == "生效"]
        self.assertEqual(len(active), 1)

    def test_stale_baseline_rejected(self) -> None:
        stale = window_dataset.resolve_dataset("WIND-0002", "v1")["批次编号"]
        self.adjust_wind_0002()
        with self.assertRaises(DatasetError):
            self.adjust_wind_0002(45, batch_code=stale)

    def test_invalid_specs_rejected(self) -> None:
        for values in (
            {"延迟容忍": -1},
            {"延迟容忍": 999},
            {"窗口长度": 0},
            {"滑动步长": 60},
            {"延迟容忍": "abc"},
            {"触发策略": "未知触发"},
        ):
            with self.assertRaises(SpecError, msg=str(values)):
                self.windows.preview(2, values)

    def test_list_and_detail_share_version(self) -> None:
        self.adjust_wind_0002()
        from app.routers.window import _decorate

        listed = next(
            _decorate(row) for row in self.windows.list_entries()[0] if row["窗口编号"] == "WIND-0002"
        )
        detail = _decorate(self.windows.get_entry(2))
        self.assertEqual(listed["生效批次编号"], detail["生效批次编号"])
        self.assertEqual(listed["生效批次阈值版本"], detail["生效批次阈值版本"])
        self.assertEqual(detail["生效批次阈值版本"], detail["阈值版本"])

    def test_checkpoint_requires_same_version(self) -> None:
        self.adjust_wind_0002()
        view = self.checkpoints.restore_view(2)
        self.assertEqual(view["批次"]["阈值版本"], "v1")
        self.assertEqual(view["批次"]["批次编号"], "DSET-0001")
        with self.assertRaises(DatasetError):
            self.checkpoints.restore_view(2, version="v2")

    def test_checkpoint_restore_not_merging_new_active(self) -> None:
        self.adjust_wind_0002()
        entry = store.find("checkpoint", 2)
        record, message = self.checkpoints.run_action(2, "恢复状态")
        self.assertIsNotNone(record, message)
        self.assertEqual(entry["恢复批次编号"], "DSET-0001")
        self.assertEqual(entry["恢复阈值版本"], "v1")
        self.assertIn("不会把同一记录编号并入当前生效批次", entry["恢复归属说明"])
        # 生效批次仍然是新的 v2，检查点没有把它改成 v1
        self.assertEqual(self.windows.get_entry(2)["阈值版本"], "v2")

    def test_checkpoint_ambiguous_source_rejected(self) -> None:
        self.adjust_wind_0002()
        # 人为制造同版本两个完整批次，模拟升级遗留的重复来源
        duplicate = dict(window_dataset.resolve_dataset("WIND-0002", "v1"))
        duplicate["id"] = 99
        duplicate["批次编号"] = "DSET-9999"
        store.rows("window_dataset").append(duplicate)
        with self.assertRaises(DatasetError) as caught:
            self.checkpoints.restore_view(2)
        self.assertIn("多个来源", str(caught.exception))


class ConsumerTests(DatasetTestCase):
    def test_consumer_follows_active_batch(self) -> None:
        self.adjust_wind_0002()
        synced = self.latency.sync_entry(1)
        self.assertEqual(synced["批次"]["阈值版本"], "v2")
        self.assertEqual(synced["窗口结果行"]["窗口开始"], 0)
        self.assertEqual(synced["窗口结果行"]["窗口结束"], 60)

    def test_consumer_rejects_other_batch(self) -> None:
        self.adjust_wind_0002()
        with self.assertRaises(DatasetError):
            self.latency.sync_entry(1, batch_code="DSET-0001")

    def test_consumer_batch_all_clean(self) -> None:
        self.adjust_wind_0002()
        report = self.latency.sync_all()
        self.assertEqual(report["问题"], [])
        self.assertEqual(len(report["已关联"]), 3)


class FailureAndRecoveryTests(DatasetTestCase):
    def test_failed_recompute_keeps_previous_dataset(self) -> None:
        active_before = window_dataset.resolve_dataset("WIND-0002", "v1")["批次编号"]
        original = window_dataset.dataset_rows

        def boom(**kwargs):
            raise RuntimeError("模拟窗口结果写入失败")

        window_dataset.dataset_rows = boom
        try:
            with self.assertRaises(RuntimeError):
                self.adjust_wind_0002()
        finally:
            window_dataset.dataset_rows = original

        active_after = window_dataset.resolve_dataset("WIND-0002", "v1")
        self.assertEqual(active_after["批次编号"], active_before)
        self.assertEqual(active_after["完整性"], "完整")
        self.assertEqual(self.windows.get_entry(2)["阈值版本"], "v1")
        self.assertEqual(self.windows.get_entry(2)["延迟容忍"], 15)
        jobs = [job for job in store.rows("window_recompute") if job["窗口编号"] == "WIND-0002"]
        self.assertEqual(jobs[-1]["状态"], "失败")
        failed = [batch for batch in window_dataset.list_batches(window_id="WIND-0002") if batch["状态"] == "失败"]
        self.assertEqual(failed, [])
        # 失败后仍可用同一份完整数据集做只读查询
        self.assertTrue(window_dataset.verify_batch(active_before)["通过"])

    def test_duplicate_ownership_blocks_publication(self) -> None:
        # 人为构造重复归集：把同一记录编号塞进另一个窗口的基线行
        baseline = window_dataset.resolve_dataset("WIND-0002", "v1")
        row = window_dataset.batch_detail(baseline["批次编号"])["rows"][0]
        row["事件记录编号"] = list(row["事件记录编号"]) + ["EVT-2001"]
        with self.assertRaises(DatasetError):
            self.adjust_wind_0002()


class RepairTests(DatasetTestCase):
    def test_repair_keeps_valid_snapshots_and_fixes_labels(self) -> None:
        result = self.adjust_wind_0002()
        batch_code = result["batch"]["批次编号"]
        rows = window_dataset.batch_detail(batch_code)["rows"]
        rows[1]["阈值版本"] = "v1"   # 把重算行的版本标错
        rows[2]["延迟容忍"] = 15      # 行内参数与所用版本不一致
        self.assertFalse(window_dataset.verify_batch(batch_code)["通过"])

        repaired = self.windows.repair(2)
        self.assertEqual(len(repaired["repaired"]), 2)
        self.assertEqual(len(repaired["reused"]), 2)
        self.assertTrue(repaired["校验"]["通过"])
        self.assertFalse(self.windows.get_entry(2).get("异常版本", False))

        after = {(row["窗口开始"], row["窗口结束"]): row for row in repaired["rows"]}
        self.assertEqual(after[(0, 60)]["阈值版本"], "v1")
        self.assertEqual(after[(0, 60)]["来源"], "沿用快照")
        self.assertEqual(after[(30, 90)]["阈值版本"], "v2")
        # 改正不新开生效批次
        active = [batch for batch in window_dataset.list_batches(window_id="WIND-0002") if batch["状态"] == "生效"]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["批次编号"], batch_code)

    def test_repair_is_idempotent(self) -> None:
        result = self.adjust_wind_0002()
        rows = window_dataset.batch_detail(result["batch"]["批次编号"])["rows"]
        rows[1]["阈值版本"] = "v1"
        self.windows.repair(2)
        second = self.windows.repair(2)
        self.assertEqual(second["repaired"], [])
        self.assertTrue(second["merged"])

    def test_repair_converges_duplicate_active_batches(self) -> None:
        result = self.adjust_wind_0002()
        duplicate = dict(result["batch"])
        duplicate["id"] = 97
        duplicate["批次编号"] = "DSET-9997"
        store.rows("window_dataset").append(duplicate)
        # 同一阈值版本出现两个生效批次时，检查点恢复拒绝挑选来源
        with self.assertRaises(DatasetError):
            self.checkpoints.restore_view(2, version="v2")
        repaired = self.windows.repair(2)
        self.assertEqual(repaired["converged"], ["DSET-9997"])
        self.assertEqual(
            len([b for b in window_dataset.list_batches(window_id="WIND-0002") if b["状态"] == "生效"]), 1
        )

    def test_repair_requires_existing_dataset(self) -> None:
        with self.assertRaises(DatasetError):
            self.windows.repair(1)


class UpgradeTests(DatasetTestCase):
    def test_upgrade_plan_covers_four_stages(self) -> None:
        plan = window_dataset.upgrade_plan()
        self.assertEqual(plan["阶段"], ["旧数据库", "新环境", "部署回滚", "依赖兼容"])

    def test_legacy_database_migration_backfills_fields(self) -> None:
        before = window_dataset.list_batches(window_id="WIND-0001")
        self.assertEqual(before, [])
        result = window_dataset.run_upgrade("旧数据库")
        self.assertTrue(result["ok"])
        batches = window_dataset.list_batches(window_id="WIND-0001")
        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["阈值版本"], "v1")
        detail = window_dataset.batch_detail(batches[0]["批次编号"])
        for row in detail["rows"]:
            self.assertIsNotNone(row["窗口开始"])
            self.assertIsNotNone(row["窗口结束"])
            self.assertEqual(row["阈值版本"], "v1")

    def test_new_environment_is_idempotent(self) -> None:
        window_dataset.run_upgrade("旧数据库")
        window_dataset.run_upgrade("新环境")
        window_dataset.run_upgrade("新环境")
        for window_id in ("WIND-0001", "WIND-0002", "WIND-0003"):
            active = [batch for batch in window_dataset.list_batches(window_id=window_id) if batch["状态"] == "生效"]
            self.assertEqual(len(active), 1, window_id)

    def test_deploy_rollback_restores_previous_version(self) -> None:
        self.windows.adjust_threshold(3, {"延迟容忍": 25})
        self.assertEqual(self.windows.get_entry(3)["阈值版本"], "v2")
        result = window_dataset.run_upgrade("部署回滚")
        self.assertEqual(result["回滚"]["回到阈值版本"], "v1")
        entry = self.windows.get_entry(3)
        self.assertEqual(entry["阈值版本"], "v1")
        self.assertEqual(entry["延迟容忍"], 10)
        batches = window_dataset.list_batches(window_id="WIND-0003")
        self.assertEqual({batch["状态"] for batch in batches}, {"生效", "已回滚"})

    def test_dependency_compat_after_changes(self) -> None:
        window_dataset.run_upgrade("旧数据库")
        self.adjust_wind_0002()
        result = window_dataset.run_upgrade("依赖兼容")
        self.assertTrue(result["ok"], result["问题"])
        self.assertEqual(len(result["检查点"]), 3)
        self.assertEqual(len(result["消费侧"]), 3)

    def test_compat_detects_ambiguous_version(self) -> None:
        self.adjust_wind_0002()
        duplicate = dict(window_dataset.resolve_dataset("WIND-0002", "v1"))
        duplicate["id"] = 98
        duplicate["批次编号"] = "DSET-9998"
        store.rows("window_dataset").append(duplicate)
        result = window_dataset.run_upgrade("依赖兼容")
        self.assertFalse(result["ok"])
        self.assertTrue(any("多个来源" in issue["原因"] for issue in result["问题"]))


if __name__ == "__main__":
    unittest.main()
