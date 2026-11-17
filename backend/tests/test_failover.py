"""数据源批量故障切端到端测试。

运行（在仓库根目录）：
  PYTHONPATH=backend:.venv python3 -m unittest backend.tests.test_failover -v
"""
from __future__ import annotations

import unittest

from app.services.datasource import DatasourceService
from app.services.failover import (
    POLICY_AFFINITY,
    POLICY_FRESHNESS,
    QUEUE_APPLIED,
    QUEUE_PENDING,
    RESULT_FAILED,
    RESULT_SKIPPED,
    RESULT_SUCCESS,
    failover_service,
)
from app.services.proxy import ProxyService
from app.services.ratelimit import RatelimitService
from app.store import store


class FailoverTestCase(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()
        self.datasource = DatasourceService()
        self.proxy = ProxyService()
        self.ratelimit = RatelimitService()

    def groups(self) -> dict[str, list[dict]]:
        result: dict[str, list[dict]] = {}
        for row in store.rows("datasource"):
            result.setdefault(row["数据源组"], []).append(row)
        return result

    def primary(self, group: str) -> dict:
        primaries = [r for r in self.groups()[group] if r["主备角色"] == "主源"]
        self.assertEqual(len(primaries), 1, f"{group} 必须恰好一个主源")
        return primaries[0]

    # 1. 亲和优先：G-B 同优先级冲突，选同协议 HTTP（DATA-0005），异协议 GRPC 留恢复清单
    def test_affinity_policy_picks_same_protocol_and_keeps_cross_protocol_standby(self):
        batch, message = failover_service.submit_batch([4], POLICY_AFFINITY)
        self.assertIsNotNone(batch)
        item = batch["逐条结果"][0]
        self.assertEqual(item["处理结果"], RESULT_SUCCESS)
        self.assertEqual(item["新主源条目编号"], "DATA-0005")
        self.assertIn("DATA-0006", item["恢复清单"], "异协议备用源应保留在恢复清单")
        self.assertEqual(self.primary("G-B")["条目编号"], "DATA-0005")
        # 原主源保留可恢复位置
        origin = store.find("datasource", 4)
        self.assertTrue(origin["原主保留位置"])
        self.assertEqual(origin["主备角色"], "备源")

    # 2. 新鲜度优先：G-B 选最新健康源 DATA-0006（GRPC），恢复清单带协议映射校验项
    def test_freshness_policy_picks_latest_health_with_protocol_mapping_check(self):
        batch, _ = failover_service.submit_batch([4], POLICY_FRESHNESS)
        item = batch["逐条结果"][0]
        self.assertEqual(item["处理结果"], RESULT_SUCCESS)
        self.assertEqual(item["新主源条目编号"], "DATA-0006")
        self.assertIn("GRPC", item["冲突裁决"]["协议映射校验项"])
        target = store.find("datasource", 6)
        self.assertIn("协议映射校验", target["协议映射校验"])
        self.assertEqual(self.primary("G-B")["条目编号"], "DATA-0006")

    # 3. G-A 同优先级同协议：两策略结果都稳定（都选新鲜度高的 DATA-0003）
    def test_same_protocol_tie_both_policies_continue(self):
        for policy in (POLICY_AFFINITY, POLICY_FRESHNESS):
            store.reset()
            batch, _ = failover_service.submit_batch([1], policy)
            self.assertEqual(batch["逐条结果"][0]["处理结果"], RESULT_SUCCESS)
            self.assertEqual(self.primary("G-A")["条目编号"], "DATA-0003")

    # 4. 重复执行幂等：重复提交同组不产生第二个主源
    def test_repeated_submission_is_idempotent_single_primary(self):
        batch1, _ = failover_service.submit_batch([1], POLICY_FRESHNESS)
        self.assertEqual(batch1["逐条结果"][0]["处理结果"], RESULT_SUCCESS)
        batch2, _ = failover_service.submit_batch([2], POLICY_FRESHNESS)
        self.assertEqual(batch2["逐条结果"][0]["处理结果"], RESULT_SKIPPED)
        self.assertEqual(self.primary("G-A")["条目编号"], "DATA-0003")
        pending = [q for q in failover_service.list_queue(QUEUE_PENDING) if q["数据源组"] == "G-A"]
        self.assertEqual(len(pending), 1, "同组待处理队列项只能有一条")

    # 5. 已移除源不能作为回退目标：G-C 仍可切到 DATA-0008；直接提交已移除入口失败
    def test_removed_source_cannot_be_fallback_or_entry(self):
        batch, _ = failover_service.submit_batch([7], POLICY_AFFINITY)
        self.assertEqual(batch["逐条结果"][0]["新主源条目编号"], "DATA-0008")
        bad, _ = failover_service.submit_batch([9], POLICY_AFFINITY)
        self.assertEqual(bad["逐条结果"][0]["处理结果"], RESULT_FAILED)
        self.assertIn("已移除", bad["逐条结果"][0]["结果说明"])
        self.assertEqual(self.primary("G-C")["条目编号"], "DATA-0008")

    # 6. 队列应用：代理路由看到唯一主源和备用恢复位置；重复应用幂等
    def test_proxy_applies_queue_and_route_shows_primary_and_recovery(self):
        failover_service.submit_batch([4], POLICY_AFFINITY)
        queue_item = failover_service.list_queue(QUEUE_PENDING)[0]
        _, msg1 = self.proxy.apply_failover(queue_item["队列id"])
        self.assertIn("DATA-0005", msg1)
        _, msg2 = self.proxy.apply_failover(queue_item["队列id"])
        self.assertIn("幂等", msg2)

        routes = self.proxy.list_routes()
        gb = next(r for r in routes if r["数据源组"] == "G-B")
        self.assertEqual(gb["唯一主源条目编号"], "DATA-0005")
        self.assertIn("DATA-0004", gb["可恢复原主源位置"])
        # 代理配置行同步回写
        cfg = next(r for r in store.rows("proxy") if r["数据源组"] == "G-B")
        self.assertEqual(cfg["主源条目编号"], "DATA-0005")
        self.assertTrue(cfg["备用恢复位置"])

    # 7. 原主源恢复回到保留位置，配额按恢复后的主源重算
    def test_restore_returns_origin_to_reserved_position(self):
        failover_service.submit_batch([4], POLICY_AFFINITY)
        self.proxy.apply_all_pending()
        result, message = self.proxy.restore_primary("G-B")
        self.assertIsNotNone(result)
        self.assertEqual(self.primary("G-B")["条目编号"], "DATA-0004")
        origin = store.find("datasource", 4)
        self.assertEqual(origin["当前日配额"], 2000, "主源恢复后配额按基数全量重算")
        standby = store.find("datasource", 5)
        self.assertEqual(standby["当前日配额"], 600)
        route = next(r for r in self.proxy.list_routes() if r["数据源组"] == "G-B")
        self.assertEqual(route["唯一主源条目编号"], "DATA-0004")
        self.assertEqual(route["可恢复原主源位置"], "")

    # 8. 配额按新主源重算且重复执行稳定
    def test_quota_recalculated_deterministically(self):
        failover_service.submit_batch([1], POLICY_FRESHNESS)
        primary_row = self.primary("G-A")
        self.assertEqual(primary_row["当前日配额"], 1000)
        for member in self.groups()["G-A"]:
            if member["主备角色"] == "备源":
                self.assertEqual(member["当前日配额"], 300)
        # 再跑一次重算，配额不变
        before = {r["条目编号"]: r["当前日配额"] for r in self.groups()["G-A"]}
        failover_service.recompute("测试重算")
        after = {r["条目编号"]: r["当前日配额"] for r in self.groups()["G-A"]}
        self.assertEqual(before, after)

    # 9. 一次提交多条：逐条成功/失败结果 + 整组重试只继续未切换条目
    def test_multi_entry_batch_per_item_results_and_group_retry(self):
        # 条目 1 成功、9 失败（已移除）、99 不存在失败
        batch, _ = failover_service.submit_batch([1, 9, 99], POLICY_FRESHNESS)
        self.assertEqual(batch["成功数"], 1)
        self.assertEqual(batch["失败数"], 2)
        self.assertEqual(batch["批次状态"], "部分成功")
        results = {item["条目id"]: item["处理结果"] for item in batch["逐条结果"]}
        self.assertEqual(results[1], RESULT_SUCCESS)
        self.assertEqual(results[9], RESULT_FAILED)
        self.assertEqual(results[99], RESULT_FAILED)

        # 让条目 9 恢复为可接入后整组重试：只继续失败条目，已成功的 G-A 不重复切换
        store.find("datasource", 9)["条目状态"] = "已接入"
        store.find("datasource", 9)["status"] = "已接入"
        retried, message = failover_service.retry_batch(batch["批次号"])
        self.assertIsNotNone(retried)
        retry_items = [i for i in retried["逐条结果"] if i["条目id"] == 9]
        self.assertTrue(retry_items)
        self.assertEqual(retry_items[-1]["处理结果"], RESULT_SUCCESS)
        self.assertEqual(self.primary("G-A")["条目编号"], "DATA-0003")
        self.assertEqual(self.primary("G-C")["条目编号"], "DATA-0009")

    # 10. 切换后新主源被移除：队列项标记失效，不更新路由，不产生第二个主源
    def test_queue_item_goes_dead_when_target_removed(self):
        failover_service.submit_batch([7], POLICY_AFFINITY)
        queue_item = failover_service.list_queue(QUEUE_PENDING)[0]
        store.find("datasource", 8)["条目状态"] = "已移除"
        queue_row, message = self.proxy.apply_failover(queue_item["队列id"])
        self.assertIsNone(queue_row)
        self.assertIn("失效", message)
        dead = failover_service.list_queue()[0]
        self.assertEqual(dead["队列状态"], "已失效")
        # 数据源池内 G-C 主源角色仍只在 DATA-0008 身上（未应用路由）
        self.assertEqual(len([r for r in self.groups()["G-C"] if r["主备角色"] == "主源"]), 1)

    # 11. 复核字段：切换后可复核，复核接口落状态
    def test_review_fields_are_verifiable(self):
        batch, _ = failover_service.submit_batch([4], POLICY_FRESHNESS)
        item = batch["逐条结果"][0]
        self.assertTrue(item["切换前快照"])
        self.assertTrue(item["切换后快照"])
        self.assertEqual(item["复核状态"], "待复核")
        self.datasource.review_entries([6])
        self.assertEqual(store.find("datasource", 6)["复核状态"], "已复核")
        overview = {g["数据源组"]: g for g in self.datasource.group_overview()}
        self.assertTrue(overview["G-B"]["主源唯一"])
        self.assertEqual(overview["G-B"]["可恢复原主源"], "DATA-0004")

    # 12. 其他业务入口（频率控制动作）触发重算，幂等不重复造队列
    def test_other_business_entry_triggers_idempotent_recompute(self):
        failover_service.submit_batch([1], POLICY_FRESHNESS)
        first = failover_service.recompute("测试")
        second = failover_service.recompute("测试")
        self.assertEqual(second["新增队列项"], [])
        # ratelimit 动作同样触发重算且不报错
        _, message = self.ratelimit.run_action(1, "启用规则")
        self.assertIn("重算", message)

    # 13. 批次号冲突被拒绝，避免重复批次覆盖结果
    def test_duplicate_batch_no_rejected(self):
        batch, _ = failover_service.submit_batch([1], POLICY_FRESHNESS, batch_no="FO-X1")
        self.assertEqual(batch["批次号"], "FO-X1")
        again, message = failover_service.submit_batch([2], POLICY_FRESHNESS, batch_no="FO-X1")
        self.assertIsNone(again)
        self.assertIn("已存在", message)

    # 14. 全量应用后所有组都有唯一主源路由
    def test_apply_all_and_cross_group_invariants(self):
        failover_service.submit_batch([1, 4, 7], POLICY_AFFINITY)
        summary = self.proxy.apply_all_pending()
        self.assertEqual(summary["应用条数"], 3)
        for group in ("G-A", "G-B", "G-C"):
            self.primary(group)
        applied = [q for q in failover_service.list_queue() if q["队列状态"] == QUEUE_APPLIED]
        self.assertEqual(len(applied), 3)

    # 15. 数据源池移除源动作触发重算，结果可复核且主源唯一性不受影响
    def test_datasource_removal_triggers_recompute(self):
        entry, message = self.datasource.run_action(8, "移除源")
        self.assertIsNotNone(entry)
        self.assertIn("重算", message)
        # G-C 主源仍是 DATA-0007，移除的备源没有改变主备结构
        self.assertEqual(self.primary("G-C")["条目编号"], "DATA-0007")

    # 16. 当前主源被移除时重算把相关队列标记失效，代理不会切到已移除源
    def test_recompute_marks_group_dead_when_primary_removed(self):
        failover_service.submit_batch([7], POLICY_AFFINITY)
        queue_item = failover_service.list_queue(QUEUE_PENDING)[0]
        store.find("datasource", 8)["条目状态"] = "已移除"
        store.find("datasource", 8)["status"] = "已移除"
        result = failover_service.recompute("主源移除场景")
        self.assertIn("G-C", result["标记失效"])
        dead = next(q for q in failover_service.list_queue() if q["队列id"] == queue_item["队列id"])
        self.assertEqual(dead["队列状态"], "已失效")

    # 17. 批次逐条快照可复核：切换前后角色、协议、配额字段都被冻结
    def test_snapshot_freezes_reviewable_fields(self):
        batch, _ = failover_service.submit_batch([4], POLICY_FRESHNESS)
        item = batch["逐条结果"][0]
        before_origin = item["切换前快照"]["原主源"]
        after_target = item["切换后快照"]["目标源"]
        self.assertEqual(before_origin["主备角色"], "主源")
        self.assertEqual(after_target["主备角色"], "主源")
        self.assertEqual(before_origin["接入协议"], "HTTP")
        self.assertEqual(after_target["接入协议"], "GRPC")
        self.assertIn("条目编号", before_origin)
        self.assertIn("当前日配额", after_target)


if __name__ == "__main__":
    unittest.main()
