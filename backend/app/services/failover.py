"""数据源批量故障切业务规则。

数据流：采集数据源池（datasource 条目）→ 批量故障切换批次/逐条结果（failover_batches）
→ 代理模块待处理队列（failover_queue）→ 采集代理路由（proxy_routes + proxy 配置）。

设计要点（对应故障切反复执行结果不稳定的问题）：
1. 同一数据源组始终只维护一个可采集主源，切换以组为单位做唯一性校验，重复提交/重复
   入队/重复应用都是幂等的，不会产生第二个主源。
2. 每个批次逐条给出成功/失败/跳过结果，并冻结切换前后快照，字段可复核；整组重试只
   继续失败与未切换条目，已切换条目直接跳过。
3. 同优先级备源健康采样新旧冲突时支持两种策略，两种都能继续处理：
   - 原数据源亲和优先：优先同协议源，异协议备源留在恢复清单；
   - 健康采样新鲜度优先：选最新健康源，异协议切换在恢复清单追加协议映射校验项。
4. 已移除（含已暂停）源不能作为回退目标；切换完成后按新主源重算配额；原主源进入
   备用恢复位置，恢复时回到保留位置。
"""
from __future__ import annotations

from typing import Any

from app.store import store

DATASOURCE_MODULE = "datasource"
BATCH_TABLE = "failover_batches"
QUEUE_TABLE = "failover_queue"
ROUTE_TABLE = "proxy_routes"

# 冲突处理策略
POLICY_AFFINITY = "原数据源亲和优先"
POLICY_FRESHNESS = "健康采样新鲜度优先"
POLICIES = (POLICY_AFFINITY, POLICY_FRESHNESS)

ROLE_PRIMARY = "主源"
ROLE_STANDBY = "备源"

# 可参与回退的条目状态（已移除/已暂停/草稿都不能作为回退目标）
FALLBACK_STATUSES = ("已接入",)

# 配额按主备角色对配额基数重算，规则固定保证重复执行结果稳定
QUOTA_RATIO = {ROLE_PRIMARY: 1.0, ROLE_STANDBY: 0.3}

REVIEW_PENDING = "待复核"
REVIEW_DONE = "已复核"

RESULT_SUCCESS = "成功"
RESULT_FAILED = "失败"
RESULT_SKIPPED = "跳过"

BATCH_DONE = "已完成"
BATCH_PARTIAL = "部分成功"
BATCH_FAILED = "全部失败"

QUEUE_PENDING = "待处理"
QUEUE_APPLIED = "已应用"
QUEUE_DEAD = "已失效"


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class FailoverService:
    """批量故障切换、待处理队列与重算逻辑都收敛在本服务。"""

    # ------------------------------------------------------------------ 读取
    def list_batches(self) -> list[dict[str, Any]]:
        return list(store.rows(BATCH_TABLE))

    def get_batch(self, batch_no: str) -> dict[str, Any] | None:
        for batch in store.rows(BATCH_TABLE):
            if batch.get("批次号") == batch_no:
                return batch
        return None

    def list_queue(self, status: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(QUEUE_TABLE)
        if status:
            rows = [row for row in rows if row.get("队列状态") == status]
        return rows

    def list_routes(self) -> list[dict[str, Any]]:
        return list(store.rows(ROUTE_TABLE))

    def group_members(self, group: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(DATASOURCE_MODULE)
            if str(row.get("数据源组") or "") == group
        ]

    def current_primary(self, group: str) -> dict[str, Any] | None:
        for row in self.group_members(group):
            if row.get("主备角色") == ROLE_PRIMARY:
                return row
        return None

    def _fallback_candidates(self, group: str) -> list[dict[str, Any]]:
        """同组可回退备源：状态可采集且当前不是主源；已移除源天然被排除。"""
        return [
            row for row in self.group_members(group)
            if row.get("主备角色") != ROLE_PRIMARY
            and row.get("条目状态") in FALLBACK_STATUSES
        ]

    # ------------------------------------------------------------ 冲突选源
    def pick_target(
        self,
        group: str,
        policy: str,
        origin_protocol: str,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        """按策略从同组备源里选切换目标，返回 (目标条目, 决策说明)。

        两种策略在同优先级、健康采样新旧冲突时都能给出确定结果：
        亲和优先先按协议亲和排序；新鲜度优先先按健康新鲜度排序。两者都用
        （优先级, 健康新鲜度分, 健康采样时间, id）做稳定兜底，保证可重复。
        """
        candidates = self._fallback_candidates(group)
        detail: dict[str, Any] = {
            "策略": policy,
            "候选条目编号": [row["条目编号"] for row in candidates],
            "冲突": False,
            "冲突说明": "",
            "协议映射校验项": "",
        }
        if not candidates:
            detail["冲突说明"] = "同组没有可回退的健康备源（已移除/已暂停源不能作为回退目标）"
            return None, detail

        ranked = sorted(
            candidates,
            key=lambda row: (
                _as_int(row.get("优先级"), 9999),
                -_as_int(row.get("健康新鲜度分")),
                str(row.get("健康采样时间") or ""),
                _as_int(row.get("id")),
            ),
        )
        top_priority = _as_int(ranked[0].get("优先级"), 9999)
        tied = [row for row in ranked if _as_int(row.get("优先级")) == top_priority]
        if len(tied) > 1:
            detail["冲突"] = True
            detail["冲突说明"] = (
                f"备源 {('、'.join(row['条目编号'] for row in tied))} 优先级相同（{top_priority}），"
                "健康采样新旧不同，按提交策略裁决"
            )

        if policy == POLICY_AFFINITY:
            same_protocol = [row for row in tied if row.get("接入协议") == origin_protocol]
            pool = same_protocol or ranked
            target = sorted(
                pool,
                key=lambda row: (
                    0 if row.get("接入协议") == origin_protocol else 1,
                    _as_int(row.get("优先级"), 9999),
                    -_as_int(row.get("健康新鲜度分")),
                    str(row.get("健康采样时间") or ""),
                    _as_int(row.get("id")),
                ),
            )[0]
            detail["协议亲和"] = origin_protocol
            detail["异协议备用保留"] = [
                row["条目编号"] for row in ranked if row.get("接入协议") != origin_protocol
            ]
        elif policy == POLICY_FRESHNESS:
            target = sorted(
                tied if tied else ranked,
                key=lambda row: (
                    -_as_int(row.get("健康新鲜度分")),
                    str(row.get("健康采样时间") or ""),
                    _as_int(row.get("优先级"), 9999),
                    _as_int(row.get("id")),
                ),
            )[0]
        else:
            detail["冲突说明"] = f"未知冲突处理策略：{policy}"
            return None, detail

        if target.get("接入协议") != origin_protocol:
            detail["协议映射校验项"] = (
                f"{origin_protocol} → {target.get('接入协议')} 协议映射校验"
            )
        return target, detail

    # ------------------------------------------------------------ 配额重算
    def recalc_quota(self, group: str) -> dict[str, int]:
        """切换完成后配额按新主源重算：主源取整基数，备源按固定比例，结果确定。"""
        changed: dict[str, int] = {}
        for row in self.group_members(group):
            base = _as_int(row.get("配额基数"))
            ratio = QUOTA_RATIO.get(str(row.get("主备角色")), 0.0)
            new_quota = int(base * ratio)
            if _as_int(row.get("当前日配额")) != new_quota:
                row["当前日配额"] = new_quota
                changed[str(row["条目编号"])] = new_quota
        return changed

    # ------------------------------------------------------------ 单条切换
    def _switch_one(
        self,
        entry_id: int,
        policy: str,
        batch_no: str,
        seq: int,
    ) -> dict[str, Any]:
        entry = store.find(DATASOURCE_MODULE, entry_id)
        if entry is None:
            return self._item_result(seq, entry_id, None, batch_no, policy, RESULT_FAILED,
                                     "数据源条目不存在或已归档，不能作为切换入口")
        if entry.get("条目状态") not in FALLBACK_STATUSES:
            return self._item_result(seq, entry_id, entry, batch_no, policy, RESULT_FAILED,
                                     f"{entry.get('条目状态')}源不能作为故障切换入口或回退目标")

        group = str(entry.get("数据源组") or "")
        primary = self.current_primary(group)
        if primary is None:
            return self._item_result(seq, entry_id, entry, batch_no, policy, RESULT_FAILED,
                                     f"数据源组 {group} 缺少主源，无法执行故障切换")

        # 批量提交中断后续跑：本组已经切过（存在带保留位置的原主源，或现主源带批次号），
        # 任何重复条目都幂等跳过，绝不产生第二个主源
        already_switched = bool(primary.get("切换批次号")) or any(
            row.get("原主保留位置") for row in self.group_members(group)
        )
        if already_switched:
            return self._item_result(
                seq, entry_id, entry, batch_no, policy, RESULT_SKIPPED,
                f"数据源组 {group} 已完成故障切换（现主源 {primary['条目编号']}），重复条目跳过")

        if entry.get("主备角色") == ROLE_PRIMARY:
            # 入口是故障主源：按冲突策略从备源里选切换目标
            target, detail = self.pick_target(
                group, policy, str(primary.get("接入协议") or ""))
        else:
            # 入口是健康备源：视为显式指定的切换目标（仍须通过可回退校验）
            target, detail = self.pick_target(
                group, policy, str(primary.get("接入协议") or ""))
            if target is not None and target["id"] != entry["id"]:
                detail["冲突说明"] = (
                    f"指定目标 {entry['条目编号']} 与策略裁决结果 {target['条目编号']} 不一致，"
                    "以提交条目为显式目标"
                )
                target = entry
                if entry.get("接入协议") != primary.get("接入协议"):
                    detail["协议映射校验项"] = (
                        f"{primary.get('接入协议')} → {entry.get('接入协议')} 协议映射校验"
                    )
        if target is None:
            return self._item_result(seq, entry_id, entry, batch_no, policy, RESULT_FAILED,
                                     detail.get("冲突说明") or "没有可用的切换目标")

        before = self._snapshot(primary, target)
        origin_code = str(primary["条目编号"])
        target_code = str(target["条目编号"])

        # 组内唯一性：先把原主源降为备源并写入可恢复的保留位置，再提升目标
        primary["主备角色"] = ROLE_STANDBY
        primary["原主保留位置"] = f"{group}#主源保留位（原主源 {origin_code}）"
        primary["切换批次号"] = batch_no
        target["主备角色"] = ROLE_PRIMARY
        target["原主保留位置"] = ""
        target["切换批次号"] = batch_no
        target["复核状态"] = REVIEW_PENDING

        protocol_check = detail.get("协议映射校验项") or ""
        if protocol_check:
            target["协议映射校验"] = protocol_check
        # 亲和优先时异协议备源保留在恢复清单；新鲜度优先时追加协议映射校验项
        recovery_list = [origin_code]
        if policy == POLICY_AFFINITY:
            recovery_list.extend(detail.get("异协议备用保留") or [])
        else:
            target["协议映射校验"] = protocol_check or "同协议切换，免映射校验"

        quota_changes = self.recalc_quota(group)
        self._assert_single_primary(group)

        after = self._snapshot(primary, target)
        message = f"数据源组 {group} 主源 {origin_code} → {target_code}（{policy}）"
        if detail.get("冲突"):
            message += f"；{detail['冲突说明']}"
        return self._item_result(
            seq, entry_id, entry, batch_no, policy, RESULT_SUCCESS, message,
            group=group, origin_code=origin_code, target_code=target_code,
            detail=detail, recovery_list=recovery_list,
            quota_changes=quota_changes, before=before, after=after,
        )

    def _assert_single_primary(self, group: str) -> None:
        primaries = [
            row for row in self.group_members(group)
            if row.get("主备角色") == ROLE_PRIMARY
        ]
        if len(primaries) != 1:
            raise RuntimeError(f"数据源组 {group} 主源不唯一：{len(primaries)} 个主源")

    @staticmethod
    def _snapshot(primary: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
        keep = ("条目编号", "数据源名称", "接入协议", "优先级", "主备角色",
                "健康采样时间", "健康新鲜度分", "当前日配额", "条目状态")
        return {
            "原主源": {key: primary.get(key) for key in keep},
            "目标源": {key: target.get(key) for key in keep},
        }

    @staticmethod
    def _item_result(
        seq: int,
        entry_id: int,
        entry: dict[str, Any] | None,
        batch_no: str,
        policy: str,
        result: str,
        message: str,
        *,
        group: str = "",
        origin_code: str = "",
        target_code: str = "",
        detail: dict[str, Any] | None = None,
        recovery_list: list[str] | None = None,
        quota_changes: dict[str, int] | None = None,
        before: dict[str, Any] | None = None,
        after: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "序号": seq,
            "条目id": entry_id,
            "条目编号": (entry or {}).get("条目编号", f"ID-{entry_id}"),
            "数据源组": group or str((entry or {}).get("数据源组") or ""),
            "策略": policy,
            "批次号": batch_no,
            "处理结果": result,
            "结果说明": message,
            "原主源条目编号": origin_code,
            "新主源条目编号": target_code,
            "冲突裁决": detail or {},
            "恢复清单": recovery_list or [],
            "配额重算": quota_changes or {},
            "切换前快照": before or {},
            "切换后快照": after or {},
            "复核状态": REVIEW_PENDING if result == RESULT_SUCCESS else "",
        }

    # ------------------------------------------------------------ 批量提交
    def submit_batch(
        self,
        entry_ids: list[int],
        policy: str,
        *,
        batch_no: str | None = None,
        source: str = "数据源池批量故障切",
    ) -> tuple[dict[str, Any] | None, str]:
        if not entry_ids:
            return None, "批量故障切换至少需要一条数据源条目"
        if policy not in POLICIES:
            return None, f"冲突处理策略必须是：{'、'.join(POLICIES)}"

        rows = store.rows(BATCH_TABLE)
        if batch_no is None:
            batch_no = f"FO-B{len(rows) + 1:04d}"
        elif self.get_batch(batch_no) is not None:
            return None, f"批次号 {batch_no} 已存在，重复提交请走整组重试入口"

        items: list[dict[str, Any]] = []
        # 同批次内按组去重：一个组一次只切一次，从入口层面杜绝第二个主源
        seen_groups: set[str] = set()
        for seq, entry_id in enumerate(entry_ids, start=1):
            entry = store.find(DATASOURCE_MODULE, entry_id)
            group = str((entry or {}).get("数据源组") or "")
            if entry is not None and group:
                if group in seen_groups:
                    items.append(self._item_result(
                        seq, entry_id, entry, batch_no, policy, RESULT_SKIPPED,
                        f"数据源组 {group} 在本批次已处理，同组重复条目跳过"))
                    continue
                seen_groups.add(group)
            items.append(self._switch_one(entry_id, policy, batch_no, seq))

        success = [item for item in items if item["处理结果"] == RESULT_SUCCESS]
        failed = [item for item in items if item["处理结果"] == RESULT_FAILED]
        skipped = [item for item in items if item["处理结果"] == RESULT_SKIPPED]
        if failed and success:
            batch_status = BATCH_PARTIAL
        elif failed and not success:
            batch_status = BATCH_FAILED
        else:
            batch_status = BATCH_DONE

        batch = {
            "批次号": batch_no,
            "冲突处理策略": policy,
            "来源": source,
            "批次状态": batch_status,
            "提交条目数": len(entry_ids),
            "成功数": len(success),
            "失败数": len(failed),
            "跳过数": len(skipped),
            "逐条结果": items,
        }
        rows.append(batch)

        # 故障切换结果数据落到代理模块的待处理队列；成功的组才入队，按组幂等
        for item in success:
            self._enqueue(batch_no, item, policy, source)
        return batch, f"批次 {batch_no} 处理完成：成功 {len(success)}、失败 {len(failed)}、跳过 {len(skipped)}"

    def _enqueue(
        self,
        batch_no: str,
        item: dict[str, Any],
        policy: str,
        source: str,
    ) -> dict[str, Any]:
        queue = store.rows(QUEUE_TABLE)
        group = item["数据源组"]
        # 同组已有待处理项时不重复入队，保证中断续跑不会产生重复主源切换
        for row in queue:
            if row.get("数据源组") == group and row.get("队列状态") == QUEUE_PENDING:
                row["来源批次"] = batch_no
                return row
        entry = {
            "队列id": len(queue) + 1,
            "数据源组": group,
            "来源批次": batch_no,
            "冲突处理策略": policy,
            "原主源条目编号": item["原主源条目编号"],
            "新主源条目编号": item["新主源条目编号"],
            "恢复清单": list(item["恢复清单"]),
            "协议映射校验项": item["冲突裁决"].get("协议映射校验项", ""),
            "配额重算": dict(item["配额重算"]),
            "来源": source,
            "队列状态": QUEUE_PENDING,
        }
        queue.append(entry)
        return entry

    # ------------------------------------------------------------ 重试
    def retry_batch(self, batch_no: str) -> tuple[dict[str, Any] | None, str]:
        """整组重试：只继续失败与未切换（跳过）条目，已成功切换的条目不再动。"""
        batch = self.get_batch(batch_no)
        if batch is None:
            return None, f"批次 {batch_no} 不存在"
        policy = str(batch["冲突处理策略"])
        retry_ids = [
            item["条目id"] for item in batch["逐条结果"]
            if item["处理结果"] in (RESULT_FAILED, RESULT_SKIPPED)
        ]
        if not retry_ids:
            return batch, f"批次 {batch_no} 没有可重试的失败条目"

        new_items: list[dict[str, Any]] = []
        seen_groups: set[str] = set()
        seq = len(batch["逐条结果"])
        for entry_id in retry_ids:
            entry = store.find(DATASOURCE_MODULE, entry_id)
            group = str((entry or {}).get("数据源组") or "")
            if entry is not None and group and group in seen_groups:
                continue
            if group:
                seen_groups.add(group)
            seq += 1
            new_items.append(self._switch_one(entry_id, policy, batch_no, seq))

        batch["逐条结果"].extend(new_items)
        results = batch["逐条结果"]
        batch["成功数"] = sum(1 for item in results if item["处理结果"] == RESULT_SUCCESS)
        batch["失败数"] = sum(1 for item in results if item["处理结果"] == RESULT_FAILED)
        batch["跳过数"] = sum(1 for item in results if item["处理结果"] == RESULT_SKIPPED)
        if batch["失败数"] and batch["成功数"]:
            batch["批次状态"] = BATCH_PARTIAL
        elif batch["失败数"] and not batch["成功数"]:
            batch["批次状态"] = BATCH_FAILED
        else:
            batch["批次状态"] = BATCH_DONE

        for item in new_items:
            if item["处理结果"] == RESULT_SUCCESS:
                self._enqueue(batch_no, item, policy, str(batch["来源"]))
        return batch, (
            f"批次 {batch_no} 整组重试完成：新增成功 "
            f"{sum(1 for item in new_items if item['处理结果'] == RESULT_SUCCESS)} 条"
        )

    # ------------------------------------------------------------ 代理应用
    def apply_queue_item(self, queue_id: int) -> tuple[dict[str, Any] | None, str]:
        """代理路由读取故障切换结果：更新唯一主源与备用恢复位置（幂等）。"""
        queue_row = next(
            (row for row in store.rows(QUEUE_TABLE) if _as_int(row.get("队列id")) == queue_id),
            None,
        )
        if queue_row is None:
            return None, f"待处理队列项 {queue_id} 不存在"
        if queue_row["队列状态"] == QUEUE_APPLIED:
            return queue_row, f"队列项 {queue_id} 已应用，重复应用保持幂等"
        if queue_row["队列状态"] == QUEUE_DEAD:
            return None, f"队列项 {queue_id} 已失效（目标源已移除），不能应用"

        group = str(queue_row["数据源组"])
        target_code = str(queue_row["新主源条目编号"])
        target = next(
            (row for row in self.group_members(group) if row.get("条目编号") == target_code),
            None,
        )
        if target is None or target.get("条目状态") == "已移除":
            queue_row["队列状态"] = QUEUE_DEAD
            return None, f"新主源 {target_code} 已移除，队列项 {queue_id} 标记失效，不更新路由"

        # 再次强制唯一主源：以数据源池当前角色为准，路由只读取结果
        self._assert_single_primary(group)
        route = self._upsert_route(queue_row, target)
        queue_row["队列状态"] = QUEUE_APPLIED
        return queue_row, f"数据源组 {group} 代理路由已更新，唯一主源 {target_code}"

    def _upsert_route(self, queue_row: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
        group = str(queue_row["数据源组"])
        routes = store.rows(ROUTE_TABLE)
        route = next((row for row in routes if row.get("数据源组") == group), None)
        if route is None:
            route = {"路由id": len(routes) + 1, "数据源组": group}
            routes.append(route)
        origin_code = str(queue_row["原主源条目编号"])
        route.update({
            "主源条目编号": target["条目编号"],
            "主源接入协议": target.get("接入协议"),
            "备用恢复位置": f"{group}#主源保留位（原主源 {origin_code}）",
            "恢复清单": list(queue_row.get("恢复清单") or []),
            "协议映射校验项": queue_row.get("协议映射校验项") or "",
            "来源批次": queue_row.get("来源批次"),
            "路由版本": _as_int(route.get("路由版本")) + 1,
        })
        return route

    # ------------------------------------------------------------ 原主源恢复
    def restore_primary(self, group: str) -> tuple[dict[str, Any] | None, str]:
        """原主源恢复时回到保留位置：当前主源退回备源，原主源重新成为唯一主源。"""
        members = self.group_members(group)
        if not members:
            return None, f"数据源组 {group} 不存在"
        current = self.current_primary(group)
        origin = next(
            (row for row in members
             if row.get("原主保留位置") and row.get("主备角色") == ROLE_STANDBY),
            None,
        )
        if origin is None:
            return None, f"数据源组 {group} 没有处于保留位置的原主源，无需恢复"
        if current is not None:
            current["主备角色"] = ROLE_STANDBY
            current["原主保留位置"] = ""
        origin["主备角色"] = ROLE_PRIMARY
        origin["原主保留位置"] = ""
        origin["复核状态"] = REVIEW_PENDING
        # 清掉整组的切换痕迹，恢复后允许重新发起故障切换并保证唯一主源
        for row in members:
            row["切换批次号"] = ""
            if row["id"] != origin["id"]:
                row["协议映射校验"] = ""
        quota_changes = self.recalc_quota(group)
        self._assert_single_primary(group)

        routes = store.rows(ROUTE_TABLE)
        route = next((row for row in routes if row.get("数据源组") == group), None)
        if route is not None:
            route["主源条目编号"] = origin["条目编号"]
            route["主源接入协议"] = origin.get("接入协议")
            route["备用恢复位置"] = ""
            route["恢复清单"] = []
            route["协议映射校验项"] = ""
            route["路由版本"] = _as_int(route.get("路由版本")) + 1
        return {"数据源组": group, "恢复主源": origin["条目编号"], "配额重算": quota_changes}, \
            f"数据源组 {group} 原主源 {origin['条目编号']} 已回到保留位置"

    # ------------------------------------------------------------ 统一重算
    def recompute(self, source: str = "其他业务入口重算") -> dict[str, Any]:
        """别的业务入口跟着重算故障切队列与结果数据。

        只做幂等对账：已应用/待处理的组不重复造数据；没有队列记录且当前主源健康的
        组补一条同步队列项；已移除的新主源对应队列标记失效。重复执行结果稳定。
        """
        groups = sorted({
            str(row.get("数据源组") or "")
            for row in store.rows(DATASOURCE_MODULE)
            if row.get("数据源组")
        })
        synced: list[str] = []
        dead: list[str] = []
        skipped: list[str] = []
        queue = store.rows(QUEUE_TABLE)
        for group in groups:
            primary = self.current_primary(group)
            if primary is None or primary.get("条目状态") == "已移除":
                # 主源缺失或已移除：未应用的队列项一律失效，代理不会切到已移除源
                for row in queue:
                    if row.get("数据源组") == group and row.get("队列状态") != QUEUE_APPLIED:
                        row["队列状态"] = QUEUE_DEAD
                dead.append(group)
                continue
            pending = next(
                (row for row in queue
                 if row.get("数据源组") == group and row.get("队列状态") == QUEUE_PENDING),
                None,
            )
            if pending is not None:
                skipped.append(group)
                continue
            # 已有路由且主源一致 → 无需再造队列；否则补一条同步项供代理路由复核
            route = next(
                (row for row in store.rows(ROUTE_TABLE) if row.get("数据源组") == group),
                None,
            )
            if route is not None and route.get("主源条目编号") == primary.get("条目编号"):
                skipped.append(group)
                continue
            queue.append({
                "队列id": len(queue) + 1,
                "数据源组": group,
                "来源批次": f"RECOMPUTE-{group}",
                "冲突处理策略": "",
                "原主源条目编号": "",
                "新主源条目编号": primary["条目编号"],
                "恢复清单": [
                    row["条目编号"] for row in self.group_members(group)
                    if row.get("原主保留位置")
                ],
                "协议映射校验项": primary.get("协议映射校验", ""),
                "配额重算": {},
                "来源": source,
                "队列状态": QUEUE_PENDING,
            })
            synced.append(group)
        return {"重算来源": source, "新增队列项": synced, "标记失效": dead, "已跳过": skipped}


failover_service = FailoverService()
