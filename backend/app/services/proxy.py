"""采集代理路由业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.services.failover import QUEUE_PENDING, failover_service
from app.store import store

MODULE = "proxy"
ROUTE_TABLE = "proxy_routes"
QUEUE_TABLE = "failover_queue"
REQUIRED_FIELDS = ["配置编号", "代理地址", "代理协议"]
STATUS_ORDER = ["草稿", "已启用", "已降级", "已关闭"]
ACTION_RULES = {"启用代理": "草稿", "降级代理": "已启用", "关闭代理": "已降级"}
NEGATIVE_ACTIONS = []


class ProxyService:
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
            rows = [row for row in rows if keyword in str(row.get("配置编号", ""))]
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
            return None, f"代理配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集代理路由可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"代理配置已{action}"

    # ------------------------------------------------- 数据源批量故障切对接
    def list_failover_queue(self, status: str | None = QUEUE_PENDING) -> list[dict[str, Any]]:
        """代理模块的数据源批量故障切待处理队列。"""
        return failover_service.list_queue(status)

    def apply_failover(self, queue_id: int) -> tuple[dict[str, Any] | None, str]:
        """代理路由读取故障切换结果，更新唯一主源与备用恢复位置。"""
        queue_row, message = failover_service.apply_queue_item(queue_id)
        if queue_row is None:
            return None, message
        self._sync_proxy_config(queue_row)
        return queue_row, message

    def apply_all_pending(self) -> dict[str, Any]:
        """一次性应用全部待处理队列项，逐项给出成功/失败结果。"""
        results: list[dict[str, Any]] = []
        for row in failover_service.list_queue(QUEUE_PENDING):
            queue_id = int(row["队列id"])
            _, message = self.apply_failover(queue_id)
            results.append({"队列id": queue_id, "数据源组": row["数据源组"], "结果说明": message})
        return {"应用条数": len(results), "逐条结果": results}

    def list_routes(self) -> list[dict[str, Any]]:
        """代理路由清单：每个数据源组一行，能看到唯一主源和可恢复的原主源位置。"""
        routes = failover_service.list_routes()
        # 以代理配置为主表，未生成路由的组也展示当前主源，保证清单完整可复核
        route_by_group = {str(row.get("数据源组")): row for row in routes}
        view: list[dict[str, Any]] = []
        for cfg in store.rows(MODULE):
            group = str(cfg.get("数据源组") or "")
            route = route_by_group.get(group)
            members = failover_service.group_members(group)
            primary = failover_service.current_primary(group)
            origin = next(
                (row for row in members if row.get("原主保留位置")),
                None,
            )
            view.append({
                "数据源组": group,
                "代理配置编号": cfg.get("配置编号"),
                "代理地址": cfg.get("代理地址"),
                "代理协议": cfg.get("代理协议"),
                "唯一主源条目编号": (primary or {}).get("条目编号", ""),
                "主源接入协议": (primary or {}).get("接入协议", ""),
                "可恢复原主源位置": (origin or {}).get("原主保留位置", ""),
                "原主源条目编号": (origin or {}).get("条目编号", ""),
                "备用恢复位置": (route or {}).get("备用恢复位置", ""),
                "恢复清单": (route or {}).get("恢复清单", []),
                "协议映射校验项": (route or {}).get("协议映射校验项", ""),
                "路由版本": (route or {}).get("路由版本", 0),
            })
        return view

    def restore_primary(self, group: str) -> tuple[dict[str, Any] | None, str]:
        """原主源恢复：回到保留位置后同步代理配置上的主源编号。"""
        result, message = failover_service.restore_primary(group)
        if result is None:
            return None, message
        self._sync_proxy_config({"数据源组": group,
                                 "新主源条目编号": result["恢复主源"],
                                 "队列状态": "恢复"})
        return result, message

    def recompute_failover(self, source: str) -> dict[str, Any]:
        """供其他业务入口调用：重算故障切队列与故障切换结果数据。"""
        return failover_service.recompute(source)

    def _sync_proxy_config(self, queue_row: dict[str, Any]) -> None:
        """把路由结果回写到代理配置行，列表页直接可见唯一主源与恢复位置。"""
        group = str(queue_row.get("数据源组") or "")
        target_code = str(queue_row.get("新主源条目编号") or "")
        route = next(
            (row for row in store.rows(ROUTE_TABLE) if row.get("数据源组") == group),
            None,
        )
        for cfg in store.rows(MODULE):
            if str(cfg.get("数据源组") or "") != group:
                continue
            if target_code:
                cfg["主源条目编号"] = target_code
            if route is not None:
                cfg["备用恢复位置"] = route.get("备用恢复位置", "")
                cfg["协议映射校验"] = route.get("协议映射校验项", "")
                cfg["路由版本"] = route.get("路由版本", 0)
