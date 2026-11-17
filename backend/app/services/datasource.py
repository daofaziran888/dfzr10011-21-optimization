"""采集数据源池业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "datasource"
REQUIRED_FIELDS = ["条目编号", "数据源名称", "源类型"]
STATUS_ORDER = ["草稿", "已接入", "已暂停", "已移除"]
ACTION_RULES = {"接入源": "草稿", "暂停源": "已接入", "移除源": "已暂停"}
NEGATIVE_ACTIONS = []


class DatasourceService:
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
            rows = [row for row in rows if keyword in str(row.get("条目编号", ""))]
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
            return None, f"数据源条目 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集数据源池可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        if action == "移除源":
            # 移除源直接影响可回退目标，跟着重算代理模块的故障切队列与结果数据（幂等）
            from app.services.failover import failover_service

            recompute = failover_service.recompute("数据源池移除源触发重算")
            return entry, (
                f"数据源条目已{action}；故障切队列已重算"
                f"（新增 {len(recompute['新增队列项'])} 项、失效 {len(recompute['标记失效'])} 项）"
            )
        return entry, f"数据源条目已{action}"

    def review_entries(self, entry_ids: list[int]) -> tuple[list[dict[str, Any]], list[str]]:
        """故障切换后的字段复核：把条目标记为已复核，便于清单逐条核对。"""
        reviewed: list[dict[str, Any]] = []
        missing: list[str] = []
        for entry_id in entry_ids:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                missing.append(str(entry_id))
                continue
            entry["复核状态"] = "已复核"
            reviewed.append(entry)
        return reviewed, missing

    def group_overview(self) -> list[dict[str, Any]]:
        """按数据源组汇总主备唯一性，供列表页和复核流程直接核对。"""
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in store.rows(MODULE):
            groups.setdefault(str(row.get("数据源组") or "未分组"), []).append(row)
        view: list[dict[str, Any]] = []
        for group, members in sorted(groups.items()):
            primaries = [row for row in members if row.get("主备角色") == "主源"]
            recoverable = next(
                (row for row in members if row.get("原主保留位置")),
                None,
            )
            view.append({
                "数据源组": group,
                "条目数": len(members),
                "主源条目编号": (primaries[0].get("条目编号") if len(primaries) == 1 else ""),
                "主源数量": len(primaries),
                "主源唯一": len(primaries) == 1,
                "可恢复原主源": (recoverable or {}).get("条目编号", ""),
                "保留位置": (recoverable or {}).get("原主保留位置", ""),
            })
        return view
