# 分布式采集调度引擎

面向大规模数据采集团队的分布式采集调度引擎。覆盖采集节点集群管理、任务分片调度、结果聚合归并、节点健康监控、采集流量均衡、断点续采恢复与全局采集看板的完整分布式采集链路。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集节点集群 | `cluster` | 集群节点 | 节点编号、节点地址、节点角色 |
| 任务分片调度 | `shard` | 分片任务 | 分片编号、采集计划、分片范围 |
| 结果聚合归并 | `merge` | 聚合任务 | 任务编号、采集计划、分片总数 |
| 节点状态巡检 | `nodecheck` | 巡检记录 | 记录编号、节点编号、CPU使用率 |
| 采集流量均衡 | `balance` | 均衡策略 | 策略编号、负载算法、权重因子 |
| 断点续采恢复 | `resume` | 断点记录 | 断点编号、采集计划、断点位置 |
| 全局采集看板 | `dashboard` | 看板面板 | 面板编号、面板名称、显示指标 |
| 采集配置模板 | `crawlconf` | 配置模板 | 模板编号、模板名称、目标类型 |
| 采集数据源池 | `datasource` | 数据源条目 | 条目编号、数据源名称、源类型、数据源组、主备角色 |
| 数据源批量故障切 | `failover` | 故障切换批次/队列 | 批次号、冲突处理策略、逐条结果、待处理队列 |
| 采集日志解析 | `logparse` | 日志模板 | 模板编号、日志类型、解析规则 |
| 采集频率控制 | `ratelimit` | 频率规则 | 规则编号、目标站点、请求间隔 |
| 采集代理路由 | `proxy` | 代理配置 | 配置编号、代理地址、代理协议 |
| RPA执行记录 | `robot` | 执行记录 | 记录编号、流程模板、执行节点 |
| 采集结果校验 | `verify` | 校验规则 | 规则编号、采集计划、校验类型 |
| 采集操作审计 | `audit` | 审计记录 | 记录编号、操作类型、操作对象 |
| 采集存储分层 | `storage` | 存储策略 | 策略编号、存储层级、文件格式 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 数据源批量故障切

采集数据源池到采集代理路由之间的故障切换链路，全部规则收敛在
`backend/app/services/failover.py`，保证反复执行结果稳定：

```text
采集数据源池（datasource 条目，按数据源组维护主备角色）
  → 批量故障切换批次 failover_batches（逐条成功/失败/跳过 + 切换前后快照，字段可复核）
  → 代理模块待处理队列 failover_queue（按组幂等，同组只有一个待处理项）
  → 采集代理路由 proxy_routes（唯一主源 + 备用恢复位置 + 协议映射校验项）
```

关键约束：

- 同一数据源组始终只有一个可采集主源；切换批次重复执行、队列重复应用都是幂等的，
  不会产生第二个主源（`_assert_single_primary` 兜底）。
- 同优先级备源健康采样新旧冲突时两种策略都能继续：
  - 原数据源亲和优先：切到同协议源，异协议备源保留在恢复清单；
  - 健康采样新鲜度优先：切到最新健康源，恢复清单追加协议映射校验项。
- 已移除/已暂停源不能作为回退目标；切换完成后配额按新主源重算（主源 100%、备源 30%
  固定比例），原主源进入保留位置，恢复时回到原位并再次重算配额。
- 批量提交中断后整组重试只继续失败与未切换条目，已切换条目直接跳过。
- 数据源池移除源、采集频率控制动作等别的业务入口会触发 `recompute` 幂等对账，
  重算故障切队列与故障切换结果数据，不重复造数据。

后端测试：`cd backend && PYTHONPATH=.venv:. python3 -m unittest discover -s tests -v`

领域：数据工程
子方向：数据采集、爬虫与RPA
