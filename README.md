# 巡检批量改派死锁预检服务

值班员在开放批量改派前，用本服务预先判断 2~3 批有序工单改派是否存在
**循环等锁（死锁）** 风险；发现风险时在本地测试库按检测到的交错顺序
实跑，以 PostgreSQL 返回 **SQLSTATE 40P01（deadlock_detected）**
作为复现成功的唯一判据。

## 1. 业务与锁模型

- 每一批是一笔 PostgreSQL 事务，按 `order_ids` 的**提交顺序**逐单
  `UPDATE work_orders SET assignee=... WHERE order_id=...`；
- `UPDATE` 对命中行加排他行锁，**行锁持有到事务 COMMIT/ROLLBACK**；
- 两批共享工单时，后执行到该单的批次阻塞等待先持锁批次；
- 死锁判据：**存在一条全局执行交错，使环上各批同时阻塞在各自等待的
  工单上**。本服务对每个候选等待环都在联合状态空间里搜索这样一条
  "见证交错"，搜得到才判有死锁风险，并把该交错用于实跑复现。

### 为什么不能用工单的跨批绝对步号判方向

旧实现比较同一工单在两批中的绝对位置（第几步），位置早就必然先持锁。
该结论只在两批同节拍起步时成立；一批可以先空跑自己的独有单，导致两类
错误：

- **误报**：两批共享工单的**相对顺序一致**（都是先 W1 后 W2），只是
  各自夹了不同数量的独有单，把 W1、W2 的绝对位置差推成相反方向，旧逻辑
  拼出纸面环。实际任意调度都只会单向等待，不可能死锁。
- **漏报**：共享工单相对顺序真实反转（一批先 W1 后 W2、另一批先 W2
  后 W1），但独有单把其中一张单推到两批的同号位置，旧规则"同位置不出
  边"，环直接丢失。

新分析器因此：① 共享工单上两个等待方向都列为**候选边**，不做单边裁剪；
② 枚举 2/3 节点候选环后做局部序检查（持锁单严格早于等待单）；③ 在环上
批次的联合状态空间做 DFS，验证存在见证交错，纸面环一律丢弃。

同号（同一工单在同一批内重复填写）在入参层直接拒绝：同一事务重复
更新同一行不会自锁，这类"同号并发试验"互不阻塞，对死锁判断无意义。

## 2. API

服务启动后访问 `http://localhost:8000/docs`（Swagger UI，内置三组
可一键填充的试跑样例）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| GET | `/orders` | 查看业务工单及负责人 |
| POST | `/orders/seed` | 幂等写入测试工单（负责人 `init`） |
| POST | `/precheck` | 死锁预检 + 可选实跑复现 |

### 请求约束

- 批次数 2~3；每批 1~6 单；
- 同一批内工单号不可重复；`batch_id` 不可重复；
- `run_replay=false` 时只做静态等待图分析，不连试跑通道。

### 响应要点（`/precheck`）

- `has_deadlock` / `cycles`：全部**可实现**的有向环（每个环都已验证存在
  见证交错），环上标注 `cycle_batches`（首尾相接的批次）、`cycle_orders`
  （每条等待边争用的工单）与 `witness`（见证交错：按此序列逐单拿锁后，
  环上各批将同时等锁）；
- `wait_edges`：完整候选等待边，含等待方/持有方批次、工单号、双方在
  本批中的加锁步号，以及 `on_feasible_cycle`（是否参与了可实现环）；
  未参与任何可行环的候选边仅作排查参考，不构成死锁结论；
- `replay.steps`：实跑全局操作序列，`acquire`（拿锁）/`block`
  （进入锁等待，含持有方 pid）/`abort`（被死锁检测中止，含 SQLSTATE）；
- `replay.batch_waits`：各批**进入环上等待瞬间**阻塞在哪张单、持有方
  是哪批、阻塞前已执行到第几步（`last_acquired_step` 在阻塞瞬间定格，
  不受幸存批次后续推进影响）；
- `replay.cleanup_ok`：收尾核查（无未结束事务、试跑 schema 已销毁）。

## 3. 实跑复现原理

1. 在独立 schema `deadlock_trial` 建与业务表同构的临时表并写入工单；
2. 不在环上的旁观批次先各自开事务按提交顺序跑完并 COMMIT（它们不持有
   环上争用单，不会干扰环）；
3. 环上批次每批一个独立连接 + 工作线程，协调器按静态分析产出的
   `witness` 全局序号**逐单下发** UPDATE，复现见证前缀；前缀完成后同时
   放行各批的等待工单——各批即使在**不同步号**进入等待也允许，协调器只
   轮询 `pg_stat_activity` 的 `wait_event_type='Lock'`，不做同节拍假设；
4. 收口阶段用**环形排空（round-robin drain）**：绝不同时阻塞等待所有
   批次（那会造成协调器自锁，等到 `lock_timeout` 才解开），而是让已就绪
   的批次继续拿后续锁并 COMMIT，幸存者提交后环自然解开；
5. 每个 UPDATE 由工作线程执行，协调线程轮询 `pg_stat_activity`
   的 `wait_event_type='Lock'`，真实记录 `block` 现场；行锁持有方通过
   行头 `xmax` 与 `pg_stat_activity.backend_xid` 关联得到；
6. PostgreSQL 死锁检测器（本环境 `deadlock_timeout=200ms`）中止
   牺牲者事务，捕获 `psycopg.errors.DeadlockDetected`，校验
   `sqlstate == '40P01'`；
7. 收尾：查 `pg_stat_activity` 确认无未结束事务 → `DROP SCHEMA
   deadlock_trial CASCADE` 物理销毁所有试跑数据与锁 → 复查 schema
   已不存在。业务库 `public.work_orders` 全程不被试跑触碰。

若未收到 40P01，响应中如实区分原因：先触发 `lock_timeout`
（SQLSTATE 55P03）、全部提交未形成环、或执行异常。

## 4. Docker Compose 启动

```bash
docker compose up --build
# API:   http://localhost:8000/docs
# PG:    localhost:5432 (postgres/postgres, 库名 patrol)
```

Compose 中 PostgreSQL 已配置 `deadlock_timeout=200ms`、
`lock_timeout=10s`、`log_lock_waits=on`。

### Swagger 试跑样例

打开 `/docs` → `POST /precheck` → `Try it out`，请求体示例下拉框
内置五组样例：AB-BA 死锁、三节点循环、同顺序安全批次，以及两组回归
样例——**误报回归**（夹独有单但共享单相对顺序一致，应判无死锁）与
**漏报回归**（独有单推偏位置、共享单相对顺序反转，应判死锁并复现
40P01）。直接 `Execute` 即可。等价 curl：

```bash
# 漏报回归：旧实现报 0 环，新实现报环并实跑 40P01
curl -X POST http://localhost:8000/precheck \
  -H 'Content-Type: application/json' \
  -d '{
    "batches": [
      {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "alice"},
      {"batch_id": "B2", "order_ids": ["W9", "W2", "W1"], "assignee": "bob"}
    ],
    "run_replay": true
  }'

# 误报回归：旧实现报 1 个假环，新实现判 has_deadlock=false
curl -X POST http://localhost:8000/precheck \
  -H 'Content-Type: application/json' \
  -d '{
    "batches": [
      {"batch_id": "B1", "order_ids": ["W1", "W9", "W9b", "W2"], "assignee": "alice"},
      {"batch_id": "B2", "order_ids": ["W8", "W1", "W2"], "assignee": "bob"}
    ],
    "run_replay": true
  }'
```

## 5. 本地无 Docker 时的自测

仓库提供 `scripts/local_pg.sh`：在用户目录下载并解压 PostgreSQL 15
官方二进制（免 root），初始化临时实例并监听 5433，再启动 API。

```bash
./scripts/local_pg.sh start          # 初始化/启动本地 PG + API
DATABASE_URL=postgresql://postgres@127.0.0.1:5433/patrol \
  python -m tests.selftest           # 11 个场景 59 项端到端自测
./scripts/local_pg.sh stop           # 停库
```

## 6. 目录结构

```
app/
  config.py     环境变量配置与业务常量（2~3 批、每批 ≤6 单）
  models.py     Pydantic 入参/出参模型与校验、Swagger 样例
  analyzer.py   候选等待边 + 2/3 节点环枚举 + 见证交错可达性搜索
  db.py         连接管理、试跑 schema 隔离、按见证交错的实跑执行器、收尾核查
  main.py       FastAPI 路由
tests/selftest.py  端到端自测（11 个场景 59 项断言，含误报/漏报回归与见证穷举校验）
docker-compose.yml / Dockerfile
```

## 7. 已知边界

- 静态分析基于"行锁按工单提交顺序获取"这一前提；若未来改派 SQL 改为
  批量 `UPDATE ... WHERE order_id = ANY(...)`，获取顺序由规划器
  决定，需要同步调整 analyzer；
- 见证交错搜索的状态空间为各批进度的笛卡尔积，当前 2~3 批、每批 ≤6 单
  （上界 7³=343 态）开销可忽略；放宽批次/单数上限时需重新评估；
- 试跑在独立 schema 的镜像表上进行，不验证触发器/外键等业务侧
  副作用，只验证锁行为本身。
