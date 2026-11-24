"""PostgreSQL 访问层与死锁实跑执行器。

试跑隔离
========
- 所有试跑在独立 schema ``deadlock_trial`` 下的临时表进行，与业务表
  ``public.work_orders`` 物理隔离；
- 每轮试跑 DROP SCHEMA ... CASCADE 后重建，上一轮的行、锁、改派结果
  全部物理清除；
- 每批一个独立连接/线程，事务只可能 COMMIT 或 ROLLBACK，结束后连接
  关闭；试跑收尾再用 ``pg_stat_activity`` 核查无未结束事务、用全表
  扫描核查无改派残留。

实跑调度（按静态分析给出的见证交错）
====================================
旧实现用全局 Barrier 让所有批次同节拍起步，这隐含"各批在同一步号等锁"
的假设；独有单会让环上批次在**不同步数**才进入等待，Barrier 节拍会把
真实交错打乱（抢跑拿齐锁、或永远到不了环上等待点）。

新执行器完全按分析器为所选环产出的 ``witness`` 调度：
1. 不在环上的旁观批次，先各自开事务按提交顺序跑完并 COMMIT，它们不
   持有任何环上争用单（否则该批也在环里），不会干扰环；
2. 环上批次各自开连接/事务，协调器按 witness 的全局序号逐单下发
   UPDATE（acquire 前缀）；
3. 前缀完成后，协调器同时放行各批的"等待工单" UPDATE——各批即使在
   不同步号进入等待也没关系，协调器轮询 ``pg_stat_activity`` 直到环上
   各批都出现 ``wait_event_type='Lock'``，真实记录 block 现场；
4. 等待 PostgreSQL 死锁检测器（``deadlock_timeout=200ms``）中止牺牲者，
   捕获 ``psycopg.errors.DeadlockDetected`` 且校验 SQLSTATE=40P01；
   幸存者解除阻塞后按各自提交顺序跑完剩余单并 COMMIT；
5. 未收到 40P01 时如实区分：先触发 lock_timeout（55P03）、全部提交
   未形成环、或执行异常。
"""

from __future__ import annotations

import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutTimeout
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import psycopg
from psycopg import sql

from .config import settings
from .models import (
    BatchInput,
    BatchWait,
    CycleReport,
    ReplayReport,
    ReplayStep,
)

DEADLOCK_SQLSTATE = "40P01"
LOCK_TIMEOUT_SQLSTATE = "55P03"


def connect(autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(settings.database_url, autocommit=autocommit)


# --------------------------------------------------------------------------- #
# 业务表（演示用，真实环境由迁移脚本维护）
# --------------------------------------------------------------------------- #
def ensure_business_schema() -> None:
    with connect(autocommit=True) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.work_orders (
                order_id   text PRIMARY KEY,
                assignee   text NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )


def seed_orders(order_ids: List[str]) -> None:
    """确保工单行存在（幂等），负责人初始为 'init'。"""
    if not order_ids:
        return
    ensure_business_schema()
    with connect(autocommit=True) as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.work_orders(order_id, assignee)
            VALUES (%s, 'init')
            ON CONFLICT (order_id) DO NOTHING
            """,
            [(oid,) for oid in sorted(set(order_ids))],
        )


def list_orders() -> List[Tuple[str, str]]:
    ensure_business_schema()
    with connect(autocommit=True) as conn:
        return conn.execute(
            "SELECT order_id, assignee FROM public.work_orders ORDER BY order_id"
        ).fetchall()


# --------------------------------------------------------------------------- #
# 试跑 schema 管理
# --------------------------------------------------------------------------- #
def reset_trial_schema(conn: psycopg.Connection) -> None:
    conn.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
            sql.Identifier(settings.trial_schema)
        )
    )
    conn.execute(
        sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(settings.trial_schema))
    )
    conn.execute(
        sql.SQL(
            """
            CREATE TABLE {schema}.work_orders (
                order_id   text PRIMARY KEY,
                assignee   text NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
            """
        ).format(schema=sql.Identifier(settings.trial_schema))
    )


def _trial_table() -> sql.Composed:
    return sql.SQL("{}.work_orders").format(sql.Identifier(settings.trial_schema))


# --------------------------------------------------------------------------- #
# 实跑执行器
# --------------------------------------------------------------------------- #
@dataclass
class _BatchRun:
    batch: BatchInput
    pid: Optional[int] = None
    error_sqlstate: Optional[str] = None
    error_message: Optional[str] = None
    committed: bool = False
    blocked_order: Optional[str] = None
    blocked_holder: Optional[str] = None
    # 进入环上等待瞬间已完成的步数（阻塞前进度，不受后续幸存推进影响）
    blocked_last_step: Optional[int] = None
    last_step: int = 0
    in_cycle: bool = False
    steps: List[ReplayStep] = field(default_factory=list)


@dataclass
class _Task:
    """协调器下发给工作线程的一单个 UPDATE 任务。"""

    step: int
    order_id: str
    expect_block: bool  # True：环上等待工单，预期进入锁等待


def _holder_of(admin_conn: psycopg.Connection, order_id: str) -> Optional[str]:
    """查当前持有指定工单行锁的试跑后端 pid。

    PostgreSQL 的行锁不体现在 pg_locks（tuple 锁只在阻塞等待时出现），
    持有者事务 id 记录在行头 xmax。通过 pg_stat_activity.backend_xid
    关联即可拿到持有行锁的后端。返回 pid 字符串，查不到返回 None。
    """
    row = admin_conn.execute(
        sql.SQL(
            """
            SELECT a.pid
            FROM {tbl} w
            JOIN pg_stat_activity a
              ON a.backend_xid = w.xmax
            WHERE w.order_id = %s
              AND a.datname = current_database()
              AND a.pid <> pg_backend_pid()
            LIMIT 1
            """
        ).format(tbl=_trial_table()),
        (order_id,),
    ).fetchone()
    return str(row[0]) if row else None


def _update_order(conn: psycopg.Connection, order_id: str, assignee: str) -> None:
    conn.execute(
        sql.SQL(
            "UPDATE {tbl} SET assignee = %s, updated_at = now() WHERE order_id = %s"
        ).format(tbl=_trial_table()),
        (assignee, order_id),
    )


def _is_waiting(admin_conn: psycopg.Connection, pid: int) -> Optional[str]:
    """指定后端当前是否在等锁；返回等待事件名，否则 None。"""
    row = admin_conn.execute(
        """
        SELECT wait_event_type, wait_event
        FROM pg_stat_activity
        WHERE pid = %s AND wait_event_type = 'Lock'
        """,
        (pid,),
    ).fetchone()
    return f"{row[0]}.{row[1]}" if row else None


def replay_deadlock(
    batches: List[BatchInput], cycle: CycleReport
) -> ReplayReport:
    """按所选环的见证交错构造调度并实跑，验证是否收到 SQLSTATE 40P01。"""
    admin = connect(autocommit=True)
    admin.execute(f"SET lock_timeout = {int(settings.lock_timeout_ms)}")
    reset_trial_schema(admin)
    try:
        return _replay_inner(batches, cycle, admin)
    finally:
        # 任何异常路径都不允许遗留试跑 schema / 未结束事务
        try:
            admin.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(settings.trial_schema)
                )
            )
        finally:
            admin.close()


class _SeqCounter:
    """全局操作序号计数器，旁观批与环上批共用，保证序号全局唯一。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._v = 0

    def next(self) -> int:
        with self._lock:
            self._v += 1
            return self._v


def _run_standalone_batch(batch: BatchInput, seq: _SeqCounter) -> _BatchRun:
    """旁观批次（不在环上）：独立事务按提交顺序跑完即提交。"""
    run = _BatchRun(batch=batch)
    # autocommit=True 后显式 BEGIN，事务边界完全可控（见 worker 同名说明）
    conn = connect(autocommit=True)
    try:
        conn.execute("BEGIN")
        # SET LOCAL 仅在当前事务内生效，且必须在 BEGIN 之后执行。
        conn.execute(f"SET LOCAL lock_timeout = {int(settings.lock_timeout_ms)}")
        for step, oid in enumerate(batch.order_ids, start=1):
            _update_order(conn, oid, batch.assignee)
            run.last_step = step
            run.steps.append(
                ReplayStep(
                    seq=seq.next(),
                    batch_id=batch.batch_id,
                    step=step,
                    order_id=oid,
                    action="acquire",
                    note="旁观批次，环外先行执行",
                )
            )
        conn.execute("COMMIT")
        run.committed = True
    except Exception:  # noqa: BLE001 - 试跑线程内兜底，保证连接关闭
        try:
            conn.rollback()
        except Exception:  # pragma: no cover
            pass
        raise
    finally:
        conn.close()
    return run


def _replay_inner(
    batches: List[BatchInput],
    cycle: CycleReport,
    admin: psycopg.Connection,
) -> ReplayReport:
    all_orders = sorted({oid for b in batches for oid in b.order_ids})
    with admin.cursor() as cur:
        cur.executemany(
            sql.SQL(
                "INSERT INTO {tbl}(order_id, assignee) VALUES (%s, 'init')"
            ).format(tbl=_trial_table()),
            [(oid,) for oid in all_orders],
        )

    cyc_batches = list(cycle.cycle_batches)
    cyc_orders = list(cycle.cycle_orders)
    n = len(cyc_batches)
    cycle_set = set(cyc_batches)

    # 每个环上批次的阻塞工单 = 它等待的下一个批次先拿到的那张单
    block_order: Dict[str, str] = dict(zip(cyc_batches, cyc_orders))
    holder_by_order: Dict[str, str] = {
        cyc_orders[k]: cyc_batches[(k + 1) % n] for k in range(n)
    }

    runs: Dict[str, _BatchRun] = {
        b.batch_id: _BatchRun(batch=b, in_cycle=b.batch_id in cycle_set)
        for b in batches
    }
    seq = _SeqCounter()

    worker_errors: List[str] = []

    # ---- 阶段 0：旁观批次先独立跑完（不持环上争用单，不影响环）-------- #
    standalone = [b for b in batches if b.batch_id not in cycle_set]
    for batch in standalone:
        try:
            runs[batch.batch_id] = _run_standalone_batch(batch, seq)
        except Exception as exc:  # noqa: BLE001
            worker_errors.append(
                f"旁观批 {batch.batch_id} 执行失败：{type(exc).__name__}: {exc}"
            )

    # ---- 阶段 1：环上批次工作线程：队列驱动，一次只执行一个 UPDATE ---- #
    task_queues: Dict[str, "queue.Queue[Optional[_Task]]"] = {}
    done_events: Dict[str, threading.Event] = {}
    results: Dict[str, Optional[Exception]] = {}

    def worker(batch: BatchInput) -> None:
        run = runs[batch.batch_id]
        # autocommit=True 保证 pg_backend_pid 查询不会隐式开事务，
        # 事务边界完全由下面的显式 BEGIN/COMMIT/ROLLBACK 控制。
        conn = connect(autocommit=True)
        run.pid = conn.execute("SELECT pg_backend_pid()").fetchone()[0]
        q = task_queues[batch.batch_id]
        try:
            conn.execute("BEGIN")
            # SET LOCAL 仅在当前事务内生效，必须在 BEGIN 之后执行。
            conn.execute(f"SET LOCAL lock_timeout = {int(settings.lock_timeout_ms)}")
            while True:
                task = q.get()
                if task is None:
                    # 协调器通知收尾：提交事务
                    conn.execute("COMMIT")
                    run.committed = True
                    done_events[batch.batch_id].set()
                    return
                try:
                    _update_order(conn, task.order_id, batch.assignee)
                    results[batch.batch_id] = None
                except Exception as exc:  # noqa: BLE001
                    results[batch.batch_id] = exc
                    # 出错（40P01/55P03/其他）后事务必须回滚，线程退出
                    try:
                        conn.rollback()
                    except Exception:  # pragma: no cover
                        pass
                    done_events[batch.batch_id].set()
                    return
                finally:
                    q.task_done()
                done_events[batch.batch_id].set()
        except Exception as exc:  # noqa: BLE001 - 兜底，保证连接关闭
            results[batch.batch_id] = exc
            done_events[batch.batch_id].set()
        finally:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                pass

    for bid in cyc_batches:
        task_queues[bid] = queue.Queue()
        done_events[bid] = threading.Event()

    deadline = time.monotonic() + settings.trial_timeout_s + 5

    with ThreadPoolExecutor(max_workers=len(cyc_batches)) as pool:
        futures = {
            bid: pool.submit(worker, next(b for b in batches if b.batch_id == bid))
            for bid in cyc_batches
        }

        def dispatch(bid: str, task: _Task) -> Optional[Exception]:
            """下发单个 UPDATE 并轮询记录 block 现场，返回执行异常（若有）。"""
            run = runs[bid]
            done_events[bid].clear()
            results[bid] = None
            task_queues[bid].put(task)
            block_seen = False
            while not done_events[bid].wait(timeout=0.02):
                if time.monotonic() > deadline:
                    raise TimeoutError(f"批次 {bid} 第 {task.step} 步执行超时")
                if not block_seen:
                    wait = _is_waiting(admin, run.pid)
                    if wait:
                        block_seen = True
                        run.blocked_order = task.order_id
                        run.blocked_holder = _holder_of(admin, task.order_id)
                        run.blocked_last_step = run.last_step
                        run.steps.append(
                            ReplayStep(
                                seq=seq.next(),
                                batch_id=bid,
                                step=task.step,
                                order_id=task.order_id,
                                action="block",
                                note=(
                                    f"进入锁等待（{wait}），持有方连接 "
                                    f"pid={run.blocked_holder}"
                                ),
                            )
                        )
            exc = results[bid]
            if exc is not None:
                return exc
            # 正常返回：行锁到手
            run.last_step = task.step
            run.steps.append(
                ReplayStep(
                    seq=seq.next(),
                    batch_id=bid,
                    step=task.step,
                    order_id=task.order_id,
                    action="acquire",
                    note="曾阻塞等待后获得锁" if block_seen else None,
                )
            )
            return None

        # 每批当前在飞任务（已下发、未收口），None 表示空闲
        in_flight: Dict[str, Optional[_Task]] = {bid: None for bid in cyc_batches}
        block_recorded: Dict[str, bool] = {bid: False for bid in cyc_batches}

        def record_block_if_waiting(bid: str, task: _Task) -> None:
            """轮询一次：若该批在飞任务已进入锁等待，补一条 block 记录。"""
            if block_recorded[bid]:
                return
            run = runs[bid]
            wait = _is_waiting(admin, run.pid)
            if wait:
                block_recorded[bid] = True
                run.blocked_order = task.order_id
                run.blocked_holder = _holder_of(admin, task.order_id)
                run.blocked_last_step = run.last_step
                run.steps.append(
                    ReplayStep(
                        seq=seq.next(),
                        batch_id=bid,
                        step=task.step,
                        order_id=task.order_id,
                        action="block",
                        note=(
                            f"进入锁等待（{wait}），持有方连接 "
                            f"pid={run.blocked_holder}"
                        ),
                    )
                )

        def try_collect(bid: str) -> str:
            """非阻塞收口在飞任务：busy / acquired / failed。"""
            task = in_flight[bid]
            if task is None:
                return "busy"
            record_block_if_waiting(bid, task)
            if not done_events[bid].is_set():
                return "busy"
            exc = results[bid]
            in_flight[bid] = None
            block_recorded[bid] = False
            if exc is not None:
                _record_failure(runs[bid], task, exc, seq)
                if not isinstance(
                    exc,
                    (psycopg.errors.DeadlockDetected,
                     psycopg.errors.LockNotAvailable),
                ):
                    worker_errors.append(
                        f"批次 {bid} 执行异常：{type(exc).__name__}: {exc}"
                    )
                return "failed"
            run = runs[bid]
            run.last_step = task.step
            run.steps.append(
                ReplayStep(
                    seq=seq.next(),
                    batch_id=bid,
                    step=task.step,
                    order_id=task.order_id,
                    action="acquire",
                    note="曾阻塞等待后获得锁"
                    if any(s.action == "block" and s.step == task.step
                           for s in run.steps)
                    else None,
                )
            )
            return "acquired"

        try:
            # 阶段 2：按见证交错的全局序号逐单下发前缀（预期全部拿锁成功，
            # 见证前缀的每张单在该调度点都无人持锁，不会阻塞）
            for wstep in cycle.witness:
                exc = dispatch(
                    wstep.batch_id,
                    _Task(step=wstep.step, order_id=wstep.order_id,
                          expect_block=False),
                )
                if exc is not None:
                    worker_errors.append(
                        f"见证前缀执行异常 {wstep.batch_id}/{wstep.order_id}："
                        f"{type(exc).__name__}: {exc}"
                    )
                    break

            # 阶段 3：同时放行环上各批的等待工单 UPDATE。各批即使在不同
            # 步号进入等待也允许——只轮询等待状态，不做同节拍假设。
            if not worker_errors:
                for bid in cyc_batches:
                    run = runs[bid]
                    wait_oid = block_order[bid]
                    wait_step = run.batch.order_ids.index(wait_oid) + 1
                    task = _Task(step=wait_step, order_id=wait_oid,
                                 expect_block=True)
                    done_events[bid].clear()
                    results[bid] = None
                    in_flight[bid] = task
                    block_recorded[bid] = False
                    task_queues[bid].put(task)

                # 等到环上各批都进入锁等待（同时等锁的死锁现场），
                # 或已有批次被死锁检测/lock_timeout 中止。
                while True:
                    if time.monotonic() > deadline:
                        worker_errors.append("等待环上各批进入锁等待超时")
                        break
                    for bid in cyc_batches:
                        if in_flight[bid] is not None:
                            record_block_if_waiting(bid, in_flight[bid])
                    if all(block_recorded.values()) or any(
                        done_events[b].is_set() and results[b] is not None
                        for b in cyc_batches
                    ):
                        break
                    time.sleep(0.02)

            # 阶段 4~5：环形排空。绝不同时阻塞等待所有批次——死锁环被
            # 打破前所有在飞任务都会挂住，必须让已就绪的批次继续推进
            # （拿后续锁、COMMIT），幸存者提交后环自然解开。
            while not worker_errors and any(
                in_flight[b] is not None
                or (runs[b].error_sqlstate is None
                    and runs[b].last_step < len(runs[b].batch.order_ids))
                for b in cyc_batches
            ):
                if time.monotonic() > deadline:
                    worker_errors.append("试跑环形排空整体超时")
                    break
                progress = False
                for bid in cyc_batches:
                    run = runs[bid]
                    if run.error_sqlstate is not None:
                        continue  # 已回滚退出，不再下发
                    status = try_collect(bid)
                    if status == "busy":
                        continue  # 还在等锁，先放行其他批次，杜绝协调器自锁
                    progress = True
                    if status == "failed":
                        continue
                    # acquired：按提交顺序继续下发下一单；跑完则通知提交
                    batch = run.batch
                    next_step = run.last_step + 1
                    if next_step <= len(batch.order_ids):
                        oid = batch.order_ids[next_step - 1]
                        done_events[bid].clear()
                        results[bid] = None
                        in_flight[bid] = _Task(
                            step=next_step, order_id=oid, expect_block=False
                        )
                        block_recorded[bid] = False
                        task_queues[bid].put(in_flight[bid])
                    else:
                        done_events[bid].clear()
                        task_queues[bid].put(None)
                        if not done_events[bid].wait(
                            timeout=max(0.1, deadline - time.monotonic())
                        ):
                            worker_errors.append(f"批次 {bid} COMMIT 超时")
                        else:
                            run.committed = True
                if not progress:
                    # 本圈全部在等锁：死锁检测器（200ms）会很快中止牺牲者，
                    # 最坏由 lock_timeout（10s）兜底，短暂让出即可。
                    time.sleep(0.02)
        finally:
            # 确保所有工作线程退出（异常路径下投放哨兵；已退出的队列无害）
            for bid in cyc_batches:
                try:
                    task_queues[bid].put(None)
                except Exception:  # pragma: no cover
                    pass
            for bid, fut in futures.items():
                try:
                    fut.result(timeout=max(0.1, deadline - time.monotonic()))
                except FutTimeout:
                    worker_errors.append(f"工作线程 {bid} 整体超时")
                except Exception as exc:  # noqa: BLE001
                    worker_errors.append(f"{bid}: {type(exc).__name__}: {exc}")

    victims = [
        bid for bid, r in runs.items() if r.error_sqlstate == DEADLOCK_SQLSTATE
    ]
    lock_timeouts = [
        bid for bid, r in runs.items()
        if r.error_sqlstate == LOCK_TIMEOUT_SQLSTATE
    ]

    all_steps: List[ReplayStep] = sorted(
        [s for r in runs.values() for s in r.steps],
        key=lambda s: s.seq,
    )

    batch_waits: List[BatchWait] = []
    for bid in cyc_batches:
        r = runs[bid]
        waits_order = block_order[bid]
        batch_waits.append(
            BatchWait(
                batch_id=bid,
                waits_for_order=waits_order,
                waits_for_holder=holder_by_order[waits_order],
                last_acquired_step=(
                    r.blocked_last_step
                    if r.blocked_last_step is not None
                    else r.last_step
                ),
            )
        )

    reproduced = len(victims) > 0
    if reproduced:
        detail = (
            f"PostgreSQL deadlock_detector 中止了批次 {sorted(victims)}，"
            f"收到 SQLSTATE {DEADLOCK_SQLSTATE}（deadlock_detected），死锁复现成功。"
        )
        sqlstate = DEADLOCK_SQLSTATE
    elif lock_timeouts:
        detail = (
            f"未收到 40P01：环上批次 {sorted(lock_timeouts)} 先触发了 lock_timeout"
            f"（SQLSTATE 55P03，等待 {settings.lock_timeout_ms}ms）。"
            "说明调度形成了真实锁等待但死锁检测未在超时前打破环；"
            "可调大 LOCK_TIMEOUT_MS 或调小服务端 deadlock_timeout 后重试。"
        )
        sqlstate = LOCK_TIMEOUT_SQLSTATE
    elif worker_errors:
        detail = "试跑执行异常，未能确认死锁：" + "；".join(worker_errors)
        sqlstate = None
    else:
        detail = (
            "未收到 40P01：所有批次均正常 COMMIT，调度未形成环。"
            "可能原因：环上工单在某批中并非排他更新、或数据库未走行锁路径。"
        )
        sqlstate = None

    cleanup_ok, cleanup_note = _verify_and_cleanup(admin, all_orders)
    if not cleanup_ok:
        detail += f" 收尾核查失败：{cleanup_note}"

    return ReplayReport(
        reproduced=reproduced,
        sqlstate=sqlstate,
        victim_batch=sorted(victims)[0] if victims else None,
        detail=detail,
        steps=all_steps,
        batch_waits=batch_waits,
        cleanup_ok=cleanup_ok,
    )


def _record_failure(
    run: _BatchRun,
    task: Optional[_Task],
    exc: Exception,
    seq: "_SeqCounter",
) -> None:
    """登记工作线程返回的死锁/锁超时/其他异常，并落 abort 步骤。"""
    run.error_sqlstate = getattr(exc, "sqlstate", None)
    run.error_message = str(exc).strip()
    if task is not None:
        run.blocked_order = task.order_id
        run.last_step = task.step - 1
        run.steps.append(
            ReplayStep(
                seq=seq.next(),
                batch_id=run.batch.batch_id,
                step=task.step,
                order_id=task.order_id,
                action="abort",
                note=(
                    f"事务被中止，SQLSTATE={run.error_sqlstate}，"
                    f"信息：{run.error_message}"
                ),
            )
        )


def _verify_and_cleanup(
    admin: psycopg.Connection,
    order_ids: List[str],
) -> Tuple[bool, str]:
    """核查无残留事务，并物理销毁试跑 schema（含全部改派结果与锁）。"""
    # 1. 无未结束的试跑事务（排除当前 admin 连接与本查询自身）
    rows = admin.execute(
        """
        SELECT pid, state, xact_start
        FROM pg_stat_activity
        WHERE datname = current_database()
          AND pid <> pg_backend_pid()
          AND xact_start IS NOT NULL
          AND state IN ('idle in transaction', 'active')
        """
    ).fetchall()
    if rows:
        return False, f"仍有未结束事务：{rows}"

    # 2. 统计 DROP 前试跑表内的改派行数（死锁复现时存活方会 COMMIT，
    # 存在已更新行是预期现象；它们随下一步 schema DROP 物理消失）。
    dirty = admin.execute(
        sql.SQL(
            "SELECT count(*) FROM {tbl} WHERE assignee <> 'init'"
        ).format(tbl=_trial_table())
    ).fetchone()[0]

    # 3. 物理销毁试跑 schema（行、锁、表全部消失）
    admin.execute(
        sql.SQL("DROP SCHEMA {} CASCADE").format(
            sql.Identifier(settings.trial_schema)
        )
    )
    left = admin.execute(
        "SELECT count(*) FROM pg_namespace WHERE nspname = %s",
        (settings.trial_schema,),
    ).fetchone()[0]
    if left:
        return False, "试跑 schema 删除后仍存在"
    return True, f"ok（DROP 前试跑表有 {dirty} 行已改派，已随 schema 清除）"
