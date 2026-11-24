"""死锁静态分析：由各批加锁顺序枚举候选等待环，并验证环的全局可实现性。

锁模型
======
每批是一笔 PostgreSQL 事务，按 ``order_ids`` 原顺序逐单 ``UPDATE``。
``UPDATE`` 对命中行加排他行锁，锁一直持有到事务 COMMIT/ROLLBACK。

为什么不能按"跨批绝对步号"判等待方向
====================================
旧实现比较同一工单在两批中的绝对位置（第几步），位置早就必然先持锁。
该结论只在两批同时起步、节拍一致时成立，而真实调度中一批可以先空跑自己
的独有单：

- 误报：两批共享单的**相对顺序一致**（B1、B2 都是先 W1 后 W2），只是
  各自夹了不同数量的独有单，使 W1、W2 上的绝对位置差方向相反，被拼成
  假环。实际上任意调度下都只会出现单向等待，不可能死锁。
- 漏报：共享单相对顺序真实反转（B1 先 W1 后 W2、B2 先 W2 后 W1），
  但独有单把其中一张单推到两批的同号位置，旧规则"同位置不出边"，环丢失。

正确判据（本模块实现）
======================
1. 对每对共享同一工单 x 的批次 (A, B)，两个等待方向 A->B(x)、B->A(x)
   都是**候选边**——总能让 B 先跑到 x 持锁、A 再到 x 等待，单边总是
   可实现；能否同时实现要看整张环。
2. 枚举批次等待图上的候选简单环（2 节点互锁环、3 节点循环环）。
3. 局部检查：环上每一批，它"持有"的工单在本批顺序中必须严格早于它
   "等待"的工单（持锁先于等锁，否则该批无法进入环上等待状态）。
4. 全局检查（关键）：在环上批次的状态空间里做 DFS，按各批的提交顺序
   逐单推进，寻找一条**见证交错**使所有环上批次同时阻塞在各自的等待
   工单上。找不到这样的交错，候选环只是纸面环（误报来源），直接丢弃；
   找到则报告，并把该交错（acquire 序列）交给实跑执行器复现。

即：报告里的每个环都附带一条能让环上各批同时等锁的交错；存在这样的环
才判定有死锁风险。

同号（同一工单在同一批内重复）在入参层已禁止；同一事务对同一行重复加锁
不会自锁，因此不产生等待边。
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from .models import BatchInput, CycleReport, WaitEdge, WitnessStep


# 候选等待边：waiter 在 order_id 上等待 holder
CandidateEdge = Tuple[str, str, str]
# 候选环：[(waiter, holder, order_id), ...]，首尾相接
CandidateCycle = List[CandidateEdge]


def _positions(batches: List[BatchInput]) -> Dict[str, Dict[str, int]]:
    """batch_id -> (order_id -> 1-based 加锁位置)。"""
    out: Dict[str, Dict[str, int]] = {}
    for b in batches:
        out[b.batch_id] = {oid: i for i, oid in enumerate(b.order_ids, start=1)}
    return out


def build_candidate_edges(batches: List[BatchInput]) -> List[WaitEdge]:
    """枚举全部候选等待边：共享工单上两个方向都列出。

    单边是否"最终成立"取决于全局调度，这里不做方向裁剪；裁剪只会把
    本可同时实现的环误杀掉（漏报）。环的联合可行性统一交给
    :func:`_search_witness` 判定。
    """
    positions = _positions(batches)
    edges: List[WaitEdge] = []
    seen: Set[CandidateEdge] = set()
    for i in range(len(batches)):
        for j in range(i + 1, len(batches)):
            a, b = batches[i], batches[j]
            shared = set(a.order_ids) & set(b.order_ids)
            for oid in sorted(shared):
                for waiter, holder in (
                    (a.batch_id, b.batch_id),
                    (b.batch_id, a.batch_id),
                ):
                    key = (waiter, holder, oid)
                    if key in seen:
                        continue
                    seen.add(key)
                    edges.append(
                        WaitEdge(
                            waiter_batch=waiter,
                            holder_batch=holder,
                            order_id=oid,
                            waiter_step=positions[waiter][oid],
                            holder_step=positions[holder][oid],
                            on_feasible_cycle=False,
                        )
                    )
    edges.sort(key=lambda e: (e.waiter_batch, e.holder_batch, e.order_id))
    return edges


def _enumerate_candidate_cycles(
    batches: List[BatchInput], edges: List[WaitEdge]
) -> List[CandidateCycle]:
    """在批次维度的候选等待图上枚举全部 2/3 节点简单环。

    业务上限 3 批，环长只可能是 2（A->B、B->A）或 3（A->B->C->A）。
    同一环有多个旋转起点，用最小旋转元组去重。
    """
    adjacency: Dict[str, List[Tuple[str, str]]] = {}
    for e in edges:
        adjacency.setdefault(e.waiter_batch, []).append(
            (e.holder_batch, e.order_id)
        )
    for nxt in adjacency.values():
        nxt.sort()

    nodes = [b.batch_id for b in batches]
    positions = _positions(batches)
    # 同一去重键可能对应两个遍历方向（如二环的正/反向），先全部收集，
    # 稍后按局部可行性挑选代表——反向环过不了局部序检查，必须让正向环
    # 胜出，不能用 setdefault 任意保留先枚举到的方向。
    grouped: Dict[Tuple[frozenset, frozenset], List[CandidateCycle]] = {}

    def remember(cycle: CandidateCycle) -> None:
        # 去重键用"批次集合 + 工单标签集合"：同一环不论从哪条边起步、
        # 沿哪个方向枚举都映射到同一键；不同工单对的二环（同批次、
        # 不同争用单）因标签集合不同而被区分，不会像旧"最小旋转元组"
        # 那样被错误合并。存储的 cycle 保留原始边顺序——下游依赖
        # cycle_batches[i] 与 cycle_orders[i] 的位置对齐以及闭环不变量
        # （边 i 的 holder == 边 i+1 的 waiter）。
        key = (
            frozenset(w for w, _, _ in cycle),
            frozenset(x for _, _, x in cycle),
        )
        grouped.setdefault(key, []).append(cycle)

    # 2 节点环：A->B(x) 与 B->A(y)，x、y 必须是两张不同工单
    for a in nodes:
        for b, oid_ab in adjacency.get(a, ()):
            if b <= a:
                continue
            for holder_b, oid_ba in adjacency.get(b, ()):
                if holder_b == a and oid_ba != oid_ab:
                    remember(
                        [
                            (a, b, oid_ab),
                            (b, a, oid_ba),
                        ]
                    )

    # 3 节点环：A->B(x) B->C(y) C->A(z)
    for a in nodes:
        for b, oid_ab in adjacency.get(a, ()):
            if b == a:
                continue
            for c, oid_bc in adjacency.get(b, ()):
                if c in (a, b):
                    continue
                for holder_c, oid_ca in adjacency.get(c, ()):
                    if holder_c == a:
                        remember(
                            [
                                (a, b, oid_ab),
                                (b, c, oid_bc),
                                (c, a, oid_ca),
                            ]
                        )

    # 每个去重键选一个代表：优先局部序可行的方向；都不可行则保留首个
    # （后续 analyze 仍会用局部检查 + 见证搜索把它淘汰）。
    chosen: List[CandidateCycle] = []
    for variants in grouped.values():
        feasible = [c for c in variants if _local_order_ok(c, positions)]
        chosen.append(feasible[0] if feasible else variants[0])

    return sorted(
        chosen,
        key=lambda cyc: (
            [e[0] for e in cyc],
            [e[2] for e in cyc],
        ),
    )


def _local_order_ok(
    cycle: CandidateCycle, positions: Dict[str, Dict[str, int]]
) -> bool:
    """局部检查：环上每批持有的工单必须严格早于它等待的工单。

    边 (w, h, x) 表示 w 等待 x、h 持有 x。一批 h 在环上恰好持有一张单
    （前驱边的标签）、等待一张单（自己出边的标签）；它能进入等待状态
    的前提是持锁动作已完成，即持有单位置 < 等待单位置。
    """
    waits_on: Dict[str, str] = {w: x for w, _, x in cycle}
    holds_on: Dict[str, str] = {h: x for _, h, x in cycle}
    for bid in waits_on:
        pos = positions[bid]
        if pos[holds_on[bid]] >= pos[waits_on[bid]]:
            return False
    return True


def _search_witness(
    batches: List[BatchInput],
    cycle: CandidateCycle,
) -> Optional[List[WitnessStep]]:
    """全局交错可达性搜索。

    在环上批次的联合状态空间中 DFS：每步选一个尚未阻塞的批次，按其
    提交顺序执行下一单 UPDATE（acquire）。

    死锁模型下，环闭合前没有任何事务提交，因此行锁一旦持有就不会释放：
    一个批次若在**非环上等待点**的工单上被另一批挡住，就会永久卡在那里，
    再也到不了自己的目标等待态——这样的调度分支只能剪枝。能走到目标的
    见证，其拿锁前缀必然不含任何跨批争用。

    当批次执行到它在环上等待的工单、且该单正被环上的下一批持有时，进入
    目标阻塞态。所有环上批次都进入目标阻塞即找到见证交错。

    状态空间上界：每批进度 0..len(order_ids)，3 批 × 每批 ≤6 单，
    备忘录去重后开销极小。
    """
    order_lists: Dict[str, List[str]] = {
        b.batch_id: list(b.order_ids) for b in batches
    }
    cycle_batches = sorted({w for w, _, _ in cycle})
    goal_wait: Dict[str, str] = {w: x for w, _, x in cycle}
    # 环上每张等待工单的持有方（来自环边）
    holder_of: Dict[str, str] = {x: h for _, h, x in cycle}

    memo: Set[Tuple[Tuple[str, int, str], ...]] = set()

    def dfs(
        progress: Dict[str, int],
        blocked: Set[str],
        held: Dict[str, str],
        path: List[WitnessStep],
        seq: int,
    ) -> Optional[List[WitnessStep]]:
        # 全部环上批次同时进入目标等待 => 见证交错成立
        if len(blocked) == len(cycle_batches):
            return list(path)

        state = tuple(
            sorted(
                (bid, progress[bid], "B" if bid in blocked else "R")
                for bid in cycle_batches
            )
        )
        if state in memo:
            return None
        memo.add(state)

        for bid in cycle_batches:
            if bid in blocked:
                continue
            idx = progress[bid]
            orders = order_lists[bid]
            if idx >= len(orders):
                continue
            oid = orders[idx]

            if oid == goal_wait[bid] and held.get(oid) == holder_of[oid]:
                # 进入环上目标等待状态（实跑时该 UPDATE 会阻塞）
                progress[bid] = idx + 1
                blocked.add(bid)
                found = dfs(progress, blocked, held, path, seq)
                blocked.discard(bid)
                progress[bid] = idx
                if found is not None:
                    return found
                continue

            if oid in held:
                # 非目标等待：环闭合前持锁方不会释放，本批将永久卡死、
                # 到不了目标态，该调度分支不可行，剪枝。
                continue

            # 正常拿到该单行锁
            progress[bid] = idx + 1
            held[oid] = bid
            path.append(
                WitnessStep(seq=seq + 1, batch_id=bid, step=idx + 1, order_id=oid)
            )
            found = dfs(progress, blocked, held, path, seq + 1)
            path.pop()
            del held[oid]
            progress[bid] = idx
            if found is not None:
                return found
        return None

    progress0 = {bid: 0 for bid in cycle_batches}
    return dfs(progress0, set(), {}, [], 0)


def analyze(batches: List[BatchInput]) -> Tuple[List[WaitEdge], List[CycleReport]]:
    """构造候选等待图，返回全部候选边与**可实现**的死锁环报告。"""
    positions = _positions(batches)
    edges = build_candidate_edges(batches)
    edge_index: Dict[CandidateEdge, WaitEdge] = {
        (e.waiter_batch, e.holder_batch, e.order_id): e for e in edges
    }

    reports: List[CycleReport] = []
    feasible_edge_keys: Set[CandidateEdge] = set()

    for cycle in _enumerate_candidate_cycles(batches, edges):
        if not _local_order_ok(cycle, positions):
            continue
        witness = _search_witness(batches, cycle)
        if witness is None:
            # 纸面环：不存在让环上各批同时等锁的交错，判为误报并丢弃
            continue
        for edge_key in cycle:
            feasible_edge_keys.add(edge_key)
        reports.append(
            CycleReport(
                cycle_batches=[c[0] for c in cycle],
                cycle_orders=[c[2] for c in cycle],
                edges=[edge_index[c] for c in cycle],
                witness=witness,
            )
        )

    for key in feasible_edge_keys:
        edge_index[key].on_feasible_cycle = True

    reports.sort(
        key=lambda c: (
            len(c.cycle_batches),
            c.cycle_batches,
            c.cycle_orders,
        )
    )
    return edges, reports


def pick_replay_cycle(
    batches: List[BatchInput], cycles: List[CycleReport]
) -> Optional[CycleReport]:
    """挑选最容易实跑复现的环：优先二节点环（经典 AB-BA），再选最短环。"""
    if not cycles:
        return None
    return sorted(
        cycles, key=lambda c: (len(c.cycle_batches), c.cycle_batches, c.cycle_orders)
    )[0]
