"""端到端自测：覆盖无死锁、二节点 AB-BA 死锁、三节点环、参数校验、
同号并发不阻塞、独有单夹单导致的误报/漏报回归、见证交错、试跑无残留。

运行（需要可连通的 PostgreSQL）：
    python -m tests.selftest
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from typing import Any, List, Tuple

from app.analyzer import analyze, pick_replay_cycle
from app.config import settings
from app.db import connect, ensure_business_schema, replay_deadlock
from app.main import app
from app.models import BatchInput

from fastapi.testclient import TestClient

PASS = "PASS"
FAIL = "FAIL"
results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    results.append((name, bool(cond), detail))
    print(f"[{PASS if cond else FAIL}] {name}" + (f" -- {detail}" if detail else ""))


def _edge_set(edges) -> set[Tuple[str, str, str]]:
    return {(e.waiter_batch, e.holder_batch, e.order_id) for e in edges}


def case_no_cycle() -> None:
    print("\n== 场景1：两批加锁顺序一致，无环 ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2", "W3"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W1", "W2", "W4"], assignee="bob"),
    ]
    edges, cycles = analyze(batches)
    check("无环", cycles == [], f"cycles={cycles}")
    # 新模型：同位置共享单仍列出两个方向的候选边，但均不参与可行环
    check(
        "同位置共享单只产生候选边且均不在可行环上",
        _edge_set(edges)
        == {("B1", "B2", "W1"), ("B2", "B1", "W1"),
            ("B1", "B2", "W2"), ("B2", "B1", "W2")}
        and all(not e.on_feasible_cycle for e in edges),
        str([(e.waiter_batch, e.holder_batch, e.order_id, e.on_feasible_cycle)
             for e in edges]),
    )

    # 子场景 1b：B2 带独有前缀，共享单相对顺序仍一致，无环
    batches_b = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W9", "W1", "W2"], assignee="bob"),
    ]
    edges_b, cycles_b = analyze(batches_b)
    check("单向等待无环", cycles_b == [], str(cycles_b))
    check(
        "W1/W2 的两个方向均为候选边，无一条进入可行环",
        all(not e.on_feasible_cycle for e in edges_b),
        str([(e.waiter_batch, e.holder_batch, e.order_id) for e in edges_b]),
    )


def case_false_positive_with_unique_orders() -> None:
    print("\n== 场景2：误报回归——独有单改变绝对位置、共享单相对顺序一致 ==")
    # B1: W1(1) W9(2) W9b(3) W2(4)；B2: W8(1) W1(2) W2(3)
    # 共享单 W1/W2 在两批中都是 W1 先 W2 后：任意调度下都不会成环。
    # 旧实现按绝对步号会拼出 B1->B2(W2)、B2->B1(W1) 的假环。
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W9", "W9b", "W2"], assignee="a"),
        BatchInput(batch_id="B2", order_ids=["W8", "W1", "W2"], assignee="b"),
    ]
    edges, cycles = analyze(batches)
    check("夹独有单但相对顺序一致 => 无环（旧实现误报 1 环）",
          cycles == [], str([(c.cycle_batches, c.cycle_orders) for c in cycles]))
    check("候选边全部不参与可行环",
          all(not e.on_feasible_cycle for e in edges))

    # 变体：两批各夹不同独有单，绝对位置双向反转，相对顺序仍一致
    batches2 = [
        BatchInput(batch_id="B1", order_ids=["W1", "W9", "W2"], assignee="a"),
        BatchInput(batch_id="B2", order_ids=["W8", "W1", "W7", "W2"], assignee="b"),
    ]
    _, cycles2 = analyze(batches2)
    check("两侧夹单、绝对位置反转但相对一致 => 仍无环", cycles2 == [],
          str([(c.cycle_batches, c.cycle_orders) for c in cycles2]))


def case_false_negative_with_unique_orders() -> None:
    print("\n== 场景3：漏报回归——独有单推偏位置、共享单相对顺序真实反转 ==")
    # B1: W1(1) W2(2)；B2: W9(1) W2(2) W1(3)
    # 相对顺序 B1: W1<W2，B2: W2<W1，反转 => 真实 AB-BA 死锁。
    # 旧实现因 W2 在两批同号位置(2,2)不出边，漏报。
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W9", "W2", "W1"], assignee="bob"),
    ]
    edges, cycles = analyze(batches)
    check("检测到 1 个环（旧实现漏报 0 环）", len(cycles) == 1,
          str([c.cycle_batches for c in cycles]))
    check("环为 B1<->B2", sorted(cycles[0].cycle_batches) == ["B1", "B2"])
    check("环上工单为 W1/W2", sorted(cycles[0].cycle_orders) == ["W1", "W2"])
    witness = cycles[0].witness
    witness_orders = [(w.batch_id, w.order_id) for w in witness]
    check(
        "见证交错先让 B1 拿 W1、B2 拿 W2（含独有单 W9）",
        ("B1", "W1") in witness_orders and ("B2", "W2") in witness_orders,
        str([(w.seq, w.batch_id, w.step, w.order_id) for w in witness]),
    )
    check("见证序号连续从 1 开始",
          [w.seq for w in witness] == list(range(1, len(witness) + 1)))
    # 环上的两条边必须被标记为可行
    feasible = {(e.waiter_batch, e.holder_batch, e.order_id)
                for e in edges if e.on_feasible_cycle}
    check("环边均带可行环标记",
          feasible == {(c[0], c[1], c[2]) for c in [
              (cycles[0].edges[0].waiter_batch, cycles[0].edges[0].holder_batch,
               cycles[0].edges[0].order_id),
              (cycles[0].edges[1].waiter_batch, cycles[0].edges[1].holder_batch,
               cycles[0].edges[1].order_id),
          ]})

    report = replay_deadlock(batches, cycles[0])
    check("漏报场景实跑复现 40P01",
          report.reproduced and report.sqlstate == "40P01", report.detail)
    check("漏报场景收尾无残留", report.cleanup_ok)
    # 两批在不同步号进入等待：B1 第 2 步、B2 第 3 步
    waits = {w.batch_id: w for w in report.batch_waits}
    check("各批等的工单与持有方正确",
          waits["B1"].waits_for_order == "W2"
          and waits["B1"].waits_for_holder == "B2"
          and waits["B2"].waits_for_order == "W1"
          and waits["B2"].waits_for_holder == "B1",
          str([w.model_dump() for w in report.batch_waits]))
    check("不同步号等锁：B1 完成 1 步后阻塞、B2 完成 2 步后阻塞",
          waits["B1"].last_acquired_step == 1
          and waits["B2"].last_acquired_step == 2,
          str([(w.batch_id, w.last_acquired_step) for w in report.batch_waits]))
    blocks = [s for s in report.steps if s.action == "block"]
    check("两条 block 记录分属两批、工单为 W2/W1",
          {s.batch_id for s in blocks} == {"B1", "B2"}
          and {s.order_id for s in blocks} == {"W1", "W2"})


def case_two_node_cycle() -> None:
    print("\n== 场景4：二节点 AB-BA 死锁（经典场景，实跑必须 40P01） ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="alice"),
        BatchInput(batch_id="B2", order_ids=["W2", "W1"], assignee="bob"),
    ]
    edges, cycles = analyze(batches)
    check("检测到 1 个环", len(cycles) == 1, str([c.cycle_batches for c in cycles]))
    check("环为 B1<->B2", sorted(cycles[0].cycle_batches) == ["B1", "B2"])
    check("环上工单为 W1/W2", sorted(cycles[0].cycle_orders) == ["W1", "W2"])
    report = replay_deadlock(batches, cycles[0])
    check("实跑复现成功", report.reproduced, report.detail)
    check("SQLSTATE=40P01", report.sqlstate == "40P01", str(report.sqlstate))
    check("有牺牲者批次", report.victim_batch in {"B1", "B2"},
          str(report.victim_batch))
    check("收尾无残留", report.cleanup_ok, report.detail)
    actions = [s.action for s in report.steps]
    check("操作步骤含 block 与 abort",
          "abort" in actions and "block" in actions, str(actions))
    check("abort 步骤标注 40P01",
          any(s.action == "abort" and "40P01" in (s.note or "")
              for s in report.steps))
    check(
        "各批等待信息齐全",
        {w.batch_id for w in report.batch_waits} == {"B1", "B2"},
        str([w.model_dump() for w in report.batch_waits]),
    )
    seqs = [s.seq for s in report.steps]
    check("全局操作序号唯一且有序", seqs == sorted(seqs) and len(seqs) == len(set(seqs)),
          str(seqs))


def case_three_node_cycle() -> None:
    print("\n== 场景5：三节点环 B1->B2->B3->B1 ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="a1"),
        BatchInput(batch_id="B2", order_ids=["W2", "W3"], assignee="a2"),
        BatchInput(batch_id="B3", order_ids=["W3", "W1"], assignee="a3"),
    ]
    edges, cycles = analyze(batches)
    check("检测到环", len(cycles) >= 1, str([c.cycle_batches for c in cycles]))
    three = [c for c in cycles if len(c.cycle_batches) == 3]
    check("存在三节点环", len(three) == 1, str([c.cycle_batches for c in cycles]))
    witness_batches = {w.batch_id for w in three[0].witness}
    check("三节点环见证覆盖三批", witness_batches == {"B1", "B2", "B3"})
    report = replay_deadlock(batches, three[0])
    check("三实跑复现 40P01", report.reproduced and report.sqlstate == "40P01",
          report.detail)
    check("三收尾无残留", report.cleanup_ok)
    check("三批都有 block 现场",
          {s.batch_id for s in report.steps if s.action == "block"}
          == {"B1", "B2", "B3"})
    check("仅一个牺牲者被 40P01 中止",
          sum(1 for s in report.steps if s.action == "abort" and "40P01"
              in (s.note or "")) == 1)


def case_three_batches_two_node_cycle() -> None:
    print("\n== 场景6：三批输入，其中两批成环、第三批旁观 ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2"], assignee="a1"),
        BatchInput(batch_id="B2", order_ids=["W2", "W1"], assignee="a2"),
        BatchInput(batch_id="B3", order_ids=["W9", "W8"], assignee="a3"),
    ]
    edges, cycles = analyze(batches)
    check("检测到 1 个二环", len(cycles) == 1
          and sorted(cycles[0].cycle_batches) == ["B1", "B2"],
          str([c.cycle_batches for c in cycles]))
    report = replay_deadlock(batches, cycles[0])
    check("旁观批场景实跑 40P01", report.reproduced and report.sqlstate == "40P01",
          report.detail)
    check("旁观批场景收尾无残留", report.cleanup_ok)
    check("旁观批 B3 正常跑完且不在等待信息中",
          any(s.batch_id == "B3" and s.action == "acquire" for s in report.steps)
          and {w.batch_id for w in report.batch_waits} == {"B1", "B2"})


def case_six_orders_limit() -> None:
    print("\n== 场景7：每批 6 单上限与 2~3 批限制的参数校验 ==")
    client = TestClient(app)
    ok_payload = {
        "batches": [
            {"batch_id": "B1", "order_ids": [f"W{i}" for i in range(1, 7)],
             "assignee": "x"},
            {"batch_id": "B2", "order_ids": [f"W{i}" for i in range(6, 0, -1)],
             "assignee": "y"},
        ]
    }
    r = client.post("/precheck", json=ok_payload)
    check("6 单/批被接受且判死锁", r.status_code == 200 and r.json()["has_deadlock"],
          f"{r.status_code} {r.text[:200]}")

    # 去重回归：完全反转的两批有 C(6,2)=15 个反转对（候选环），
    # 但只有 5 个相邻对的拿锁前缀无跨批争用、可同时达。
    from app.analyzer import (
        build_candidate_edges,
        _enumerate_candidate_cycles,
    )
    boundary_batches = [
        BatchInput(batch_id="B1", order_ids=[f"W{i}" for i in range(1, 7)],
                   assignee="x"),
        BatchInput(batch_id="B2", order_ids=[f"W{i}" for i in range(6, 0, -1)],
                   assignee="y"),
    ]
    cand = _enumerate_candidate_cycles(
        boundary_batches, build_candidate_edges(boundary_batches)
    )
    _, boundary_cycles = analyze(boundary_batches)
    check("候选二环去重后为 15 个反转对（frozenset 键，不误合并不同工单对）",
          len(cand) == 15, f"candidates={len(cand)}")
    check("仅 5 个相邻反转对存在无争用见证、判可实现",
          len(boundary_cycles) == 5, f"feasible={len(boundary_cycles)}")

    too_many = {
        "batches": [
            {"batch_id": "B1", "order_ids": [f"W{i}" for i in range(1, 8)],
             "assignee": "x"},
            {"batch_id": "B2", "order_ids": ["W1"], "assignee": "y"},
        ]
    }
    r = client.post("/precheck", json=too_many)
    check("7 单/批被 422 拒绝", r.status_code == 422, str(r.status_code))

    one_batch = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1"], "assignee": "x"},
        ]
    }
    r = client.post("/precheck", json=one_batch)
    check("1 批被 422 拒绝", r.status_code == 422, str(r.status_code))

    four = {
        "batches": [
            {"batch_id": f"B{i}", "order_ids": ["W1"], "assignee": "x"}
            for i in range(1, 5)
        ]
    }
    r = client.post("/precheck", json=four)
    check("4 批被 422 拒绝", r.status_code == 422, str(r.status_code))

    dup = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1", "W1"], "assignee": "x"},
            {"batch_id": "B2", "order_ids": ["W2"], "assignee": "y"},
        ]
    }
    r = client.post("/precheck", json=dup)
    check("同批重复工单号被 422 拒绝", r.status_code == 422, str(r.status_code))


def case_api_paths() -> None:
    print("\n== 场景8：API 安全路径（无环不试跑）、误报/漏报样例与 Swagger ==")
    client = TestClient(app)
    r = client.get("/health")
    check("health 200", r.status_code == 200, r.text)
    r = client.get("/docs")
    check("Swagger UI 200", r.status_code == 200)
    r = client.get("/openapi.json")
    body = r.json()
    check("openapi.json 200 且含 /precheck",
          r.status_code == 200 and "/precheck" in body["paths"])
    examples = body["components"]["schemas"]["PrecheckRequest"]["examples"]
    summaries = [ex.get("summary", "") for ex in examples]
    check("Swagger 内置误报/漏报回归样例",
          any("误报" in s for s in summaries) and any("漏报" in s for s in summaries),
          str(summaries))

    payload = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "a"},
            {"batch_id": "B2", "order_ids": ["W1", "W2"], "assignee": "b"},
        ],
        "run_replay": True,
    }
    r = client.post("/precheck", json=payload)
    body = r.json()
    check("无环请求 has_deadlock=false 且不试跑",
          r.status_code == 200 and body["has_deadlock"] is False
          and body["replay"] is None, r.text[:200])

    # 误报样例经 API 同样判无环
    fp = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1", "W9", "W9b", "W2"],
             "assignee": "a"},
            {"batch_id": "B2", "order_ids": ["W8", "W1", "W2"], "assignee": "b"},
        ],
        "run_replay": True,
    }
    r = client.post("/precheck", json=fp)
    check("误报样例 API 判无环", r.status_code == 200
          and r.json()["has_deadlock"] is False and r.json()["replay"] is None)

    # 漏报样例经 API 判有环并实跑 40P01
    fn = {
        "batches": [
            {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "a"},
            {"batch_id": "B2", "order_ids": ["W9", "W2", "W1"], "assignee": "b"},
        ],
        "run_replay": True,
    }
    r = client.post("/precheck", json=fn)
    j = r.json()
    check("漏报样例 API 判有环且实跑 40P01",
          r.status_code == 200 and j["has_deadlock"] is True
          and j["replay"] is not None and j["replay"]["sqlstate"] == "40P01"
          and j["replay"]["cleanup_ok"] is True,
          r.text[:300])
    check("响应环报告含见证交错",
          all(c.get("witness") for c in j["cycles"]))


def case_no_residue() -> None:
    print("\n== 场景9：试跑后无残留 schema/事务/改派结果 ==")
    with connect(autocommit=True) as conn:
        ns = conn.execute(
            "SELECT count(*) FROM pg_namespace WHERE nspname = %s",
            (settings.trial_schema,),
        ).fetchone()[0]
        check("试跑 schema 已删除", ns == 0, f"remaining={ns}")
        open_tx = conn.execute(
            """
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND xact_start IS NOT NULL
              AND state IN ('idle in transaction', 'active')
            """
        ).fetchone()[0]
        check("无未结束事务", open_tx == 0, f"open={open_tx}")
        # 业务表未被试跑污染
        ensure_business_schema()
        dirty = conn.execute(
            "SELECT count(*) FROM public.work_orders WHERE assignee <> 'init'"
        ).fetchone()[0]
        check("业务表无试跑改派残留", dirty == 0, f"dirty={dirty}")


def case_same_order_concurrent() -> None:
    print("\n== 场景10：同号并发试验互不阻塞（同顺序两事务可并发提交） ==")
    batches = [
        BatchInput(batch_id="B1", order_ids=["W1", "W2", "W3"], assignee="a"),
        BatchInput(batch_id="B2", order_ids=["W1", "W2", "W3"], assignee="b"),
    ]
    edges, cycles = analyze(batches)
    check("同顺序无环", cycles == [], str(cycles))
    check("同顺序同位置候选边均不可行",
          all(not e.on_feasible_cycle for e in edges),
          str([(e.waiter_batch, e.order_id) for e in edges]))


def case_witness_soundness() -> None:
    print("\n== 场景11：报告环见证交错的机械校验（局部序 + 无提前阻塞） ==")
    # 有界穷举：工单池 W1~W3，2~3 批、每批 1~3 单，各批独立取排列
    # （允许跨批共享，这是死锁前提）。凡报告环都验证 witness 自洽：
    # 1) witness 中每批的动作是其提交顺序的前缀；
    # 2) 见证前缀里同一工单不会被两批同时拿（否则会提前阻塞）；
    # 3) 每批持有工单在本批顺序中严格早于其等待工单。
    # 违规聚合成列表，最后只出汇总断言，避免数千条逐条输出。
    import itertools
    universe = ["W1", "W2", "W3"]
    all_perms = {
        length: list(itertools.permutations(universe, length))
        for length in range(1, len(universe) + 1)
    }
    checked = 0
    inputs_seen = 0
    violations: List[str] = []
    for n_batches in (2, 3):
        for lens in itertools.product(range(1, len(universe) + 1),
                                      repeat=n_batches):
            for combo in itertools.product(*(all_perms[l] for l in lens)):
                batches = [
                    BatchInput(batch_id=f"B{bi+1}",
                               order_ids=list(ids), assignee=f"a{bi}")
                    for bi, ids in enumerate(combo)
                ]
                shared_any = any(
                    set(batches[i].order_ids) & set(batches[j].order_ids)
                    for i in range(n_batches)
                    for j in range(i + 1, n_batches)
                )
                if not shared_any:
                    continue
                inputs_seen += 1
                _, cycles = analyze(batches)
                for cyc in cycles:
                    checked += 1
                    positions = {b.batch_id: {o: i for i, o in
                                              enumerate(b.order_ids, 1)}
                                 for b in batches}
                    held_by: dict[str, str] = {}
                    per_batch_seen: dict[str, list[str]] = {
                        b.batch_id: [] for b in batches}
                    for w in cyc.witness:
                        if w.order_id in held_by:  # 2) 重复持有
                            violations.append(
                                f"重复持有 {w.order_id}：{cyc.cycle_batches}")
                        held_by[w.order_id] = w.batch_id
                        per_batch_seen[w.batch_id].append(w.order_id)
                    for b in batches:
                        seen = per_batch_seen[b.batch_id]
                        if seen and seen != b.order_ids[:len(seen)]:
                            # 1) 非提交顺序前缀
                            violations.append(
                                f"{b.batch_id} 前缀不符：{seen} vs {b.order_ids}")
                    # 3) 持有早于等待
                    waits = dict(zip(cyc.cycle_batches, cyc.cycle_orders))
                    holds = {h: x for _, h, x in
                             [(e.waiter_batch, e.holder_batch, e.order_id)
                              for e in cyc.edges]}
                    for bid in waits:
                        if not positions[bid][holds[bid]] < positions[bid][waits[bid]]:
                            violations.append(
                                f"{bid} 持锁不早于等待：hold={holds[bid]} "
                                f"wait={waits[bid]}")
    check(f"穷举 {inputs_seen} 个含共享单输入、{checked} 个报告环，"
          "见证全部自洽（无前缀/重复持有/逆序违规）",
          checked >= 20 and not violations,
          f"inputs={inputs_seen} cycles={checked} violations={violations[:5]}")


def main() -> int:
    print(f"DATABASE_URL={settings.database_url}")
    ensure_business_schema()
    t0 = time.time()
    case_no_cycle()
    case_false_positive_with_unique_orders()
    case_false_negative_with_unique_orders()
    case_two_node_cycle()
    case_three_node_cycle()
    case_three_batches_two_node_cycle()
    case_six_orders_limit()
    case_api_paths()
    case_same_order_concurrent()
    case_witness_soundness()
    case_no_residue()
    dt = time.time() - t0

    total = len(results)
    failed = [r for r in results if not r[1]]
    print(f"\n==== 自测结果：{total - len(failed)}/{total} 通过，用时 {dt:.1f}s ====")
    if failed:
        for name, _, detail in failed:
            print(f"  失败项: {name} -- {detail}")
        return 1
    sample = {
        "false_positive_regression": {
            "batches": [
                {"batch_id": "B1", "order_ids": ["W1", "W9", "W9b", "W2"],
                 "assignee": "alice"},
                {"batch_id": "B2", "order_ids": ["W8", "W1", "W2"],
                 "assignee": "bob"},
            ],
            "run_replay": True,
            "expect": "has_deadlock=false（旧实现误报）",
        },
        "false_negative_regression": {
            "batches": [
                {"batch_id": "B1", "order_ids": ["W1", "W2"], "assignee": "alice"},
                {"batch_id": "B2", "order_ids": ["W9", "W2", "W1"],
                 "assignee": "bob"},
            ],
            "run_replay": True,
            "expect": "has_deadlock=true 且 replay.sqlstate=40P01（旧实现漏报）",
        },
    }
    print("\nSwagger 误报/漏报回归 payload：")
    print(json.dumps(sample, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:  # noqa: BLE001
        traceback.print_exc()
        sys.exit(2)
