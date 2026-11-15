"""维表版本切换的契约测试。

命题：
1. 一次刷新要么完整旧版本、要么完整新版本，不出现空维度/新旧混用；
2. 每条结果行都带有可校验的版本标识；
3. 加载失败/回退/刷新快于加载/内存峰值/检查点恢复都有确定行为。
"""

from __future__ import annotations

import json
import tempfile
import threading
import time
from pathlib import Path

import pytest

from streamforge.dim import (
    DimError,
    DimLoadError,
    DimMemoryLimitError,
    DimTable,
    DimVersionStore,
    DimVersionVerifyError,
    content_digest,
    row_provenance_ok,
)
from streamforge.events import generate
from streamforge.job import Job


def _rows(n: int = 40, tag: str = "v1") -> list[dict]:
    return [
        {
            "shop_id": f"SHOP-{i:03d}",
            "shop_name": f"{tag}-店{i:02d}",
            "region": "华东",
            "tier": "A",
        }
        for i in range(1, n + 1)
    ]


def _store(tmp: Path) -> DimVersionStore:
    return DimVersionStore(tmp / "_dim_versions")


# ---------------------------------------------------------------- 版本标识


def test_version_id_is_content_addressed_and_self_consistent():
    tmp = Path(tempfile.mkdtemp())
    store = _store(tmp)
    table = DimTable(lambda: _rows(), store=store, background=False)
    version = table.load_blocking()

    assert version.id == f"dim-000001-{version.digest[:12]}"
    assert version.verify()
    assert version.digest == content_digest(_rows())
    # 同样的内容再加载：复用同一个版本 id，不产生无意义的切换。
    again = table.load_blocking()
    assert again.id == version.id
    assert table.report()["loads"] == 1

    persisted = store.get(version.id)
    assert persisted.digest == version.digest
    assert persisted.row_count == version.row_count
    assert table.assert_consistent()["version"] == version.id


def test_store_detects_tampered_snapshot():
    tmp = Path(tempfile.mkdtemp())
    store = _store(tmp)
    table = DimTable(lambda: _rows(), store=store, background=False)
    version = table.load_blocking()

    rows_path = store.root / f"{version.id}.jsonl"
    lines = rows_path.read_text(encoding="utf-8").splitlines()
    lines[0] = lines[0].replace("v1-店01", "tampered")
    rows_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(DimVersionVerifyError):
        store.get(version.id)
    assert table.active_id == version.id  # 内存中的活动版本不受影响


# ---------------------------------------------------------------- 原子切换


def test_lookup_never_sees_partial_snapshot_while_loading():
    """慢速加载期间，并发查询始终命中的是完整版本。"""
    tmp = Path(tempfile.mkdtemp())
    old = _rows(tag="old")
    new = _rows(tag="new")
    gate = threading.Event()

    def slow_loader():
        gate.wait(2.0)
        # 分片产出，模拟逐批拉取。
        for i in range(0, len(new), 5):
            time.sleep(0.001)
            yield from new[i : i + 5]

    table = DimTable(slow_loader, store=_store(tmp), background=True)
    table.load_blocking(lambda: old, reason="startup")

    table.request_refresh()
    observed: set[str] = set()
    misses = 0
    stop = threading.Event()

    def reader():
        nonlocal misses
        while not stop.is_set():
            v = table.current
            for sid in (r["shop_id"] for r in old):
                if v.lookup(sid) is None:
                    misses += 1
            observed.add(v.id)

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.02)
    gate.set()
    assert table.wait_idle(timeout=5)
    stop.set()
    for t in threads:
        t.join(2)

    assert misses == 0, "加载期间不允许出现维度落空"
    assert len(observed) <= 2, "只应看到旧版本或新版本"
    assert table.active_id != table.report()["previous_version"]
    assert table.report()["miss_during_reload"] == 0
    table.close()


def test_job_rollback_keeps_old_rows_provable():
    """作业层回退：切换后旧行的版本标识不会失效。"""
    tmp = Path(tempfile.mkdtemp())
    job = Job(_rows(tag="v1"), root=tmp, batch_size=25, two_phase=True)
    events = generate(n=100, seed=41)
    job.run(events)

    v2 = job.refresh_dim_blocking(lambda: _rows(tag="v2"))
    job.run(generate(n=40, seed=42))
    v1_id = job.dim.report()["previous_version"]
    assert v2.id == job.dim.active_id

    job.rollback_dim(v1_id)
    assert job.dim.active_id == v1_id
    snap = job.snapshot()
    assert snap["dim"]["provenance_checked"] == len(events) + 40
    assert snap["dim"]["provenance_failures"] == []
    assert set(snap["dim"]["row_versions"]) == {v1_id, v2.id}
    job.close()


def test_job_rows_carry_verifiable_version_and_never_mix():
    """事件处理与刷新并发时，每行关联字段都能用它声明的版本复算。"""
    tmp = Path(tempfile.mkdtemp())
    job = Job(_rows(tag="v1"), root=tmp, batch_size=50, two_phase=True)
    events = generate(n=600, seed=17)

    job.run(events[:300])
    job.dim.request_refresh(lambda: _rows(tag="v2"))
    assert job.dim.wait_idle(5)
    job.run(events[300:])
    snap = job.snapshot()

    assert snap["dim"]["miss_during_reload"] == 0
    assert snap["dim"]["provenance_checked"] == len(events)
    assert snap["dim"]["provenance_failures"] == []
    # 两次运行分别落在两个完整版本上，且每行的字段与版本一致。
    assert len(snap["dim"]["row_versions"]) == 2
    for row in job.sink.read_all():
        ok, why = row_provenance_ok(row, job.dim_store)
        assert ok, why
    job.close()


def test_background_refresh_does_not_block_event_processing():
    """慢速加载在后台进行时，事件处理链路必须继续推进。"""
    tmp = Path(tempfile.mkdtemp())
    release = threading.Event()
    calls = {"n": 0}

    def loader():
        calls["n"] += 1
        if calls["n"] == 1:
            return _rows(tag="v1")  # 启动快照
        release.wait(5)
        return _rows(tag="v2")  # 后台刷新快照

    job = Job(_rows(tag="v1"), root=tmp, batch_size=25, two_phase=True,
              dim_loader=loader)
    events = generate(n=300, seed=23)

    job.dim.request_refresh()
    started = time.monotonic()
    report = job.run(events)
    elapsed = time.monotonic() - started

    assert report["job"]["emitted"] == len(events)
    assert elapsed < 2.0, "加载未完成时事件处理不应被阻塞"
    assert job.dim.current.lookup("SHOP-001")["shop_name"].startswith("v1")

    release.set()
    assert job.dim.wait_idle(5)
    assert job.dim.current.lookup("SHOP-001")["shop_name"].startswith("v2")
    # 新版本切换后写出的事件绑定新版本，之前的行仍绑定旧版本。
    job.run(generate(n=20, seed=24))
    snap = job.snapshot()
    assert len(snap["dim"]["row_versions"]) == 2
    assert snap["dim"]["provenance_failures"] == []
    job.close()


# ---------------------------------------------------------------- 失败与回退


def test_loader_failure_keeps_old_version_serving():
    tmp = Path(tempfile.mkdtemp())
    table = DimTable(lambda: _rows(tag="v1"), store=_store(tmp), background=True)
    good = table.load_blocking()

    def boom():
        raise RuntimeError("upstream 503")

    with pytest.raises(DimLoadError):
        table.load_blocking(boom)
    assert table.active_id == good.id
    assert table.report()["load_failures"] == 1
    assert "upstream 503" in table.report()["last_error"]

    # 后台刷新失败同样不能杀死线程，后续请求仍能成功。
    assert table.request_refresh(boom) is True
    assert table.wait_idle(5)
    assert table.active_id == good.id
    assert table.request_refresh(lambda: _rows(tag="v2")) in (True, False)
    assert table.wait_idle(5)
    assert table.active_id != good.id
    table.close()


def test_invalid_snapshots_are_rejected_without_switching():
    tmp = Path(tempfile.mkdtemp())
    table = DimTable(lambda: _rows(tag="v1"), store=_store(tmp), background=False)
    good = table.load_blocking()

    for supplier in (
        lambda: [],
        lambda: [{"shop_name": "no id"}],
        lambda: _rows(tag="v2") + [_rows(tag="v2")[0]],
    ):
        with pytest.raises(DimLoadError):
            table.load_blocking(supplier)
        assert table.active_id == good.id
    assert table.report()["load_failures"] == 3


def test_rollback_restores_previous_version_atomically():
    tmp = Path(tempfile.mkdtemp())
    table = DimTable(lambda: _rows(tag="v1"), store=_store(tmp), background=False)
    v1 = table.load_blocking()
    v2 = table.load_blocking(lambda: _rows(tag="v2"))
    assert table.active_id == v2.id

    restored = table.rollback()
    assert restored.id == v1.id
    assert table.active_id == v1.id
    assert table.report()["rollbacks"] == 1
    # 回退复用原版本 id：此前写出的 v1 行仍然可证明。
    assert table.active_digest == v1.digest
    assert table.assert_consistent()["version"] == v1.id

    # 再回退一次即撤销上一次回退（previous 现在是 v2）。
    assert table.rollback().id == v2.id
    assert table.report()["rollbacks"] == 2

    with pytest.raises(DimError):
        table.rollback("dim-999999-doesnotexist")
    assert table.active_id == v2.id


# ---------------------------------------------------------------- 刷新快于加载


def test_refresh_requests_coalesce_while_loading():
    tmp = Path(tempfile.mkdtemp())
    started = threading.Event()
    release = threading.Event()
    loads = {"n": 0}

    def slow_loader():
        loads["n"] += 1
        started.set()
        release.wait(5)
        return _rows(tag=f"v{loads['n']}")

    table = DimTable(slow_loader, store=_store(tmp), background=True)
    table.load_blocking(lambda: _rows(tag="v0"))

    table.request_refresh()
    assert started.wait(2)
    # 加载在途时涌入 50 次刷新：只应合并成一次后续加载。
    for _ in range(50):
        table.request_refresh()
    release.set()
    assert table.wait_idle(10)

    report = table.report()
    assert loads["n"] == 2, "在途期间的刷新必须合并，不能堆积"
    assert report["refresh_requests"] >= 51
    assert report["coalesced_requests"] >= 49
    # 启动版本 + 后台两次加载 = 3 次成功发布。
    assert report["active_version"] and report["loads"] == 3
    table.close()


# ---------------------------------------------------------------- 内存峰值


def test_memory_budget_rejects_candidate_but_keeps_serving():
    tmp = Path(tempfile.mkdtemp())
    rows = _rows(n=100)
    table = DimTable(lambda: rows, store=_store(tmp), background=False, max_bytes=64 * 1024)
    good = table.load_blocking()
    assert table.report()["peak_bytes"] > 0

    big = _rows(n=2000, tag="big")
    with pytest.raises(DimMemoryLimitError):
        table.load_blocking(lambda: big)
    assert table.active_id == good.id
    assert table.report()["memory_rejections"] == 1
    assert table.report()["peak_bytes"] <= table.report()["max_bytes"]


def test_memory_peak_is_bounded_by_active_plus_candidate():
    tmp = Path(tempfile.mkdtemp())
    rows = _rows(n=50)
    table = DimTable(lambda: rows, store=_store(tmp), background=False, max_bytes=1024 * 1024)
    table.load_blocking()
    for tag in ("v2", "v3"):
        table.load_blocking(lambda tag=tag: _rows(n=50, tag=tag))
        report = table.report()
        assert report["peak_bytes"] <= report["max_bytes"]
    # peak 至少覆盖过一次"旧 + 新"两份完整快照。
    single = table.current.size_bytes
    assert table.report()["peak_bytes"] >= 2 * single


# ---------------------------------------------------------------- 检查点恢复


def test_checkpoint_binds_dim_version_and_restore_reuses_it():
    tmp = Path(tempfile.mkdtemp())
    job = Job(_rows(tag="v1"), root=tmp, batch_size=100, two_phase=True,
              checkpoint_interval=50)
    job.run(generate(n=200, seed=31))

    ck = job.latest_checkpoint()
    assert ck["dim_version"] == job.dim.active_id
    assert ck["dim_digest"] == job.dim.active_digest
    expected = job.dim.active_id
    job.close()

    # 重启后维表源换成了 v2；按检查点恢复必须回到 v1 的内容与 id。
    restarted = Job(_rows(tag="v2"), root=tmp, batch_size=100, two_phase=True)
    assert restarted.dim.active_id != expected
    result = restarted.restore_dim(ck)
    assert result["restored"] is True
    assert restarted.dim.active_id == expected
    assert restarted.dim.current.lookup("SHOP-001")["shop_name"].startswith("v1")
    restarted.close()


def test_checkpoint_restore_failure_falls_back_and_records():
    tmp = Path(tempfile.mkdtemp())
    job = Job(_rows(tag="v1"), root=tmp, batch_size=100, two_phase=True)
    version_id = job.dim.active_id
    job.close()

    # 删掉快照模拟版本仓库丢失。
    store = job.dim_store
    (store.root / f"{version_id}.jsonl").unlink()
    restarted = Job(_rows(tag="v2"), root=tmp, batch_size=100, two_phase=True)
    result = restarted.restore_dim({"dim_version": version_id})
    assert result["restored"] is False
    assert restarted.dim.active_id != version_id  # 继续用启动版本服务
    assert restarted.dim.report()["restore_failures"] == 1
    restarted.close()


def test_checkpoint_digest_mismatch_is_not_restored():
    """检查点声明的 digest 与磁盘内容不符时，必须拒绝恢复。"""
    tmp = Path(tempfile.mkdtemp())
    job = Job(_rows(tag="v1"), root=tmp, batch_size=100, two_phase=True)
    version_id = job.dim.active_id
    started_with = version_id
    job.close()

    restarted = Job(_rows(tag="v2"), root=tmp, batch_size=100, two_phase=True)
    before = restarted.dim.active_id
    result = restarted.restore_dim(
        {"dim_version": version_id, "dim_digest": "0" * 64}
    )
    assert result["restored"] is False
    assert restarted.dim.active_id == before  # 旧版本继续服务
    assert restarted.dim.active_id != started_with
    restarted.close()


# ---------------------------------------------------------------- 内容不变


def test_same_content_refresh_is_noop_for_identity():
    tmp = Path(tempfile.mkdtemp())
    table = DimTable(lambda: _rows(), store=_store(tmp), background=False)
    first = table.load_blocking()
    report_before = table.report()["reloads"]
    second = table.load_blocking()
    assert second.id == first.id
    assert table.report()["reloads"] == report_before
    # 顺序不同的同一份内容也必须是同一个 digest。
    shuffled = list(reversed(_rows()))
    assert content_digest(shuffled) == first.digest
    table.close()


def test_version_store_ignores_corrupt_older_snapshot_on_latest():
    tmp = Path(tempfile.mkdtemp())
    store = _store(tmp)
    table = DimTable(lambda: _rows(tag="v1"), store=store, background=False)
    v1 = table.load_blocking()
    v2 = table.load_blocking(lambda: _rows(tag="v2"))
    (store.root / f"{v2.id}.jsonl").write_text("not json\n", encoding="utf-8")
    assert store.latest().id == v1.id
    assert json.loads((store.root / f"{v1.id}.json").read_text())["version"] == v1.id
