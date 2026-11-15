"""毒消息隔离与死信队列语义测试。

覆盖命题（对应生产事故的每一类坏数据与边界）：

    截断 / 非法 JSON   → deserialization_error，进 DLQ，不污染窗口
    未知 schema 版本   → unknown_version，保留版本号
    字段类型漂移       → contract_violation，保留失败字段
    DLQ 下游不可用     → 本地溢出，恢复后补投，坏消息不丢
    批量失败           → 整批隔离，正常批次不受影响
    消息去重           → 合法消息重投递不产生第二行，状态随 checkpoint 持久化
    毒丸重投递         → 重启后同一条毒丸被台账直接跳过，不无限失败
    顺序与吞吐         → 异常流量不改变正常输入顺序，不长期拖垮处理
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

import pytest

from streamforge.events import OrderEvent, generate
from streamforge.job import Job, make_dim
from streamforge.poison import (
    DeadLetter,
    DeadLetterQueue,
    DecodeError,
    FailureCategory,
    FailingDeadLetterSink,
    InMemoryDeadLetterSink,
    PoisonLedger,
    RawMessage,
    decode_message,
    make_dead_letter,
    message_fingerprint,
    redeliver,
    wrap_event,
)


# ---------------------------------------------------------------- 夹具 / 构造器


def _root() -> Path:
    return Path(tempfile.mkdtemp(prefix="sf-dlq-"))


def _valid_payload(i: int) -> dict:
    return {
        "event_id": f"EV{i:06d}",
        "order_id": f"ORD-{i % 50:05d}",
        "shop_id": f"SHOP-{i % 40 + 1:03d}",
        "status": "paid",
        "amount_cents": 1000 + i,
        "channel": "app",
        "event_time": f"2026-11-10T00:{i % 60:02d}:00+00:00",
        "ingest_time": f"2026-11-10T00:{i % 60:02d}:01+00:00",
    }


def _valid(i: int, *, offset: int | None = None) -> RawMessage:
    return wrap_event(_valid_payload(i), partition=0, offset=i if offset is None else offset)


def _job(root: Path, **kw) -> Job:
    kw.setdefault("batch_size", 50)
    kw.setdefault("checkpoint_interval", 100000)  # 默认关周期性 checkpoint，测显式行为
    return Job(make_dim(), root=root, **kw)


def _main_event_ids(job: Job) -> list[str]:
    return [r["event_id"] for r in job.sink.read_all()]


# ---------------------------------------------------------------- 解码与失败类别


def test_truncated_json_is_deserialization_error():
    msg = RawMessage('{"event_id": "EV1", "amount_cents": 12', partition=0, offset=7)
    with pytest.raises(DecodeError) as exc:
        decode_message(msg)
    assert exc.value.category is FailureCategory.DESERIALIZATION


def test_non_object_json_is_deserialization_error():
    with pytest.raises(DecodeError) as exc:
        decode_message(RawMessage("[1, 2, 3]"))
    assert exc.value.category is FailureCategory.DESERIALIZATION


def test_unknown_version_is_classified_and_preserved():
    payload = _valid_payload(1)
    payload["schema_version"] = "42"
    with pytest.raises(DecodeError) as exc:
        decode_message(wrap_event(payload))
    assert exc.value.category is FailureCategory.UNKNOWN_VERSION
    assert exc.value.version == "42"


def test_field_type_drift_is_contract_violation():
    payload = _valid_payload(1)
    payload["amount_cents"] = "1000"  # 上游把金额序列化成了字符串
    with pytest.raises(DecodeError) as exc:
        decode_message(wrap_event(payload))
    assert exc.value.category is FailureCategory.CONTRACT_VIOLATION
    assert "amount_cents" in str(exc.value)


def test_bool_is_not_accepted_as_int():
    payload = _valid_payload(1)
    payload["amount_cents"] = True
    with pytest.raises(DecodeError) as exc:
        decode_message(wrap_event(payload))
    assert exc.value.category is FailureCategory.CONTRACT_VIOLATION


def test_missing_required_field_is_contract_violation():
    payload = _valid_payload(1)
    del payload["order_id"]
    with pytest.raises(DecodeError) as exc:
        decode_message(wrap_event(payload))
    assert exc.value.category is FailureCategory.CONTRACT_VIOLATION
    assert "order_id" in str(exc.value)


def test_bad_timestamp_is_contract_violation():
    payload = _valid_payload(1)
    payload["event_time"] = "not-a-timestamp"
    with pytest.raises(DecodeError) as exc:
        decode_message(wrap_event(payload))
    assert exc.value.category is FailureCategory.CONTRACT_VIOLATION


def test_valid_v1_message_decodes():
    decoded = decode_message(_valid(1))
    assert decoded.version == "1"
    assert decoded.event_id == "EV000001"
    assert decoded.fingerprint == message_fingerprint(
        decoded.topic, decoded.partition, decoded.offset, _valid(1).raw
    )


# ---------------------------------------------------------------- 隔离：坏消息不进窗口


def test_bad_messages_never_reach_main_window():
    root = _root()
    job = _job(root)
    msgs = [
        _valid(0),
        RawMessage('{"truncated": ', partition=0, offset=100),
        wrap_event({**_valid_payload(2), "schema_version": "7"}, offset=101),
        wrap_event({**_valid_payload(3), "shop_id": 9}, offset=102),  # 类型漂移
        _valid(4, offset=103),
    ]
    report = job.run_raw(msgs)

    assert report["job"]["raw_in"] == 5
    assert report["job"]["dead_lettered"] == 3
    assert report["job"]["consumed"] == 2
    ids = _main_event_ids(job)
    assert ids == ["EV000000", "EV000004"]

    letters = job.dlq.read_all()
    assert {l.failure_category for l in letters} == {
        FailureCategory.DESERIALIZATION.value,
        FailureCategory.UNKNOWN_VERSION.value,
        FailureCategory.CONTRACT_VIOLATION.value,
    }


def test_dead_letter_keeps_reprojection_context():
    """死信必须保留原始标识、版本、类别、处理时间与原文。"""
    root = _root()
    job = _job(root)
    raw = '{"event_id": "EV9", "amount_cents": 1'  # 截断，连 event_id 都未必完整
    msg = RawMessage(raw, topic="order_events", partition=3, offset=4242)
    job.run_raw([msg])

    letter = job.dlq.read_all()[0]
    assert letter.raw_payload == raw            # 原文可复现
    assert letter.topic == "order_events"       # 原始标识三元组
    assert letter.partition == 3
    assert letter.offset == 4242
    assert letter.failure_category == FailureCategory.DESERIALIZATION.value
    assert letter.failed_at                      # 处理时间
    assert letter.fingerprint
    assert letter.attempt == 1
    # 全部字段可 JSON 序列化（要直接落 JSONL / 对象存储）
    json.dumps(letter.to_dict())


def test_unknown_version_letter_preserves_version():
    root = _root()
    job = _job(root)
    payload = {**_valid_payload(1), "schema_version": "99"}
    job.run_raw([wrap_event(payload, offset=5)])
    letter = job.dlq.read_all()[0]
    assert letter.schema_version == "99"
    assert letter.event_id == "EV000001"  # 能抢救的标识一并保留


def test_poison_message_does_not_kill_the_job():
    """单条坏消息不能让整个作业抛异常终止。"""
    root = _root()
    job = _job(root)
    msgs = [RawMessage("{bad json", offset=i) for i in range(5)]
    report = job.run_raw(msgs)  # 不抛异常即通过
    assert report["job"]["dead_lettered"] == 5
    assert "crash" not in report


# ---------------------------------------------------------------- 毒丸台账与重启


def test_poison_ledger_blocks_infinite_redelivery_after_restart():
    """重启后重放同一条毒丸：直接跳过，不再解析、不再写 DLQ、不产生失败。"""
    root = _root()
    poison = RawMessage('{"broken":', partition=0, offset=99)
    good = _valid(0, offset=0)

    j1 = _job(root)
    j1.run_raw([good, poison])
    assert len(j1.dlq.read_all()) == 1
    assert len(j1.dlq.ledger) == 1

    # 模拟重启：新 Job 从磁盘重建台账，Kafka 原样重放两条消息。
    j2 = _job(root)
    report = j2.run_raw([good, redeliver(poison)])
    assert report["job"]["poison_skipped"] == 1
    assert report["job"]["dead_lettered"] == 0          # 没有新死信
    assert len(j2.dlq.read_all()) == 1                  # DLQ 不重复落盘
    entry = j2.dlq.ledger.all()[0]
    assert entry.occurrences == 2                        # 重投递被计数，可审计


def test_poison_ledger_is_persisted_on_disk():
    path = _root() / "dlq" / "poison_ledger.jsonl"
    ledger = PoisonLedger(path)
    msg = RawMessage("x", partition=0, offset=1)
    letter = make_dead_letter(
        msg, DecodeError(FailureCategory.DESERIALIZATION, "boom")
    )
    assert ledger.record(letter) is True
    assert ledger.record(letter) is False  # 同指纹重复登记只累加

    reloaded = PoisonLedger(path)
    assert reloaded.is_poison(letter.fingerprint)
    assert reloaded.all()[0].occurrences == 2


def test_repeated_poison_in_same_process_is_skipped():
    root = _root()
    job = _job(root)
    poison = RawMessage("nope", partition=0, offset=3)
    job.run_raw([poison, redeliver(poison), redeliver(poison)])
    assert job.stats.dead_lettered == 1       # 只有首次落 DLQ
    assert job.stats.poison_skipped == 2      # 后两次直接跳过
    assert len(job.dlq.read_all()) == 1


def test_crash_before_checkpoint_then_replay_does_not_rewrite_dlq():
    """崩溃在 DLQ 落盘之后：重启重放时台账仍能拦住毒丸。"""
    root = _root()
    poison = RawMessage("{oops", partition=0, offset=10)
    msgs = [_valid(i, offset=i) for i in range(60)] + [poison]

    j1 = _job(root, checkpoint_interval=100000)
    j1.run_raw(msgs, crash_at=61)  # 毒丸是第 61 条，处理它时"崩溃"
    assert len(j1.dlq.read_all()) == 1

    j2 = _job(root, checkpoint_interval=100000)
    report = j2.run_raw(msgs)
    assert report["job"]["poison_skipped"] == 1
    assert len(j2.dlq.read_all()) == 1  # 无重复死信


# ---------------------------------------------------------------- DLQ 下游不可用与溢出


def test_dlq_downstream_outage_spills_locally_without_loss():
    root = _root()
    job = _job(root, dlq_downstream=FailingDeadLetterSink())
    job.run_raw([_valid(0), RawMessage("{bad", offset=9)])

    # 主表照常提交，坏消息已隔离
    assert _main_event_ids(job) == ["EV000000"]
    assert job.dlq.stats.publish_failures >= 1
    assert job.dlq.spill_count() == 1
    # 本地 DLQ 事实文件仍在，溢出是额外副本，消息不丢
    assert len(job.dlq.read_all()) == 1
    spilled = list((root / "dead_letters" / "_spill").glob("spill-*.jsonl"))
    assert len(spilled) == 1


def test_spill_is_drained_after_downstream_recovery():
    root = _root()
    failing = FailingDeadLetterSink()
    j1 = _job(root, dlq_downstream=failing)
    j1.run_raw([RawMessage("{bad1", offset=1), RawMessage("{bad2", offset=2)])
    assert j1.dlq.spill_count() == 2

    # 下游恢复：换一个健康 sink 重启，drain 补投并清理溢出文件。
    healthy = InMemoryDeadLetterSink()
    j2 = Job(make_dim(), root=root, batch_size=50, dlq_downstream=healthy)
    drained = j2.dlq.drain()
    assert drained == 2
    assert j2.dlq.spill_count() == 0
    assert {l.offset for l in healthy.published} == {1, 2}


def test_drain_keeps_files_when_downstream_still_down():
    root = _root()
    j1 = _job(root, dlq_downstream=FailingDeadLetterSink())
    j1.run_raw([RawMessage("{bad", offset=1)])
    assert j1.dlq.spill_count() == 1

    # 仍然不可用：drain 返回 0 且文件保留，等下一轮重试。
    j2 = _job(root, dlq_downstream=FailingDeadLetterSink())
    assert j2.dlq.drain() == 0
    assert j2.dlq.spill_count() == 1


def test_batch_failure_is_isolated_from_healthy_batches():
    """一批全坏不影响其它批次；坏批整体溢出，好批正常发布。"""
    root = _root()
    healthy = InMemoryDeadLetterSink()
    dlq = DeadLetterQueue(root, downstream=healthy)
    bad_batch = [
        make_dead_letter(
            RawMessage("{", partition=0, offset=i),
            DecodeError(FailureCategory.DESERIALIZATION, "bad"),
        )
        for i in range(3)
    ]
    # 先制造一个坏下游，让整批失败溢出
    dlq.downstream = FailingDeadLetterSink()
    result = dlq.accept_batch(bad_batch)
    assert result == {"accepted": 3, "duplicate": 0}
    # 整批写进一个溢出文件，文件内 3 条死信
    spill_files = list((root / "dead_letters" / "_spill").glob("spill-*.jsonl"))
    assert len(spill_files) == 1
    assert dlq.spill_count() == 3

    # 下游恢复后的新批次走正常发布
    dlq.downstream = healthy
    good_letter = make_dead_letter(
        RawMessage("[", partition=0, offset=9),
        DecodeError(FailureCategory.DESERIALIZATION, "bad2"),
    )
    dlq.accept_batch([good_letter])
    assert len(healthy.published) == 1
    assert dlq.stats.accepted == 4


def test_accept_batch_dedups_poison_without_republish():
    root = _root()
    dlq = DeadLetterQueue(root, downstream=InMemoryDeadLetterSink())
    msg = RawMessage("x", partition=0, offset=1)
    letter = make_dead_letter(
        msg, DecodeError(FailureCategory.DESERIALIZATION, "boom")
    )
    first = dlq.accept_batch([letter])
    second = dlq.accept_batch([letter])  # 重投递的同一毒丸
    assert first == {"accepted": 1, "duplicate": 0}
    assert second == {"accepted": 0, "duplicate": 1}
    assert len(dlq.read_all()) == 1
    assert len(dlq.downstream.published) == 1


# ---------------------------------------------------------------- 去重与重投递


def test_redelivered_valid_message_is_deduped():
    root = _root()
    job = _job(root)
    msg = _valid(0)
    job.run_raw([msg, redeliver(msg), redeliver(msg)])
    assert job.stats.emitted == 1
    assert job.stats.duplicates == 2
    assert len(_main_event_ids(job)) == 1


def test_dedup_state_survives_restart_via_checkpoint():
    root = _root()
    msgs = [_valid(i) for i in range(10)]

    j1 = _job(root, checkpoint_interval=5)
    j1.run_raw(msgs)  # 中途产生两次 checkpoint，seen_event_ids 已持久化
    assert (root / "_checkpoints" / "seen_event_ids.json").exists()

    # 重启后全量重放：全部命中去重，主表不出现第二行。
    j2 = _job(root, checkpoint_interval=5)
    report = j2.run_raw(msgs)
    assert report["job"]["duplicates"] == 10
    assert _main_event_ids(j2) == [f"EV{i:06d}" for i in range(10)]
    assert j2.snapshot()["duplicate_rows"] == 0


def test_dedup_only_committed_at_checkpoint_matches_replay_semantics():
    """checkpoint 之前崩溃：去重状态未落盘，重放会重复——与 sink 的重放口径一致。"""
    root = _root()
    msgs = [_valid(i) for i in range(10)]
    j1 = _job(root, checkpoint_interval=100000)
    j1.run_raw(msgs, crash_at=4)

    j2 = _job(root, checkpoint_interval=100000)
    j2.run_raw(msgs)  # 无 2PC + 无 checkpoint，重放区间重复写入（既有语义）
    assert j2.snapshot()["duplicate_rows"] > 0


def test_dedup_with_2pc_crash_is_exactly_once():
    """2PC + 去重台账：崩溃恢复后既不重复落表，也不重复处理。"""
    root = _root()
    msgs = [_valid(i) for i in range(20)]
    # 小批次让数据在 checkpoint 前就进入 staging，提交与去重提升才真实发生。
    j1 = _job(root, two_phase=True, batch_size=5, checkpoint_interval=5)
    j1.run_raw(msgs, crash_at=12)
    # 前 10 条已随两次 checkpoint 提交并去重落盘；10..11 在 staging，崩溃后 abort。
    assert sorted(j1.sink.committed_ids) == [0, 1]

    j2 = _job(root, two_phase=True, batch_size=5, checkpoint_interval=5)
    report = j2.run_raw(msgs)
    snap = j2.snapshot()
    assert snap["rows_written"] == 20
    assert snap["duplicate_rows"] == 0
    # 已提交区间重放时命中去重；abort 区间被重新处理（不丢、不重）。
    assert report["job"]["duplicates"] == 10


def test_2pc_crash_before_any_commit_replays_everything_once():
    """首次提交前崩溃：去重状态与 staging 一起回滚，重放不丢数据。"""
    root = _root()
    msgs = [_valid(i) for i in range(20)]
    # 批次很大，checkpoint 时没有可提交的 staging，去重状态绝不提前落盘。
    j1 = _job(root, two_phase=True, batch_size=1000, checkpoint_interval=5)
    j1.run_raw(msgs, crash_at=12)
    assert not (root / "_checkpoints" / "seen_event_ids.json").exists()

    j2 = _job(root, two_phase=True, batch_size=1000, checkpoint_interval=5)
    report = j2.run_raw(msgs)
    assert report["job"]["duplicates"] == 0      # 没有任何"已去重"状态被带入
    assert j2.snapshot()["rows_written"] == 20   # 全部重放且恰好一次


# ---------------------------------------------------------------- 顺序与吞吐


def test_poison_traffic_preserves_valid_ordering():
    """坏消息被摘走后，剩余消息的相对顺序与输入完全一致。"""
    root = _root()
    job = _job(root, processing_time=True)
    msgs: list[RawMessage] = []
    for i in range(100):
        msgs.append(_valid(i))
        if i % 10 == 4:
            msgs.append(RawMessage(f"{{broken {i}", partition=0, offset=1000 + i))
    job.run_raw(msgs)
    assert _main_event_ids(job) == [f"EV{i:06d}" for i in range(100)]


def test_poison_skip_is_constant_time_under_poison_flood():
    """毒丸洪峰：命中台账的消息走 O(1) 跳过，万条坏消息不拖垮吞吐。"""
    root = _root()
    job = _job(root)
    poison = RawMessage("{flood", partition=0, offset=7)
    # 首次落 DLQ，之后构造大量重投递（同指纹，台账命中即跳过）
    msgs = [poison] + [redeliver(poison) for _ in range(20000)]
    start = time.perf_counter()
    report = job.run_raw(msgs)
    elapsed = time.perf_counter() - start

    assert report["job"]["dead_lettered"] == 1
    assert report["job"]["poison_skipped"] == 20000
    assert elapsed < 5  # 纯跳过路径，2 万条应在毫秒级完成


def test_mixed_stream_throughput_not_starved_by_bad_messages():
    """坏消息占比很高时，正常消息仍按批次稳定落表，无阻塞、无丢失。"""
    root = _root()
    job = _job(root, batch_size=25)
    msgs: list[RawMessage] = []
    for i in range(500):
        msgs.append(_valid(i))
        if i % 3 == 0:
            msgs.append(RawMessage(f"{{bad {i}", partition=0, offset=2000 + i))
    report = job.run_raw(msgs)
    assert report["job"]["consumed"] == 500
    assert report["job"]["dead_lettered"] == 167  # 每 3 条好消息 1 条坏的
    assert len(_main_event_ids(job)) == 500
    assert job.sink.file_count() == 20  # 500 / 25，批次节奏不受坏消息影响


# ---------------------------------------------------------------- 与结构化入口的兼容


def test_structured_run_path_still_works_alongside_dlq():
    root = _root()
    events = generate(n=300, seed=5)
    job = _job(root)
    job.run(events)
    snap = job.snapshot()
    assert snap["rows_written"] == 300
    assert snap["dead_letters"]["count"] == 0
    assert snap["stats"]["job"]["dead_lettered"] == 0


def test_snapshot_exposes_dlq_accounting():
    root = _root()
    job = _job(root, dlq_downstream=FailingDeadLetterSink())
    job.run_raw([RawMessage("{x", offset=1), _valid(0)])
    dlq_view = job.snapshot()["dead_letters"]
    assert dlq_view["count"] == 1
    assert dlq_view["spilled"] == 1
    assert dlq_view["poison_ledger"] == 1
    assert dlq_view["stats"]["by_category"][FailureCategory.DESERIALIZATION.value] == 1
