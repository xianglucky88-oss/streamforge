SHELL := /bin/sh
PY ?= python3
N ?= 5000

.PHONY: help
help:
	@echo "make replay   生成事件流到 data/events.jsonl"
	@echo "make run      跑一次作业"
	@echo "make verify   一致性断言（含维表版本与溯源）"
	@echo "make demo     固定演示序列（90 秒内可录完）"
	@echo "make test     单元测试"
	@echo "make crash    演示崩溃恢复（无 2PC → 重复）"
	@echo "make exactly-once  演示 2PC 下恰好一次"
	@echo "make small-files   演示小文件数量"
	@echo "make dim      演示维表原子版本切换（无空窗）"

.PHONY: replay
replay:
	$(PY) -m streamforge replay --n $(N) --out data/events.jsonl

.PHONY: run
run:
	$(PY) -m streamforge run

.PHONY: verify
verify:
	$(PY) scripts/verify_stream.py --n $(N)

.PHONY: demo
demo:
	sh scripts/demo.sh

.PHONY: test
test:
	$(PY) -m pytest -q

.PHONY: crash
crash:
	rm -rf /tmp/sf-crash
	SINK_ROOT=/tmp/sf-crash $(PY) -m streamforge verify --input data/events.jsonl --crash-at 800 || true

.PHONY: exactly-once
exactly-once:
	rm -rf /tmp/sf-2pc
	SINK_ROOT=/tmp/sf-2pc TWO_PHASE_COMMIT=1 $(PY) -m streamforge verify --input data/events.jsonl --crash-at 800

.PHONY: small-files
small-files:
	rm -rf /tmp/sf-small
	SINK_ROOT=/tmp/sf-small BATCH_SIZE=50 $(PY) -m streamforge verify --input data/events.jsonl --max-files 20 || true

.PHONY: dim
dim:
	rm -rf /tmp/sf-dim
	SINK_ROOT=/tmp/sf-dim $(PY) -m streamforge dim-reload --n 2000 --delay-ms 20
