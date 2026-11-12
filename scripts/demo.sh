#!/bin/sh
# demo.sh —— 90 秒内录完的固定演示。
#
#   1. 生成 3000 条事件（含 4% 迟到）
#   2. 默认配置跑一遍：迟到被吸收，侧输出 0
#   3. 崩溃恢复：重复 800 条
#   4. 打开 2PC：同样崩溃但恰好一次
#   5. 切事件时间：迟到进侧输出
#   6. 小文件演示
set -eu
cd "$(dirname "$0")/.."

say() { printf '\n=== %s ===\n' "$1"; }

EV=data/events.jsonl

say "1. 生成事件流"
python3 -m streamforge replay --n 3000 --out "$EV"

say "2. 默认配置（处理时间 / 无 2PC）"
rm -rf /tmp/sf-baseline
SINK_ROOT=/tmp/sf-baseline python3 -m streamforge verify --input "$EV" --max-files 50 || true

say "3. 在 800 条处崩溃，然后重放（无 2PC → 重复）"
rm -rf /tmp/sf-crash
SINK_ROOT=/tmp/sf-crash python3 -m streamforge verify --input "$EV" --crash-at 800 || true

say "4. 同样崩溃，但开启 2PC（→ 恰好一次）"
rm -rf /tmp/sf-2pc
SINK_ROOT=/tmp/sf-2pc TWO_PHASE_COMMIT=1 python3 -m streamforge verify --input "$EV" --crash-at 800 || true

say "5. 切换到事件时间（→ 迟到进侧输出）"
rm -rf /tmp/sf-evt
SINK_ROOT=/tmp/sf-evt PROCESSING_TIME=0 python3 -m streamforge verify --input "$EV" --max-files 50 || true

say "6. 小文件（BATCH_SIZE=50）"
rm -rf /tmp/sf-small
SINK_ROOT=/tmp/sf-small BATCH_SIZE=50 python3 -m streamforge verify --input "$EV" --max-files 20 || true

say "7. 维表 reload 空窗"
SINK_ROOT=/tmp/sf-dim python3 -m streamforge dim-reload --n 2000

echo
