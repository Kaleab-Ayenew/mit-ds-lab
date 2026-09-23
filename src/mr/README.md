# Lab 1: MapReduce

MIT 6.5840 distributed MapReduce implementation.

## Files

| File | Role |
|------|------|
| `coordinator.go` | Task scheduling, timeouts, crash recovery |
| `worker.go` | Map/reduce execution, RPC client |
| `rpc.go` | Shared RPC types |

## Features

- Coordinator assigns map and reduce tasks over UNIX-domain RPC
- 10-second task timeout with reassignment on worker failure
- Atomic temp-file + rename for intermediate and output files
- Separate map and reduce phase tracking with completion RPCs

## Running tests

From `6.5840/src/main`:

```bash
bash test-mr.sh          # all 7 tests (~90 s)
bash test-mr-many.sh 3   # 3 consecutive full runs (~5 min)
```

## Test results

**Status: PASS** — all tests passed on 2026-09-23.

| # | Test | Result |
|---|------|--------|
| 1 | wc | PASS |
| 2 | indexer | PASS |
| 3 | map parallelism | PASS |
| 4 | reduce parallelism | PASS |
| 5 | job count | PASS |
| 6 | early exit | PASS |
| 7 | crash recovery | PASS |

**Stress test:** `test-mr-many.sh 3` — 3/3 trials passed.

| Run | Wall time |
|-----|-----------|
| `test-mr.sh` | ~92 s |
| `test-mr-many.sh 3` | ~319 s |

Compared to the MIT lab reference (~87 s for `make mr`), these times are within normal variance.

Full logs and a detailed report: [`test-results/RESULTS.md`](test-results/RESULTS.md)
