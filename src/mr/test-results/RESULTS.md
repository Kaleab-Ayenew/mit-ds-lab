# MapReduce Lab — Test Results

**Date:** 2026-09-23  
**Environment:** Linux (Pop!_OS), Go 1.25.4, amd64  
**Working directory:** `6.5840/src/main`

## Summary

| Run | Command | Result | Wall time |
|-----|---------|--------|-----------|
| Full suite | `bash test-mr.sh` | **PASS** (7/7) | ~92 s |
| Stress test | `bash test-mr-many.sh 3` | **PASS** (3/3 trials) | ~319 s total (~106 s/trial) |

## Individual tests (`test-mr.sh`)

| # | Test | Result |
|---|------|--------|
| 1 | wc | PASS |
| 2 | indexer | PASS |
| 3 | map parallelism | PASS |
| 4 | reduce parallelism | PASS |
| 5 | job count | PASS |
| 6 | early exit | PASS |
| 7 | crash recovery | PASS |

## Stress test (`test-mr-many.sh 3`)

| Trial | Result |
|-------|--------|
| 1 | PASSED ALL TESTS |
| 2 | PASSED ALL TESTS |
| 3 | PASSED ALL TESTS |

**Final:** `*** PASSED ALL 3 TESTING TRIALS`

## Baseline comparison

MIT publishes reference timings from `make mr` (Go unit tests with `-race`):

| Metric | MIT reference | This implementation |
|--------|---------------|---------------------|
| Full suite | ~87 s | ~92 s |
| Crash test alone | ~40 s | variable (~40–180 s) |

Times are in the same ballpark. The lab grades correctness and parallelism, not raw speed.

## Artifacts

| File | Description |
|------|-------------|
| `test-mr-2026-09-23.summary.txt` | Pass/fail lines from full suite |
| `test-mr-2026-09-23.full.log` | Complete stdout from full suite run |
| `test-mr-many-2026-09-23.summary.txt` | Pass/fail lines from 3-trial stress run |
| `test-mr-many-2026-09-23.full.log` | Complete stdout from stress run |

## How to reproduce

```bash
cd 6.5840/src/main
bash test-mr.sh
bash test-mr-many.sh 3
```
