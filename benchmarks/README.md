# CommandGraph Parallelism Benchmark

Measures how CommandGraph's parallel execution primitives scale across multiple SSH targets.
Spawns N containers as deployment targets, then runs the same workload with different concurrency strategies and compares wall-clock time.

## Requirements

- Podman (or Docker as fallback)
- ~500MB disk for container images
- Approximately 1 minute for the default 8-target run

## Quick start

```bash
./run-benchmark.sh          # 8 targets (default)
./run-benchmark.sh 12       # 12 targets
./run-benchmark.sh shell    # interactive shell with targets running
./run-benchmark.sh teardown # clean up everything
```

## What it measures

Five scenarios deploy the **same workload** to the **same N hosts**, varying only the concurrency:

| Scenario | Strategy | Expected behavior |
|----------|----------|-------------------|
| Sequential | `--parallel 1`, separate target blocks | One host at a time. Baseline. |
| Each (2) | `each host, 2 at a time` | Sliding window of 2 concurrent SSH sessions |
| Each (4) | `each host, 4 at a time` | Sliding window of 4 |
| Each (all) | `each host, N at a time` | All hosts concurrently |
| Staged | `stage/phase`: 1 canary → half → rest | Phased rollout with verification gate |

## Workload per host

Each target receives 6 steps over SSH, totaling ~1 second of simulated work:

1. **Create workspace** — `mkdir -p /opt/bench` (as root, chown to deploy)
2. **Download package** — `sleep 0.3` + write file (simulates download)
3. **Compile binary** — `sleep 0.3` + write file (simulates build)
4. **Write config** — write config file (instant)
5. **Install binary** — `sleep 0.2` + copy to `/usr/local/bin` (as root)
6. **Verify** — check all files exist

The sleeps are essential — without them, SSH connection setup dominates and you're benchmarking OpenSSH, not CommandGraph.

## Expected results (8 targets)

```
Scenario                           Time   Speedup
─────────────────────────────────────────────────────
Sequential (1 at a time)          ~9.0s    1.00x
Parallel each (2 at a time)       ~4.8s    1.88x
Parallel each (4 at a time)       ~2.7s    3.33x
Parallel each (all at once)       ~1.8s    5.00x
Staged (1 → half → rest)          ~3.5s    2.57x
```

The speedup is sub-linear because:
- SSH connection setup has fixed overhead (~50ms per connection)
- The staged scenario has sequential phase barriers (canary must complete and verify before the next phase starts)
- Thread scheduling overhead increases with concurrency

## Architecture

```
┌───────────────────────────┐
│   Control container       │
│   (cgr engine)   │
│                           │    ┌──────────┐
│   generates .cgr files    │───→│ target-01 │──┐
│   for each scenario,     │    └──────────┘  │
│   times each run,        │    ┌──────────┐  │
│   prints comparison      │───→│ target-02 │  ├── cg-bench-net
│                           │    └──────────┘  │   (podman network)
│   SSH keys pre-shared    │    ┌──────────┐  │
│   at build time          │───→│ target-03 │  │
│                           │    └──────────┘  │
│                           │         ...      │
│                           │    ┌──────────┐  │
│                           │───→│ target-08 │──┘
│                           │    └──────────┘
└───────────────────────────┘
```

The control container generates `.cgr` graph files dynamically based on the target count, then runs `cgr apply` for each scenario. State files are cleared between runs so every step executes fresh.

## Team

Benchmark designed by:
- **Marcus Chen (SRE)** — infrastructure and benchmark harness
- **Priya Kapoor (Systems SRE)** — `each`/`stage` scenario design
- **Dr. Elena Voss (CS)** — measurement methodology, workload calibration
