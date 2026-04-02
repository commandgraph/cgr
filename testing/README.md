# CommandGraph Testing — Container-based Demos

Self-contained demo suite that runs in Podman containers. No SSH, no real infrastructure — every scenario uses local file operations or a disposable local stub service, so you can watch the engine work without configuring anything.

## Requirements

- Podman (or Docker as fallback)
- ~200MB disk for the container image (python:3.12-slim base)

## Quick start

```bash
# Run all 10 demos in sequence
./run-demos.sh

# Run a specific demo
./run-demos.sh 3          # crash recovery & state tracking
./run-demos.sh 7          # HTTP steps + collected reports

# Drop into an interactive shell
./run-demos.sh shell      # engine in PATH, examples in /opt/cgr/examples

# Just build the image
./run-demos.sh build

# List available demos
./run-demos.sh list
```

## Demos

| # | Name | What it shows | Time |
|---|------|--------------|------|
| 0 | Validate | Parses all example files, both formats | ~1s |
| 1 | Basic execution | Plan → apply → idempotent re-run | ~3s |
| 2 | Templates | Repo index, template expansion, dedup, provenance | ~3s |
| 3 | Crash recovery | Run, fail, inspect state, fix, resume, manual override | ~5s |
| 4 | Parallelism | parallel, each (3-at-a-time), stage/phase rolling deploy | ~5s |
| 5 | Race | Three mirrors compete, first wins, rest cancelled | ~3s |
| 6 | Drift detection | Apply, delete a file, state test detects drift, auto-fix | ~5s |
| 7 | HTTP + reports | Built-in HTTP verbs, status expectations, collected output reports | ~4s |
| 8 | CLI tooling | `fmt`, `lint`, `explain`, `why`, `convert` | ~4s |
| 9 | State isolation | `--run-id` and `--state` keep parameterized runs separate | ~4s |

Total suite runtime: ~35 seconds on a modern machine.

## How it works

The `run-demos.sh` script:

1. Builds a container image from `Containerfile` — installs the engine, repo, examples, and demo scripts
2. Runs each demo as an ephemeral container (`--rm`)
3. Each demo is a narrated bash script that explains what it's doing, runs the commands, and shows the output

The container image contains:

```
/opt/cgr/
├── cgr.py                   # the engine (in PATH as 'cgr')
├── repo/                    # template repository
├── examples/                # .cgr scenario files
│   ├── basic.cgr            # simple 8-step file pipeline
│   ├── cli-tooling-demo.cgr # lint/explain/why/convert graph
│   ├── crashtest.cgr        # step 3 fails until a file exists
│   ├── race-demo.cgr        # fast/slow/flaky mirror race
│   ├── fleet-deploy.cgr     # 6-server parallel + staged rollout
│   ├── http-report-demo.cgr # HTTP verbs + collected reports
│   ├── state-isolation-demo.cgr # run-id/state journal isolation
│   ├── template-demo.cgr    # uses repo templates
│   ├── nginx_setup.cg       # project example (structured)
│   ├── nginx_setup.cgr      # project example (readable)
│   ├── webserver.cg          # complex project example
│   ├── webserver.cgr         # complex project example
│   └── parallel_test.cgr    # all four parallel constructs
└── demos/                   # narrated demo scripts
    ├── lib.sh               # shared colors/helpers
    ├── 00-validate.sh
    ├── 01-basic-execution.sh
    ├── 02-templates.sh
    ├── 03-state-resume.sh
    ├── 04-parallel.sh
    ├── 05-race.sh
    ├── 06-drift-detection.sh
    ├── 07-http-report.sh
    ├── 08-cli-tooling.sh
    ├── 09-state-isolation.sh
    └── http_stub_server.py
```

## Interactive exploration

`./run-demos.sh shell` drops you into the container with the engine in PATH:

```bash
# Inside the container:
cgr validate /opt/cgr/examples/fleet-deploy.cgr
cgr plan /opt/cgr/examples/fleet-deploy.cgr
cgr apply /opt/cgr/examples/fleet-deploy.cgr
cgr state show /opt/cgr/examples/fleet-deploy.cgr

# Write your own graph:
cat > /workspace/test.cgr << 'EOF'
--- My test ---
target "local" local:
  [hello]:
    run $ echo "Hello from CommandGraph!"
EOF
cgr apply /workspace/test.cgr
```

## Design notes

The demos avoid external infrastructure. Most scenarios use only file operations (create, check, echo, cat), and the HTTP demo talks to a disposable local stub server started inside the container. The engine's SSH execution, privilege escalation (`as root`), timeouts, retries, and failure policies all work the same whether the command is `echo "hello" > /tmp/file`, `get "http://127.0.0.1:18080/health"`, or `ssh web-1 'systemctl restart nginx'` — the execution model doesn't care about the underlying command shape.

The crash recovery demo (03) is the most interesting: it runs a graph with a deliberate failure, shows the JSON Lines state file, applies a fix, and resumes — demonstrating the append-only journal and the resume matrix (skip success, re-run failed, run pending).
