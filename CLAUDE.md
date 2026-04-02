# CLAUDE.md — CommandGraph Project Context

CommandGraph is a DSL and execution engine for declaring CLI command dependencies as a DAG. Two formats (`.cg` brace-delimited, `.cgr` indentation-based) produce identical ASTs. Single-file engine (`cgr.py`, Python 3.9+, zero deps). CLI is `cgr`.

## File layout

```
cgr.py                   # Engine (~8000 lines, single file)
ide.html                 # Web IDE frontend, served by `cgr serve`
MANUAL.md                # Authoritative syntax reference
COMMANDGRAPH_SPEC.md     # Formal language spec (PEG grammar, for code generators)
repo/                    # Template repository (44 .cgr stdlib templates)
testing/                 # Container demos (7 local demos)
testing-ssh/             # Container SSH demos (5 two-node demos)
benchmarks/              # Parallelism benchmarks
.claude/docs/            # Design docs (historical)
```

## Engine pipeline

```
.cg  → §1 Lexer → §3 Parser ─┐
                               ├→ §2 AST → §4 Repo → §5 Resolver → waves
.cgr → §3b CGR Parser ────────┘              │
                                    §6 Plan/Apply + §6b State
                                    §7 DOT │ §8 HTML Viz │ §9 IDE+CLI
```

## Critical invariants

1. **Crash safety**: A resource is written to `.state` only AFTER execution completes. Missing from state = re-runs on resume.
2. **Resume matrix**: `success`/`skip_check`/`skip_when` → skip. `warned`/`failed`/`timeout` → re-run. Missing → run.
3. **Both formats in sync**: The resolver doesn't know which parser produced the AST. Changes to one parser must be mirrored.
4. **Parallel desugaring**: All constructs (`parallel`, `race`, `each`, `stage`) desugar into gate→fork→join DAG patterns. No special executor concepts.
5. **Cross-node dedup disabled**: Identity hash includes node_name — same command on different hosts is never deduplicated.
6. **HTTP steps**: HTTP verbs (`get`/`post`/`put`/`patch`/`delete`) are first-class. They desugar into the same `(rc, stdout, stderr)` execution model. Local targets use `urllib`, SSH targets construct `curl` commands. Auth tokens are auto-redacted.

## Known limitations

- Heredocs break the `.cgr` parser (use `printf` or `.cg`)
- No rollback (forward-only by design)
- Race cancellation doesn't kill already-started subprocesses
- `state test` only works for resources with `check` clauses
- HTTP syntax only supported in `.cgr` parser (not `.cg` yet)
- **State files are not run-instance–unique** (unimplemented): The state file is always `FILE.cgr.state`, keyed only to the graph file path — not to the `--set` variable values. Two concurrent runs of the same `.cgr` with different parameters (e.g., `--set node=worker-1` vs `--set node=worker-2`) share one state file and identical resource IDs, so they clobber each other. The advisory PID lock (written as a sentinel in the state file) prevents two simultaneous runs on the same machine, but it does not isolate distinct parameterized invocations. A future `--run-id` or `--state FILE` flag would allow per-invocation state isolation.

## How to test

```bash
# Compile check
python3 -c "import py_compile; py_compile.compile('cgr.py', doraise=True)"

# Pytest suite
python3 -m pytest test_commandgraph.py -x -q

# Validate all example files
cgr validate nginx_setup.cg && cgr validate nginx_setup.cgr
cgr validate webserver.cg --repo ./repo && cgr validate webserver.cgr --repo ./repo
cgr validate parallel_test.cgr && cgr validate multinode_test.cgr && cgr validate multinode_test.cg
cgr validate system_audit.cgr --repo ./repo
cgr validate api_integration.cgr

# Container demos (if available)
testing/run-demos.sh          # 7 local demos
testing-ssh/run-ssh-demos.sh  # 5 SSH demos
```

## Working guidelines

- Syntax should read like English. `.cgr` uses `first` (not `after`/`before`/`needs`).
- Test with all example files after any change.
- Templates can be `.cgr` or `.cg` — all 44 stdlib are `.cgr`. Repo loader prefers `.cgr`.
- Reference `MANUAL.md` for syntax details, `.claude/docs/design_doc_*.md` for design rationale.
