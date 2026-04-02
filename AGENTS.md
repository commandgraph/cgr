# AGENTS.md — CommandGraph Project Context

CommandGraph is a DSL and execution engine for declaring CLI command dependencies as a DAG. Two formats (`.cg` brace-delimited, `.cgr` indentation-based) produce identical ASTs. The engine is a single file (`cgr.py`), requires Python 3.9+, and has zero dependencies. The CLI is `cgr`.

## File layout

```text
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

```text
.cg  → §1 Lexer → §3 Parser ─┐
                               ├→ §2 AST → §4 Repo → §5 Resolver → waves
.cgr → §3b CGR Parser ────────┘              │
                                    §6 Plan/Apply + §6b State
                                    §7 DOT │ §8 HTML Viz │ §9 IDE+CLI
```

## Critical invariants

1. Crash safety: A resource is written to `.state` only after execution completes. Missing from state means it must re-run on resume.
2. Resume matrix: `success` / `skip_check` / `skip_when` skip. `warned` / `failed` / `timeout` re-run. Missing means run.
3. Both formats stay in sync: The resolver does not know which parser produced the AST. Parser changes must preserve parity between `.cg` and `.cgr` unless the difference is explicitly intentional and documented.
4. Parallel desugaring: `parallel`, `race`, `each`, and `stage` all desugar into gate → fork → join DAG patterns. Do not add executor-only special cases unless that architecture is being intentionally changed.
5. Cross-node dedup is disabled: The identity hash includes `node_name`, so identical commands on different hosts are never deduplicated.
6. HTTP steps are first-class: `get` / `post` / `put` / `patch` / `delete` desugar into the same `(rc, stdout, stderr)` execution model. Local targets use `urllib`; SSH targets construct `curl` commands. Auth tokens must remain redacted.

## Known limitations

- Heredocs break the `.cgr` parser. Use `printf` or `.cg` instead.
- No rollback. The system is forward-only by design.
- Race cancellation does not kill subprocesses that already started.
- `state test` only works for resources with `check` clauses.
- HTTP syntax is only supported in the `.cgr` parser, not `.cg`, at present.
- State files are not run-instance unique: the state file is always `FILE.cgr.state`, keyed only to the graph file path, not to `--set` values. Two concurrent runs of the same graph with different parameters share one state file and identical resource IDs, so they can clobber each other. The advisory PID lock prevents simultaneous runs on the same machine, but it does not isolate distinct parameterized invocations. A future `--run-id` or `--state FILE` flag would be needed for per-invocation isolation.

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

- Prefer changes that preserve English-like syntax. In `.cgr`, use `first`, not `after`, `before`, or `needs`.
- Treat `MANUAL.md` as the syntax authority and `COMMANDGRAPH_SPEC.md` as the formal grammar reference.
- Use `.claude/docs/design_doc_*.md` for historical design rationale when behavior is unclear.
- Keep `.cg` and `.cgr` behavior aligned. If you touch one parser or syntax path, inspect the corresponding path in the other format.
- Test against the example graphs after changes, not just unit tests.
- Templates may exist in either `.cgr` or `.cg`, but the stdlib in `repo/` is `.cgr`, and the repo loader prefers `.cgr`.
- Be cautious with state semantics, dependency resolution, and desugaring changes. Small regressions in those areas can silently break resume behavior or execution ordering.
