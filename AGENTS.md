# AGENTS.md — CommandGraph Project Context

CommandGraph is a DSL and execution engine for declaring CLI command dependencies as a DAG. Two formats (`.cg` brace-delimited, `.cgr` indentation-based) produce identical ASTs. Development source lives in `cgr_src/`; the shipped single-file artifact `cgr.py` is generated from those modules. The CLI is `cgr`.

Recent engine changes to keep in mind:

- Sub-graph inclusion as a step unit: `[deploy app] from ./deploy_app.cgr:`
- Wait gates: `wait for webhook "..."` and `wait for file "..."`
- Machine-readable apply output: `cgr apply FILE --output json`
- Execution timing persisted in state via per-wave and per-run metric records

## File layout

```text
cgr_src/                 # Source modules used during development
cgr_dev.py               # Dev entrypoint that imports `cgr_src.cli:main()`
cgr.py                   # Generated single-file artifact shipped to users
MODULE_MAP.md            # Quick lookup for where to make changes
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
7. Sub-graph inclusion is an execution type, not resolver flattening: the parent graph tracks the outer step, and the child graph keeps its own state file.
8. Wait gates are first-class steps: preserve timeout, cancellation, and resume behavior; do not replace them with ad hoc shell polling unless intentionally redesigning the feature.
9. State journals now contain both resource records and metric records (`_wave`, `_run`). Any state tooling or compaction logic must preserve both.

## Known limitations

- Heredocs break the `.cgr` parser. Use `printf` or `.cg` instead.
- No rollback. The system is forward-only by design.
- Race cancellation does not kill subprocesses that already started.
- `state test` only works for resources with `check` clauses.
- HTTP syntax is only supported in the `.cgr` parser, not `.cg`, at present.
- Sub-graph inclusion currently uses `.cgr` step syntax; there is no matching `.cg` surface syntax yet.
- `wait for webhook` is a lightweight local listener for `apply`, not a general distributed signal system.
- State files are not run-instance unique: the state file is always `FILE.cgr.state`, keyed only to the graph file path, not to `--set` values. Two concurrent runs of the same graph with different parameters share one state file and identical resource IDs, so they can clobber each other. The advisory PID lock prevents simultaneous runs on the same machine, but it does not isolate distinct parameterized invocations. A future `--run-id` or `--state FILE` flag would be needed for per-invocation isolation.

## How to test

```bash
# Compile check
python3 -c "import py_compile; py_compile.compile('cgr.py', doraise=True)"

# Pytest suite
python3 -m pytest test_commandgraph.py -x -q

# Validate root feature-exercise example files
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
- Make code changes in `cgr_src/`, not `cgr.py`; the canonical rebuild path is `python3 cgr_dev.py apply build.cgr` (or `python3 cgr.py apply build.cgr`) after changing source modules, `ide.html`, or `visualize_template.py`.
- Treat `MANUAL.md` as the syntax authority and `COMMANDGRAPH_SPEC.md` as the formal grammar reference.
- Use `.claude/docs/design_doc_*.md` for historical design rationale when behavior is unclear.
- Keep `.cg` and `.cgr` behavior aligned. If you touch one parser or syntax path, inspect the corresponding path in the other format.
- If you touch apply output, keep `--output json` and `--report FILE.json` aligned by deriving both from the same execution/result data when possible.
- If you touch state handling, inspect both resource-entry behavior and `_wave` / `_run` metric behavior.
- If you touch sub-graph inclusion, preserve the contract that the child graph runs as a unit rather than as flattened parent resources.
- Test against the root feature-exercise graphs after changes, not just unit tests.
- Templates may exist in either `.cgr` or `.cg`, but the stdlib in `repo/` is `.cgr`, and the repo loader prefers `.cgr`.
- Be cautious with state semantics, dependency resolution, and desugaring changes. Small regressions in those areas can silently break resume behavior or execution ordering.
