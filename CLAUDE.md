# CLAUDE.md — CommandGraph Project Context

CommandGraph is a DSL and execution engine for declaring CLI command dependencies as a DAG. Two formats (`.cg` brace-delimited, `.cgr` indentation-based) produce identical ASTs. Single-file engine (`cgr.py`, Python 3.9+, zero deps). CLI is `cgr`.

Current notable features beyond the original core:

- Sub-graph inclusion as a step unit: `[deploy app] from ./deploy_app.cgr:`
- Wait gates: `wait for webhook "..."` and `wait for file "..."`
- Machine-readable apply output: `cgr apply FILE --output json`
- Execution timing persisted in state: per-step, per-wave, total run, latest bottleneck

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
7. **Sub-graph steps are outer-step state only**: parent state tracks the outer step; the included graph keeps its own state journal. Do not assume child resources are flattened into the parent DAG.
8. **Wait gates are first-class execution types**: webhook and file waits are not shell polling hacks. Preserve timeout, cancellation, and state semantics when changing them.
9. **Metrics live in the state journal**: `_wave` and `_run` records now coexist with per-resource entries. Tooling that compacts or reads state must preserve them.

## Known limitations

- Heredocs break the `.cgr` parser (use `printf` or `.cg`)
- No rollback (forward-only by design)
- Race cancellation doesn't kill already-started subprocesses
- `state test` only works for resources with `check` clauses
- HTTP syntax only supported in `.cgr` parser (not `.cg` yet)
- Sub-graph inclusion is currently `.cgr` step syntax, not `.cg` syntax
- `wait for webhook` starts a lightweight local listener during `apply`; it is intentionally local-only and not a distributed signal bus
- **State file isolation** requires explicit flags: by default the state file is `FILE.cgr.state`, keyed only to the graph file path. Use `--run-id ID` to salt the default state path or `--state FILE` to pin an explicit state file for concurrent parameterized runs.

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
- For composition, prefer sub-graph steps when the user wants another graph executed as one unit rather than flattened child resources.
- When touching state code, account for both resource entries and `_wave` / `_run` metric records.
- When touching apply output, keep `--report FILE.json` and `--output json` consistent by deriving both from the same result data when possible.
- Test with all example files after any change.
- Templates can be `.cgr` or `.cg` — all 44 stdlib are `.cgr`. Repo loader prefers `.cgr`.
- Reference `MANUAL.md` for syntax details, `.claude/docs/design_doc_*.md` for design rationale.
