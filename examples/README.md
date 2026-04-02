# CommandGraph Examples

Standalone, runnable examples demonstrating advanced `.cgr` features.
Run any example with: `cgr apply examples/NN_name.cgr`

| # | File | Features |
|---|------|----------|
| 01 | `01_conditional_flags.cgr` | `flag "..." when "..."`, `env KEY when "..."` |
| 02 | `02_assertions.cgr` | `assert $ CMD, "message"` |
| 03 | `03_retry_backoff.cgr` | `until "VALUE"`, `retry Nx backoff Xs..Ys jitter Z%` |
| 04 | `04_reduce_collect.cgr` | `collect "key"`, `reduce "key"`, `each` iteration |
| 05 | `05_script_external.cgr` | `script "path"` external script execution |
| 06 | `06_success_failure_vars.cgr` | `on success: set`, `on failure: set`, `when` branching |
| 07 | `07_secrets.cgr` | `set x = secret "env:KEY"`, `"file:PATH"`, `"cmd:CMD"` |
| 08 | `08_inventory_fleet.cgr` | `inventory "file.ini"`, fleet iteration |
| 09 | `09_cross_node.cgr` | `first [node/step]`, `after "node"` cross-target deps |
| 10 | `10_kitchen_sink.cgr` | All features combined in a CI-like pipeline |

**Prerequisites:** Python 3.9+, `cgr.py` on PATH (or use `python3 cgr.py`).

Start with the [Tutorial](../TUTORIAL.md) if you are new to CommandGraph.
