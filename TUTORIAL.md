# Tutorial: Your First Hour with CommandGraph

This tutorial takes you from zero to writing real infrastructure graphs. Each lesson
builds on the previous one, with a complete runnable example and expected output.

**Prerequisites**: Python 3.9+, a terminal, a text editor.

---

## Lesson 1: Hello World

Every CommandGraph file (`.cgr`) has three parts: a title, a target, and steps.

Create `lesson1.cgr`:

```
--- Hello World ---

target "local" local:

  [say hello]:
    run $ echo "Hello from CommandGraph!"
```

Run it:

```bash
cgr apply lesson1.cgr
```

**Expected output:**

```
── Wave 1/1 (1 resources) ──
  [1/1] ✓ ok    say_hello
              say hello
              2ms
```

**What just happened:**
- `--- Hello World ---` is a title comment (optional, for humans)
- `target "local" local:` declares a target named "local" that runs commands locally
- `[say hello]:` is a step — you choose the name, brackets are required
- `run $ echo "..."` is the shell command to execute

---

## Lesson 2: Dependencies

Steps often depend on each other. Use `first` to declare ordering.

Create `lesson2.cgr`:

```
--- Dependencies ---

target "local" local:

  [create directory]:
    run $ mkdir -p /tmp/cgr-tutorial

  [write greeting]:
    first [create directory]
    run $ echo "Hello, world!" > /tmp/cgr-tutorial/greeting.txt

  [read greeting]:
    first [write greeting]
    run $ cat /tmp/cgr-tutorial/greeting.txt
```

See the execution plan first:

```bash
cgr plan lesson2.cgr
```

**Expected output:**

```
── Wave 1 (1 resources) ──
    ▶ run        create_directory

── Wave 2 (1 resources) ──
    ▶ run        write_greeting             needs [create_directory]

── Wave 3 (1 resources) ──
    ▶ run        read_greeting              needs [write_greeting]
```

Now execute:

```bash
cgr apply lesson2.cgr
```

**Key concept:** `first [create directory]` means "run that step before me." Steps
without dependencies run in the earliest possible wave. The engine figures out the
optimal ordering automatically.

---

## Lesson 3: Idempotency with `skip if`

Running a graph twice should be safe. The `skip if` clause makes this possible —
if the check command succeeds (exit 0), the step is skipped.

Create `lesson3.cgr`:

```
--- Idempotent Steps ---

target "local" local:

  [create directory]:
    skip if $ test -d /tmp/cgr-tutorial
    run $ mkdir -p /tmp/cgr-tutorial

  [write config]:
    first [create directory]
    skip if $ test -f /tmp/cgr-tutorial/config.ini
    run $ printf "[app]\nport = 8080\ndebug = false\n" > /tmp/cgr-tutorial/config.ini

  [show config]:
    first [write config]
    run $ cat /tmp/cgr-tutorial/config.ini
```

```bash
cgr apply lesson3.cgr          # First run: all steps execute
cgr apply lesson3.cgr          # Second run: first two skip, third re-runs
```

**Expected second run:**

```
  [1/3] ✓ skip   create_directory
              check passed — already satisfied

  [2/3] ✓ skip   write_config
              check passed — already satisfied

  [3/3] ✓ ok     show_config
```

**Key concept:** `skip if $ test -f /tmp/cgr-tutorial/config.ini` runs the test
command. If it exits 0 (file exists), the step's `run` command is skipped. This
makes your entire graph safe to re-run — steps that already succeeded are skipped.

---

## Lesson 4: Variables

Hardcoded paths and values make graphs brittle. Variables fix that.

Create `lesson4.cgr`:

```
--- Variables ---
set app_name = "myapp"
set app_dir = "/tmp/cgr-tutorial/${app_name}"
set port = "8080"

target "local" local:

  [create app directory]:
    skip if $ test -d ${app_dir}
    run $ mkdir -p ${app_dir}

  [write config]:
    first [create app directory]
    run $ printf "[server]\nname = ${app_name}\nport = ${port}\n" > ${app_dir}/config.ini

  [show config]:
    first [write config]
    run $ cat ${app_dir}/config.ini
```

Run with defaults, then override:

```bash
cgr apply lesson4.cgr                        # Uses port=8080
cgr apply lesson4.cgr --set port=3000        # Overrides to port=3000
```

**Key concept:** `set` declares variables. `${var}` expands them. `--set` on the
command line overrides any variable. This lets you write one graph and customize
it per environment.

---

## Lesson 5: The Web IDE

CommandGraph includes a browser-based IDE with live visualization.

```bash
cgr serve lesson4.cgr
```

This opens a three-panel interface:

- **Left:** Code editor with syntax highlighting
- **Right:** Live dependency graph that updates as you type
- **Bottom:** Execution log with Apply button

Try editing a step name in the editor — watch the graph update instantly. Click
**Apply** to execute. Click any node in the graph to inspect its details.

For remote or headless environments:

```bash
cgr serve lesson4.cgr --host 0.0.0.0 --no-open
# Then open http://your-host:8420 in a browser
```

---

## Lesson 6: Crash Recovery

What happens when a step fails? CommandGraph saves progress to a state file, so
you can fix the problem and resume without re-running completed steps.

Create `lesson6.cgr`:

```
--- Crash Recovery ---

target "local" local:

  [step one]:
    skip if $ test -f /tmp/cgr-tutorial/step1.done
    run $ echo "Step 1 complete" && touch /tmp/cgr-tutorial/step1.done

  [step two]:
    first [step one]
    skip if $ test -f /tmp/cgr-tutorial/step2.done
    run $ echo "Step 2 complete" && touch /tmp/cgr-tutorial/step2.done

  [step three]:
    first [step two]
    skip if $ test -f /tmp/cgr-tutorial/step3.done
    run $ echo "Step 3" && ls /nonexistent/path
```

Step three will fail (the path doesn't exist):

```bash
mkdir -p /tmp/cgr-tutorial
cgr apply lesson6.cgr
```

**Expected:** Steps 1 and 2 succeed, step 3 fails.

Inspect the state:

```bash
cgr state show lesson6.cgr
```

You'll see `step_one: success`, `step_two: success`, `step_three: failed`.

Now fix step three (edit the file to use a valid path) and re-run:

```bash
cgr apply lesson6.cgr
```

**Steps 1 and 2 are skipped** (already in state as `success`). Only step 3 re-runs.

Other state commands:

```bash
cgr state show  lesson6.cgr              # View all step states
cgr state reset lesson6.cgr              # Clear state, start fresh
cgr state set   lesson6.cgr step_three redo   # Force one step to re-run
cgr state test  lesson6.cgr              # Check for drift (re-run checks)
```

---

## Lesson 7: Templates

Templates are reusable step patterns. CommandGraph ships with a standard library
of templates for common tasks.

Create `lesson7.cgr`:

```
--- Using Templates ---
using apt/install_package

target "local" local:

  [install curl] from apt/install_package:
    name = "curl"

  [install wget] from apt/install_package:
    name = "wget"
```

See what this expands to:

```bash
cgr plan lesson7.cgr --repo ./repo -v
```

**Expected:** Each `install_package` call expands into two steps: `update_cache`
(shared and deduplicated) and the actual `install` step. The engine detects that
both calls need `update_cache` and runs it only once.

```bash
cgr validate lesson7.cgr --repo ./repo
```

```
  3 resource(s), 2 wave(s)
  1 deduplication(s) across templates
```

The template library lives in `repo/`. You can write your own templates — they're
just `.cgr` files with a `template name(params):` block.

---

## Lesson 8: Remote Targets (SSH)

To run commands on a remote host, change `local` to `ssh user@host`:

```
--- Remote Execution ---
set host = "10.0.1.5"
set user = "deploy"

target "web" ssh ${user}@${host}:

  [check uptime]:
    run $ uptime

  [check disk]:
    run $ df -h /
```

```bash
cgr ping remote.cgr       # Test SSH connectivity first
cgr apply remote.cgr      # Execute remotely
```

**Key concepts:**
- `ssh ${user}@${host}` — the engine SSHes to the target for every command
- `cgr ping` verifies connectivity before you run anything
- SSH ControlMaster pooling is automatic — multiple steps reuse one connection
- State is tracked locally, commands execute remotely

You can mix local and remote targets in the same graph:

```
target "builder" local:
  [build release]:
    run $ make release

target "web" ssh deploy@10.0.1.5, after "builder":
  [deploy]:
    first [builder/build release]
    run $ ./deploy.sh
```

---

## Lesson 9: What's Next

You now know the core concepts. Here's where to go from here:

### Parallel execution

Run steps concurrently, race for the fastest option, or deploy to a fleet:

```
[configure everything]:
  parallel 3 at a time:
    [write config a]:
      run $ echo "config a"
    [write config b]:
      run $ echo "config b"
    [write config c]:
      run $ echo "config c"
```

See `parallel_test.cgr` for a full example with `parallel`, `race`, `each`, and `stage`.

### Multi-node orchestration

Coordinate across database, web, and cache servers with cross-node dependencies:

```
target "db" ssh admin@db-host:
  [setup schema]:
    run $ psql -f schema.sql

target "web" ssh deploy@web-host, after "db":
  [deploy app]:
    first [db/setup schema]
    run $ ./deploy.sh
```

See `multinode_test.cgr` for a complete 4-node example.

### Conditional execution

Run steps only when conditions are met:

```
[install apt packages]:
  when os_family == "debian"
  run $ apt install -y nginx
```

### Tag-based filtering

Tag steps and run subsets:

```
[configure firewall]:
  tags security, network
  run $ ufw allow 80
```

```bash
cgr apply infra.cgr --tags security        # Only security-tagged steps
cgr apply infra.cgr --skip-tags network    # Everything except network
```

### Data collection and reporting

Collect command output for reporting:

```
[check disk]:
  run $ df -h
  collect "disk_usage"
```

```bash
cgr report infra.cgr --format csv
```

### Advanced feature examples

Each of these is a standalone, runnable `.cgr` file in the `examples/` directory:

| Feature | Example file | Key syntax |
|---------|-------------|------------|
| Conditional flags | `examples/01_conditional_flags.cgr` | `flag "..." when "..."` |
| Assertions | `examples/02_assertions.cgr` | `assert $ CMD, "msg"` |
| Retry with backoff | `examples/03_retry_backoff.cgr` | `retry Nx backoff Xs..Ys` |
| Collect and reduce | `examples/04_reduce_collect.cgr` | `reduce "key"` |
| External scripts | `examples/05_script_external.cgr` | `script "path"` |
| Runtime variables | `examples/06_success_failure_vars.cgr` | `on success: set` |
| Secrets | `examples/07_secrets.cgr` | `set x = secret "env:Y"` |
| Inventory files | `examples/08_inventory_fleet.cgr` | `inventory "file.ini"` |
| Cross-node deps | `examples/09_cross_node.cgr` | `first [node/step]` |
| All combined | `examples/10_kitchen_sink.cgr` | Multiple features |

### Further reading

| Document | What it covers |
|----------|---------------|
| [COOKBOOK.md](COOKBOOK.md) | 10 real-world recipes with complete examples |
| [MANUAL.md](MANUAL.md) | Complete syntax reference for both `.cgr` and `.cg` formats |
| [COMMANDGRAPH_SPEC.md](COMMANDGRAPH_SPEC.md) | Formal language specification (for code generators) |

### Clean up tutorial files

```bash
rm -rf /tmp/cgr-tutorial
rm -f lesson*.cgr .state/lesson*.cgr.state
```
