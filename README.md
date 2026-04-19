# CommandGraph

CommandGraph is a DSL and execution engine for declaring CLI command dependencies as a directed acyclic graph. Write a plain-text file describing your steps; the engine resolves dependencies, parallelizes independent work, and executes locally or over SSH.

One Python file, zero dependencies. Python 3.9+.

## Installation

```bash
curl -O https://raw.githubusercontent.com/commandgraph/cgr/main/cgr.py
chmod +x cgr.py
sudo mv cgr.py /usr/local/bin/cgr

# or run directly
python3 cgr.py apply mysetup.cgr
```

---

## A single step

The minimal unit is a named step with a command:

```python
target "local" local:

  [say hello]:
    run $ echo "Hello, world"
```

```bash
cgr apply hello.cgr
```

---

## Dependencies and idempotency

`first` declares what must run before a step. `skip if` checks before running and skips if the work is already done:

```python
target "local" local:

  [create dir]:
    skip if $ test -d /tmp/myapp
    run    $ mkdir -p /tmp/myapp

  [write file]:
    first [create dir]
    skip if $ test -f /tmp/myapp/index.html
    run    $ echo "<h1>Hello</h1>" > /tmp/myapp/index.html
```

Run it twice — the second run skips both steps. The engine shows the execution plan before running:

```bash
cgr plan hello.cgr   # show waves and dependency order without executing
```

---

## SSH targets

Point a target at an SSH host and every command runs remotely. State stays on your machine. No agent or runtime needed on the server.

```python
target "web" ssh deploy@10.0.1.5:

  [install nginx] as root:
    skip if $ command -v nginx
    run    $ apt-get install -y nginx

  [start nginx] as root:
    first [install nginx]
    skip if $ systemctl is-active nginx
    run    $ systemctl reload-or-restart nginx

  verify "site is live":
    first [start nginx]
    run $ curl -sf http://localhost/
    retry 3x wait 2s
```

`as root` wraps commands in `sudo` on the remote side. Multiple targets in one file run in parallel. `verify` is a labeled smoke test — it runs last and its failure is reported distinctly.

---

## File management

Write configs, patch lines, manage INI files — all with built-in atomic validation:

```python
[write nginx config]:
  content > /etc/nginx/sites-available/myapp:
    server {
        listen 80;
        server_name example.com;
    }
  validate $ nginx -t

[harden sshd]:
  line "PermitRootLogin no" in /etc/ssh/sshd_config, replacing "^#?PermitRootLogin"
  line "PasswordAuthentication no" in /etc/ssh/sshd_config, replacing "^#?PasswordAuthentication"
  validate $ sshd -t

[tune postgres]:
  ini /etc/postgresql/14/main/postgresql.conf:
    shared_buffers = "256MB"
    max_connections = "200"
```

Writes are atomic. If `validate` fails, the file is reverted. Inline `content >` bodies preserve literal `#` characters so config comments stay intact.

---

## HTTP as a first-class operation

```python
[register host]:
  post "${api_host}/hosts"
  auth bearer "${api_token}"
  body json '{"hostname": "web-1", "status": "active"}'
  expect 200..299
  collect "registration"
```

Supports `get`, `post`, `put`, `patch`, `delete`. Auth tokens are automatically redacted from output. On SSH targets, requests execute via `curl`.

---

## Conditionals and runtime facts

`gather facts` makes controller-local runtime facts available as variables:

```python
gather facts

[install packages (apt)]:
  when os_family == "debian"
  run $ apt-get install -y nginx

[install packages (yum)]:
  when os_family == "redhat"
  run $ yum install -y nginx
```

Steps can also set variables based on runtime results:

```python
[detect pigz]:
  run $ command -v pigz
  on success: set compressor = "pigz"
  on failure: set compressor = "gzip"
  if fails ignore

[compress]:
  first [detect pigz]
  run $ ${compressor} archive.tar
```

Override anything at runtime: `cgr apply --set os_family=redhat`

Available facts: `os_name`, `os_release`, `os_machine`, `hostname`, `arch`, `os_family`, `os_pretty`, `os_version`, `os_id`, `cpu_count`, `memory_mb`, `python_version`, `current_user`. See `MANUAL.md` for the full reference.

---

## Parallel constructs

Four constructs for explicit concurrency, all composable with everything else:

<p align="center">
  <img src="docs/img/parallel-constructs.svg" alt="Four parallel constructs: parallel, race, each, stage" width="700"/>
</p>

### `parallel` — fork/join with bounded concurrency

```python
[build everything]:
  parallel 2 at a time:
    [compile frontend]: run $ npm run build
    [compile backend]:  run $ cargo build --release
    [build docs]:       run $ mkdocs build
```

### `race` — first to succeed wins, rest cancelled

```python
[download package]:
  race into pkg.tar.gz:
    [us mirror]:  run $ curl -sf https://us.example.com/pkg.tar.gz -o ${_race_out}
    [eu mirror]:  run $ curl -sf https://eu.example.com/pkg.tar.gz -o ${_race_out}
```

Each branch writes to its own temp file. The winner is atomically renamed.

### `each` — parallel iteration over a list

```python
set servers = "web-1,web-2,web-3,web-4"

[deploy to fleet]:
  each server in ${servers}, 3 at a time:
    [deploy to ${server}]:
      run $ ssh ${server} '/opt/activate.sh'
```

### `stage`/`phase` — canary rollouts with verification gates

```python
[rolling deploy]:
  stage "production":
    phase "canary" 1 from ${servers}:
      [deploy ${server}]: run $ activate.sh
      verify "healthy": run $ curl -sf http://${server}/health
        retry 10x wait 3s

    phase "rest" remaining from ${servers}:
      each server, 4 at a time:
        [deploy ${server}]: run $ activate.sh
```

The canary deploys to 1 server. Its `verify` must pass before the rest begin.

---

## Templates

44 standard templates across 21 categories — packages, containers, TLS, firewalls, databases, monitoring, backups, and more. Import them with `using`:

```python
using apt/install_package, firewall/allow_port, tls/certbot, nginx/vhost

set domain = "app.example.com"

target "web-1" ssh deploy@10.0.1.5:

  [install nginx] from apt/install_package:
    name = "nginx curl"

  [open https] from firewall/allow_port:
    port = "443"

  [get tls cert] from tls/certbot:
    domain = "${domain}"
    email  = "ops@example.com"

  [configure vhost] from nginx/vhost:
    domain = "${domain}"
    port   = "443"
```

Templates are plain `.cgr` files in the `repo/` directory. Write your own by dropping a file in the right category. Categories include: `apt`, `dnf`, `nginx`, `tls`, `firewall`, `systemd`, `docker`, `k8s`, `user`, `ssh`, `security`, `file`, `backup`, `db`, `monitoring`, `webhook`, `cron`, `pkg`.

---

## Sub-graph composition

Execute another graph as a single step unit:

```python
set deploy_id = "abc123"

target "local" local:

  [deploy app] from ./deploy_app.cgr:
    version = "2.1.0"

  [wait for approval]:
    first [deploy app]
    wait for webhook "/approve/${deploy_id}"
    timeout 4h

  [resume rollout]:
    first [wait for approval]
    run $ echo approved
```

The parent graph tracks the outer step; the included graph keeps its own state journal.

---

## Wait gates

Two primitives for pausing execution until an external condition is met:

```python
[wait for file]:
  wait for file "./ready.flag"
  timeout 30m

[wait for approval]:
  wait for webhook "/approve/${deploy_id}"
  timeout 4h
```

`wait for file` polls for file existence locally or over SSH. `wait for webhook` starts a small local HTTP listener during `apply`; a `GET` or `POST` to the configured path releases the step.

---

## Crash recovery and drift detection

Every completed step is written to a `.state` file atomically. If a run crashes, fix the problem and rerun — completed steps skip, failed steps re-run:

```bash
cgr apply deploy.cgr   # crashes mid-run
# fix the problem
cgr apply deploy.cgr   # resumes from the failed step
```

For concurrent parameterized runs, use `--run-id` to salt the state path or `--state` to pin an explicit file:

```bash
cgr apply deploy.cgr --run-id canary
cgr apply deploy.cgr --state /tmp/deploy-staging.state
```

Drift detection re-runs `skip if` checks against live state:

```bash
cgr state test deploy.cgr
#   write_config: DRIFTED -- check now fails (was: success)

cgr apply deploy.cgr   # only the drifted step re-runs
```

---

## Reporting

Mark any step with `collect "key"` and its stdout is saved after execution:

```python
target "web-1" ssh ops@10.0.1.5:

  [hostname]:
    run $ hostname
    collect "hostname"

  [kernel]:
    run $ uname -r
    collect "kernel"

  [disk]:
    run $ df -h /
    collect "disk_usage"
```

View or export collected data:

```bash
cgr report audit.cgr
cgr report audit.cgr --format json
cgr report audit.cgr --format csv -o audit.csv
cgr report audit.cgr --keys hostname,kernel
```

For a machine-readable run summary — timing, per-step statuses, provenance, dedup info, and collected outputs:

```bash
cgr apply deploy.cgr --report run.json   # write JSON to file
cgr apply deploy.cgr --output json       # print JSON to stdout
```

Execution timing is tracked automatically in the state journal: wall-clock time per step, per wave, total run time, and the latest bottleneck step.

---

## Secrets

```bash
cgr secrets create vault.enc   # create encrypted vault
cgr secrets edit vault.enc     # edit in $EDITOR
```

```python
secrets "vault.enc"

target "db" ssh deploy@10.0.2.3:
  [configure db]:
    run $ echo "${db_password}" | psql -c "ALTER USER app PASSWORD '$(cat)'"
```

Secrets are decrypted at runtime, never written to disk, and auto-redacted from all output.

---

## Ansible inventory compatibility

Use existing inventory files directly:

```python
inventory "hosts.ini"

each name, addr in ${webservers}:
  target "${name}" ssh ${addr}:
    [deploy to ${name}]:
      run $ /opt/deploy.sh ${version}
```

For a full example combining Ansible playbooks, canary rollout, API registration, and a human approval gate in a single graph, see [Cookbook Recipe 11: Full Production Rollout](COOKBOOK.md#recipe-11-full-production-rollout).

---

## Web IDE

`cgr serve FILE` launches a browser-based IDE with live DAG visualization and an execution panel. The left pane is an editor; the right pane shows the dependency graph updating in real time as you type. Run `apply`, stream step output, inspect state and history, and view collected report data — all from the browser.

<p align="center">
  <img src="docs/screenshot.png" alt="cgr serve web IDE showing the install nginx example with editor and live DAG" width="900"/>
</p>

```bash
cgr serve mysetup.cgr   # opens http://localhost:8080
```

---

## Local demo

To try CommandGraph before touching a server, use the container demo suite in [`testing/`](./testing):

```bash
git clone <repository-url>
cd commandgraph/testing
./run-demos.sh list    # see the 10 demos
./run-demos.sh 1       # plan -> apply -> idempotent re-run
./run-demos.sh 3       # crash recovery and resume
./run-demos.sh         # full suite
```

No SSH targets, cloud accounts, or real services required. For an interactive shell with `cgr` on `PATH` and examples preloaded:

```bash
./run-demos.sh shell
```

---

## CLI reference

| Command | What it does |
|---------|-------------|
| `cgr plan FILE` | Show execution order and parallel waves |
| `cgr apply FILE` | Execute the graph (`--dry-run`, `--parallel N`, `--tags`, `--run-id`, `--state`) |
| `cgr validate FILE` | Check syntax and dependencies |
| `cgr check FILE` | Run checks to detect drift |
| `cgr visualize FILE` | Generate interactive HTML DAG visualization |
| `cgr serve FILE` | Web IDE with live graph and execution |
| `cgr explain FILE STEP` | Show the dependency chain for a step |
| `cgr why FILE STEP` | Show what depends on a step |
| `cgr state show FILE` | Show done/failed/pending state |
| `cgr state test FILE` | Re-run checks, detect drift |
| `cgr state reset FILE` | Wipe state, start fresh |
| `cgr diff FILE FILE2` | Structural graph comparison |
| `cgr ping FILE` | Verify SSH connectivity to all targets |
| `cgr report FILE` | View collected outputs (table, JSON, CSV) |
| `cgr lint FILE` | Best-practice linter |
| `cgr fmt FILE` | Auto-formatter |
| `cgr convert FILE` | Convert between `.cg` and `.cgr` formats |
| `cgr secrets CMD FILE` | Manage encrypted secrets |
| `cgr init` | Scaffold a new `.cgr` file |
| `cgr doctor` | Check environment for common issues |

---

## Design principles

**Files are the interface.** A `.cgr` file is a complete, portable, version-controllable description of your workflow. No web UI required, no database, no daemon.

**Idempotent by default.** Steps have `skip if` checks. Running the same graph repeatedly produces the same result.

**Crash-safe.** State is append-only with `fsync` after each write. A power failure loses at most one in-progress step.

**Zero dependencies.** One Python file, stdlib only. Copy it to an air-gapped server and it works.

**Readable syntax.** The syntax reads like English: "First install nginx. Skip if already installed. Run apt-get install." No YAML indentation wars, no JSON escaping, no template engine edge cases.

**Declare dependencies, not order.** You declare what depends on what. The engine computes execution order and maximizes parallelism. Reorder your file however you want — the result is the same.

---

## Testing

```bash
python3 -m py_compile cgr.py                   # syntax check
python3 -m pytest test_commandgraph.py -q      # test suite
cd testing/ && ./run-demos.sh                  # 10 local container demos
cd testing-ssh/ && ./run-ssh-demos.sh          # 5 SSH demos
```

---

## Documentation

| Document | For whom | What's in it |
|----------|----------|-------------|
| [QUICKSTART.md](QUICKSTART.md) | New users | Zero to running in 5 minutes |
| [TUTORIAL.md](TUTORIAL.md) | Beginners | 9 guided lessons, ~1 hour |
| [COOKBOOK.md](COOKBOOK.md) | Operators | 11 real-world recipes, including a full production rollout |
| [MANUAL.md](MANUAL.md) | Reference | Complete syntax for `.cgr` and `.cg` |
| [COMMANDGRAPH_SPEC.md](COMMANDGRAPH_SPEC.md) | Code generators | Formal PEG grammar |
| [AGENTS.md](AGENTS.md) | Contributors | Architecture, internals, and build workflow |
| [RELEASE.md](RELEASE.md) | Maintainers | Version policy, release checklist, and installer version behavior |

---

### Maintainer note

The release artifact is a single `cgr.py`. Development happens in `cgr_src/`, with `cgr_dev.py` as the thin dev entrypoint. Rebuild after changing source modules, `ide.html`, or `visualize_template.py`:

```bash
python3 cgr_dev.py apply build.cgr --no-resume
```

The engine version is maintained manually in `cgr_src/common.py`. See [RELEASE.md](RELEASE.md) for the formal version bump and release process, [AGENTS.md](AGENTS.md) for the full build workflow, and [MODULE_MAP.md](MODULE_MAP.md) for the quick reference on where parser, resolver, executor, state, IDE, and CLI changes live.
