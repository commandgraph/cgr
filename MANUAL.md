# CommandGraph DSL — Reference Manual

> **Audience**: Human operators and AI agents that need to create, customize, or debug dependency graphs for CLI command execution across hosts.

## What this is

CommandGraph is a domain-specific language (DSL) for declaring CLI command dependencies as a directed acyclic graph (DAG). You describe *what* needs to happen on a target host, declare *which steps depend on which*, and the engine figures out the execution order, runs idempotency checks, handles retries, and parallelizes independent steps.

Two file formats are supported — the engine detects which to use by extension:

| Format | Extension | Style | Best for |
|--------|-----------|-------|----------|
| **Readable** | `.cgr` | Indentation-based, English keywords | Human authoring, runbooks, quick edits |
| **Structured** | `.cg` | Brace-delimited, keyword-value pairs | Machine generation, complex nesting, AI agents |

Both formats produce the same execution graph. All CLI commands work identically with either.

## Quick start

```bash
# See what would happen (works with .cg or .cgr)
cgr plan mysetup.cgr --repo ./repo

# Execute for real
cgr apply mysetup.cgr --repo ./repo

# Dry-run (resolves everything, prints plan, runs nothing)
cgr apply mysetup.cgr --repo ./repo --dry-run

# Generate interactive HTML visualization
cgr visualize mysetup.cgr --repo ./repo -o mysetup.html

# Validate syntax and dependencies without executing
cgr validate mysetup.cgr --repo ./repo
```

---

## Core concepts

### The graph

A `.cg` file defines a dependency graph. The engine parses it, resolves all dependencies into a flat DAG, groups resources into parallel "waves", and executes wave by wave. Resources within the same wave have no inter-dependencies and run concurrently.

### Node

A `node` is a target machine. Everything inside a node block executes on that host. A node declares how to reach the host (`via ssh` or `via local`).

### Resource

A `resource` is a single unit of work — one shell command with an optional idempotency check. Resources declare their dependencies on other resources with `needs`.

### Template

A `template` is a parameterized, reusable resource tree stored in the repository. Templates are imported with `use` and instantiated with arguments.

### Verify

A `verify` block is a special resource that always runs (no `check`), is expected to be the terminal step, and defaults to `on_fail warn` so it reports failure without aborting.

---

## Readable format (.cgr) — recommended for humans

The `.cgr` format is designed for hand-authoring. It uses indentation instead of braces, natural English keywords, and `[bracketed names]` so you name things once and reference them by that name everywhere.

### Complete example

```
--- Nginx setup on web-1 ---

set nginx_port = "80"
set doc_root   = "/var/www/html"

using apt/install_package, firewall/allow_port

target "web-1" ssh deploy@10.0.1.5:

  [install nginx] from apt/install_package:
    name = "nginx"

  [open http] from firewall/allow_port:
    port = "80"

  [create docroot] as root:
    first [install nginx]
    skip if $ test -d /var/www/html
    run    $ mkdir -p /var/www/html

  [start nginx] as root, if fails stop:
    first [create docroot], [open http]
    skip if $ systemctl is-active nginx | grep -q active
    run    $ systemctl reload-or-restart nginx

  verify "HTTP 200 on localhost":
    first [start nginx]
    run   $ curl -sf http://localhost/
    retry 3x wait 2s
```

### Syntax reference

**Document header** (optional, decorative):
```
--- My deployment plan ---
```

**Variables** — referenced everywhere as `${name}`:
```
set domain = "example.com"
set port   = "443"
```

**Facts** — populate controller-local system variables:
```
gather facts
```

**Imports** — load templates from the repository:
```
using apt/install_package, tls/certbot, nginx/vhost
```

**Target** — the machine to execute on:
```
target "web-1" ssh deploy@10.0.1.5:
target "db-1" ssh root@10.0.2.3 port 5422:
target "local-box" local:
```

**Steps** — the core building block (replaces `resource` in `.cg`):
```
[step name] as root, timeout 2m, retry 2x wait 10s, if fails warn:
  first [other step], [another step]
  skip if $ some_check_command
  run    $ some_action_command
  env KEY = "value"
```

**Template instantiation** — you name it, then say what implements it:
```
[install nginx] from apt/install_package:
  name = "nginx"
```

**Nested steps** — children are indented inside the parent:
```
[deploy the app] as root:
  [create venv]:
    skip if $ test -d /opt/app/venv
    run    $ python3 -m venv /opt/app/venv
  first [create venv]
  run $ gunicorn app:app -D
```

**Verification** — terminal smoke test:
```
verify "HTTPS on example.com":
  first [start nginx], [install curl]
  run   $ curl -sfk https://example.com/
  retry 3x wait 2s
```

### Step body keywords

| Keyword | Meaning |
|---------|---------|
| `first [name], [name]` | These steps must complete before this one runs. |
| `skip if $ command` | If the command exits 0, this step is skipped (already done). |
| `skip if:` | Multiline form: each indented line is a check command, joined with `&&`. All must exit 0 to skip. |
| `run $ command` | The shell command to execute. |
| `run:` | Multiline form: each indented line is a command, joined with `&&`. Fails fast on any non-zero exit. |
| `always run $ command` | Run unconditionally (even if a `skip if` would match). |
| `always run:` | Multiline unconditional form, same `&&` join semantics as `run:`. |
| `collect "key"` | Capture stdout under the named key for reporting (`cgr report`). |
| `collect "key" as varname` | Capture stdout AND bind it to a runtime variable for use by later steps. |
| `on success: set var = "val"` | Set a runtime variable when this step succeeds. |
| `on failure: set var = "val"` | Set a runtime variable when this step fails (useful with `if fails ignore`). |
| `reduce "key"` | Aggregate all collected outputs for "key" from prior steps and use that as this step's output. |
| `reduce "key" as varname` | Aggregate collected outputs and bind the result to a runtime variable. |
| `env KEY = "value"` | Set an environment variable. |
| `when "expr == 'value'"` | Conditional execution. |
| `get "URL"` | HTTP GET request (see HTTP operations below). |
| `post "URL"` | HTTP POST request. |
| `put "URL"` | HTTP PUT request. |
| `patch "URL"` | HTTP PATCH request. |
| `delete "URL"` | HTTP DELETE request. |
| `auth bearer "TOKEN"` | Bearer token authentication. |
| `auth basic "USER" "PASS"` | Basic authentication. |
| `auth header "NAME" = "VAL"` | Custom auth header. |
| `header "NAME" = "VALUE"` | Add an HTTP request header. |
| `body json '...'` | JSON request body. |
| `body form "..."` | Form-encoded request body. |
| `expect 200` | Expected HTTP status code(s): `200`, `200..299`, or `200, 201, 204`. |
| `content > PATH:` | Write inline text block to PATH atomically. |
| `content from FILE > PATH` | Write file contents (with `${var}` interpolation) to PATH atomically. |
| `line "TEXT" in FILE` | Ensure TEXT is present as a line in FILE. |
| `line absent "TEXT" in FILE` | Ensure TEXT is absent from FILE. |
| `line "TEXT" in FILE, replacing "REGEX"` | Replace first line matching REGEX with TEXT; append if no match. |
| `put SRC > DEST [, mode XXXX]` | Copy a local file to DEST on the target. |
| `validate $ command` | Run after writes, before `run $`. Reverts all writes if non-zero. |
| `block in FILE, marker "M":` | Insert/replace a named block in FILE (marker-delimited). |
| `block from FILE in DEST, marker "M"` | Block content from local file. |
| `block absent in FILE, marker "M"` | Remove a named block from FILE. |
| `ini FILE:` | Edit INI/config key-value pairs (with optional `section "name":` grouping). |
| `json FILE:` | Patch a JSON file using dot-path key notation. |
| `assert $ CMD [, "message"]` | Fail the step before any writes if CMD exits non-zero. |
| `flag "TEXT" [when "EXPR"]` | Append a conditional shell-safe command fragment to `run $`. |
| `env KEY = "VAL" when "EXPR"` | Only set an environment variable when EXPR is true. |
| `until "VALUE"` | With `retry`, require the last non-empty stdout line to equal VALUE. |

### Step header properties

Written inline on the step header line, comma-separated:

| Property | Example | Default |
|----------|---------|---------|
| `as USER` | `as root` | current user |
| `timeout Ns` or `timeout Nm` | `timeout 3m` | 300s |
| `timeout Ns reset on output` | `timeout 30s reset on output` | disabled |
| `retry Nx wait Ns` | `retry 2x wait 10s` | no retries |
| `retry Nx backoff Xs` | `retry 3x backoff 30s` | exponential backoff, no cap |
| `retry Nx backoff Xs..Ys` | `retry 3x backoff 30s..300s` | exponential backoff, capped at Ys |
| `retry Nx backoff Xs..Ys jitter Z%` | `retry 3x backoff 30s..300s jitter 10%` | backoff with ±10% jitter |
| `until "VALUE"` | `until "0"` | disabled |
| `if fails stop\|warn\|ignore` | `if fails warn` | stop |
| `interactive` | (bare keyword) | disabled |

`timeout` is a hard execution deadline by default. If you add `reset on output`, it becomes an inactivity timeout instead: every new stdout or stderr chunk from the running command resets the timer.

Example:

```cgr
[stream logs] timeout 30s reset on output:
  run $ ./follow-progress.sh
```

This step can run longer than 30 seconds overall as long as it keeps producing CLI output at least once every 30 seconds. If output stops for 30 seconds, the step times out.

### The `interactive` keyword — terminal-interactive steps

The engine automatically detects steps that read from the terminal (`read`, `/dev/tty`, `ssh-copy-id`) and suspends live-progress rendering while they own the terminal. For commands the heuristic cannot detect, add the `interactive` keyword to the step body:

```cgr
[set password]:
  run $ passwd deploy_user
  interactive
```

This forces PTY allocation and stdin forwarding regardless of the command name. Use it for tools like `passwd`, `kinit`, `gpg --gen-key`, or any other utility that opens the terminal directly.

### The `first` keyword — why not `after`?

When reading `[install nginx]`, the dependency keyword should answer "what does this step need?" from the perspective of the step you're currently reading.

`after [update cache]` reads as temporal sequencing — "this happens after that" — which makes you think about *time* instead of *prerequisites*. Worse, it puts the cognitive focus on the *other* step rather than the current one.

`first [update cache]` reads as a natural instruction: "first, make sure the cache is updated." It's unambiguous about direction, and it matches how ops people actually talk: "first update the cache, then install the package."

### File documentation headers

A `.cgr` file can carry a documentation header that `cgr how FILE` renders for users and agents discovering the file for the first time. The header lives before any `set` or `target` declarations and consists of two optional parts:

**Title line** — a `--- text ---` line on the very first content line:

```
--- deploy web application ---
```

**Comment block** — contiguous `#` lines immediately following the title (or at the top if there is no title):

```
--- authorize SSH key ---
#
# Copies your local public key to each host using ssh-copy-id.
#
# Usage:
#   cgr apply authorize_ssh_key.cgr --set hosts="web01,web02,db01"
#   cgr apply authorize_ssh_key.cgr --set hosts="host1" --set ssh_port=2222
#
# Variables:
#   hosts     — comma-separated list of hostnames or IPs (required)
#   ssh_user  — remote user (default: $USER from environment)
#   ssh_port  — SSH port (default: 22)

set hosts    = "192.168.1.10,192.168.1.11"
set ssh_user = env("USER", "ubuntu")
set ssh_port = "22"
```

`cgr how` renders the header and then a **Defaults** table showing each top-level `set` declaration name. Default expressions are hidden so graph files cannot accidentally echo secrets or credentials into terminal logs.

Conventions for the comment block:
- Start with a one-line summary sentence.
- Add a `Usage:` block with copy-pasteable `cgr apply` examples.
- Add a `Variables:` block listing each user-facing `set` variable with a short description and whether it is required or has a default.
- For graphs with manual resume steps, add a `Resuming after interruption:` block with `cgr state set` examples.

The parser strips the header before building the AST; it has no effect on execution.

---

## Parallel constructs (.cgr format)

Four constructs make parallelism explicit and controllable. They nest inside steps and compose with everything else — all existing keywords (`first`, `skip if`, `run $`, `from template`, etc.) work unchanged.

### `parallel:` — explicit fork/join

Run multiple steps concurrently, wait for all to finish:

```
[setup everything]:
  first [install nginx]

  parallel:
    [write config a]:
      run $ echo "config_a" > /etc/app/a.conf

    [write config b]:
      run $ echo "config_b" > /etc/app/b.conf

    [write config c]:
      run $ echo "config_c" > /etc/app/c.conf

  run $ nginx -t
```

The three config writes run concurrently. `nginx -t` only runs after all three complete.

**Bounded concurrency** — limit how many run simultaneously:

```
parallel 3 at a time:
  [deploy to host-1]: ...
  [deploy to host-2]: ...
  [deploy to host-3]: ...
  [deploy to host-4]: ...
  [deploy to host-5]: ...
```

Runs 3 at a time. When one finishes, the next starts (sliding window).

**Failure policies** within parallel blocks:

```
parallel, if one fails stop all:    # cancel remaining on first failure
parallel, if one fails wait for rest:  # default — finish all, report failures
parallel, if one fails ignore:      # keep going, failures become warnings
```

### `race:` — first to succeed wins

Start all branches, keep the first success, cancel the rest:

```
[fetch dependencies]:
  race:
    [try us mirror]:
      run $ curl -sf https://us.mirror.example.com/pkg.tar.gz -o /tmp/pkg.tar.gz

    [try eu mirror]:
      run $ curl -sf https://eu.mirror.example.com/pkg.tar.gz -o /tmp/pkg.tar.gz
```

If the US mirror responds first, the EU branch is cancelled. If ALL fail, the race fails.

### `each ... in ..., N at a time:` — parallel iteration

Apply the same operation to a list with bounded concurrency:

```
set servers = "web-1,web-2,web-3,web-4,web-5"

[distribute to fleet]:
  each server in ${servers}, 3 at a time:
    [upload to ${server}]:
      skip if $ ssh ${server} "test -f /opt/release.tar.gz"
      run    $ scp release.tar.gz ${server}:/opt/
```

Expands into 5 resources, runs 3 concurrently. The `${server}` variable is substituted in all commands. State tracking records each iteration independently — crash mid-iteration resumes from where it stopped.

The concurrency limit may also be a variable, resolved at plan time:

```
set parallelism = "4"

[distribute to fleet]:
  each server in ${servers}, ${parallelism} at a time:
    [upload to ${server}]:
      run $ scp release.tar.gz ${server}:/opt/
```

If the variable expands to a non-integer at plan time, resolution fails with an error (rather than silently running without a limit).

### `stage`/`phase` — rolling deploys

Sequential phases with automatic item selection from a list:

```
set servers = "web-1,web-2,web-3,web-4,web-5,web-6"

[rolling deploy]:
  stage "deploy v2":
    phase "canary" 1 from ${servers}:
      [deploy to ${server}]:
        run $ ssh ${server} "/opt/activate.sh"

      verify "canary healthy":
        run $ curl -sf http://${server}:8080/health
        retry 10x wait 3s

    phase "half" 50% from ${servers}:
      each server, 2 at a time:
        [deploy to ${server}]:
          run $ ssh ${server} "/opt/activate.sh"

    phase "remaining" rest from ${servers}:
      each server, 4 at a time:
        [deploy to ${server}]:
          run $ ssh ${server} "/opt/activate.sh"
```

Phase "canary" picks 1 server and deploys. Its `verify` must pass before "half" begins. Phase "half" picks 50% of the remaining servers. Phase "remaining" takes the rest. Each phase is a gate — if any verify fails, the rollout stops.

Count expressions: `1` (exact count), `50%` (percentage), `rest` or `remaining` (all unprocessed items).

### How parallel constructs resolve into the DAG

All four constructs desugar into normal resources with synthetic barrier edges at resolve time. The pattern is:

```
parent's first deps → gate node → parallel children → join/barrier → parent's run
```

The resolver, executor, state tracker, and visualizer all see the same flat DAG — just with more resources. This means:

- **State tracking**: each parallel child is an independent state entry. Crash mid-parallel resumes only the incomplete children.
- **Visualization**: `cgr visualize` shows parallel children in the same wave, grouped by their parallel group ID.
- **Plan output**: shows gate, fork, and barrier nodes so you can see the parallel structure.

---

## Multi-node orchestration

When your infrastructure spans multiple hosts, you can declare dependencies across targets.

### Cross-node step references (.cgr)

Use `[node_name/step name]` in `first` to reference a step on another target:

```cgr
target "db" ssh deploy@db-host:
  [setup schema]:
    run $ psql -f /opt/schema.sql

target "web" ssh deploy@web-host:
  [start app]:
    first [db/setup schema]
    run $ systemctl start myapp
```

The engine creates a global DAG across all targets. Steps execute on their owning target (local or SSH), but the execution order respects cross-node dependencies.

### Cross-node step references (.cg)

Use dot-qualified names in `needs`:

```cg
node "monitor" {
  via local
  resource register_check {
    needs db.setup_schema
    run `echo "monitoring db"`
  }
}
```

### Node-level ordering

Instead of listing individual step dependencies, you can declare that an entire target runs after another:

```cgr
target "db" ssh deploy@db-host:
  [setup schema]:
    run $ psql -f /opt/schema.sql

target "web" ssh deploy@web-host, after "db":
  [start app]:
    run $ systemctl start myapp
```

In `.cg` format:

```cg
node "web" {
  via ssh user = "deploy" host = "web-host"
  after "db"
  resource start_app { run `systemctl start myapp` }
}
```

`after "db"` means: every root step in "web" implicitly depends on all steps in "db" completing first. The engine creates a synthetic barrier node to enforce this ordering.

Multiple predecessors: `after "db", "cache"` (`.cgr`) or `after "db", "cache"` (`.cg`).

### Target-level `each` (dynamic host lists)

For workflows that repeat the same work across many hosts (audits, fleet deploys, data collection), use a top-level `each` to stamp out targets from a list:

```cgr
set hosts = "pi:pi@192.168.1.8,nas:pi@192.168.1.9,desktop:coder@192.168.1.10"

each name, addr in ${hosts}:
  target "${name}" ssh ${addr}, after "collector":
    [audit ${name}] from audit/system_audit:
      dns_test = "google.com"
```

Each item in the list is colon-delimited (`name:user@host`). The two loop variables (`name`, `addr`) are destructured from each item. If only one variable is given (`each host in ${hosts}:`), it receives the entire item.

**`after each`** — a target that should run after all dynamically generated targets:

```cgr
target "report" local, after each:
  [write report]:
    run $ printf "Done\n"
```

In `.cg` format:

```cg
each name, addr in hosts {
  node "${name}" {
    via ssh user = "${name}" host = "${addr}"
    after "collector"
    system_audit { dns_test = "google.com" }
  }
}

node "report" {
  via local
  after each
  resource write_report { run `printf "Done\n"` }
}
```

**External injection via `--set`:**

```bash
# Override the host list at runtime
cgr apply audit.cgr --set 'hosts=pi:pi@192.168.1.8,nas:pi@192.168.1.9'

# Pipe from an external source
cgr apply audit.cgr --set "hosts=$(cat hosts.csv)"
```

The `each` expansion happens at resolve time (after `--set` overrides are applied), so the host list can come entirely from the command line.

### Inventory files

For teams migrating from Ansible or managing many hosts, inventory files separate *what hosts* from *what to do*. The format is Ansible-style INI:

```ini
# fleet.ini
[fleet]
pi       host=192.168.1.8   user=pi
nas      host=192.168.1.9   user=pi
desktop  host=192.168.1.10  user=coder

[fleet:vars]
dns_test = google.com
http_test = https://google.com
```

Reference from a graph file with `inventory`:

```cgr
inventory "fleet.ini"

each name, addr in ${fleet}:
  target "${name}" ssh ${addr}:
    [audit ${name}] from audit/system_audit:
      dns_test = "${dns_test}"
```

Or pass via CLI (overrides graph-declared inventory):

```bash
cgr apply audit.cgr -i production.ini
cgr apply audit.cgr -i staging.ini
```

**How it works:**

- Each `[group]` section becomes a list variable named after the group, in `name:user@host` format (compatible with `each` destructuring)
- `[group:vars]` entries become regular variables
- Ansible-style `ansible_host`, `ansible_user`, `ansible_port` are recognized (the `ansible_` prefix is stripped)
- Non-standard ports are appended: `name:user@host:2222`

**Variable precedence** (lowest to highest):

1. Gathered facts (`gather facts`)
2. Inventory file (`[group:vars]`)
3. Graph file (`set name = "value"`)
4. Vars file (`--vars-file`)
5. Secrets file (`secrets "vault.enc"`)
6. CLI flags (`--set KEY=VALUE`)

### Facts

`gather facts` collects facts from the machine running `cgr` and exposes them as normal variables. These are controller-local facts, not per-SSH-target facts. Override them with inventory, `set`, `--vars-file`, secrets, or `--set` when a graph needs target-specific values.

```cgr
gather facts

target "local" local:
  [install packages (apt)]:
    when os_family == "debian"
    run $ apt-get install -y nginx
```

| Variable | Meaning |
|----------|---------|
| `os_name` | Lowercase `uname` system name, such as `linux`, `darwin`, or `freebsd`. |
| `os_release` | OS/kernel release from `uname`. |
| `os_machine` | Machine type from `uname`, such as `x86_64`, `aarch64`, or `arm64`. |
| `hostname` | Local hostname from `uname`. |
| `arch` | CPU architecture from Python `platform.machine()`. |
| `os_family` | Normalized OS family: `debian`, `redhat`, `suse`, `arch`, `alpine`, `darwin`, `unknown`, or an `/etc/os-release` fallback. |
| `os_pretty` | `/etc/os-release` `PRETTY_NAME`, when present. |
| `os_version` | `/etc/os-release` `VERSION_ID`, falling back to Python `platform.version()`. |
| `os_id` | `/etc/os-release` `ID`, falling back to the lowercase `uname` system name. |
| `cpu_count` | CPU count from Python `multiprocessing.cpu_count()`. |
| `memory_mb` | Total memory in MiB from Linux `/proc/meminfo`; `0` when unavailable. |
| `python_version` | Python runtime version used by `cgr`. |
| `current_user` | `USER` or `LOGNAME` from the environment, falling back to `unknown`. |

### How it works

- **No coordinator process.** The engine runs from one machine and SSHes to targets. Cross-node dependencies simply push resources into later topological waves.
- **Cross-node deduplication is disabled.** The same command on different hosts is never deduplicated (they are different operations even if the command text is identical).
- **Explicit references required.** Fuzzy matching stays within the same node. To reference a step on another node, you must use `[node/step]` or `node.step` syntax.
- **Cycle detection works across nodes.** If `db.step_a` depends on `web.step_b` and vice versa, the cycle is caught.
- **State/resume works unchanged.** The state file is global per graph file. Cross-node steps are tracked by their fully qualified ID (e.g., `db.setup_schema`).

### Plan and validate output

`cgr plan` shows cross-node dependencies with node prefixes:

```
  needs [db/setup_schema]
```

`cgr validate` reports cross-node dependencies and node ordering:

```
  Cross-node dependencies:
    monitor.register_check → db.setup_schema
  Node ordering:
    web after db, cache
```

---

## HTTP operations (.cgr format)

Steps can make HTTP requests directly instead of shelling out to `curl`. HTTP verbs are first-class keywords that produce the same `(rc, stdout, stderr)` execution model as shell commands.

### Supported verbs

`get`, `post`, `put`, `patch`, `delete` — followed by a quoted URL:

```
[health check]:
  get "https://api.example.com/health"
  expect 200
  timeout 10s
  retry 3x every 2s
```

### Authentication

Three auth methods are supported. Auth tokens are automatically added to the redaction set — they will never appear in logs or collected output.

```
# Bearer token
auth bearer "${api_token}"

# Basic auth (username + password)
auth basic "admin" "s3cret"

# Custom auth header
auth header "X-API-Key" = "${api_key}"
```

### Headers and body

```
[create resource]:
  post "https://api.example.com/resources"
  auth bearer "${token}"
  header "X-Request-Id" = "cgr-001"
  header "Accept" = "application/json"
  body json '{"name": "web-1", "status": "active"}'
  expect 200..299
  collect "create_result"
```

Body types: `body json '...'` sets `Content-Type: application/json`. `body form "..."` sets `Content-Type: application/x-www-form-urlencoded`.

Wait gates are also first-class steps:

```cgr
[wait for approval]:
  wait for webhook "/approve/${deploy_id}"
  timeout 4h

[wait for file]:
  wait for file "./ready.flag"
  timeout 30m
```

`wait for webhook` starts a lightweight local HTTP listener during `apply`. A `GET` or `POST` to that path unblocks the step. `wait for file` polls for file existence locally or over SSH, depending on the target node.

### Expect expressions

The `expect` keyword validates the HTTP status code. Three forms:

| Form | Example | Matches |
|------|---------|---------|
| Single code | `expect 200` | Exactly 200 |
| Range | `expect 200..299` | 200 through 299 inclusive |
| List | `expect 200, 201, 204` | Any of these codes |

If omitted, any 2xx status is accepted. A non-matching status code results in a failed step (rc=1).

### Execution model

- **Local targets**: Uses Python's `urllib.request` — zero dependencies.
- **SSH targets**: Constructs a `curl` command and executes it over SSH.
- **Response body** is captured as stdout. Status line appears as stderr.
- All standard step properties work: `timeout`, `retry`, `first`, `when`, `collect`, `if fails`.

### Mixing HTTP and shell

HTTP and shell steps coexist naturally in the same target:

```
[register in CMDB]:
  post "https://cmdb.example.com/hosts"
  auth bearer "${cmdb_token}"
  body json '{"hostname": "${hostname}", "role": "web"}'
  expect 200..299
  collect "registration"

[configure server]:
  first [register in CMDB]
  run $ nginx -t && systemctl reload nginx
```

---

## Provisioning operations (.cgr format)

Steps can write files, manage lines in config files, and transfer local files to targets — without shelling out to `tee`, `sed`, or `scp`. These keywords produce the same `(rc, stdout, stderr)` execution model as shell commands and compose naturally with `skip if`, `first`, and `validate`.

### `content` — write a file

**Inline content** (variable interpolation applies):

```
[write sshd config]:
  content > /etc/ssh/sshd_config.d/hardened.conf:
    PermitRootLogin no
    PasswordAuthentication no
    Port ${ssh_port}
  validate $ sshd -t
  run $ systemctl restart sshd
```

The content body is everything indented under the `content > PATH:` line. Variables (`${name}`) are expanded. Literal `#` characters are preserved inside the body. The write is atomic — the engine writes to a `.tmp` file and renames it into place.

**From a file** (keeps `#` comments, still interpolates `${vars}`):

```
[write nginx vhost]:
  content from ./files/myapp.nginx.conf > /etc/nginx/sites-available/myapp
  validate $ nginx -t
  run $ systemctl reload nginx
```

The source path is relative to the `.cgr` file. Variable interpolation applies to the file contents.

---

### `validate` — verify writes before reloading

`validate $ COMMAND` runs after all `content`/`line`/`put` operations in the step but before any `run $`. If the command fails (non-zero exit), all file writes in the step are reverted from automatic backups and the step fails immediately — no reload happens.

```
[deploy app config]:
  content > /etc/myapp/config.toml:
    [server]
    port = "${app_port}"
    workers = "4"
  validate $ myapp --config /etc/myapp/config.toml --check
  run $ systemctl restart myapp
```

If `myapp --check` exits non-zero, `/etc/myapp/config.toml` is restored to its previous contents.

---

### `line` — manage lines in config files

Ensure a specific line is present, absent, or replace a matching line:

```
[enable ip forwarding]:
  line "net.ipv4.ip_forward = 1" in /etc/sysctl.conf
  run $ sysctl -p

[disable root login]:
  line "PermitRootLogin no" in /etc/ssh/sshd_config, replacing "^#?PermitRootLogin"
  run $ systemctl reload sshd

[remove legacy entry]:
  line absent "UseDNS yes" in /etc/ssh/sshd_config
  run $ systemctl reload sshd
```

Three forms:
| Form | Behaviour |
|------|-----------|
| `line "TEXT" in FILE` | Appends the line if not already present (exact match) |
| `line absent "TEXT" in FILE` | Removes all lines equal to TEXT |
| `line "TEXT" in FILE, replacing "REGEX"` | Replaces the first line matching REGEX with TEXT; appends if no match |

Multiple `line` keywords in one step all target the same file. Each write is atomic. If a `validate` keyword is also present, all `line` writes are backed up and restored on validation failure.

---

### `put` — transfer a file to the target

```
[install binary]:
  put ./dist/myapp > /usr/local/bin/myapp, mode 0755
  run $ myapp --version

[deploy TLS cert]:
  put ./certs/fullchain.pem > /etc/ssl/certs/myapp.pem, mode 0644
```

The source path is relative to the `.cgr` file. For local targets, the engine uses a direct copy. `mode` is optional and takes a 4-digit octal value.

**Transfer method (SSH targets):** By default, the engine auto-detects: it tries `rsync` first (delta transfers, checksum verification, resume support) and falls back to `scp` if rsync is not available on the remote host. Availability is probed once per node per run and cached.

Use the `transfer` option to override auto-detection:

```
# Explicit rsync — fail if rsync is not available on the remote
put ./dist/myapp > /usr/local/bin/myapp, mode 0755, transfer rsync

# Explicit scp — never try rsync
put ./dist/myapp > /usr/local/bin/myapp, transfer scp
```

**Pass-through flags:** Use `transfer_opts` to pass raw flags to the selected transfer tool:

```
# Bandwidth-limited rsync with compression
put ./big.tar.gz > /opt/release.tar.gz, transfer rsync, transfer_opts "--bwlimit=5000 -z"

# scp with a specific cipher
put ./data.bin > /tmp/data.bin, transfer scp, transfer_opts "-c aes128-gcm@openssh.com"
```

All options (`mode`, `transfer`, `transfer_opts`) are comma-separated and order-independent. `transfer_opts` supports variable expansion.

> **Note:** When using `transfer_opts` with auto-detection (no explicit `transfer`), the flags must be valid for whichever tool is selected at runtime. Pin the transfer method if your flags are tool-specific.

---

### `secret` — load secret values into variables

Four backends load secrets at parse time. All are automatically added to the redaction set — values never appear in logs, plan output, or collected output.

Encrypted `secrets "FILE"` bundles now include integrity protection. Newly written bundles use a versioned format with HMAC-SHA256 over the encrypted payload. Older bundles without integrity protection still decrypt, but emit a deprecation warning; re-encrypt them with `cgr secrets rekey`.

```
# From an environment variable (fails if not set)
set db_password = secret "env:DB_PASSWORD"

# From a file (fails if not found)
set api_key = secret "file:/run/secrets/api_key"

# From a shell command — stdout is the secret value
set token = secret "cmd:pass show myapp/token"
set pw    = secret "cmd:aws secretsmanager get-secret-value --secret-id myapp/db --query SecretString --output text"

# From HashiCorp Vault KV (requires VAULT_ADDR and VAULT_TOKEN env vars)
set db_pw = secret "vault:secret/data/myapp#password"
```

**Environment variable backend:**
```
DB_PASSWORD=hunter2 cgr apply myapp.cgr
```

**File backend** — useful with Docker secrets, Kubernetes secrets mounted as files:
```
set token = secret "file:/run/secrets/vault_token"
```

**Command backend** — run any command and capture stdout. Works with any secrets manager that has a CLI (`pass`, `aws secretsmanager`, `gcloud secrets`, `1password`, etc.):
```
set pw = secret "cmd:op read op://vault/myapp/password"
```

**Vault backend** — reads a KV secret via the Vault HTTP API. Requires:
- `VAULT_ADDR` — Vault server URL (default: `http://127.0.0.1:8200`)
- `VAULT_TOKEN` — authentication token

```
set pw = secret "vault:secret/myapp#password"
#                       ^^^^^^^^^^^^^^^^  ^^^^^^^^
#                       KV path           field name (default: "value")
```

Both KV v1 (`secret/myapp`) and KV v2 (`secret/data/myapp`) path styles are supported.

For encrypted `secrets "FILE"` bundles used during graph resolution, provide the passphrase with `--vault-pass PASS`, `--vault-pass-file FILE`, `--vault-pass-ask` (or `-A`), or `CGR_VAULT_PASS`. `cgr apply --vault-pass-ask FILE.cgr` prompts interactively, and the IDE prompts in-browser when a request needs the vault passphrase. The IDE does not persist the passphrase across unrelated requests.

---

### Combining provisioning keywords

All provisioning keywords compose with standard step keywords:

```
[harden ssh]:
  as root
  first [install openssh]
  # Disable password auth and root login atomically
  line "PermitRootLogin no" in /etc/ssh/sshd_config, replacing "^#?PermitRootLogin"
  line "PasswordAuthentication no" in /etc/ssh/sshd_config, replacing "^#?PasswordAuthentication"
  validate $ sshd -t
  run $ systemctl reload sshd
  retry 2x wait 5s
```

```
[deploy app]:
  as deploy
  first [create directories]
  put ./release/app.tar.gz > /opt/myapp/release.tar.gz
  content > /opt/myapp/config.env:
    APP_ENV=production
    PORT=${app_port}
    DATABASE_URL=${db_url}
  run $ cd /opt/myapp && tar xzf release.tar.gz && systemctl restart myapp
```

---

## Structured config operations (.cgr format)

For config files that have recognizable structure (INI, JSON) or that multiple tools share (marker-delimited blocks), these keywords edit with surgical precision rather than rewriting the whole file.

### `block` — manage a named section of a file

Insert or replace a block delimited by marker comments. If the markers are absent, the block is appended:

```
[configure logrotate]:
  block in /etc/logrotate.d/myapp, marker "# cgr:myapp":
    /var/log/myapp/*.log {
        daily
        rotate 14
        compress
        missingok
        notifempty
    }
  run $ logrotate --debug /etc/logrotate.d/myapp
```

The engine writes:
```
# cgr:myapp begin
/var/log/myapp/*.log {
    daily
    rotate 14
    ...
}
# cgr:myapp end
```

On subsequent runs, everything between `# cgr:myapp begin` and `# cgr:myapp end` is replaced with the new content. Lines outside the markers are untouched.

To remove a block:
```
[remove logrotate config]:
  block absent in /etc/logrotate.d/myapp, marker "# cgr:myapp"
```

The block body is collected from the indented lines under `block in FILE, marker "MARKER":` — same rules as `content >`, including preserving literal `#` characters in the body.

**From a file:**
```
[configure nginx]:
  block from ./configs/upstream.nginx in /etc/nginx/nginx.conf, marker "# cgr:upstream"
  validate $ nginx -t
  run $ systemctl reload nginx
```

---

### `ini` — edit INI/config file keys

Set or remove keys in an INI-style config file (supports `[section]` headers):

```
[tune postgres]:
  ini /etc/postgresql/15/main/postgresql.conf:
    max_connections = "200"
    shared_buffers = "256MB"
    work_mem = "4MB"
  run $ systemctl reload postgresql
```

```
[configure app]:
  ini /etc/myapp/config.ini:
    section "database":
      host = "${db_host}"
      port = "${db_port}"
      pool = "20"
    section "cache":
      backend = "redis"
      url = "${redis_url}"
  validate $ myapp --check-config /etc/myapp/config.ini
  run $ systemctl restart myapp
```

To remove a key:
```
  ini /etc/myapp/config.ini:
    section "legacy":
      old_key = absent
```

Rules:
- If the key already has the correct value, the file is not written (true idempotency).
- If the section doesn't exist, it is appended.
- If the key doesn't exist in the section, it is appended to the section.
- If `absent` is the value, the key is removed. If the section then becomes empty, it is also removed.
- Keys outside any `section` block go into the global (no-header) section at the top of the file.

---

### `json` — patch a JSON file

Set or remove keys in a JSON file using dot-path notation:

```
[configure daemon]:
  json /etc/docker/daemon.json:
    log-driver = "json-file"
    log-opts.max-size = "100m"
    log-opts.max-file = "3"
  run $ systemctl reload docker
```

```
[add registry mirror]:
  json /etc/docker/daemon.json:
    insecure-registries[] = "registry.internal:5000"
  run $ systemctl reload docker
```

To remove a key:
```
  json /etc/docker/daemon.json:
    experimental = absent
```

Path notation:
| Pattern | Meaning |
|---------|---------|
| `key = "value"` | Set top-level key |
| `parent.child = "value"` | Set nested key (creates parent object if absent) |
| `array[] = "value"` | Append to array (creates array if absent); skips if value already present |
| `key = absent` | Remove key (no-op if key doesn't exist) |
| `key = null` | Set key to JSON null |
| `key = true` / `key = false` | Set key to JSON boolean |
| `key = 42` | Set key to JSON number |

The file is always written with 2-space indentation. If the file doesn't exist, it is created as `{}` first.

---

### `assert` — precondition checks

`assert $ COMMAND` fails immediately with a clear message if the command exits non-zero — before any writes in the step execute:

```
[provision database]:
  assert $ test -f /etc/apt/sources.list.d/pgdg.list, "PostgreSQL APT repo not configured — run apt/add_repo first"
  assert $ id postgres 2>/dev/null, "postgres system user must exist"
  content > /etc/postgresql/15/main/conf.d/tuning.conf:
    max_connections = 200
  run $ systemctl reload postgresql
```

Multiple asserts are evaluated in order. The first failure stops the step and prints the message. Unlike `skip if` (which skips on success), `assert` fails on failure.

```
[deploy to production]:
  assert $ test "${environment}" = "production", "This step only runs in production"
  assert $ curl -sf https://internal-health-check/ready, "Health check endpoint not reachable"
  run $ ./deploy.sh
```

---

### Updated keyword reference

| Keyword | Meaning |
|---------|---------|
| `block in FILE, marker "M":` | Insert/replace marked block in FILE. |
| `block from FILE in DEST, marker "M"` | Block content from local file. |
| `block absent in FILE, marker "M"` | Remove marked block from FILE. |
| `ini FILE:` | Edit INI keys/sections (indented key = value block). |
| `json FILE:` | Patch JSON keys using dot-path notation. |
| `assert $ CMD [, "message"]` | Fail step immediately if CMD exits non-zero. |

---

## Structured format (.cg) — reference

The `.cg` format uses braces, backtick-delimited commands, and the `needs` keyword. It is fully preserved and recommended for machine generation, complex nesting with heredocs, and AI agent authoring.

### DSL syntax reference

### Comments

Lines beginning with `#` are comments. Comments can appear anywhere.

```
# This is a comment
var name = "value"  # Inline comments are NOT supported — this would break
```

### Variables

Declared at the top level. Referenced in strings and commands as `${var_name}`.

```
var app_user  = "deploy"
var nginx_port = "80"
var domain     = "example.com"
```

Variables are expanded in: `description`, `check`, `run`, `env` values, `via` properties, and template arguments. Variable names must match `[a-zA-Z_][a-zA-Z0-9_-]*`.

### Imports

Import templates from the repository. The path is relative to the repo root, without the `.cg` extension.

```
use "apt/install_package"              # imports template, available as install_package
use "nginx/vhost" as nginx_vhost       # imports with an alias
```

### Node block

```
node "hostname-or-label" {
  via ssh host="10.0.1.5" user="deploy" port=22

  # resources, groups, template instantiations, and verify blocks go here
}
```

The `via` clause is optional. If omitted, defaults to `via local` (execute on the current host). The `via ssh` clause supports `host`, `user`, and `port` key-value pairs. SSH uses `BatchMode=yes` (no interactive prompts) and key-based auth.

A `.cg` file can contain multiple `node` blocks for multi-host orchestration.

### Resource block

```
resource install_nginx {
  description "Install the Nginx web server"
  needs update_apt_cache
  check `dpkg -l nginx 2>/dev/null | grep -q '^ii'`
  run   `apt-get install -y nginx`
  as root
  timeout 180
  retry 1 delay 5
  on_fail stop
  when "debian == 'debian'"
  env DEBIAN_FRONTEND = "noninteractive"
}
```

**All fields:**

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `description` | No | `""` | Human-readable label. Shown in plan/apply output. |
| `needs` | No | `[]` | Comma-separated list of resource names this depends on. Can appear multiple times (values accumulate). |
| `check` | No | `null` | Shell command (backtick-delimited). If exits 0, the resource is **skipped** (already satisfied). Set to `` `false` `` to force the resource to always run. |
| `run` | **Yes** | — | Shell command to execute. This is the action. |
| `as` | No | current user | Run as this Unix user (uses `sudo -u`). |
| `timeout` | No | `300` | Max seconds before the command is killed. Append `s` or `m` for units. Add `reset on output` to treat it as an inactivity timeout instead. |
| `retry` | No | `0` | Number of retry attempts after failure. |
| `delay` | No | `5` | Seconds between retries. Follows `retry`. |
| `on_fail` | No | `stop` | What to do on failure: `stop` (abort the graph), `warn` (continue, mark as warned), `ignore` (treat as success). |
| `when` | No | `null` | Quoted boolean expression. If false, the resource is skipped. Supports `==` and `!=`. |
| `env` | No | `{}` | Set environment variables. Syntax: `env KEY = "value"`. Repeatable. |
| `interactive` | No | `false` | Force terminal-interactive mode (PTY + stdin forwarding). Use for commands like `passwd`, `kinit`, or any tool that reads directly from the terminal. The engine auto-detects common patterns (`read`, `/dev/tty`, `ssh-copy-id`); use this keyword for others. |

### Command delimiters

Shell commands use **backticks**: `` `command here` ``. Multi-line commands work:

```
run `cat > /etc/nginx/conf.d/site.conf << 'EOF'
server {
    listen 80;
    server_name example.com;
}
EOF`
```

String values use **double quotes**: `"like this"`. Supports `\n`, `\t`, `\\`, `\"` escapes.

### Nested resources

Resources can contain child resources. Children run **before** the parent automatically — the parent implicitly depends on all its children.

```
resource setup_tls {
  description "Full TLS setup"

  resource install_certbot {
    description "Install certbot"
    check `command -v certbot`
    run   `apt-get install -y certbot`
    as root
  }

  resource generate_cert {
    description "Generate certificate"
    needs install_certbot
    check `test -f /etc/letsencrypt/live/example.com/fullchain.pem`
    run   `certbot certonly --standalone -d example.com`
    as root
  }

  # setup_tls implicitly depends on install_certbot and generate_cert.
  # It also explicitly depends on generate_cert for ordering.
  needs generate_cert
  run `echo "TLS is ready"`
}
```

Nesting creates scoped names. `install_certbot` inside `setup_tls` becomes `node_name.setup_tls.install_certbot` internally. But you reference it by its short name in `needs`.

### Groups

Groups apply shared properties to multiple resources:

```
group packages as root on_fail stop timeout 120 {
  resource install_nginx {
    # inherits: as root, on_fail stop, timeout 120
    check `dpkg -l nginx | grep -q '^ii'`
    run   `apt-get install -y nginx`
  }

  resource install_curl {
    check `command -v curl`
    run   `apt-get install -y curl`
  }
}
```

Group-level properties (`as`, `on_fail`, `timeout`) become defaults for all children. A child can override any inherited property.

### Template instantiation

After importing a template with `use`, call it by name inside a `node` block:

```
use "apt/install_package"

node "web-1" {
  via local

  install_package { name = "nginx" }
  install_package { name = "curl" }
}
```

Arguments are `key = "value"` pairs inside braces. The template expands into resources scoped under a generated prefix (e.g., `install_package_nginx`).

### Verify block

```
verify "HTTP 200 on localhost" {
  needs start_nginx, install_curl
  run   `curl -sf http://localhost/ | grep -q 200`
  retry 3 delay 2
  on_fail warn
}
```

Verify blocks differ from resources: they have no `check` clause (always run), default to `on_fail warn`, and are typically the terminal step.

---

## How to create a dependency graph from scratch

Follow these steps in order. This section is written for AI agents and can be followed literally.

### Step 1: Identify the end state

Write one sentence describing what should be true when the graph completes. Example: "Nginx is running on web-1 with a TLS certificate for app.example.com, serving content from /var/www/app."

### Step 2: Decompose into resources

Work **backwards** from the end state. Ask: "What must be true immediately before this step?" Repeat until you reach steps with no prerequisites.

Example decomposition:
```
Nginx is serving HTTPS                      ← verify
├── Nginx is running                        ← start service
│   ├── Config is valid (nginx -t)          ← validate
│   │   ├── Vhost config exists             ← write config
│   │   │   ├── Document root exists        ← mkdir
│   │   │   └── Nginx is installed          ← apt install
│   │   │       └── APT cache is fresh      ← apt update
│   │   └── TLS cert exists                 ← certbot
│   │       └── Certbot is installed        ← apt install
│   │           └── APT cache is fresh      ← (shared)
│   └── Service is enabled                  ← systemctl enable
├── Firewall allows 443                     ← ufw allow
└── curl is installed (for verify)          ← apt install
```

### Step 3: Identify what's reusable

Look for repeated patterns. "Install a package via APT" appears three times above. Check the repository (`cgr repo index --repo ./repo`) for existing templates before writing inline resources.

### Step 4: Write the .cg file

Start with variables and imports, then the node block, then resources from the bottom of the dependency tree upward.

```
# ─── Imports ─────────────────────────────────────
use "apt/install_package"
use "firewall/allow_port"
use "tls/certbot"
use "systemd/enable_service"

# ─── Variables ───────────────────────────────────
var domain = "app.example.com"

# ─── Target ──────────────────────────────────────
node "web-1" {
  via ssh host="10.0.1.5" user="deploy" port=22

  # Layer 1: packages (roots — no dependencies)
  install_package { name = "nginx" }
  install_package { name = "curl" }
  allow_port { port = "443" }

  # Layer 2: TLS
  certbot { domain = "${domain}" }

  # Layer 3: configuration (depends on packages)
  resource write_vhost {
    description "Write Nginx vhost config"
    needs install_package_nginx
    check `test -f /etc/nginx/sites-available/${domain}`
    run   `cat > /etc/nginx/sites-available/${domain} << 'EOF'
server { listen 443 ssl; server_name ${domain}; root /var/www/${domain}; }
EOF`
    as root
  }

  resource validate_config {
    description "Validate Nginx config"
    needs write_vhost
    check `false`
    run   `nginx -t`
    as root
    on_fail stop
  }

  # Layer 4: start service
  resource start_nginx {
    description "Start Nginx"
    needs validate_config, certbot_app_example_com, allow_port_443
    check `systemctl is-active nginx | grep -q active`
    run   `systemctl reload-or-restart nginx`
    as root
  }

  enable_service { service = "nginx" }

  # Layer 5: verification
  verify "HTTPS on ${domain}" {
    needs start_nginx, enable_service_nginx, install_package_curl
    run `curl -sfk https://${domain}/`
    retry 3 delay 2
  }
}
```

### Step 5: Validate and visualize

```bash
cgr validate mysetup.cg --repo ./repo
cgr plan mysetup.cg --repo ./repo -v
cgr visualize mysetup.cg --repo ./repo
```

Fix any errors. The validator catches: undefined variable references, missing `run` commands, nonexistent `needs` targets, and dependency cycles.

### Step 6: Dry-run, then apply

```bash
cgr apply mysetup.cg --repo ./repo --dry-run
cgr apply mysetup.cg --repo ./repo
```

---

## How to create a reusable template

Templates live in the repository directory (default: `~/.cgr/repo/`). Each file contains exactly one `template` block. Both `.cgr` (readable) and `.cg` (structured) formats are supported; `.cgr` is preferred when both exist.

### Template file structure (.cgr — recommended)

```
# category/template_name.cgr
# Brief description of what this template does.
#
# Parameters:
#   param1  (required) — what it does
#   param2  (optional) — what it does, default "value"

template template_name(param1, param2 = "default_value"):
  description "Human-readable description using ${param1}"

  [prerequisite step] as root:
    skip if $ some_check_command
    run    $ some_command

  first [prerequisite step]
  skip if $ check if ${param1} is already done
  run    $ do the thing with ${param1}
  as root, timeout 2m
```

The template body uses the same `.cgr` step syntax as target blocks: `[child steps]` for nested prerequisites, `first`/`skip if`/`run` for the root resource, and body-level properties like `as root, timeout 2m` on standalone lines.

You can also include another graph file as a single step:

```cgr
[deploy app] from ./deploy_app.cgr:
  version = "2.1.0"
```

This is `.cgr`-only for now. The included graph runs as a unit with its own state file, while the parent graph tracks only the outer step.

### Template file structure (.cg — alternative)

```
template template_name(param1, param2 = "default_value") {
  description "Human-readable description using ${param1}"

  resource prerequisite_step {
    check `some_check_command`
    run   `some_command`
    as root
  }

  needs prerequisite_step
  check `check if ${param1} is already done`
  run   `do the thing with ${param1}`
  as root
  timeout 120
}
```

### Rules for templates

1. **One template per file.** The filename should match the template name.
2. **File path becomes the import path.** `repo/apt/install_package.cgr` is imported as `use "apt/install_package"`. The engine searches for `.cgr` first, then `.cg`.
3. **Parameters use `${param_name}` syntax** in all fields (description, check, run, etc.).
4. **First parameter is the primary key.** The engine uses it to generate the instance prefix (e.g., `install_package_nginx`). Choose a parameter that differentiates instances.
5. **Default values are optional.** Required params have no `= default`. Defaults can reference other params: `email = "admin@${domain}"`.
6. **Nesting is encouraged.** If a template needs prerequisites (like updating a package cache before installing), put them as `[child step]` blocks.
7. **After adding a template, re-index the repo:**
   ```bash
   cgr repo index --repo ./repo
   ```

### Template naming conventions

Organize by category in the repo directory:

```
repo/
├── apt/                  # Debian/Ubuntu package management
│   ├── install_package.cgr
│   └── add_repo.cgr
├── systemd/              # Service management
│   └── enable_service.cgr
├── firewall/             # Firewall rules
│   └── allow_port.cgr
├── nginx/                # Nginx-specific
│   └── vhost.cgr
├── tls/                  # TLS/SSL
│   └── certbot.cgr
└── docker/               # Container runtime
    └── compose_up.cgr
```

---

## Template versioning

Templates can declare a semantic version, and consumers can specify version constraints on imports.

### Declaring a version (.cgr)

```cgr
template install_package(name, version = "latest"):
  version 1.0.0
  description "Install ${name} via APT"
  ...
```

### Declaring a version (.cg)

```cg
template install_package(name, version = "latest") {
  version "1.0.0"
  description "Install ${name} via APT"
  ...
}
```

### Constraining versions on import

Three operators:

| Operator | Meaning | Example |
|----------|---------|---------|
| `>=` | Minimum version | `using apt/install_package >= 1.0` |
| `~>` | Compatible (same major) | `using apt/install_package ~> 1.2` |
| `=` | Exact match | `using apt/install_package = 1.2.0` |

`.cgr` format:
```cgr
using apt/install_package >= 1.0, firewall/allow_port ~> 1.0
```

`.cg` format:
```cg
use "apt/install_package" >= "1.0"
use "firewall/allow_port" ~> "1.0"
```

### Compatibility operator `~>`

- `~> 1.2` means `>= 1.2.0` and `< 2.0.0` (lock major version)
- `~> 1.2.3` means `>= 1.2.3` and `< 1.3.0` (lock major.minor)

This is the same semantics as Bundler and Terraform.

### Rules

- **No constraint = any version.** Existing files work unchanged.
- **Constraint + unversioned template = error.** The template must declare a version.
- **Version appears in provenance.** `cgr plan`, `cgr validate`, and `--report` show template versions.

---

## Resource naming and how `needs` resolves

This is the most important section for avoiding errors. The engine generates fully-qualified resource IDs, but `needs` references are resolved with fuzzy matching.

### How IDs are generated

| Source | Resource ID pattern | Example |
|--------|-------------------|---------|
| Inline resource | `node.resource_name` | `web-1.start_nginx` |
| Nested resource | `node.parent.child` | `web-1.start_nginx.configure_ssl` |
| Template instance | `node.templatename_primaryparam` | `web-1.install_package_nginx` |
| Template child | `node.templatename_primaryparam.child` | `web-1.install_package_nginx.update_cache` |

### How `needs` looks up targets

When you write `needs install_package_nginx`, the resolver:

1. Tries the literal qualified name: `web-1.install_package_nginx`
2. If not found, searches all resource IDs for one ending in `.install_package_nginx`
3. If multiple matches, prefers the same-node match
4. If still ambiguous, takes the first alphabetical match

**Best practice:** Use the short name that appears after the last dot. For template instances, this is `templatename_primaryparam` (e.g., `install_package_nginx`, `allow_port_443`, `certbot_app_example_com`).

### Special characters in generated names

The primary parameter value is sanitized: non-alphanumeric characters become `_`. So:

| Template call | Generated prefix |
|--------------|-----------------|
| `install_package { name = "nginx" }` | `install_package_nginx` |
| `certbot { domain = "app.example.com" }` | `certbot_app_example_com` |
| `allow_port { port = "443" }` | `allow_port_443` |
| `vhost { domain = "my-site.io" }` | `vhost_my_site_io` |

Use these sanitized names in `needs` declarations.

---

## Execution model

### The pipeline

```
Source (.cg) → Lexer → Parser → Resolver → Plan → Apply
                                    ↓
                              Repo templates
                              are loaded here
```

1. **Lex**: Tokenizes the source. Catches unterminated strings, bad characters.
2. **Parse**: Builds an AST. Catches structural errors (missing braces, unknown keywords).
3. **Resolve**: Expands templates, flattens nesting, resolves `needs` references, detects cycles, computes topological waves. This is where deduplication happens.
4. **Plan**: Prints the execution order (no side effects).
5. **Apply**: Executes resources wave by wave.

### Wave execution

Resources with no unmet dependencies form Wave 1. After Wave 1 completes, newly-unblocked resources form Wave 2, etc. Within a wave, resources run in parallel (default: 4 concurrent, configurable with `--parallel`).

### Check-then-run

For each resource:

1. Evaluate `when` clause. If false → skip.
2. Run `check` command. If exits 0 → skip (already satisfied).
3. Run `run` command. If fails, retry up to `retry` times with `delay` between.
4. If still failing, apply `on_fail` policy.

### Deduplication

When two template instances produce a resource with identical `check`, `run`, and `run_as`, the engine deduplicates: only one copy exists in the DAG, and both call sites are tracked for provenance. This typically happens with shared prerequisites like `apt-get update`.

---

## Available stdlib templates

Run `cgr repo index --repo ./repo` for the live catalog. 62 templates across 17 categories:

| Category | Templates | Description |
|----------|-----------|-------------|
| `apt/` | `install_package`, `add_repo`, `upgrade_all_packages` | Debian/Ubuntu package management |
| `dnf/` | `install_package`, `add_repo`, `upgrade_all_packages`, `enable_epel`, `remove_package` | RHEL/Fedora package management |
| `pkg/` | `install_from_url`, `pin_version` | Cross-distro package operations |
| `backup/` | `install_restic`, `restic_init`, `restic_backup`, `restic_prune`, `restic_check`, `restic_restore`, `install_backrest`, `backrest_add_repo` | Restic backup + Backrest web UI |
| `docker/` | `compose_up`, `pull_image`, `prune_system` | Docker container management |
| `firewall/` | `allow_port` (UFW), `firewalld_allow_port`, `firewalld_allow_service` | UFW + firewalld |
| `security/` | `setup_fail2ban`, `selinux_boolean`, `selinux_port`, `selinux_mode` | SSH protection + SELinux |
| `ssh/` | `harden_sshd`, `authorize_key` | SSH hardening and key management |
| `user/` | `create_user`, `create_group`, `add_to_group`, `set_password` | User and group provisioning |
| `monitoring/` | `check_disk`, `check_service` | Health checks and monitoring |
| `db/` | `pg_backup`, `mysql_backup` | Database backup with rotation |
| `webhook/` | `slack_notify`, `discord_notify`, `generic_webhook` | Notifications via HTTP |
| `file/` | `create_swap`, `set_sysctl`, `logrotate_app`, `write_config`, `ensure_line`, `patch_json`, `patch_ini` | System file and config management |
| `service/` | `create_unit`, `timer_unit` | Systemd service/timer unit installation |
| `cron/` | `add_cron_job` | Cron job management |
| `nginx/` | `vhost` | Nginx virtual host configuration |
| `systemd/` | `enable_service` | Systemd service management |
| `tls/` | `certbot`, `self_signed`, `install_ca_cert` | TLS certificate provisioning |
| `k8s/` | `apply_manifest`, `wait_ready`, `helm_install` | Kubernetes provisioning workflows |
| `audit/` | `system_audit` | Full read-only system audit |

---

## State tracking and crash recovery

When you run `cgr apply`, the engine writes a persistent execution journal under `./.state/` in your current working directory. If your graph is `webserver.cgr` and you run from that directory, the default state file is `.state/webserver.cgr.state`.

While an apply run is active, CommandGraph also holds an advisory OS lock on `<statefile>.lock`. That lock is what prevents concurrent runs from clobbering the same state file. The `_lock` / `_unlock` journal records are still written for history and diagnostics, but the OS lock is the correctness mechanism.

### How it works

The state file is JSON Lines — one record per line, appended after each resource completes:

```json
{"id":"web-1.install_nginx","status":"success","rc":0,"ms":1423,"wave":2,"ts":"2026-03-19T14:22:01Z"}
{"id":"web-1.start_nginx","status":"failed","rc":1,"ms":234,"wave":6,"ts":"2026-03-19T14:22:03Z"}
```

The critical safety invariant: a resource is written to the state file **only after** its execution completes. If the process is killed mid-step, that step is NOT in state, so it re-runs on resume.

The journal also records per-wave wall-clock time and the latest run summary (`wall_ms`, bottleneck step, bottleneck duration). `state show` surfaces the latest run wall time and bottleneck.

### Resume behavior

On re-run with an existing state file:

| State says | What happens |
|------------|-------------|
| `success`, `skip_check`, `skip_when` | Skipped entirely. No check runs. Instant. |
| `warned` | Re-executed. Worth retrying. |
| `failed` | Re-executed. This is the recovery point. |
| Not in state | Executed. Never attempted. |

### Inspecting state

`state show` displays a table of every resource's execution status:

```
$ cgr state show webserver.cgr

State: .state/webserver.cgr.state
────────────────────────────────────────────────────
  ✓ done    update_cache         423ms  2026-03-19T14:22:00Z
  ✓ done    install_nginx       1891ms  2026-03-19T14:22:01Z
  ✗ failed  start_nginx          234ms  2026-03-19T14:22:03Z  exit=1
  · pending configure_vhost         —   (never ran)
────────────────────────────────────────────────────
  Done: 12 | Failed: 1 | Pending: 6
```

### Testing for drift

`state test` re-runs every completed resource's `check` command against the live system. If something changed outside the graph (someone uninstalled a package, deleted a file), the test catches it and auto-marks the drifted step for re-run:

```
$ cgr state test webserver.cgr

  ✓ valid  update_cache          check still passes
  ✗ drift  install_nginx         check FAILED — marking for re-run
  ✓ valid  allow_port_80         check still passes
```

### Manual overrides

Force-mark a step as done (if you fixed it by hand):
```
$ cgr state set webserver.cgr "install nginx" done
```

Force a step to re-run:
```
$ cgr state set webserver.cgr "start nginx" redo
```

Start completely fresh:
```
$ cgr state reset webserver.cgr
```

Ignore state for one run without deleting it:
```
$ cgr apply webserver.cgr --no-resume
```

For graphs that always run fresh (CI pipelines, deployment scripts), declare stateless mode in the graph file itself so `--no-resume` is never needed:
```
set stateless = true

target "local" local:
  [build]:
    run $ npm run build
  [deploy]:
    first [build]
    run $ ./deploy.sh
```

A stateless graph never reads or writes a `.state` file. `--report` and `--output json` still capture timing and results in memory. The `--state FILE` flag overrides `set stateless = true` with a warning (CLI wins). Stateless mode does not propagate to sub-graphs — each included graph manages its own state independently.

Remove one step from state so it re-runs next time:
```
$ cgr state drop webserver.cgr "start nginx"
```

The drop command appends a tombstone entry. On the next load, that tombstone removes the step from the in-memory state view so normal resume behavior treats it as not yet run.

### Locking notes

- If another process already holds the lock, `cgr apply` exits with a lock error instead of racing.
- If a process crashes, the OS releases the advisory lock automatically.
- Advisory locks may be unreliable on some network filesystems such as NFS. Prefer local disks for state files.

---

## Data collection and reporting

Steps can capture their stdout for later reporting using the `collect` clause. This is useful for audit workflows, system surveys, and any graph that gathers data across hosts.

### Marking steps for collection

In `.cgr`:
```
[check disk usage]:
  run $ df -h
  collect "disk_usage"
```

In `.cg`:
```
resource check_disk {
  run `df -h`
  collect "disk_usage"
}
```

The string after `collect` is the **output key** — it becomes the column header in CSV and the field key in JSON reports.

### Viewing collected data

After running `cgr apply`, collected outputs are stored in `<graph>.output` (JSONL format). Use `cgr report` to view or export:

```bash
cgr report system_audit.cgr                    # pretty table
cgr report system_audit.cgr --format json       # JSON export
cgr report system_audit.cgr --format csv        # CSV export
cgr report system_audit.cgr -o report.csv       # write to file
cgr report system_audit.cgr --keys hostname,kernel  # filter by key
```

### Multi-node CSV format

For multi-node graphs, CSV output uses nodes as rows and collect keys as columns:

```csv
node,hostname,kernel,disk_usage
web-1,web-prod-1,5.15.0-91,/dev/sda1 50G 12G 38G 24%
web-2,web-prod-2,5.15.0-91,/dev/sda1 50G 18G 32G 37%
```

### How it works

- Only steps with `collect "key"` have their stdout persisted
- Output is stored in `<graph>.output` (append-only JSONL, crash-safe)
- Outputs are capped at 1MB per entry (truncated with a marker)
- On resume, re-run steps append new entries (latest per key wins)
- The `--report` JSON includes an `"outputs"` field with collected data
- The web IDE shows collected outputs in the **Report** tab with JSON/CSV export buttons

### Interaction with existing features

- `collect` does **not** change execution semantics — check/run/skip works identically
- Collected outputs are separate from the `.state` file (which tracks execution status only)
- Variable expansion works in collect keys: `collect "${name}_disk"` expands at resolve time
- Templates can include `collect` clauses — the key is expanded with template parameters

---

## Runtime variable binding

Steps can write values into the variable namespace at runtime, making decisions from one step available as `${var}` references in later steps (and in `when` conditions).

### `collect "key" as varname`

Capture a step's stdout both to the output log **and** into a live variable:

```
[probe compression]:
  run    $ ... ; echo "yes"   # or "no" as final line
  collect "compression_decision" as compress
  timeout 120s

[compress iso]:
  first  [probe compression]
  when   "${compress} == 'yes'"
  run    $ zstd -1 -T0 ...
```

The variable `compress` is set to the trimmed stdout of the step. Later steps can gate on it with `when`.

### `on success: set` / `on failure: set`

Set variables based on whether a step succeeded or failed, without relying on stdout:

```
[deploy service]:
  run  $ systemctl start myapp
  on success: set deploy_status = "ok"
  on failure: set deploy_status = "failed"
  if fails ignore

[notify]:
  first [deploy service]
  run   $ curl -d "status=${deploy_status}" https://hooks.example.com/notify
```

Multiple `on success` / `on failure` clauses are allowed per step.

### `phase "name" when "COND":` — conditional group blocks

When a graph has two or more modes (install vs. rollback, upgrade vs. downgrade), repeating `when "action == 'install'"` on every step is noise. A `phase` block applies the condition to every step inside it:

```
set action = "install"

target "local" local:

  phase "install" when "action == 'install'":

    [install packages]:
      skip if $ dpkg -s nginx >/dev/null 2>&1
      run    $ apt install -y nginx

    [start service]:
      first [install packages]
      run $ systemctl enable --now nginx

  phase "rollback" when "action == 'rollback'":

    [stop service]:
      always run $ systemctl stop nginx

    [remove packages]:
      first [stop service]
      always run $ apt remove -y nginx
```

**Semantics:**
- Every step inside the phase inherits `when "COND"` unless it already has its own `when` clause. The step's own `when` always takes priority.
- `cgr plan` shows a **Phase:** label before the first step of each phase to make the structure clear.
- Phase blocks flatten into the normal resource graph — they are invisible to the state journal and the resolver. Resume, dedup, and all other invariants are unaffected.
- Phases do **not** nest. A `phase` inside a `phase` is a parse error.
- Override the action at runtime: `cgr apply FILE --set action=rollback`

**Multiline `run:` inside a phase:**

```
phase "install" when "action == 'install'":

  [configure wayvnc] as root, if fails ignore:
    skip if:
      test ! -f /etc/wayvnc/config
      grep -q '^enable_auth=false' /etc/wayvnc/config
    run:
      sed -i 's/enable_auth=true/enable_auth=false/' /etc/wayvnc/config
      systemctl restart wayvnc || true
```

### Multiline `run:` and `skip if:` blocks

Long shell one-liners can be split into indented blocks. Lines are run as a POSIX shell script with `set -e`, so if a simple command exits non-zero, the remaining lines do not run (fail-fast). For simple command lists, CommandGraph also reports the failing command when the shell exits without stderr.

**`run:` block:**
```
[setup directories] as root:
  run:
    mkdir -p /opt/app/logs
    chown app:app /opt/app
    chmod 750 /opt/app/logs
```
Equivalent to:
```
run:
  set -e
  mkdir -p /opt/app/logs
  chown app:app /opt/app
  chmod 750 /opt/app/logs
```

To continue past a failure on one line, append `|| true`:
```
run:
  cp /etc/wayvnc/config /tmp/config.bak || true
  sed -i 's/enable_auth=true/enable_auth=false/' /etc/wayvnc/config
```

**`always run:` block:**
```
[restart services] as root:
  always run:
    systemctl daemon-reload
    systemctl restart nginx
    systemctl restart novnc
```

**`skip if:` block:**
```
[create cert] as root:
  skip if:
    test -s /etc/nginx/certs/vnc.key
    test -s /etc/nginx/certs/vnc.crt
  run $ openssl req -x509 ...
```
All conditions must exit 0 for the step to be skipped — equivalent to joining with `&&`.

### `when` with runtime variables

`when` expressions are evaluated at execution time, so they can reference variables set by prior steps:

```
when "${compress} == 'yes'"
when "${deploy_status} != 'failed'"
```

Operators supported: `==` and `!=`. Values are compared as strings. Quote the literal with single quotes inside the double-quoted expression.

### `reduce "key"`

Aggregate all outputs collected under a key by prior steps into a single value. No shell command is run — the step's output IS the aggregated text:

```
[verify host-1]:
  run     $ sha256sum check ...
  collect "host_result"

[verify host-2]:
  run     $ sha256sum check ...
  collect "host_result"

[fleet summary]:
  first  [verify host-1], [verify host-2]
  reduce "host_result"          # joins all host_result outputs with newlines
  collect "fleet_summary"
  if fails warn
```

`reduce "key" as varname` additionally binds the aggregated text to a runtime variable.

### Retry with exponential backoff

For network-bound steps that should wait longer between each retry:

```
[push to host]:
  run    $ rsync -aP ...
  retry  3x backoff 30s..300s jitter 10%
  timeout 7200s
```

- `30s` — initial delay after first failure
- `..300s` — cap; delay doubles each attempt but never exceeds this
- `jitter 10%` — randomises ±10% to avoid thundering herds
- Minutes are also supported: `retry 5x backoff 1m..15m jitter 20%`

The old fixed-delay form still works: `retry 3x wait 5s`.

---

## HTML visualizer and demo mode

`cgr visualize` generates a self-contained HTML file with an interactive DAG visualization. Open it in any browser — no server required.

### Features

The visualizer shows resources grouped by execution wave, with SVG dependency arrows, click-to-inspect detail panels, search filtering, and provenance color coding. If a `.state` file exists, resources show their execution status (green for done, red for failed, gray for pending).

### One-click demo

Every generated HTML file includes a **▶ Demo** button in the toolbar. Clicking it simulates the graph executing wave by wave, directly in the browser — no Python, no SSH, no setup:

- Resources transition through visual states: checking → running → success/skip
- Waves resolve in sequence, with parallel resources staggering within a wave
- One resource in a late wave is randomly selected to **fail**, demonstrating the crash-recovery story
- After the failure, the demo pauses, then **resumes** — showing the failed step retrying and succeeding
- Toast notifications narrate what's happening

The speed dropdown (0.5x, 1x, 2x, 4x) controls simulation speed. The demo resets cleanly — it doesn't alter the visualization's normal state.

This makes the HTML file a self-contained pitch: send someone `webserver.html`, they open it, click Demo, and see exactly what CommandGraph does in 30 seconds.

---

## CLI reference

| Command | Description |
|---------|-------------|
| `plan FILE` | Show execution plan. `-v` shows check/run commands. |
| `apply FILE` | Execute the graph. `--dry-run` simulates. `--parallel N` sets concurrency. `--report FILE.json` writes JSON results with `wall_clock_ms`. `--output json` prints machine-readable execution results to stdout. `--timeout SECS` aborts after SECS seconds. `--start-from STEP` skips waves before STEP. `--no-resume` ignores state. `-v` shows all commands and output. `--no-color` disables color. |
| `validate FILE` | Parse and validate without executing. Shows node/resource/wave counts and provenance. `-q`/`--quiet` exits 0/1 only (for CI/CD). `--json` outputs machine-readable JSON with all validation details (nodes, resources, waves, variables, provenance, cross-node deps). On error, outputs `{"valid": false, "error": "..."}`. |
| `visualize FILE` | Generate self-contained interactive HTML visualization. `-o FILE.html` sets the output path. `--state FILE.state` overlays execution results (auto-detected if omitted). |
| `dot FILE` | Emit Graphviz DOT format to stdout. Pipe to `dot -Tpng -o graph.png`. |
| `state show FILE` | Display the state table for a graph. |
| `state test FILE` | Re-run check commands against live system, detect drift. |
| `state reset FILE` | Delete the state file. |
| `state set FILE STEP done\|redo` | Force-mark a step as completed or for re-run. |
| `state drop FILE STEP` | Remove a step from the state file. |
| `state diff FILE1 FILE2` | Compare two state files, show added/removed/changed resources. |
| `report FILE` | View or export collected outputs. `--format table\|json\|csv` sets format (default: table). `-o FILE` writes to file. `--keys K1,K2` filters by collect key. |
| `repo index` | Scan the repo and print the template catalog. Writes `repo.index` JSON. |
| `lint FILE` | Check for best-practice issues. Returns warning count (0 = clean). Checks include: missing `skip if`, long commands, `sed -i` without backup, chained `sed -i`, `echo`/`printf`/`tee` writes to files, local steps manually calling `ssh`, shell-built optional flags, shell polling loops, missing `validate` after provisioning writes to config files, unused variables, duplicate collect keys, templates without version constraints. |
| `fmt FILE` | Auto-format a `.cgr` file (normalises indentation, spacing). |
| `convert FILE` | Convert between `.cg` and `.cgr` formats. |
| `diff FILE1 FILE2` | Compare two graph files structurally — shows added/removed/changed resources and dependencies. |
| `init [FILE]` | Create a minimal `.cgr` scaffold file (default: `setup.cgr`). |
| `doctor` | Check environment: Python version, SSH client, sudo, template repo, Vault passphrase, graph files in CWD. |
| `secrets` | Manage encrypted `.secrets` files for offline secret storage. |
| `ping FILE` | Verify SSH connectivity to all targets defined in the graph. |
| `how FILE` | Show the file's documentation header and a **Defaults** table of all top-level `set` declarations. Default expressions are hidden. Works without resolving the graph. |
| `explain FILE STEP` | Show the full dependency chain for a step. |
| `why FILE STEP` | Show what steps depend on a step (reverse of explain). |
| `check FILE` | Re-run all check clauses against the live system and report drift. |
| `test` | Run template tests: parse, resolve, and validate all templates in the repo. |
| `serve [FILE]` | Start the web IDE (editor + live visualization + execution). `--port N` sets the port (default: 8420). `--host ADDR` sets the bind address (default: 127.0.0.1). `--allow-host HOST` allows a reverse-proxy hostname through the Host-header check. If `VSCODE_PROXY_URI` is set, its hostname is auto-allowed. `--no-open` suppresses automatic browser launch (for headless/CI environments). |
| `version` | Show engine version and Python version. |

All commands that load a graph file accept `--repo DIR` to set the repository path. The engine searches: `--repo` flag → `CGR_REPO` env var → `repo/` next to the graph file → `repo/` in CWD → `~/.cgr/repo/` → `~/.commandgraph/repo/` (legacy).

### `cgr serve` security model

- The IDE is intended for local use. The default bind address is `127.0.0.1`.
- Browser access is restricted to same-port localhost origins only: `localhost`, `127.0.0.1`, and `::1`.
- Reverse proxies that rewrite the `Host` header can be allowlisted with `--allow-host proxy.example.com` while keeping the server bound to loopback.
- If `VSCODE_PROXY_URI` is present, `cgr serve` auto-allows that proxy hostname.
- Mutating API calls require a CSRF token generated on server startup.
- The token is written to `~/.config/cgr/.ide-token` with mode `0600` and is also available to the served IDE via `GET /api/csrf`.
- Side-editor writes reject symlink targets and use atomic replace-on-write semantics.

If you expose `cgr serve` beyond the local machine with reverse proxies or port forwarding, treat it as a privileged local admin surface.

---

## Common patterns and recipes

### Pattern: Multi-package install with shared cache

```
use "apt/install_package"

node "server" {
  via local
  install_package { name = "nginx" }
  install_package { name = "curl" }
  install_package { name = "jq" }
  # update_cache runs once (deduped), then all three installs run in parallel
}
```

### Pattern: Sequential pipeline (no parallelism)

Chain resources with `needs` to force sequential execution:

```
resource step_1 {
  run `echo "first"`
}
resource step_2 {
  needs step_1
  run `echo "second"`
}
resource step_3 {
  needs step_2
  run `echo "third"`
}
```

### Pattern: Fan-out / fan-in

Multiple resources depend on the same root, then a single resource depends on all of them:

```
resource download_source {
  run `git clone https://repo.example.com/app /opt/app`
}
resource build_frontend {
  needs download_source
  run `cd /opt/app/frontend && npm install && npm run build`
}
resource build_backend {
  needs download_source
  run `cd /opt/app/backend && pip install -r requirements.txt`
}
resource run_tests {
  needs build_frontend, build_backend
  run `cd /opt/app && pytest`
}
```

### Pattern: Conditional resources (multi-distro)

```
var distro = "debian"

resource install_on_debian {
  when "${distro} == 'debian'"
  run `apt-get install -y nginx`
}
resource install_on_rhel {
  when "${distro} == 'rhel'"
  run `yum install -y nginx`
}
```

### Pattern: Multi-host orchestration

```
node "db-1" {
  via ssh host="10.0.1.10" user="deploy"
  resource install_postgres {
    run `apt-get install -y postgresql`
    as root
  }
}

node "web-1" {
  via ssh host="10.0.1.5" user="deploy"
  resource install_nginx {
    run `apt-get install -y nginx`
    as root
  }
}
```

Note: cross-node `needs` references are not supported — each node's resources form an independent DAG. Orchestrate cross-node ordering externally.

### Pattern: CI / deployment pipeline (stateless)

For graphs that run fresh on every push — no crash recovery needed, no leftover state files — declare stateless at the top:

```
set stateless = true

target "local" local:
  [test]:
    run $ npm test

  [build]:
    first [test]
    run $ npm run build

  [deploy]:
    first [build]
    run $ ./scripts/deploy.sh
```

`set stateless = true` is equivalent to always passing `--no-resume`. The engine never reads or writes a `.state` file. `--report` and `--output json` still work — timing and results are captured in memory and flushed at the end.

Statelessness does not propagate to sub-graphs included with `from ./other.cgr:`. Each graph manages its own state independently.

### Pattern: Inline nested for one-off subtrees

When a prerequisite is only needed by one resource, nest it:

```
resource deploy_app {
  description "Deploy the application"

  resource create_venv {
    check `test -d /opt/app/venv`
    run   `python3 -m venv /opt/app/venv`
  }

  resource install_deps {
    needs create_venv
    run   `/opt/app/venv/bin/pip install -r /opt/app/requirements.txt`
  }

  needs install_deps
  run   `/opt/app/venv/bin/gunicorn app:app --bind 0.0.0.0:8000 -D`
}
```

---

## Writing idempotent check commands

The `check` clause is the most important part of making a graph safe to re-run. A good check answers: "Is this step already done?"

| Operation | Good check |
|-----------|-----------|
| Package installed | `` `dpkg -l PACKAGE 2>/dev/null \| grep -q '^ii'` `` |
| Command available | `` `command -v BINARY >/dev/null 2>&1` `` |
| File exists | `` `test -f /path/to/file` `` |
| Directory exists | `` `test -d /path/to/dir` `` |
| Symlink exists | `` `test -L /path/to/link` `` |
| Service running | `` `systemctl is-active SERVICE \| grep -q active` `` |
| Service enabled | `` `systemctl is-enabled SERVICE \| grep -q enabled` `` |
| Port open in firewall | `` `ufw status \| grep -q 'PORT/tcp.*ALLOW'` `` |
| Config value set | `` `grep -q 'expected_value' /etc/config/file` `` |
| User exists | `` `id USERNAME >/dev/null 2>&1` `` |
| Docker container running | `` `docker ps --format '{{.Names}}' \| grep -q CONTAINER` `` |
| APT cache fresh (<60 min) | `` `find /var/lib/apt/lists -mmin -60 \| grep -q .` `` |

Set `check` to `` `false` `` to force a resource to always run (useful for validation commands like `nginx -t`).

---

## Error messages and debugging

The parser produces errors with exact line/column pointers:

```
error: Expected 'run', got RBRACE '}'  at mysetup.cg:14:3
    14 │   }
         ^
  Hint: Resource 'install_nginx' missing 'run'
```

Cycle detection shows the dependency path:

```
error: Dependency cycle detected!

    resource a
      └─ needs b
        └─ needs a  ← cycle

  Break the cycle by removing one 'needs' declaration.
```

Undefined references show candidates:

```
error: Resource 'web-1.start_nginx' needs 'web-1.instal_nginx' which doesn't exist.
  Known resources:
    • web-1.install_package_nginx
    • web-1.install_package_curl
```

---

## Instructions for AI agents

If you are an AI agent tasked with creating a CommandGraph dependency graph, follow this procedure:

Suggested automation agents: Codex or Claude Code. Use them to author and validate graphs from the operator's machine; CommandGraph still does not require any resident agent on target hosts.

1. **Read this entire document first.** Pay particular attention to the "Resource naming and how needs resolves" section — this is where most errors occur.

2. **Choose a format.** Use `.cg` (structured) for complex graphs with heredocs, deep nesting, or when machine-generating. Use `.cgr` (readable) if the output will be hand-edited by humans.

3. **Identify the goal.** What end state must be true on which host(s)?

4. **Check the repo.** Run or simulate `cgr repo index --repo ./repo`. Reuse existing templates wherever possible. Do not reinvent `apt-get install` — use `apt/install_package`. To understand an existing `.cgr` file's interface before running it, use `cgr how FILE` — it prints the documentation header and a table of all `set` variables with their default expressions.

5. **Decompose backwards.** Start from the end state and work backwards to roots. Draw the tree mentally or in comments.

6. **Write the file.** Follow this order:

   For `.cgr` (readable):
   - `---` title (optional)
   - `set` variables
   - `using` imports
   - `target` blocks, each containing:
     - `[named steps]` with `first` for dependencies
     - Template steps with `from template/path`
     - `verify` block (last)

   For `.cg` (structured):
   - `use` imports (at top)
   - `var` declarations
   - `node` blocks, each containing:
     - Template instantiations
     - Inline resources with `needs`
     - `verify` block (last)

7. **Get dependency names right.**
   - In `.cgr`: you choose the name in `[brackets]` and reference the exact same `[name]` in `first`. No naming conventions to memorize.
   - In `.cg`: template instances are named `templatename_sanitizedparam` (non-alphanumeric → `_`). When in doubt, use `validate` to check.

8. **Always include an idempotency check** (`skip if` in `.cgr`, `check` in `.cg`) unless the step must run every time. This makes the graph safe to re-run.

9. **Always include a description.** In `.cgr`, the `[step name]` IS the description. In `.cg`, add an explicit `description` field.

10. **Validate before applying.** Always run `validate` and `plan` before `apply`.

11. **If creating a new template** for the repo:
    - Templates can be written in `.cgr` (preferred) or `.cg` format
    - One template per file
    - File path = import path (e.g., `docker/compose_up.cgr` → `use "docker/compose_up"`)
    - Document parameters in a comment header
    - Nest prerequisites as `[child step]` blocks
    - Run `repo index` after adding

### Common agent mistakes to avoid

- **Wrong `needs`/`first` reference for template instances.** In `.cg`: `install_package { name = "nginx" }` creates `install_package_nginx`, NOT `install_nginx`. In `.cgr`: `[install nginx] from apt/install_package` is referenced as `first [install nginx]` — the name you chose.
- **Using `${var}` in identifiers.** Variable expansion works in strings and commands, NOT in `needs`/`first` references or step names. Write `first [install nginx]`, not `first [install ${pkg}]`.
- **Forgetting imports.** Template calls fail at parse time without `using`/`use`.
- **Circular dependencies.** If A needs B and B needs A, the resolver rejects the graph.
- **Missing `as root` for privileged commands.** `apt-get`, `systemctl`, `ufw`, and file writes to `/etc/` all need `as root`.
- **Heredocs in `.cgr`.** The indentation-based parser can't handle multi-line heredocs. Use `printf` or a single-line `echo` instead, or switch to `.cg` for that graph.

---

## File format cheat sheets

### Readable format (.cgr)

```
--- Title ---

set NAME = "value"

using repo/template, repo/other

target "host" ssh user@10.0.1.5:

  [step name] as root, timeout 2m, retry 1x wait 5s, if fails warn:
    first [dependency], [other dep]
    skip if $ exit-0-if-already-done
    run    $ do-the-thing
    env KEY = "value"

  [reusable step] from repo/template:
    param = "value"

  [parent step]:
    [nested child]:
      skip if $ check
      run    $ action
    first [nested child]
    run $ parent-action

  verify "smoke test":
    first [step name]
    run   $ curl -sf http://localhost/
    retry 3x wait 2s

  # Parallel constructs:
  [step with parallel]:
    parallel 3 at a time:
      [branch a]: run $ cmd-a
      [branch b]: run $ cmd-b
    run $ after-all-branches

  [step with race]:
    race:
      [try option 1]: run $ cmd-1
      [try option 2]: run $ cmd-2

  [step with each]:
    each server in ${servers}, 3 at a time:
      [deploy to ${server}]:
        run $ ssh ${server} 'deploy.sh'

  [rolling deploy]:
    stage "deploy":
      phase "canary" 1 from ${list}:
        [action ${server}]: run $ cmd
        verify "ok": run $ check
      phase "rest" remaining from ${list}:
        each server, 4 at a time:
          [action ${server}]: run $ cmd

# Template file (.cgr):
template install_package(name, version = "latest"):
  description "Install ${name} via APT"
  [update cache] as root, timeout 2m:
    skip if $ find /var/lib/apt/lists -mmin -60 | grep -q .
    run    $ apt-get update -y
  first [update cache]
  skip if $ dpkg -l ${name} | grep -q '^ii'
  run    $ apt-get install -y ${name}
  as root, timeout 3m
```

### Structured format (.cg)

```
# ─── Top level: var, use, template, node ─────────
var NAME = "value"
use "repo/path"
use "repo/path" as alias

template name(param1, param2 = "default") {
  description "..."
  resource child { ... }
  needs child
  check `...`
  run   `...`
  as root
}

node "host-label" {
  via ssh host="..." user="..." port=22
  # or: via local

  template_name { param = "value" }

  group name as root on_fail stop {
    resource name { ... }
  }

  resource name {
    description "..."
    needs dep1, dep2
    check `exit 0 if done`
    run   `do the thing`
    as root
    timeout 120
    retry 2 delay 10
    on_fail stop|warn|ignore
    when "expr == 'value'"
    env KEY = "value"

    resource nested_child { ... }
  }

  verify "description" {
    needs dep1, dep2
    run   `smoke test command`
    retry 3 delay 2
    on_fail warn
  }
}
```
