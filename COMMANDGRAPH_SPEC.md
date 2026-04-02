# CommandGraph Language Specification

> **Purpose**: This document is a complete, formal specification of the CommandGraph DSL. It is intended for AI agents and human developers who need to *programmatically generate* valid CommandGraph files — for example, an LLM that converts natural-language task descriptions into executable `.cgr` graphs, or a tool that imports Ansible playbooks into CommandGraph format.
>
> If you are reading this as an AI agent tasked with generating `.cgr` files: follow the grammar rules exactly. Every production rule has examples. The "Code Generation Patterns" section at the end gives you templates for common tasks.

---

## 1. Overview

CommandGraph has two file formats:

| Format | Extension | Use when |
|--------|-----------|----------|
| **Readable** | `.cgr` | Human authoring, AI generation, runbooks |
| **Structured** | `.cg` | Machine generation, complex nesting |

**This spec covers `.cgr` exclusively** — it is the recommended format for generation. The `.cg` format is a brace-delimited variant that produces the same AST; see the MANUAL for its syntax.

A `.cgr` file describes a **directed acyclic graph (DAG)** of shell commands to execute on one or more hosts. The engine resolves dependencies, groups independent steps into parallel waves, tracks state across crashes, and executes over SSH or locally.

---

## 2. File Structure

A `.cgr` file has four top-level constructs, all optional except `target`:

```
--- Title ---                          # optional, decorative only

set VAR = "value"                      # 0+ variable declarations
set VAR2 = "value2"

using category/template_name           # 0+ template imports

target "node-name" ssh user@host:      # 1+ target blocks
  [step name]:                         #   steps inside the target
    ...
```

**Processing order**: variables are collected first, then `using` imports are resolved, then each `target` block is parsed and its steps are resolved into a DAG.

### 2.1 Title

```
--- My Deployment ---
```

The title line starts and ends with `---`. It is purely decorative and has no semantic meaning. It is optional.

### 2.2 Variables (`set`)

```
set domain = "example.com"
set version = "2.1.0"
set servers = "web-1,web-2,web-3"
```

**Grammar**:
```
"set" SPACE IDENTIFIER SPACE? "=" SPACE? QUOTED_STRING
```

- `IDENTIFIER`: `[a-zA-Z_][a-zA-Z0-9_]*`
- `QUOTED_STRING`: `"` (any chars except `"`) `"`
- Variables are referenced as `${name}` in any string field (commands, descriptions, template args).
- Variables are expanded at resolve time, not parse time.
- There is no variable interpolation nesting (`${${var}}` is not supported).

### 2.3 Template Imports (`using`)

```
using apt/install_package
using apt/install_package, tls/certbot, nginx/vhost
```

**Grammar**:
```
"using" SPACE import_path ("," SPACE? import_path)*
```

- `import_path`: a `/`-separated path into the template repository (e.g., `apt/install_package`).
- The engine searches for `REPO_DIR/apt/install_package.cgr` first, then `.cg`.
- Continuation lines (indented below `using`) are joined.

### 2.4 Target Blocks

```
target "web-1" ssh deploy@10.0.1.5:
target "db" ssh deploy@10.0.2.3 port 2222:
target "local-build" local:
```

**Grammar**:
```
"target" SPACE QUOTED_STRING SPACE via_clause ":"
```

**Via clauses**:
| Syntax | Meaning |
|--------|---------|
| `ssh USER@HOST` | Execute commands via SSH as USER on HOST (port 22) |
| `ssh USER@HOST port N` | SSH on a non-standard port |
| `local` | Execute commands on the local machine |

- `USER` and `HOST` may contain `${var}` references.
- The target name (in quotes) is an arbitrary label used in output and state files.
- Each target block is a local DAG. Targets can declare ordering with `after "node"`, and steps can reference steps in other targets with `first [node/step_name]`. The engine builds a global DAG across all targets.

---

## 3. Steps

Steps are the core unit. Each step is a resource in the DAG with an optional idempotency check and a command to run.

### 3.1 Step Header

```
[step name]:
[step name] as root:
[step name] as root, timeout 2m, retry 3x wait 10s, if fails warn:
```

**Grammar**:
```
"[" STEP_NAME "]" (SPACE header_props)? ":"
```

- `STEP_NAME`: any text except `]`. This is the human-readable name.
- The name is converted to a slug for internal use: `[install nginx]` → `install_nginx`.
- The colon at the end is **required**.

### 3.2 Header Properties

Properties appear after the `]` and before the `:`, comma-separated:

| Property | Syntax | Default | Meaning |
|----------|--------|---------|---------|
| `as USER` | `as root` | (SSH user or local user) | Run command as this user via sudo |
| `timeout` | `timeout 30s` or `timeout 2m` | `300s` | Kill command after this duration |
| `retry` | `retry 3x wait 10s` | 0 retries | Retry on failure, with delay |
| `if fails` | `if fails stop\|warn\|ignore` | `stop` | What to do on failure |

**Properties can also appear as standalone lines in the step body** (useful in templates):
```
[step name]:
  skip if $ some check
  run    $ some command
  as root, timeout 2m, retry 1x wait 5s
```

### 3.3 Step Body Keywords

Inside a step body (indented under the header), the following keywords are recognized:

#### `first` — declare dependencies

```
first [other step]
first [step a], [step b], [step c]
```

- References are by the human-readable name in `[brackets]`.
- Multiple dependencies are comma-separated on one line.
- `needs` is accepted as a synonym for `first`.
- Dependencies are resolved by slug: `[install nginx]` → looks for `install_nginx` in the same target.

#### `skip if` — idempotency check

```
skip if $ dpkg -l nginx 2>/dev/null | grep -q '^ii'
skip if $ test -f /etc/nginx/nginx.conf
```

- The `$` prefix is conventional but optional (stripped by the parser).
- If the command exits `0`, the step is **skipped** (already satisfied).
- If it exits non-zero, the `run` command executes.
- Omitting `skip if` means the step always runs (unless state says it already succeeded).

#### `run` — the command to execute

```
run $ apt-get install -y nginx
run $ mkdir -p /opt/app && chown deploy:deploy /opt/app
```

- The `$` prefix is conventional but optional.
- Multi-line commands: continuation lines that are indented deeper than `run` and don't start with a keyword are joined with newlines.
- **Every non-construct step must have a `run` command.**

#### `always run` — unconditional execution

```
always run $ systemctl reload nginx
```

- Equivalent to `skip if $ false` + `run $ ...`. The step always executes regardless of state.

#### `env` — environment variables

```
env RAILS_ENV = "production"
env NODE_ENV = "production"
```

- Sets environment variables for the `run` and `skip if` commands.

#### `when` — conditional execution

```
when "os == 'debian'"
```

- If the expression evaluates to false, the entire step is skipped (status: `skip_when`).
- Supports `==` and `!=` operators on string values.

### 3.4 Nested Steps (Children)

Steps can contain child steps, creating implicit dependencies:

```
[parent step]:
  [child a]:
    run $ echo "a"
  [child b]:
    first [child a]
    run $ echo "b"
  first [child a], [child b]
  run $ echo "parent runs after children"
```

- Children are defined by indenting a `[name]:` block inside a parent.
- The parent's `run` command executes after all children complete.
- Children can reference siblings with `first [sibling name]`.

### 3.5 Template Instantiation

```
[install nginx] from apt/install_package:
  name = "nginx"

[get cert] from tls/certbot:
  domain = "${domain}"
  email = "ops@example.com"
```

**Grammar**:
```
"[" HUMAN_NAME "]" SPACE "from" SPACE import_path ":" NEWLINE
  (PARAM_NAME SPACE? "=" SPACE? QUOTED_STRING NEWLINE)*
```

- The `from` keyword triggers template instantiation.
- Parameters are passed as `key = "value"` lines in the body.
- The template's steps are expanded inline, with `${param}` substituted.
- The human name in brackets becomes the step's display name.

### 3.6 Verify Blocks

```
verify "HTTPS responds":
  first [start nginx]
  run $ curl -sf https://example.com/
  retry 5x wait 2s
```

**Grammar**:
```
"verify" SPACE QUOTED_STRING ":" NEWLINE
  body_lines
```

- Verify blocks are special steps with `is_verify = true`.
- They always run (no `skip if`).
- Default failure policy is `warn` (not `stop`).
- They support `first`, `run`, and `retry`.

### 3.7 HTTP Steps

Steps can make HTTP requests instead of running shell commands. HTTP verbs replace the `run` keyword:

```
[register host]:
  first [health check]
  post "https://api.example.com/hosts"
  auth bearer "${api_token}"
  header "X-Request-Id" = "cgr-001"
  body json '{"hostname": "web-1", "status": "active"}'
  expect 200..299
  collect "registration"
```

**HTTP step keywords:**

| Keyword | Syntax | Notes |
|---------|--------|-------|
| HTTP verb | `get\|post\|put\|patch\|delete "URL"` | Replaces `run $`. URL supports variable expansion. |
| Auth | `auth bearer "TOKEN"` | Also: `auth basic "USER" "PASS"`, `auth header "NAME" = "VAL"` |
| Header | `header "NAME" = "VALUE"` | Repeatable. |
| Body | `body json '...'` or `body form "..."` | Sets Content-Type automatically. |
| Expect | `expect 200` | Also: `expect 200..299`, `expect 200, 201, 204`. Default: any 2xx. |

**Execution model:** Local targets use `urllib.request`. SSH targets construct a `curl` command. Response body → stdout, status line → stderr. Auth tokens are automatically redacted.

**Validation:** A step must have either `run $` or an HTTP verb — not both, not neither (unless it has a parallel construct as its body).

---

## 4. Parallel Constructs

Four constructs for explicit parallelism. They appear **inside step bodies** and define how child steps are scheduled.

### 4.1 `parallel:` — fork/join

```
[setup everything]:
  parallel:
    [write config a]:
      run $ echo "a" > /etc/a.conf
    [write config b]:
      run $ echo "b" > /etc/b.conf
  run $ echo "all configs written"
```

**With bounded concurrency**:
```
parallel 3 at a time:
  [task 1]: run $ ...
  [task 2]: run $ ...
  [task 3]: run $ ...
  [task 4]: run $ ...
  [task 5]: run $ ...
```

**With failure policy**:
```
parallel, if one fails stop all:
parallel, if one fails wait for rest:
parallel, if one fails ignore:
```

**Grammar**:
```
"parallel" (SPACE INT SPACE "at a time")? ("," SPACE "if one fails" SPACE ("stop all" | "wait for rest" | "ignore"))? ":"
```

### 4.2 `race:` — first to succeed wins

```
race:
  [try mirror a]:
    run $ curl -sf https://a.example.com/pkg.tar.gz -o /tmp/pkg.tar.gz
  [try mirror b]:
    run $ curl -sf https://b.example.com/pkg.tar.gz -o /tmp/pkg.tar.gz
```

- All branches start concurrently.
- The first branch to exit `0` wins. All others are cancelled.
- If all fail, the race fails.

**Grammar**:
```
"race:"
```

### 4.3 `each:` — parallel iteration

```
set servers = "web-1,web-2,web-3,web-4"

[distribute]:
  each server in ${servers}, 2 at a time:
    [deploy to ${server}]:
      run $ ssh ${server} 'deploy.sh'
```

**Grammar**:
```
"each" SPACE IDENTIFIER SPACE "in" SPACE VAR_REF ("," SPACE INT SPACE "at a time")? ":"
```

- `VAR_REF`: `${variable_name}` — must expand to a comma-separated list.
- The loop variable (`server`) is available as `${server}` in child commands and step names.
- Each iteration becomes a separate resource in the DAG.
- Bounded concurrency is optional (default: all at once within the wave).

### 4.4 `stage`/`phase` — rolling deploys

```
set servers = "web-1,web-2,web-3,web-4,web-5,web-6"

[rolling deploy]:
  stage "production":
    phase "canary" 1 from ${servers}:
      [deploy ${server}]:
        run $ ssh ${server} 'activate.sh'
      verify "canary healthy":
        run $ curl -sf http://${server}:8080/health
        retry 10x wait 3s

    phase "half" 50% from ${servers}:
      each server, 2 at a time:
        [deploy ${server}]:
          run $ ssh ${server} 'activate.sh'

    phase "rest" remaining from ${servers}:
      each server, 4 at a time:
        [deploy ${server}]:
          run $ ssh ${server} 'activate.sh'
```

**Grammar**:
```
"stage" SPACE QUOTED_STRING ":"
  ("phase" SPACE QUOTED_STRING SPACE count_expr SPACE "from" SPACE VAR_REF ":" NEWLINE phase_body)+
```

**Count expressions**:
| Syntax | Meaning |
|--------|---------|
| `1` | Exactly 1 item |
| `3` | Exactly 3 items |
| `50%` | 50% of the list (rounded up) |
| `rest` or `remaining` | All items not yet assigned to a previous phase |

- Phases execute **sequentially** — each phase is a barrier.
- Within a phase, items execute in parallel (optionally bounded with `each server, N at a time:`).
- The loop variable is always `server` inside phases.
- `verify` blocks inside a phase act as gates — the next phase won't start until verification passes.

---

## 5. Template Files

Templates are reusable step trees stored in the repository. A template file contains exactly one `template` block.

### 5.1 Template Syntax (.cgr)

```
template install_package(name, version = "latest"):
  description "Install ${name} via APT"

  [update cache] as root, timeout 2m:
    skip if $ find /var/lib/apt/lists -mmin -60 | grep -q .
    run    $ apt-get update -y

  first [update cache]
  skip if $ dpkg -l ${name} | grep -q '^ii'
  run    $ apt-get install -y ${name}
  as root, timeout 3m, retry 1x wait 5s
```

**Grammar**:
```
"template" SPACE IDENTIFIER "(" param_list ")" ":" NEWLINE
  template_body
```

**Parameter list**:
```
param_list = IDENTIFIER ("," SPACE IDENTIFIER)* 
           | IDENTIFIER SPACE? "=" SPACE? QUOTED_STRING ("," ...)*
```

- Parameters without `= default` are required.
- Default values can reference other params: `email = "admin@${domain}"`.
- The template body uses the same step syntax as target blocks.
- The body's top-level `skip if`, `run`, `first`, and properties define the **root resource**.
- `[child step]` blocks inside the body define nested prerequisite resources.
- `description "text"` sets the template's description (supports `${param}` interpolation).

### 5.2 File Organization

```
repo/
├── apt/
│   ├── install_package.cgr     # template install_package(name, version = "latest")
│   └── add_repo.cgr            # template add_repo(repo_name, repo_line, key_url)
├── firewall/
│   └── allow_port.cgr          # template allow_port(port, proto = "tcp")
└── systemd/
    └── enable_service.cgr      # template enable_service(service)
```

- One template per file.
- File path = import path: `repo/apt/install_package.cgr` → `using apt/install_package`.
- The engine searches: `--repo` flag → `CGR_REPO` env var → `./repo` next to graph file → `./repo` in CWD → `~/.cgr/repo` → `~/.commandgraph/repo` (legacy).

---

## 6. Variable Expansion

Variables are expanded with `${name}` syntax in these contexts:

| Context | Example |
|---------|---------|
| `run` / `skip if` commands | `run $ apt-get install -y ${name}` |
| Step names | `[deploy to ${server}]` |
| Descriptions | `description "Install ${name} via APT"` |
| `when` expressions | `when "os == '${target_os}'"` |
| Template parameters | `domain = "${domain}"` |
| Target via clauses | `ssh ${user}@${host}` |

**Scope**: Global `set` variables + template parameters + `each` loop variable. Inner scopes shadow outer scopes.

**Not expanded**: Comments, title lines, property keywords.

---

## 7. Resource Naming and Dependency Resolution

### 7.1 Slug Generation

Every step name is converted to a slug:
```
[install nginx]        → install_nginx
[deploy to web-1]      → deploy_to_web_1
[update APT cache]     → update_apt_cache
```

Rule: replace all non-alphanumeric characters with `_`, strip leading/trailing `_`, lowercase.

### 7.2 Fully Qualified Resource IDs

```
target_name.step_slug                     # top-level step
target_name.parent_slug.child_slug        # nested child
target_name.template_name_primary_param   # template instance
```

### 7.3 Dependency Lookup

When you write `first [install nginx]`, the engine:
1. Slugifies: `install_nginx`
2. Looks for `target_name.current_parent.install_nginx` (sibling)
3. Falls back to `target_name.install_nginx` (top-level)
4. Raises an error if not found.

**Common mistake**: referencing a step in a different target. Cross-target dependencies are not supported.

---

## 8. Execution Model

### 8.1 Waves

The engine sorts resources topologically (Kahn's algorithm) and groups them into **waves**. All resources in a wave can execute in parallel — they have no dependencies on each other.

### 8.2 Check-then-Run

For each resource in a wave:
1. If `when` is set and evaluates to false → skip.
2. If `skip if` is set, run the check command. If exit `0` → skip (already done).
3. Run the `run` command. If it fails, apply the failure policy.
4. Record result to the state file.

### 8.3 State and Resume

The state file (`<graph>.state` by default, or a run-specific variant when `--run-id` / `--state` is used) is a JSON Lines journal. On re-run:
- `success` / `skip_check` → skip (don't re-execute)
- `failed` / `warned` → re-run
- Not in state → run (never attempted)
- `cancelled` (race) → re-run the race

### 8.4 SSH Connection Pooling

The engine opens a persistent SSH connection (ControlMaster) per unique host at the start of `apply` and reuses it for all commands. This eliminates the ~50ms TCP+handshake overhead per command.

---

## 9. Code Generation Patterns

Use these patterns when generating `.cgr` files programmatically.

### 9.1 Single-host setup (most common)

```
--- Deploy ${app_name} to ${environment} ---

set app_name = "${APP}"
set version = "${VERSION}"
set domain = "${DOMAIN}"

target "server" ssh deploy@${HOST}:

  [create directories] as root:
    skip if $ test -d /opt/${app_name}
    run    $ mkdir -p /opt/${app_name} && chown deploy:deploy /opt/${app_name}

  [download release]:
    first [create directories]
    skip if $ test -f /opt/${app_name}/${version}.tar.gz
    run    $ curl -sfL https://releases.example.com/${version}.tar.gz -o /opt/${app_name}/${version}.tar.gz

  [extract and install]:
    first [download release]
    skip if $ test -f /opt/${app_name}/current/bin/${app_name}
    run    $ cd /opt/${app_name} && tar xzf ${version}.tar.gz && ln -sfn ${version} current

  [write config]:
    first [create directories]
    skip if $ test -f /opt/${app_name}/config.toml
    run    $ cat > /opt/${app_name}/config.toml << 'EOF'
[server]
host = "0.0.0.0"
port = 8080
domain = "${domain}"
EOF

  [start service] as root:
    first [extract and install], [write config]
    skip if $ systemctl is-active ${app_name} | grep -q active
    run    $ systemctl restart ${app_name}

  verify "service responds":
    first [start service]
    run $ curl -sf http://localhost:8080/health
    retry 5x wait 2s
```

### 9.2 Multi-host fleet deployment

```
--- Deploy to fleet ---

set servers = "web-1,web-2,web-3,web-4"
set version = "2.1.0"

target "control" local:

  [build release]:
    skip if $ test -f /tmp/release-${version}.tar.gz
    run    $ make release VERSION=${version}

  [distribute to fleet]:
    first [build release]
    each server in ${servers}, 4 at a time:
      [upload to ${server}]:
        skip if $ ssh deploy@${server} "test -f /opt/app/${version}.tar.gz"
        run    $ scp /tmp/release-${version}.tar.gz deploy@${server}:/opt/app/

  [rolling deploy]:
    first [distribute to fleet]
    stage "production":
      phase "canary" 1 from ${servers}:
        [activate ${server}]:
          run $ ssh deploy@${server} "cd /opt/app && tar xzf ${version}.tar.gz && ln -sfn ${version} current && sudo systemctl restart app"
        verify "canary healthy":
          run $ curl -sf http://${server}:8080/health
          retry 10x wait 3s

      phase "remaining" rest from ${servers}:
        each server, 3 at a time:
          [activate ${server}]:
            run $ ssh deploy@${server} "cd /opt/app && tar xzf ${version}.tar.gz && ln -sfn ${version} current && sudo systemctl restart app"

  verify "all healthy":
    first [rolling deploy]
    run $ for s in web-1 web-2 web-3 web-4; do curl -sf http://$s:8080/health || exit 1; done
    retry 3x wait 5s
```

### 9.3 Using templates

```
--- Web server setup ---

using apt/install_package, firewall/allow_port, tls/certbot, nginx/vhost

set domain = "example.com"

target "web" ssh deploy@web-1:

  [install nginx] from apt/install_package:
    name = "nginx"

  [install certbot] from apt/install_package:
    name = "certbot"

  [open http] from firewall/allow_port:
    port = "80"

  [open https] from firewall/allow_port:
    port = "443"

  [get certificate] from tls/certbot:
    domain = "${domain}"
    email = "ops@example.com"

  [configure vhost] from nginx/vhost:
    domain = "${domain}"
    port = "443"

  verify "HTTPS works":
    first [configure vhost], [get certificate]
    run $ curl -sf https://${domain}/
    retry 5x wait 3s
```

### 9.4 Writing idempotent checks

The `skip if` command is the key to idempotency. Here are reliable patterns:

| What to check | `skip if` command |
|---------------|-------------------|
| Package installed | `dpkg -l PACKAGE \| grep -q '^ii'` |
| Command exists | `command -v BINARY >/dev/null 2>&1` |
| File exists | `test -f /path/to/file` |
| Directory exists | `test -d /path/to/dir` |
| Symlink exists | `test -L /path/to/link` |
| Service running | `systemctl is-active SERVICE \| grep -q active` |
| Service enabled | `systemctl is-enabled SERVICE \| grep -q enabled` |
| Port open (ufw) | `ufw status \| grep -q 'PORT/tcp.*ALLOW'` |
| User exists | `id USERNAME >/dev/null 2>&1` |
| File contains text | `grep -q 'PATTERN' /path/to/file` |
| Recent file (< 1hr) | `find /path -mmin -60 \| grep -q .` |
| Force re-run always | (omit `skip if` entirely, or use `always run`) |

### 9.5 Privilege escalation pattern

```
# Create directory as root, chown to deploy user, then work as deploy
[create workspace] as root:
  skip if $ test -d /opt/myapp
  run    $ mkdir -p /opt/myapp && chown deploy:deploy /opt/myapp

[write files]:
  first [create workspace]
  skip if $ test -f /opt/myapp/config.yml
  run    $ echo "key: value" > /opt/myapp/config.yml
```

- Use `as root` only on the steps that need it.
- Create directories as root, chown to the SSH user, then subsequent steps run unprivileged.
- The engine wraps `as root` as `sudo -u root -- bash -c '...'` over SSH.

---

## 10. Formal Grammar (PEG-style)

```
program       = (title / set_stmt / using_stmt / template_block / target_block)*

title         = "---" TEXT "---"

set_stmt      = "set" IDENT "=" QUOTED

using_stmt    = "using" import_path ("," import_path)*
import_path   = IDENT ("/" IDENT)*

template_block = "template" IDENT "(" param_list? ")" ":" INDENT template_body DEDENT
param_list    = param ("," param)*
param         = IDENT ("=" QUOTED)?
template_body = ("description" QUOTED)? step_body

target_block  = "target" QUOTED via_clause ":" INDENT target_body DEDENT
via_clause    = "ssh" USER "@" HOST ("port" INT)? / "local"
target_body   = (step / verify / template_inst / stage_block)*

step          = "[" STEP_NAME "]" header_props? ":" INDENT step_body DEDENT
header_props  = (as_prop / timeout_prop / retry_prop / fail_prop)*
as_prop       = "as" IDENT
timeout_prop  = "timeout" INT ("s" / "m")?
retry_prop    = "retry" INT "x" ("wait" INT ("s" / "m")?)?
fail_prop     = "if" "fails" ("stop" / "warn" / "ignore")

step_body     = (first_stmt / skip_stmt / run_stmt / always_stmt / env_stmt
               / when_stmt / prop_line / desc_line / child_step
               / parallel_block / race_block / each_block / stage_block)*

first_stmt    = ("first" / "needs") ("[" STEP_NAME "]" ","?)+
skip_stmt     = "skip" "if" "$"? COMMAND
run_stmt      = "run" "$"? COMMAND
always_stmt   = "always" "run" "$"? COMMAND
env_stmt      = "env" IDENT "=" QUOTED
when_stmt     = "when" QUOTED
prop_line     = as_prop / timeout_prop / retry_prop / fail_prop
desc_line     = "description" QUOTED
child_step    = step

verify        = "verify" QUOTED ":" INDENT verify_body DEDENT
verify_body   = (first_stmt / run_stmt / retry_line)*
retry_line    = "retry" INT "x" ("wait" INT ("s" / "m")?)?

template_inst = "[" STEP_NAME "]" "from" import_path ":" INDENT (IDENT "=" QUOTED)* DEDENT

parallel_block = "parallel" (INT "at a time")? fail_clause? ":"
                 INDENT step+ DEDENT
fail_clause    = "," "if one fails" ("stop all" / "wait for rest" / "ignore")

race_block    = "race:" INDENT step+ DEDENT

each_block    = "each" IDENT "in" VAR_REF ("," INT "at a time")? ":"
                INDENT step DEDENT

stage_block   = "stage" QUOTED ":" INDENT phase+ DEDENT
phase         = "phase" QUOTED count_expr "from" VAR_REF ":" INDENT phase_body DEDENT
count_expr    = INT / INT "%" / "rest" / "remaining"
phase_body    = (step / verify / each_phase)*
each_phase    = "each" IDENT ("," INT "at a time")? ":" INDENT step DEDENT

IDENT         = [a-zA-Z_][a-zA-Z0-9_]*
QUOTED        = '"' [^"]* '"'
VAR_REF       = "${" IDENT "}"
INT           = [0-9]+
COMMAND       = .+ (to end of line; continuation: indented non-keyword lines)
STEP_NAME     = [^\]]+
INDENT/DEDENT = relative indentation (2+ spaces per level, consistent within block)
```

---

## 11. Validation Rules

When generating `.cgr` files, ensure:

1. **Every step has `run`** (except construct-only steps with `parallel:`, `race:`, `each:`, or `stage:`).
2. **No circular dependencies.** `first [A]` in A is invalid.
3. **Dependencies must exist.** `first [nonexistent]` is an error.
4. **Cross-target dependencies use qualified syntax.** `first [target_name/step name]` (`.cgr`) or `needs target_name.step_name` (`.cg`). Node-level ordering: `target "web" ssh ..., after "db":` or `after "db"` inside a `node` block.
5. **Template parameters must be provided.** Required params (no default) must have `key = "value"` in the instantiation body.
6. **Variables must be defined.** `${undefined}` in a command is a resolve-time error.
7. **Indentation must be consistent.** Use 2 spaces per level. Mixing tabs and spaces is not supported.
8. **Step names must be unique within their scope.** Two steps named `[install nginx]` in the same target will collide.
9. **Colon is required at the end of headers.** `[step name]` without `:` is not parsed.

---

## 12. CLI Reference

```bash
cgr plan FILE [--repo DIR] [-v]           # show execution order
cgr apply FILE [--repo DIR] [--parallel N] # execute
                        [--dry-run] [--no-resume] [-v] [--no-color]
                        [--start-from STEP] [--timeout SECS]
                        [--report FILE.json]
cgr validate FILE [--repo DIR] [-q] [--no-color] # check syntax
cgr visualize FILE [--repo DIR] [-o F.html]      # interactive HTML
                            [--state FILE.state]
cgr dot FILE [--repo DIR]                  # Graphviz DOT output
cgr state show|test|reset|set|drop|diff FILE [STEP|FILE2] [VALUE]
cgr repo index [--repo DIR]                # catalog templates
cgr serve [FILE] [--port 8420] [--host ADDR] [--repo DIR] [--no-open] # web IDE
cgr version                                # show version info
```

Key flags:
- `--report FILE.json` writes a JSON report including `wall_clock_ms`, `total_resources`, per-resource status, and provenance data.
- `--run-id ID` salts the default state file path for this apply run.
- `--state FILE` uses an explicit state journal path for this apply run.
- `--start-from STEP` skips all waves before the named step (for debugging mid-graph).
- `--timeout SECS` aborts the entire run after SECS seconds (checked between waves, state preserved).
- `-q` / `--quiet` on validate exits 0/1 with no stdout (for CI/CD scripts).
- `--state FILE.state` on visualize overlays execution results on the graph (auto-detected if omitted).
- `state diff FILE1.state FILE2.state` compares two state files and shows added/removed/changed resources.

---

## 13. Common Mistakes When Generating

| Mistake | Fix |
|---------|-----|
| Missing `:` after `[step name]` | Always end the header with `:` |
| `$` in `skip if` without space | Use `skip if $ cmd` (space after `$`) |
| `first [step]` referencing a step in another target | Move the dependency into the same target or restructure |
| Using `as root` on steps that write to user-owned dirs | Only use `as root` when the command needs privilege |
| Forgetting `first` on dependent steps | If B needs A's output, B must say `first [A]` |
| Template param without quotes | Use `name = "nginx"` not `name = nginx` (quotes required for multi-word values) |
| `each` variable not in a `set` | The list variable must be defined with `set` before use |
| Spaces in step names causing slug collisions | `[install nginx]` and `[install  nginx]` both slug to `install_nginx` |
