# CommandGraph Cookbook

Real-world recipes you can copy and adapt. Each recipe is a complete, runnable
`.cgr` example with explanations.

**Prerequisites:** You've read the [Quickstart](QUICKSTART.md) or [Tutorial](TUTORIAL.md)
and understand `target`, `first`, `skip if`, `run`, and `set`.

---

## Patterns

### Optional CLI flags without shell glue

```
target "backup" local:
  [run backup]:
    run $ restic backup ${paths} --verbose
    flag "--tag ${tags}" when "${tags} != ''"
    flag "--exclude ${exclude}" when "${exclude} != ''"
```

### Poll until a query result changes

```
target "cluster" local:
  [wait for drain]:
    run $ kubectl get pods -o name | wc -l
    until "0"
    retry 60x wait 10s
```

### Generate config files with DSL-native writes

```
target "web" local:
  [write nginx config]:
    content > /etc/nginx/sites-available/example.com:
      server {
          listen 80;
          server_name example.com;
      }
    validate $ nginx -t
```

### Edit existing config lines atomically

```
target "ssh" local:
  [harden sshd]:
    line "PermitRootLogin no" in /etc/ssh/sshd_config, replacing "^#?PermitRootLogin"
    line "PasswordAuthentication no" in /etc/ssh/sshd_config, replacing "^#?PasswordAuthentication"
    validate $ sshd -t
```

### Feature detection with runtime variables

```
  [detect pigz]:
    run $ command -v pigz
    on success: set compressor = "pigz"
    on failure: set compressor = "gzip"
    if fails ignore

  [compress]:
    first [detect pigz]
    run $ ${compressor} archive.tar
```

---

## Recipe 1: Cross-Distro Package Install

**When to use:** Your fleet has both Debian and RHEL-family hosts.
**Concepts:** `when` expressions, variables, `--set` overrides.

```
--- Cross-Distro Install ---
set os_family = "debian"
set packages = "nginx curl jq"

target "server" local:

  [install packages (apt)]:
    when os_family == "debian"
    skip if $ dpkg -l ${packages} 2>/dev/null | grep -q "^ii"
    run $ apt-get install -y ${packages}

  [install packages (yum)]:
    when os_family == "redhat"
    skip if $ rpm -q ${packages} >/dev/null 2>&1
    run $ yum install -y ${packages}

  [verify install]:
    first [install packages (apt)]
    first [install packages (yum)]
    run $ which nginx && echo "OK"
```

**How it works:**
- `when os_family == "debian"` skips the step if the condition is false
- Both install steps target the same verify step — whichever runs, verify follows
- Override per host: `cgr apply --set os_family=redhat`

---

## Recipe 2: Deploy with Rollback Safety

**When to use:** Deploying an app where you need to verify success and can roll back.
**Concepts:** `skip if`, verify blocks, state recovery.

```
--- Safe App Deploy ---
set app_dir = "/opt/myapp"
set version = "2.4.1"

target "web" local:

  [backup current]:
    skip if $ test -f ${app_dir}/backup-${version}.tar.gz
    run $ tar czf ${app_dir}/backup-${version}.tar.gz -C ${app_dir} current/

  [download release]:
    first [backup current]
    skip if $ test -f ${app_dir}/releases/${version}.tar.gz
    run $ curl -sL https://releases.example.com/${version}.tar.gz -o ${app_dir}/releases/${version}.tar.gz

  [deploy release]:
    first [download release]
    run $ ln -sfn ${app_dir}/releases/${version} ${app_dir}/current

  [restart service]:
    first [deploy release]
    run $ systemctl restart myapp

  verify "app responds to health check":
    first [restart service]
    run $ curl -sf http://localhost:8080/health
    retry 3x wait 5s
    if fails warn
```

**How it works:**
- `backup current` is idempotent — won't re-backup if archive exists
- `download release` skips if already downloaded
- The verify block retries 3 times with 5s delay, warns instead of failing
- If deploy fails midway, `cgr apply` resumes from the failed step

**Customization:** Change `version` with `--set version=2.5.0`.

---

## Recipe 3: Parallel Fleet Deployment

**When to use:** Deploying the same thing to N servers concurrently.
**Concepts:** `each` loops, inventory files, concurrency limits.

Create `fleet.ini`:

```ini
[webservers]
web-1 ansible_host=10.0.1.1 ansible_user=deploy
web-2 ansible_host=10.0.1.2 ansible_user=deploy
web-3 ansible_host=10.0.1.3 ansible_user=deploy
web-4 ansible_host=10.0.1.4 ansible_user=deploy
```

```
--- Fleet Deploy ---
inventory "fleet.ini"
set version = "2.4.1"

each name, addr in ${webservers}:
  target "${name}" ssh ${addr}:
    [deploy to ${name}]:
      run $ /opt/deploy.sh ${version}

    [verify ${name}]:
      first [deploy to ${name}]
      run $ curl -sf http://localhost:8080/health
      retry 3x wait 5s
```

**How it works:**
- `inventory "fleet.ini"` loads hosts into the `${webservers}` variable
- `each name, addr in ${webservers}:` stamps out a target per host
- All targets run in parallel (default: 4 at a time)
- Override concurrency: `cgr apply --parallel 2`

**Customization:** Add/remove hosts in the INI file. Override with
`cgr apply -i production.ini --set version=2.5.0`.

---

## Recipe 4: Canary Rollout with Stages

**When to use:** Rolling out to one server first, verifying, then the rest.
**Concepts:** `stage`/`phase` blocks, verification gates.

```
--- Canary Rollout ---
set servers = "web-1,web-2,web-3,web-4,web-5"
set version = "2.4.1"

target "deploy" local:

  [build release]:
    skip if $ test -f /tmp/releases/${version}.tar.gz
    run $ echo "Building ${version}..." && mkdir -p /tmp/releases && touch /tmp/releases/${version}.tar.gz

  [rolling deploy]:
    first [build release]
    stage "rollout":
      phase "canary" 1 from ${servers}:
        [deploy to ${server}]:
          run $ echo "Deploying ${version} to ${server}"

        verify "canary healthy":
          run $ echo "Health check for ${server}: OK"

      phase "remaining" rest from ${servers}:
        each server, 2 at a time:
          [deploy to ${server}]:
            run $ echo "Deploying ${version} to ${server}"

  verify "all healthy":
    run $ echo "All servers running ${version}"
```

**How it works:**
- Phase "canary" deploys to 1 server, runs a verify gate
- If the canary verify fails, the rollout stops — remaining servers are untouched
- Phase "remaining" deploys to the rest, 2 at a time
- The final verify runs only after all phases complete

---

## Recipe 5: Race for Fastest Mirror

**When to use:** Downloading from multiple mirrors — take whichever responds first.
**Concepts:** `race into` blocks, safe output handling, cancellation.

```
--- Mirror Race ---
set version = "2.4.1"
set dest = "/tmp/package-${version}.tar.gz"

target "local" local:

  [fetch package]:
    skip if $ test -f ${dest}
    race into ${dest}:
      [try primary mirror]:
        run $ curl -sfL https://mirror1.example.com/pkg-${version}.tar.gz -o ${_race_out}
        timeout 30s

      [try backup mirror]:
        run $ curl -sfL https://mirror2.example.com/pkg-${version}.tar.gz -o ${_race_out}
        timeout 30s

      [try local cache]:
        run $ cp /var/cache/packages/pkg-${version}.tar.gz ${_race_out}
        timeout 5s

  [install]:
    first [fetch package]
    run $ tar xzf ${dest} -C /opt/
```

**How it works:**
- `race into ${dest}:` tells the engine to manage output safely
- Each branch writes to `${_race_out}` — a unique temp file per branch (e.g., `.race.0`, `.race.1`)
- All three branches start concurrently
- The first to succeed wins — its temp file is atomically renamed to `${dest}`
- Loser temp files are cleaned up automatically
- If all fail, the step fails and the race can be retried on resume

**Why `race into` instead of plain `race`?** Without it, all branches would write to the
same file simultaneously. If branch A is halfway through a download when branch B finishes,
branch B's completed file could be corrupted by branch A's ongoing write. `race into` makes
the safe pattern the default pattern.

---

## Recipe 6: Encrypted Secrets

**When to use:** Graphs that need passwords, API keys, or tokens.
**Concepts:** `cgr secrets`, vault passphrase, automatic redaction.

Create an encrypted secrets file:

```bash
cgr secrets create secrets.vault
# Enter vault passphrase when prompted
# Opens your $EDITOR — add key=value pairs:
#   db_password=super-secret-123
#   api_key=sk-abc123xyz
```

Reference it in your graph:

```
--- Secrets Example ---
secrets "secrets.vault"
set db_host = "10.0.2.3"

target "web" local:

  [configure database]:
    run $ echo "host=${db_host} password=${db_password}" > /tmp/db.conf

  [test connection]:
    first [configure database]
    run $ echo "Connecting to ${db_host} with password ${db_password}..."
```

```bash
cgr apply secrets.cgr --vault-pass mypassphrase
# Or: export CGR_VAULT_PASS=mypassphrase && cgr apply secrets.cgr
```

**How it works:**
- `secrets "secrets.vault"` loads the encrypted file at resolve time
- Secret values are decrypted into normal variables (`${db_password}`)
- All output is automatically redacted: `***REDACTED***` replaces secret values
  in plan output, apply logs, state files, reports, and the web IDE
- The vault file is AES-256-CBC encrypted — safe to commit to version control

**Managing secrets:**

```bash
cgr secrets view secrets.vault            # View decrypted keys
cgr secrets add secrets.vault api_key     # Add a new key
cgr secrets rm secrets.vault old_key      # Remove a key
cgr secrets edit secrets.vault            # Edit in $EDITOR
```

---

## Recipe 7: CI/CD Drift Detection

**When to use:** Detect configuration drift in CI pipelines.
**Concepts:** `cgr check --json`, exit codes, JSON output.

```bash
#!/bin/bash
# ci-drift-check.sh — run in CI pipeline

# Check what needs to run (exit 0 = all satisfied, 1 = drift detected)
cgr check infra.cgr --json --repo ./repo > /tmp/drift.json
rc=$?

if [ $rc -eq 0 ]; then
  echo "No drift detected"
  exit 0
fi

# Extract drift details
echo "Drift detected:"
jq '.needs_run[] | .name' /tmp/drift.json

# Optionally auto-fix
# cgr apply infra.cgr --repo ./repo --report /tmp/report.json
# exit $?

exit 1
```

**How it works:**
- `cgr check` runs every `skip if` clause without executing `run` commands
- Exit 0 means all checks pass (no drift)
- Exit 1 means at least one step needs to run (drift detected)
- `--json` gives machine-readable output with per-step results

---

## Recipe 8: Fleet Audit with Report

**When to use:** Collecting system information across multiple hosts.
**Concepts:** `collect` clause, `cgr report`, CSV export.

```
--- Fleet Audit ---
set fleet = "web-1:deploy@10.0.1.1,db-1:admin@10.0.2.3"

each name, addr in ${fleet}:
  target "${name}" ssh ${addr}:
    [hostname on ${name}]:
      run $ hostname -f
      collect "hostname"

    [disk on ${name}]:
      run $ df -h / | tail -1
      collect "disk"

    [memory on ${name}]:
      run $ free -h | grep Mem
      collect "memory"

    [uptime on ${name}]:
      run $ uptime -p
      collect "uptime"
```

Run and generate reports:

```bash
cgr apply audit.cgr
cgr report audit.cgr                     # Pretty table
cgr report audit.cgr --format json       # JSON for processing
cgr report audit.cgr --format csv -o audit.csv  # CSV for spreadsheets
```

**How it works:**
- `collect "key"` saves each step's stdout to a `.output` file
- `cgr report` reads the output file and formats it
- Multi-node CSV auto-pivots: nodes as rows, collect keys as columns
- Use `--keys hostname,disk` to filter which keys appear in the report

---

## Recipe 9: Graph Comparison in Pull Requests

**When to use:** Reviewing infrastructure changes before merging.
**Concepts:** `cgr diff`, `--json` output, CI integration.

```bash
#!/bin/bash
# pr-diff-check.sh — compare graph changes in a PR

# Compare current branch vs main
git show main:infra.cgr > /tmp/infra-main.cgr

cgr diff /tmp/infra-main.cgr infra.cgr --repo ./repo --json > /tmp/diff.json
rc=$?

if [ $rc -eq 0 ]; then
  echo "No structural changes to graph"
  exit 0
fi

# Show human-readable diff
cgr diff /tmp/infra-main.cgr infra.cgr --repo ./repo

# Extract counts for PR comment
added=$(jq '.added | length' /tmp/diff.json)
removed=$(jq '.removed | length' /tmp/diff.json)
changed=$(jq '.changed | length' /tmp/diff.json)
echo "Graph changes: +${added} -${removed} ~${changed}"
```

**How it works:**
- `cgr diff` compares two resolved graphs structurally (not text diff)
- Reports added/removed/changed steps, dependency changes, wave movement
- Exit 0 = identical, exit 1 = differences found
- `--json` for machine-readable output, plain text for humans

---

## Recipe 10: Tag-Based Partial Runs

**When to use:** Running only security checks, or skipping slow build steps.
**Concepts:** `tags` keyword, `--tags`/`--skip-tags` CLI flags.

```
--- Tagged Infrastructure ---
set app_dir = "/opt/myapp"

target "local" local:

  [update packages]:
    tags packages
    run $ echo "apt update && apt upgrade -y"

  [install nginx]:
    tags packages, web
    first [update packages]
    run $ echo "apt install -y nginx"

  [configure firewall]:
    tags security, network
    run $ echo "ufw allow 80 && ufw allow 443"

  [setup fail2ban]:
    tags security
    run $ echo "apt install -y fail2ban && systemctl enable fail2ban"

  [deploy application]:
    tags web, deploy
    first [install nginx]
    run $ echo "deploying to ${app_dir}"

  [run security audit]:
    tags security, audit
    first [configure firewall]
    first [setup fail2ban]
    run $ echo "running lynis audit"
```

Run subsets:

```bash
cgr plan infra.cgr --tags security         # Only security-tagged steps
cgr apply infra.cgr --tags web,deploy      # Only web and deploy steps
cgr apply infra.cgr --skip-tags packages   # Everything except package updates
cgr apply infra.cgr --tags security --skip-tags audit  # Security, but not the audit
```

**How it works:**
- `tags keyword1, keyword2` on a step assigns tags (comma-separated, no quotes)
- `--tags web` runs only steps tagged `web` (untagged steps are skipped)
- `--skip-tags packages` runs everything except steps tagged `packages`
- Tags combine: `--tags security --skip-tags audit` includes security-tagged steps
  but excludes any that are also tagged `audit`
- Barrier and verify steps are never tag-filtered (they're structural)
- Tag-skipped steps show as `○ skip-tag` in plan and apply output

---

## Feature Reference

Quick lookup: which feature solves your problem?

| I need to... | Feature | Syntax | Docs |
|-------------|---------|--------|------|
| Run steps in order | Dependencies | `first [step name]` | MANUAL.md |
| Skip already-done work | Idempotency | `skip if $ command` | MANUAL.md |
| Parameterize graphs | Variables | `set key = "val"` / `--set` | MANUAL.md |
| Reuse step patterns | Templates | `using path/template` | MANUAL.md |
| Run on remote hosts | SSH targets | `target "x" ssh user@host:` | MANUAL.md |
| Run steps concurrently | Parallel | `parallel N at a time:` | MANUAL.md |
| Pick fastest option | Race | `race:` | MANUAL.md |
| Deploy to N hosts | Each + inventory | `each var in ${list}:` | MANUAL.md |
| Phased rollout | Stages | `stage "name":` / `phase` | MANUAL.md |
| Conditional steps | When | `when VAR == "val"` | MANUAL.md |
| Subset execution | Tags | `tags x, y` / `--tags` | MANUAL.md |
| Collect output | Reporting | `collect "key"` / `cgr report` | MANUAL.md |
| Store secrets | Encryption | `cgr secrets` / `secrets "file"` | MANUAL.md |
| Detect drift | Check | `cgr check --json` | MANUAL.md |
| Compare changes | Diff | `cgr diff a.cgr b.cgr` | MANUAL.md |
| Visualize graph | IDE | `cgr serve` | MANUAL.md |
| Resume after crash | State | `cgr state show/reset` | MANUAL.md |
