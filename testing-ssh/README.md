# CommandGraph SSH Testing — Two-container demos

Demonstrates CommandGraph executing commands on a remote host over SSH. Two containers — a control node (has the engine) and a target node (runs sshd) — are linked via a Podman network with pre-shared SSH keys.

## Requirements

- Podman (or Docker as fallback)
- ~400MB disk for two container images

## Quick start

```bash
# Run all 5 SSH demos
./run-ssh-demos.sh

# Run a specific demo
./run-ssh-demos.sh 2          # crash recovery over SSH

# Interactive shell on the control node (target running in background)
./run-ssh-demos.sh shell      # ssh deploy@target is pre-configured

# Clean up everything
./run-ssh-demos.sh teardown
```

## Architecture

```
┌─────────────────────┐         SSH         ┌─────────────────────┐
│   Control node      │ ──────────────────> │   Target node       │
│                     │   deploy@target     │                     │
│  cgr.py             │   key-based auth    │  openssh-server     │
│  /root/.ssh/id_*    │   passwordless sudo │  deploy user        │
│  .state files       │                     │  sudo NOPASSWD      │
│  template repo      │                     │                     │
└─────────────────────┘                     └─────────────────────┘
        cgr-net (podman network)
```

The control node has the engine, repo, examples, and SSH keys. State files live on the control node. Every `check`, `run`, and `verify` command is executed on the target over SSH. `as root` steps use `sudo -u root` on the remote side.

## Demos

| # | Name | What it shows |
|---|------|--------------|
| 0 | Verify connectivity | SSH between containers, passwordless sudo, graph validation |
| 1 | Remote execution | Plan, apply, and idempotent re-run — all commands execute on the target |
| 2 | Crash recovery | Step fails on target, state is local, fix applied remotely, resume |
| 3 | Parallel + sudo | 3 services built concurrently over SSH, installed 2-at-a-time, sudo for links |
| 4 | Remote drift detection | Apply config, delete a file on target, `state test` detects it via SSH |

## How it works

`run-ssh-demos.sh` does the following:

1. **Builds two container images**:
   - `cgr-ssh-control` — Python 3.12-slim + engine + SSH client + ed25519 key pair
   - `cgr-ssh-target` — Python 3.12-slim + openssh-server + `deploy` user

2. **Extracts the public key** from the control image and saves it to `.ssh_pubkey`

3. **Creates a Podman network** (`cgr-net`) so both containers can reach each other by hostname

4. **Starts the target** as a detached container running sshd, injects the public key into `deploy`'s `authorized_keys`

5. **Runs each demo** as an ephemeral control container on the same network — the demo scripts SSH to `target` via the engine's normal `via ssh` execution path

6. **Stops the target** after all demos complete

## Key differences from the local testing suite

| Aspect | Local (`testing/`) | SSH (`testing-ssh/`) |
|--------|-------------------|---------------------|
| Execution | `via local` | `via ssh deploy@target` |
| State file | Same host | Control node (local) |
| Commands | Direct shell | `ssh -o BatchMode=yes deploy@target 'cmd'` |
| Privilege | Direct sudo | `sudo -u root -- bash -c 'cmd'` over SSH |
| Containers | 1 (ephemeral per demo) | 2 (target persistent, control per demo) |

## Interactive exploration

`./run-ssh-demos.sh shell` gives you a control node shell with everything pre-configured:

```bash
# Inside the control shell:

# Verify connectivity
ssh deploy@target hostname

# Run a remote graph
cgr apply /opt/cgr/examples/ssh-basic.cgr --repo /opt/cgr/repo

# Check state
cgr state show /opt/cgr/examples/ssh-basic.cgr

# Inspect the target
ssh deploy@target 'ls -la /opt/myapp/'

# Write your own remote graph:
cat > /workspace/test.cgr << 'EOF'
--- Remote test ---
target "remote" ssh deploy@target:
  [hello from remote]:
    run $ echo "Hello from $(hostname) at $(date)"
EOF
cgr apply /workspace/test.cgr
```

## Troubleshooting

If SSH demos fail with "connection refused":
- The target container may not have started. Run `./run-ssh-demos.sh build` then check with `podman ps`.
- The network may not exist. Run `podman network create cgr-net`.

If `as root` steps fail with "permission denied":
- The target's sudoers file may be wrong. Run `podman exec cg-target cat /etc/sudoers.d/deploy`.
