# Migration Guide

## From Ansible

Map common patterns like this:

- `tasks` -> `[step]:`
- `when:` -> `when ...`
- `creates:` or idempotent guards -> `skip if $ ...`
- `register:` -> `collect "key"` or `collect "key" as varname`
- `notify/handler` -> explicit dependent step
- `serial:` -> `stage:` or `each ..., N at a time:`

Example:

```yaml
- name: Install nginx
  apt:
    name: nginx
    state: present
```

```cgr
target "local" local:
  [install nginx]:
    run $ apt-get install -y nginx
    skip if $ dpkg -s nginx >/dev/null 2>&1
```

## From Terraform

CommandGraph is closer to procedural orchestration than declarative resource convergence.

Use it when you want:
- shell-first automation
- explicit DAG ordering
- resume after crash from a local state journal
- SSH execution without agents

Do not expect:
- provider/plugin ecosystems
- cloud resource drift reconciliation
- rollback planning

## State model differences

- CommandGraph state is execution history, not desired-state snapshots.
- A successful step is skipped on resume until you reset or drop it from state.
- Use `--run-id` or `--state FILE` when distinct invocations of the same graph need isolated journals.
