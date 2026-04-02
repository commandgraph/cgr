# Troubleshooting

## Another run appears active

Message:
`error: Another run appears active (PID ...)`

Cause:
The state file is locked by another `cgr apply` process.

Fix:
- Wait for the active run to finish.
- If you intentionally need isolated concurrent runs, use `cgr apply FILE --run-id ID`.
- If the previous process crashed and left a stale lock, run `cgr state reset FILE`.

## State from one apply run is affecting another

Cause:
By default, applies for the same graph share one state journal.

Fix:
- Use `cgr apply FILE --run-id canary`.
- Or pin the exact journal path with `cgr apply FILE --state /path/to/custom.state`.

## A race branch kept running after another branch succeeded

Current behavior:
Race branches are terminated when another branch wins. If the losing branch was already inside a remote SSH command, local cleanup happens immediately but the remote host may still need to reap its own process depending on SSH/curl behavior.

Fix:
- Prefer idempotent race branches.
- Use `race into DEST:` when branches compete to write the same artifact.

## Inline config content contains `#`

Current behavior:
Literal `#` characters are preserved inside inline `content > PATH:` and `block in FILE, marker "X":` bodies.

If output still looks wrong:
- Check shell commands separately; comment stripping rules for shell lines are different from inline content bodies.

## Apply needs a different timeout for one slow step

Fix:
Set a step-local timeout:

```cgr
[backup]:
  run $ pg_dump ...
  timeout 10m
```

The step timeout overrides the default per-step execution timeout without changing the global `cgr apply --timeout`.

