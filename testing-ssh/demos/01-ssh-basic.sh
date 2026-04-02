#!/usr/bin/env bash
source /opt/cgr/demos/lib.sh

banner "SSH Demo 1: Remote plan → apply → re-run"

wait_for_target || exit 1

cp "$EXAMPLES/ssh-basic.cgr" /workspace/ssh-basic.cgr

section "Step 1: Plan — see what will execute on the target"
narrate "Every command in the plan runs on 'target' via SSH."
narrate "The engine opens an SSH connection per command."
run_cmd "cgr plan /workspace/ssh-basic.cgr --repo $REPO"

section "Step 2: Apply — execute on the remote host"
narrate "Watch the check→run cycle happen over SSH:"
narrate "  1. SSH to target, run the 'skip if' check"
narrate "  2. If check fails (not done yet), SSH again to run the command"
narrate "  3. 'as root' steps use sudo on the remote side"
run_cmd "cgr apply /workspace/ssh-basic.cgr --repo $REPO"

section "Step 3: Verify on the target"
narrate "Files were created on the TARGET, not here:"
run_on_target "ls -la /opt/myapp/"
run_on_target "cat /opt/myapp/app.conf"
run_on_target "cat /var/log/myapp/started"

section "Step 4: Re-run — idempotent over SSH"
narrate "Running again: all checks pass remotely, everything skips."
run_cmd "cgr apply /workspace/ssh-basic.cgr --repo $REPO"

success "Remote execution complete. All artifacts live on the target."

# Cleanup on target
run_on_target "sudo rm -rf /opt/myapp /var/log/myapp /etc/myapp.service"
rm -f /workspace/ssh-basic.cgr /workspace/.state/ssh-basic.cgr.state
