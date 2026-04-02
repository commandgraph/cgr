#!/usr/bin/env bash
source /opt/cgr/demos/lib.sh

banner "SSH Demo 3: Parallel execution over SSH"

wait_for_target || exit 1

cp "$EXAMPLES/ssh-parallel.cgr" /workspace/ssh-parallel.cgr
rm -f /workspace/.state/ssh-parallel.cgr.state

section "The graph"
narrate "Deploys 3 services (api, worker, scheduler) to the target via SSH."
narrate "  • parallel:  — build all 3 services concurrently (3 SSH sessions)"
narrate "  • each:      — install each service binary, 2 at a time"
narrate "  • as root:   — uses sudo on the remote host for privileged steps"
echo ""

section "Step 1: Validate and plan"
run_cmd "cgr validate /workspace/ssh-parallel.cgr --repo $REPO"
run_cmd "cgr plan /workspace/ssh-parallel.cgr --repo $REPO"

section "Step 2: Apply — parallel SSH execution"
narrate "Watch how multiple SSH commands run simultaneously."
narrate "The parallel block opens 3 concurrent SSH sessions for the builds."
narrate "The 'each' block installs 2 at a time."
run_cmd "cgr apply /workspace/ssh-parallel.cgr --repo $REPO"

section "Step 3: Verify on target"
narrate "All binaries and configs were created on the remote host:"
run_on_target "ls -la /opt/releases/3.2.0/"
run_on_target "ls -la /usr/local/bin/api /usr/local/bin/worker /usr/local/bin/scheduler"
run_on_target "cat /etc/platform.conf"

success "Parallel SSH execution with sudo complete."

# Cleanup
run_on_target "sudo rm -rf /opt/releases /usr/local/bin/api /usr/local/bin/worker /usr/local/bin/scheduler /etc/platform.conf"
rm -f /workspace/ssh-parallel.cgr /workspace/.state/ssh-parallel.cgr.state
