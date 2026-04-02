#!/usr/bin/env bash
source /opt/cgr/demos/lib.sh

banner "SSH Demo 4: Remote drift detection"

wait_for_target || exit 1

cp "$EXAMPLES/ssh-drift.cgr" /workspace/ssh-drift.cgr
rm -f /workspace/.state/ssh-drift.cgr.state

section "Step 1: Deploy configuration to the target"
narrate "Writing 3 config files and a data directory on the remote host."
run_cmd "cgr apply /workspace/ssh-drift.cgr --repo $REPO"

section "Step 2: State test — everything matches"
narrate "'state test' re-runs every check command OVER SSH to verify"
narrate "that what the state file says matches reality on the target."
run_cmd "cgr state test /workspace/ssh-drift.cgr --repo $REPO"

section "Step 3: Simulate drift — someone deletes a config on the target"
narrate "An operator manually deleted the TLS config on the remote host."
narrate "The state file (local) still says it's done."
run_on_target "sudo rm -f /etc/myservice/tls.conf"
narrate "Verify it's gone on the target:"
run_on_target "ls /etc/myservice/"

section "Step 4: State test detects the drift over SSH"
narrate "The engine SSHes to the target, runs the check for tls.conf,"
narrate "gets exit code 1 (file missing), and marks it for re-run."
run_cmd "cgr state test /workspace/ssh-drift.cgr --repo $REPO"

section "Step 5: Resume — only the drifted step re-runs"
narrate "Steps with valid state skip. The drifted step re-runs via SSH."
run_cmd "cgr apply /workspace/ssh-drift.cgr --repo $REPO"

section "Step 6: Verify everything is back in sync"
run_on_target "ls -la /etc/myservice/"
run_on_target "cat /etc/myservice/tls.conf"

success "Drift detected over SSH, auto-corrected, verified."

# Cleanup
run_on_target "sudo rm -rf /etc/myservice /var/lib/myservice"
rm -f /workspace/ssh-drift.cgr /workspace/.state/ssh-drift.cgr.state
