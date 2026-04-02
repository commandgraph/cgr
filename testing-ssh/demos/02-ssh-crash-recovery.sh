#!/usr/bin/env bash
source /opt/cgr/demos/lib.sh

banner "SSH Demo 2: Crash recovery over SSH"

wait_for_target || exit 1

cp "$EXAMPLES/ssh-crashtest.cgr" /workspace/ssh-crashtest.cgr
rm -f /workspace/.state/ssh-crashtest.cgr.state

section "The scenario"
narrate "A 6-step deployment runs on the remote host."
narrate "Step 4 (run migrations) requires a file that doesn't exist yet."
narrate "Steps 1-3 succeed over SSH. Step 4 fails. Steps 5-6 never run."

section "Step 1: First run — step 4 will fail on the target"
cgr apply /workspace/ssh-crashtest.cgr --repo "$REPO" 2>&1
echo ""

section "Step 2: Inspect state (stored locally on the control node)"
narrate "The state file lives HERE (control), not on the target."
narrate "It records what completed over SSH."
run_cmd "cgr state show /workspace/ssh-crashtest.cgr"

section "Step 3: Fix the problem ON THE TARGET"
narrate "Creating the missing file on the remote host..."
run_on_target "touch /opt/deploy_test/db_ready"
narrate "The file now exists on the target. Re-running will succeed."

section "Step 4: Resume — steps 1-3 skip from state, 4-6 execute"
narrate "State file is local, but check/run commands go over SSH."
narrate "Steps 1-3: skip (state says done, no SSH needed)."
narrate "Step 4: re-run via SSH (was failed, now the file exists)."
narrate "Steps 5-6: run for the first time via SSH."
run_cmd "cgr apply /workspace/ssh-crashtest.cgr --repo $REPO"

section "Step 5: Verify on target"
run_on_target "cat /opt/deploy_test/running"

success "Crash recovery across SSH: state is local, execution is remote."

# Cleanup
run_on_target "sudo rm -rf /opt/deploy_test"
rm -f /workspace/ssh-crashtest.cgr /workspace/.state/ssh-crashtest.cgr.state
