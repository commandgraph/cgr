#!/usr/bin/env bash
# Demo 01: Basic plan → apply workflow
source /opt/cgr/demos/lib.sh

banner "Demo 1: Basic plan → apply"

cp "$EXAMPLES/basic.cgr" /workspace/basic.cgr

section "Step 1: Plan (shows what would happen, runs nothing)"
narrate "The plan command resolves the graph and shows execution order."
narrate "Resources are grouped into parallel waves."
pause 1

run_cmd "cgr plan /workspace/basic.cgr"

section "Step 2: Apply (execute for real)"
narrate "The apply command runs each wave, checking idempotency first."
narrate "Steps whose 'skip if' check passes are skipped (already done)."
pause 1

run_cmd "cgr apply /workspace/basic.cgr"

section "Step 3: Re-run (idempotent — everything skips)"
narrate "Running again: state file says everything is done."
narrate "No commands are re-executed — instant resume."
pause 1

run_cmd "cgr apply /workspace/basic.cgr"

section "Step 4: Verify the results"
run_cmd "ls -la /usr/local/bin/myapp /etc/myapp.conf /var/log/myapp/"
run_cmd "cat /etc/myapp.conf"

success "Basic workflow complete. Plan → Apply → Idempotent re-run."

# Clean up
rm -rf /workspace/build /workspace/basic.cgr /workspace/.state/basic.cgr.state
