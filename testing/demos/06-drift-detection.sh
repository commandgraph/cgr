#!/usr/bin/env bash
# Demo 06: Drift detection — state test verifies reality matches state
source /opt/cgr/demos/lib.sh

banner "Demo 6: Drift detection"

cp "$EXAMPLES/basic.cgr" /workspace/drift-test.cgr
rm -rf /workspace/build /workspace/.state/drift-test.cgr.state

section "Step 1: Initial apply — everything succeeds"
narrate "First, run the graph so all steps are marked 'done' in state."
pause 1

run_cmd "cgr apply /workspace/drift-test.cgr"

section "Step 2: State test — everything matches"
narrate "'state test' re-runs every completed step's 'skip if' check"
narrate "against the live system to verify reality matches the state file."
pause 1

run_cmd "cgr state test /workspace/drift-test.cgr"

section "Step 3: Introduce drift — delete a file"
narrate "Someone manually deleted the config file. The state says it's done,"
narrate "but the file is gone. This is drift."
pause 1

run_cmd "rm -f /etc/myapp.conf"
narrate "Config file deleted. State still says 'done'. Let's detect the drift."

section "Step 4: State test — drift detected"
run_cmd "cgr state test /workspace/drift-test.cgr"

narrate "The drifted step is auto-marked for re-run in the state file."

section "Step 5: Resume — only the drifted step re-runs"
run_cmd "cgr apply /workspace/drift-test.cgr"

section "Step 6: Verify — back in sync"
run_cmd "cgr state test /workspace/drift-test.cgr"

success "Drift detected, auto-corrected, verified."

# Clean up
rm -rf /workspace/build /workspace/drift-test.cgr /workspace/.state/drift-test.cgr.state
