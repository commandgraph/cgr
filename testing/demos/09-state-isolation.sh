#!/usr/bin/env bash
# Demo 09: Run-scoped state files
source /opt/cgr/demos/lib.sh

banner "Demo 9: Isolated state journals"

cp "$EXAMPLES/state-isolation-demo.cgr" /workspace/state-isolation-demo.cgr
rm -rf /workspace/state-isolation /workspace/custom-canary.state /workspace/.state/state-isolation-demo.cgr.blue.state /workspace/.state/state-isolation-demo.cgr.green.state

section "Step 1: Apply the same graph with a blue run-id"
narrate "run-id salts the default state path, so separate invocations keep separate journals."
pause 1

run_cmd "cgr apply /workspace/state-isolation-demo.cgr --set release=blue --run-id blue"

section "Step 2: Apply again with a green run-id"
narrate "This is the same graph file, but a different parameter set and a different state journal."
pause 1

run_cmd "cgr apply /workspace/state-isolation-demo.cgr --set release=green --run-id green"

section "Step 3: Show the isolated journals"
run_cmd "ls -la /workspace/.state"
run_cmd "cat /workspace/.state/state-isolation-demo.cgr.blue.state"
run_cmd "cat /workspace/.state/state-isolation-demo.cgr.green.state"

section "Step 4: Use an explicit custom state file"
narrate "The --state flag pins the journal path directly."
pause 1

run_cmd "cgr apply /workspace/state-isolation-demo.cgr --set release=canary --state /workspace/custom-canary.state"
run_cmd "cat /workspace/custom-canary.state"

section "Step 5: Re-run one invocation from its own journal"
run_cmd "cgr apply /workspace/state-isolation-demo.cgr --set release=blue --run-id blue"
run_cmd "ls -la /workspace/state-isolation"

success "Run-scoped state isolation demonstrated."

rm -rf /workspace/state-isolation /workspace/state-isolation-demo.cgr /workspace/custom-canary.state /workspace/.state/state-isolation-demo.cgr.blue.state /workspace/.state/state-isolation-demo.cgr.green.state
