#!/usr/bin/env bash
# Demo 03: Crash recovery and state tracking
source /opt/cgr/demos/lib.sh

banner "Demo 3: Crash recovery & state tracking"

cp "$EXAMPLES/crashtest.cgr" /workspace/crashtest.cgr
rm -rf /workspace/crashtest /workspace/.state/crashtest.cgr.state

section "Step 1: First run — step 3 will FAIL"
narrate "This graph has 6 steps. Step 3 requires a file that doesn't exist."
narrate "Steps 1-2 succeed. Step 3 fails. Steps 4-5 never run."
pause 1

cgr apply /workspace/crashtest.cgr 2>&1
echo ""

section "Step 2: Inspect the state file"
narrate "The .state file is a JSON Lines journal — one line per completed step."
narrate "Steps 1 and 2 are marked 'success'. Step 3 is 'failed'."
narrate "Steps 4-5 have no entries (never attempted)."
pause 1

run_cmd "cgr state show /workspace/crashtest.cgr"

section "Step 3: Look at the raw state file"
narrate "Each line is a self-contained JSON record. Crash-safe: a power failure"
narrate "can only corrupt the last line. Everything before it is intact."
pause 1

run_cmd "cat /workspace/.state/crashtest.cgr.state"

section "Step 4: Fix the problem and resume"
narrate "Creating the missing file that step 3 needs..."
pause 1

run_cmd "mkdir -p /workspace/crashtest && touch /workspace/crashtest/magic_fix"

narrate "Re-running the graph. Steps 1-2 skip from state (instant)."
narrate "Step 3 re-runs (was failed). Steps 4-5 run for the first time."
pause 1

run_cmd "cgr apply /workspace/crashtest.cgr"

section "Step 5: Verify full state"
run_cmd "cgr state show /workspace/crashtest.cgr"

success "Crash recovery complete. 2 steps from state, 4 freshly executed."

section "Step 6: Manual state manipulation"
narrate "Force step 5 to re-run with 'state set STEP redo':"
run_cmd "cgr state set /workspace/crashtest.cgr step_five redo"

narrate "Show the updated state:"
run_cmd "cgr state show /workspace/crashtest.cgr"

narrate "Resume — only step 5 re-runs:"
run_cmd "cgr apply /workspace/crashtest.cgr"

success "Manual state override demonstrated."

# Clean up
rm -rf /workspace/crashtest /workspace/crashtest.cgr /workspace/.state/crashtest.cgr.state
