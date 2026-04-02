#!/usr/bin/env bash
# Demo 05: Race — first to succeed wins, rest cancelled
source /opt/cgr/demos/lib.sh

banner "Demo 5: Race cancellation"

cp "$EXAMPLES/race-demo.cgr" /workspace/race-demo.cgr
rm -rf /workspace/racetest /workspace/.state/race-demo.cgr.state

section "The scenario"
narrate "Three mirrors compete to download a package:"
narrate "  • fast mirror:  instant echo (wins)"
narrate "  • slow mirror:  2-second sleep (cancelled)"
narrate "  • flaky mirror: exit 1 (fails)"
narrate ""
narrate "The race starts all three. The first SUCCESS wins."
narrate "The rest are cancelled. The graph continues with the winner's output."
pause 1

section "Step 1: Apply"
run_cmd "cgr apply /workspace/race-demo.cgr"

section "Step 2: Check state — see the race result"
run_cmd "cgr state show /workspace/race-demo.cgr"

narrate "Notice the CANCELLED entries — those branches were stopped"
narrate "because the fast mirror won the race."

success "Race cancellation demonstrated."

# Clean up
rm -rf /workspace/racetest /workspace/race-demo.cgr /workspace/.state/race-demo.cgr.state
