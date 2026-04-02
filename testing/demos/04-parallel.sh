#!/usr/bin/env bash
# Demo 04: Parallel constructs — parallel, race, each, stage
source /opt/cgr/demos/lib.sh

banner "Demo 4: First-class parallelism"

cp "$EXAMPLES/fleet-deploy.cgr" /workspace/fleet-deploy.cgr
rm -rf /workspace/fleet /workspace/.state/fleet-deploy.cgr.state

section "The graph"
narrate "This deploys v3.1.0 to 6 servers using all four parallel constructs:"
narrate "  1. parallel:   — run unit tests, integration tests, lint simultaneously"
narrate "  2. each:       — upload to 6 servers, 3 at a time"
narrate "  3. stage/phase — canary (1 server) → 50% → remaining"
narrate "  4. race:       — (shown separately in demo 5)"
echo ""
narrate "Each construct desugars into normal DAG resources with synthetic"
narrate "gate → fork → join barriers. The executor respects per-group concurrency."
pause 1

section "Step 1: Validate and show resource count"
run_cmd "cgr validate /workspace/fleet-deploy.cgr"

section "Step 2: Plan — see the wave structure"
run_cmd "cgr plan /workspace/fleet-deploy.cgr"

section "Step 3: Apply — watch the phased rollout"
narrate "Watch how:"
narrate "  • Tests run in parallel (wave 3)"
narrate "  • Uploads run 3-at-a-time (wave 3)"
narrate "  • Canary deploys first, verify gates the rest"
narrate "  • 50% phase then remaining phase"
pause 1

run_cmd "cgr apply /workspace/fleet-deploy.cgr"

section "Step 4: Verify the artifacts"
narrate "All 6 servers should have .uploaded and .live files:"
run_cmd "ls -la /workspace/fleet/"
run_cmd "cat /workspace/fleet/alpha.live"

success "Full parallel deployment complete across 6 servers."

# Clean up
rm -rf /workspace/fleet /workspace/fleet-deploy.cgr /workspace/.state/fleet-deploy.cgr.state
