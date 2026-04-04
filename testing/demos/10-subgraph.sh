#!/usr/bin/env bash
# Demo 10: Sub-graph inclusion — invoke another .cgr file as a step unit
source /opt/cgr/demos/lib.sh

banner "Demo 10: Sub-graph inclusion"

cp "$EXAMPLES/subgraph-demo.cgr"         /workspace/subgraph-demo.cgr
cp "$EXAMPLES/subgraph-install-step.cgr" /workspace/subgraph-install-step.cgr
rm -rf /workspace/subgraph /workspace/.state/subgraph-demo.cgr.state

section "The graphs"
narrate "subgraph-demo.cgr is the parent. It has two steps that each invoke"
narrate "subgraph-install-step.cgr as a self-contained unit, with different vars."
narrate ""
narrate "  [install frontend] from ./subgraph-install-step.cgr:"
narrate "    app_name = \"frontend\""
narrate "    version  = \"2.1.0\""
narrate ""
narrate "The child graph has its own state journal. The parent only tracks"
narrate "whether the outer step succeeded — not the individual child resources."
pause 1

section "Step 1: Validate both graphs"
run_cmd "cgr validate /workspace/subgraph-demo.cgr"
run_cmd "cgr validate /workspace/subgraph-install-step.cgr"

section "Step 2: Plan the parent graph"
narrate "Notice [install frontend] and [install backend] appear as single steps."
narrate "Their internal DAG is hidden inside the sub-graph."
pause 1
run_cmd "cgr plan /workspace/subgraph-demo.cgr"

section "Step 3: Apply — both sub-graphs run as units"
pause 1
run_cmd "cgr apply /workspace/subgraph-demo.cgr -v"

section "Step 4: Inspect the installed artifacts"
run_cmd "find /workspace/subgraph -type f | sort"
run_cmd "cat /workspace/subgraph/apps/frontend/VERSION"
run_cmd "cat /workspace/subgraph/apps/backend/VERSION"

section "Step 5: Re-run — outer step is idempotent"
narrate "The parent state records the outer step as successful, so it is skipped."
narrate "The child graph's own state file also skips its completed resources."
pause 1
run_cmd "cgr apply /workspace/subgraph-demo.cgr"

section "Step 6: Inspect state journals"
run_cmd "cgr state show /workspace/subgraph-demo.cgr"

success "Sub-graph inclusion demonstrated: two independent versioned installs, each a self-contained graph."

rm -rf /workspace/subgraph /workspace/subgraph-demo.cgr /workspace/subgraph-install-step.cgr \
       /workspace/.state/subgraph-demo.cgr.state
