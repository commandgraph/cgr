#!/usr/bin/env bash
# Demo 02: Template repository
source /opt/cgr/demos/lib.sh

banner "Demo 2: Reusable template repository"

cp "$EXAMPLES/template-demo.cgr" /workspace/template-demo.cgr

section "Step 1: List available templates"
narrate "The repo stores parameterized, reusable step trees."
narrate "Each template is a .cg file with a template block."
pause 1

run_cmd "cgr repo index --repo $REPO"

section "Step 2: Plan with templates"
narrate "Templates are imported with 'using' and instantiated with 'from'."
narrate "The resolver expands templates, deduplicates shared prerequisites,"
narrate "and tracks provenance (which template produced which resource)."
pause 1

run_cmd "cgr plan /workspace/template-demo.cgr --repo $REPO -v"

section "Step 3: Apply"
run_cmd "cgr apply /workspace/template-demo.cgr --repo $REPO"

success "Templates expanded, deduplicated, and executed."

# Clean up
rm -rf /workspace/template-demo.cgr /workspace/.state/template-demo.cgr.state /workspace/tpl_test
