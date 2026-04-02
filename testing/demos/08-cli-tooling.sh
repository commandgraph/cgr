#!/usr/bin/env bash
# Demo 08: CLI tooling commands
source /opt/cgr/demos/lib.sh

banner "Demo 8: CLI tooling"

cp "$EXAMPLES/cli-tooling-demo.cgr" /workspace/cli-tooling-demo.cgr
rm -rf /workspace/cli-tooling /workspace/cli-tooling-demo.cg /workspace/cli-tooling-ugly.cgr /workspace/.state/cli-tooling-demo.cgr.state

section "Step 1: Format a messy .cgr file"
narrate "fmt normalizes spacing and indentation, but it does not change workflow semantics."
pause 1

run_cmd "printf '%s\n' '--- CLI tooling demo ---' '' 'set work_dir= \"/workspace/cli-tooling\"' '' 'target \"local\" local:' '' '  [prepare workspace]:' '    run $ mkdir -p \${work_dir}' '' '  [render config]:' '    first [prepare workspace]' '    skip if $ test -f \${work_dir}/app.conf' '    run $ printf '\\''mode=demo\\nworkers=2\\n'\\'' > \${work_dir}/app.conf' '' '  [start service]:' '    first    [render config]' '    run $ printf '\\''started\\n'\\'' > \${work_dir}/service.log' '' '  verify \"service log exists\":' '    first [start service]' '    run $ test -f \${work_dir}/service.log' > /workspace/cli-tooling-ugly.cgr"
run_cmd "sed -n '1,120p' /workspace/cli-tooling-ugly.cgr"
run_cmd "cgr fmt /workspace/cli-tooling-ugly.cgr"
run_cmd "sed -n '1,120p' /workspace/cli-tooling-ugly.cgr"

section "Step 2: Lint for behavioral issues"
narrate "lint flags risky workflow patterns. Here, two steps intentionally lack 'skip if' checks."
pause 1

cgr lint /workspace/cli-tooling-demo.cgr || true
echo ""

section "Step 3: Explain and why"
narrate "explain shows the dependency chain for one step. why shows what depends on it."
pause 1

run_cmd "cgr explain /workspace/cli-tooling-demo.cgr 'start service'"
run_cmd "cgr why /workspace/cli-tooling-demo.cgr 'render config'"

section "Step 4: Convert between .cgr and .cg"
run_cmd "cgr convert /workspace/cli-tooling-demo.cgr -o /workspace/cli-tooling-demo.cg"
run_cmd "sed -n '1,120p' /workspace/cli-tooling-demo.cg"
run_cmd "cgr validate /workspace/cli-tooling-demo.cg"

success "Formatting, linting, dependency inspection, and format conversion demonstrated."

rm -rf /workspace/cli-tooling /workspace/cli-tooling-demo.cgr /workspace/cli-tooling-demo.cg /workspace/cli-tooling-ugly.cgr /workspace/.state/cli-tooling-demo.cgr.state
