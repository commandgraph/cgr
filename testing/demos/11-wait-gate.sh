#!/usr/bin/env bash
# Demo 11: Wait gates (webhook + file) and machine-readable apply output
source /opt/cgr/demos/lib.sh

banner "Demo 11: Wait gates & machine-readable output"

cp "$EXAMPLES/wait-gate-demo.cgr" /workspace/wait-gate-demo.cgr
rm -rf /workspace/wait-gate /workspace/.state/wait-gate-demo.cgr.state

# ── Pick a free port and hand it to the webhook server ────────────────
WEBHOOK_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('',0)); print(s.getsockname()[1]); s.close()")
export CGR_WEBHOOK_PORT="$WEBHOOK_PORT"

section "The graph"
narrate "wait-gate-demo.cgr has a [await QA sign-off] step that blocks on a webhook."
narrate "Downstream steps run only after that path is hit."
narrate ""
narrate "  [await QA sign-off]:"
narrate "    wait for webhook \"/approve/\${deploy_id}\""
narrate "    timeout 30s"
narrate ""
narrate "This covers human-in-the-loop gates, CI signals, and external approvals"
narrate "without polling loops or dedicated message infrastructure."
pause 1

section "Step 1: Validate and plan"
run_cmd "cgr validate /workspace/wait-gate-demo.cgr"
run_cmd "cgr plan /workspace/wait-gate-demo.cgr"

section "Step 2: Apply — webhook triggers unblocking"
narrate "Apply will start a local HTTP listener on port $WEBHOOK_PORT."
narrate "A background curl fires after 1 second, simulating an external signal."
pause 1

# Fire the approval after a short delay (background)
(
    sleep 1
    curl -sf "http://127.0.0.1:${WEBHOOK_PORT}/approve/demo-run-1" -o /dev/null
    echo ""
    echo "  [curl] POST /approve/demo-run-1 → 200 OK"
    echo ""
) &
CURL_PID=$!

run_cmd "cgr apply /workspace/wait-gate-demo.cgr -v"
wait "$CURL_PID" 2>/dev/null || true

section "Step 3: Inspect results"
run_cmd "cat /workspace/wait-gate/manifest.txt"
run_cmd "cgr state show /workspace/wait-gate-demo.cgr"

# ── wait for file ──────────────────────────────────────────────────────
section "Step 4: wait for file"
narrate "'wait for file' polls for a path on the local filesystem (or SSH target)."
narrate "We create a one-step graph inline, write the flag from the background."
pause 1

FILE_GRAPH=$(mktemp /workspace/file-wait-XXXXXX.cgr)
FLAG_FILE="/workspace/wait-gate/ready.flag"
cat > "$FILE_GRAPH" <<'EOF'
target "local" local:
  [wait for readiness flag]:
    wait for file "/workspace/wait-gate/ready.flag"
    timeout 10s
  [record ready]:
    first [wait for readiness flag]
    run $ echo "ready" >> /workspace/wait-gate/manifest.txt
EOF

(sleep 1; touch "$FLAG_FILE"; echo "  [bg] wrote $FLAG_FILE") &
FLAG_PID=$!

run_cmd "cgr apply $FILE_GRAPH -v"
wait "$FLAG_PID" 2>/dev/null || true

# ── machine-readable output ────────────────────────────────────────────
section "Step 5: Machine-readable apply output (--output json)"
narrate "'cgr apply --output json' suppresses human output and prints a JSON"
narrate "result document to stdout — per-step statuses, timing metrics, bottleneck."
narrate "Pipe it to any monitoring or CI system."
pause 1

rm -f /workspace/.state/wait-gate-demo.cgr.state
run_cmd "cgr apply /workspace/wait-gate-demo.cgr --no-resume --output json 2>/dev/null | python3 -m json.tool" &
APPLY_PID=$!
sleep 1
curl -sf "http://127.0.0.1:${WEBHOOK_PORT}/approve/demo-run-1" -o /dev/null || true
wait "$APPLY_PID" 2>/dev/null || true

section "Step 6: Execution metrics in the state journal"
narrate "Per-wave wall times and the overall bottleneck are persisted automatically."
narrate "'cgr state show' surfaces the latest run summary."
pause 1
run_cmd "cgr state show /workspace/wait-gate-demo.cgr"

success "Webhook gate, file wait, and machine-readable output demonstrated."

# Clean up
rm -rf /workspace/wait-gate "$FILE_GRAPH" \
       /workspace/.state/wait-gate-demo.cgr.state \
       /workspace/.state/"$(basename $FILE_GRAPH)".state \
       /workspace/wait-gate-demo.cgr
unset CGR_WEBHOOK_PORT
