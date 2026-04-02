#!/usr/bin/env bash
# Demo 07: HTTP steps and collected reports
source /opt/cgr/demos/lib.sh

banner "Demo 7: HTTP steps & collected reports"

cp "$EXAMPLES/http-report-demo.cgr" /workspace/http-report-demo.cgr
rm -rf /workspace/http-api-data /workspace/http-report-demo.cgr.output /workspace/.state/http-report-demo.cgr.state

python3 /opt/cgr/demos/http_stub_server.py \
    --host 127.0.0.1 \
    --port 18080 \
    --data-dir /workspace/http-api-data \
    >/workspace/http-api-server.log 2>&1 &
SERVER_PID=$!
trap 'kill "$SERVER_PID" >/dev/null 2>&1 || true' EXIT

for _ in $(seq 1 20); do
    if curl -sf http://127.0.0.1:18080/health >/dev/null 2>&1; then
        break
    fi
    sleep 0.2
done

section "Step 1: Start a local API stub"
narrate "This demo uses CommandGraph's built-in HTTP verbs against a disposable local server."
narrate "No external network or real service is involved."
pause 1

run_cmd "curl -sf http://127.0.0.1:18080/health"

section "Step 2: Apply the graph"
narrate "The graph performs GET and POST requests, validates status codes, and collects responses."
pause 1

run_cmd "cgr apply /workspace/http-report-demo.cgr -v"

section "Step 3: Inspect the collected report"
narrate "Collected HTTP responses are stored in the graph's .output journal."
pause 1

run_cmd "cgr report /workspace/http-report-demo.cgr"
run_cmd "cgr report /workspace/http-report-demo.cgr --format json -o /workspace/http-report.json"
run_cmd "cat /workspace/http-report.json"

section "Step 4: Verify what the API saw"
run_cmd "cat /workspace/http-api-data/events.log"
run_cmd "cat /workspace/http-api-data/records.jsonl"

success "HTTP operations and collected reporting demonstrated."

rm -rf /workspace/http-api-data /workspace/http-report.json /workspace/http-report-demo.cgr /workspace/http-report-demo.cgr.output /workspace/.state/http-report-demo.cgr.state /workspace/http-api-server.log
