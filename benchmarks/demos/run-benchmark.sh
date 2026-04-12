#!/usr/bin/env bash
# CommandGraph Parallelism Benchmark
#
# Compares wall-clock time deploying the same workload to N hosts
# with varying concurrency levels.
#
# Expects TARGET_HOSTS env var: comma-separated list of hostnames
source /opt/cgr/demos/lib.sh

banner "CommandGraph Parallelism Benchmark"

narrate "Engine: $(cgr version | head -1)"

if [ -z "$TARGET_HOSTS" ]; then
    echo -e "${RED}TARGET_HOSTS not set${RESET}"; exit 1
fi

BENCH_OUTPUT_DIR="${BENCH_OUTPUT_DIR:-/workspace}"
BENCH_KEEP_GRAPHS="${BENCH_KEEP_GRAPHS:-0}"
mkdir -p "$BENCH_OUTPUT_DIR"

IFS=',' read -ra HOSTS <<< "$TARGET_HOSTS"
NUM_HOSTS=${#HOSTS[@]}

narrate "Targets: ${NUM_HOSTS} hosts (${TARGET_HOSTS})"
echo ""

# ── Verify connectivity ──────────────────────────────────────────────
section "Verifying SSH connectivity to all ${NUM_HOSTS} hosts"
ALL_OK=true
for h in "${HOSTS[@]}"; do
    if wait_for_host "$h"; then
        success "$h reachable"
    else
        ALL_OK=false
    fi
done
$ALL_OK || { echo -e "${RED}Not all hosts reachable, aborting.${RESET}"; exit 1; }

# ── Workload description ─────────────────────────────────────────────
section "Workload"
narrate "Per host: single SSH command with ~0.8s of simulated work"
narrate "  mkdir + sleep 0.3 (download) + sleep 0.3 (compile)"
narrate "  + write config + sleep 0.2 (install) + verify"
narrate ""
narrate "Total: ${NUM_HOSTS} hosts x 1 SSH command each"
narrate "Sequential estimate: ~$(echo "scale=1; $NUM_HOSTS * 1.1" | bc)s"
echo ""

# ── Scenario generator ───────────────────────────────────────────────
DEPLOY_CMD='sudo mkdir -p /opt/bench && sudo chown deploy:deploy /opt/bench && sleep 0.3 && echo "pkg-1.0" > /opt/bench/pkg.tar.gz && sleep 0.3 && echo "bin-1.0" > /opt/bench/server && printf "host=$(hostname)\nver=1.0\n" > /opt/bench/server.conf && sleep 0.2 && sudo cp /opt/bench/server /usr/local/bin/bench-server'

VERIFY_HOSTS=$(echo "$TARGET_HOSTS" | tr ',' ' ')

generate_each_scenario() {
    local file="$1" concurrency="$2" label="$3"
    cat > "$file" << EOF
--- ${label} ---
set targets = "${TARGET_HOSTS}"

target "control" local:
  [deploy to fleet]:
    each host in \${targets}, ${concurrency} at a time:
      [deploy to \${host}]:
        run \$ ssh deploy@\${host} '${DEPLOY_CMD}'

  verify "all hosts deployed":
    first [deploy to fleet]
    run \$ for h in ${VERIFY_HOSTS}; do ssh deploy@\$h 'test -f /usr/local/bin/bench-server' || exit 1; done
EOF
}

generate_staged_scenario() {
    local file="$1"
    local half=$(( NUM_HOSTS / 2 ))
    [ "$half" -lt 1 ] && half=1
    local first_host="${HOSTS[0]}"
    cat > "$file" << EOF
--- Staged rollout ---
set targets = "${TARGET_HOSTS}"

target "control" local:
  [rolling deploy]:
    stage "production":
      phase "canary" 1 from \${targets}:
        [deploy to \${server}]:
          run \$ ssh deploy@\${server} '${DEPLOY_CMD}'
        verify "canary healthy":
          run \$ ssh deploy@${first_host} 'test -f /usr/local/bin/bench-server'
          retry 2x wait 1s

      phase "half" ${half} from \${targets}:
        each server, ${half} at a time:
          [deploy to \${server}]:
            run \$ ssh deploy@\${server} '${DEPLOY_CMD}'

      phase "rest" remaining from \${targets}:
        each server, ${NUM_HOSTS} at a time:
          [deploy to \${server}]:
            run \$ ssh deploy@\${server} '${DEPLOY_CMD}'

  verify "all deployed":
    first [rolling deploy]
    run \$ for h in ${VERIFY_HOSTS}; do ssh deploy@\$h 'test -f /usr/local/bin/bench-server' || exit 1; done
EOF
}

clean_targets() {
    for h in "${HOSTS[@]}"; do
        ssh "deploy@${h}" "sudo rm -rf /opt/bench /usr/local/bin/bench-server" 2>/dev/null &
    done
    wait
}

# ── Benchmark runner ──────────────────────────────────────────────────
declare -a LABELS
declare -a TIMES

run_scenario() {
    local label="$1" file="$2"
    clean_targets
    rm -f "${file}.state"
    time_run "$label" "$file"
    LABELS+=("$label")
    TIMES+=("$LAST_SECONDS")
}

section "Generating and validating scenarios"

generate_each_scenario "${BENCH_OUTPUT_DIR}/bench-seq.cgr" 1 "Sequential (1 at a time)"
generate_each_scenario "${BENCH_OUTPUT_DIR}/bench-par2.cgr" 2 "Parallel (2 at a time)"
generate_each_scenario "${BENCH_OUTPUT_DIR}/bench-par4.cgr" 4 "Parallel (4 at a time)"
generate_each_scenario "${BENCH_OUTPUT_DIR}/bench-parN.cgr" "$NUM_HOSTS" "Parallel (all at once)"
generate_staged_scenario "${BENCH_OUTPUT_DIR}/bench-staged.cgr"

for f in "${BENCH_OUTPUT_DIR}"/bench-*.cgr; do
    name=$(basename "$f" .cgr)
    if cgr validate "$f" >/dev/null 2>&1; then
        success "$name validated"
    else
        echo -e "${RED}  ✗ $name FAILED:${RESET}"
        cgr validate "$f" 2>&1 | head -5
        exit 1
    fi
done

section "Running benchmarks against ${NUM_HOSTS} hosts"
echo ""

echo -e "  ${BOLD}▶ Scenario 1/5: Sequential${RESET}"
narrate "Each host deploys one after the other. This is the baseline."
run_scenario "Sequential (1 at a time)" "${BENCH_OUTPUT_DIR}/bench-seq.cgr"
SEQ_TIME=$LAST_SECONDS
echo ""

echo -e "  ${BOLD}▶ Scenario 2/5: Parallel (2 at a time)${RESET}"
narrate "Sliding window of 2 concurrent SSH sessions."
run_scenario "Parallel (2 at a time)" "${BENCH_OUTPUT_DIR}/bench-par2.cgr"
echo ""

echo -e "  ${BOLD}▶ Scenario 3/5: Parallel (4 at a time)${RESET}"
narrate "Sliding window of 4 concurrent SSH sessions."
run_scenario "Parallel (4 at a time)" "${BENCH_OUTPUT_DIR}/bench-par4.cgr"
echo ""

echo -e "  ${BOLD}▶ Scenario 4/5: Parallel (all ${NUM_HOSTS} at once)${RESET}"
narrate "All hosts deploy simultaneously."
run_scenario "Parallel (all at once)" "${BENCH_OUTPUT_DIR}/bench-parN.cgr"
echo ""

echo -e "  ${BOLD}▶ Scenario 5/5: Staged rollout${RESET}"
narrate "1 canary -> verify -> half the fleet -> rest of the fleet"
run_scenario "Staged (1->half->rest)" "${BENCH_OUTPUT_DIR}/bench-staged.cgr"
echo ""

# ── Results table ────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}${CYAN}║  Results: ${NUM_HOSTS} hosts, ~0.8s work per host                      ║${RESET}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
echo ""
printf "  ${BOLD}%-34s %8s %8s${RESET}\n" "Scenario" "Time" "Speedup"
echo "  ───────────────────────────────────────────────────────"

for i in "${!LABELS[@]}"; do
    label="${LABELS[$i]}"
    t="${TIMES[$i]}"
    if [ "$i" -eq 0 ]; then
        speedup="1.00x"
        color="$DIM"
    else
        speedup=$(echo "scale=2; $SEQ_TIME / $t" | bc 2>/dev/null || echo "?")
        speedup="${speedup}x"
        color="$GREEN"
    fi
    printf "  ${color}%-34s %7ss %8s${RESET}\n" "$label" "$t" "$speedup"
done

echo ""
echo "  ───────────────────────────────────────────────────────"

ideal=$(echo "scale=2; $SEQ_TIME / $NUM_HOSTS" | bc 2>/dev/null || echo "?")
best="${TIMES[3]}"
if [ "$best" != "0" ] && [ -n "$best" ]; then
    efficiency=$(echo "scale=0; ($ideal / $best) * 100" | bc 2>/dev/null || echo "?")
else
    efficiency="?"
fi

echo ""
echo -e "  ${DIM}Sequential baseline:       ${SEQ_TIME}s${RESET}"
echo -e "  ${DIM}Theoretical ideal (/$NUM_HOSTS):   ${ideal}s${RESET}"
echo -e "  ${DIM}Actual best (all-at-once): ${best}s${RESET}"
echo -e "  ${DIM}Parallel efficiency:       ~${efficiency}%${RESET}"
echo ""

success "Benchmark complete."

clean_targets
if [ "$BENCH_KEEP_GRAPHS" != "1" ]; then
    rm -f "${BENCH_OUTPUT_DIR}"/bench-*.cgr
fi
rm -f "${BENCH_OUTPUT_DIR}"/bench-*.cgr.report.json
rm -f /workspace/.state/bench-*.cgr.state
