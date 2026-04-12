#!/usr/bin/env bash
set -euo pipefail

# ─── CommandGraph Parallelism Benchmark Runner ────────────────────────
#
# Spawns N target containers, connects them via a network, and runs
# the benchmark from a control container.
#
# Usage:
#   ./run-benchmark.sh              # 8 targets (default)
#   ./run-benchmark.sh 12           # 12 targets
#   ./run-benchmark.sh teardown     # clean up everything
#   ./run-benchmark.sh shell        # control shell with targets running
# ─────────────────────────────────────────────────────────────────────

NUM_TARGETS="${1:-8}"
IMAGE_CONTROL="cg-bench-control"
IMAGE_TARGET="cg-bench-target"
NETWORK="cg-bench-net"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'
YELLOW='\033[1;33m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'
TARGET_MEM_MB="${BENCH_TARGET_MEM_MB:-96}"
TARGETS_PER_CPU="${BENCH_TARGETS_PER_CPU:-8}"
MEM_RESERVE_MB="${BENCH_MEM_RESERVE_MB:-1024}"
LAST_OUTPUT_DIR=""
AUTO_TARGET_COUNT=""

mount_arg() {
    local host_dir="$1"
    if [ "$RUNTIME" = "podman" ]; then
        printf '%s' "${host_dir}:/workspace/generated:Z"
    else
        printf '%s' "${host_dir}:/workspace/generated"
    fi
}

# ─── Detect runtime ──────────────────────────────────────────────────
detect_runtime() {
    if command -v podman &>/dev/null; then RUNTIME=podman
    elif command -v docker &>/dev/null; then echo -e "${DIM}podman not found, using docker${RESET}"; RUNTIME=docker
    else echo -e "${RED}Error: neither podman nor docker found.${RESET}"; exit 1; fi
}

# ─── Network ──────────────────────────────────────────────────────────
ensure_network() {
    if ! $RUNTIME network inspect "$NETWORK" >/dev/null 2>&1; then
        $RUNTIME network create "$NETWORK" >/dev/null 2>&1
    fi
}

require_podman() {
    if [ "$RUNTIME" != "podman" ]; then
        echo -e "${RED}Error: auto/max benchmark mode requires podman.${RESET}"
        exit 1
    fi
}

# ─── Build images ─────────────────────────────────────────────────────
build_images() {
    echo -e "${BOLD}${CYAN}Building benchmark images...${RESET}"
    local ctx; ctx=$(mktemp -d)

    # Control image
    echo -e "${DIM}  Building control: ${IMAGE_CONTROL}${RESET}"
    cp "$PROJECT_DIR/cgr.py" "$ctx/"
    cp -r "$PROJECT_DIR/repo" "$ctx/"
    mkdir -p "$ctx/demos"
    cp "$SCRIPT_DIR/demos/"*.sh "$ctx/demos/"
    cp "$SCRIPT_DIR/Containerfile.control" "$ctx/Containerfile"
    $RUNTIME build -t "$IMAGE_CONTROL" -f "$ctx/Containerfile" "$ctx" --quiet
    echo -e "${GREEN}  ✓ ${IMAGE_CONTROL}${RESET}"

    # Extract pubkey
    PUBKEY=$($RUNTIME run --rm "$IMAGE_CONTROL" -c "cat /opt/cgr/ssh_pubkey")
    echo "$PUBKEY" > "$SCRIPT_DIR/.ssh_pubkey"

    # Target image (reuse from testing-ssh)
    rm -rf "$ctx"/*
    echo -e "${DIM}  Building target: ${IMAGE_TARGET}${RESET}"
    cp "$PROJECT_DIR/testing-ssh/Containerfile.target" "$ctx/Containerfile"
    cp "$PROJECT_DIR/testing-ssh/entrypoint-target.sh" "$ctx/"
    $RUNTIME build -t "$IMAGE_TARGET" -f "$ctx/Containerfile" "$ctx" --quiet
    echo -e "${GREEN}  ✓ ${IMAGE_TARGET}${RESET}"

    rm -rf "$ctx"
    echo ""
}

# ─── Spawn target fleet ──────────────────────────────────────────────
start_targets() {
    local count="$1"
    ensure_network

    local pubkey; pubkey=$(cat "$SCRIPT_DIR/.ssh_pubkey")

    echo -e "${BOLD}Starting ${count} target containers...${RESET}"
    for i in $(seq 1 "$count"); do
        local name="cg-target-$(printf '%02d' $i)"
        $RUNTIME rm -f "$name" 2>/dev/null || true
        $RUNTIME run -d --rm \
            --name "$name" \
            --network "$NETWORK" \
            --hostname "$name" \
            --network-alias "$name" \
            -e "AUTHORIZED_KEY=$pubkey" \
            "$IMAGE_TARGET" >/dev/null
    done

    # Wait for all to be SSH-reachable
    echo -e "${DIM}  Waiting for SSH on all targets...${RESET}"
    local all_ready=false
    for attempt in $(seq 1 30); do
        all_ready=true
        for i in $(seq 1 "$count"); do
            local name="cg-target-$(printf '%02d' $i)"
            if ! $RUNTIME run --rm --network "$NETWORK" "$IMAGE_CONTROL" \
                -c "ssh -o ConnectTimeout=2 deploy@${name} 'echo ok'" 2>/dev/null | grep -q ok; then
                all_ready=false; break
            fi
        done
        $all_ready && break
        sleep 1
    done

    if $all_ready; then
        echo -e "${GREEN}✓ All ${count} targets ready${RESET}"
    else
        echo -e "${RED}✗ Some targets not reachable after 30s${RESET}"
        stop_targets
        return 1
    fi
    echo ""
    return 0
}

# ─── Stop targets ─────────────────────────────────────────────────────
stop_targets() {
    echo -e "${DIM}Stopping targets...${RESET}"
    for name in $($RUNTIME ps --filter "name=cg-target-" --format '{{.Names}}' 2>/dev/null); do
        $RUNTIME rm -f "$name" 2>/dev/null || true
    done
}

# ─── Build target host list ──────────────────────────────────────────
target_hostlist() {
    local count="$1"
    local hosts=""
    for i in $(seq 1 "$count"); do
        [ -n "$hosts" ] && hosts="${hosts},"
        hosts="${hosts}cg-target-$(printf '%02d' $i)"
    done
    echo "$hosts"
}

mem_available_mb() {
    awk '/MemAvailable:/ { printf "%d\n", $2 / 1024 }' /proc/meminfo
}

estimate_auto_targets() {
    local cpus mem_mb mem_limited cpu_limited estimate
    cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc)
    mem_mb=$(mem_available_mb)
    mem_limited=$(( (mem_mb - MEM_RESERVE_MB) / TARGET_MEM_MB ))
    cpu_limited=$(( cpus * TARGETS_PER_CPU ))
    [ "$mem_limited" -lt 1 ] && mem_limited=1
    [ "$cpu_limited" -lt 1 ] && cpu_limited=1
    if [ "$mem_limited" -lt "$cpu_limited" ]; then
        estimate="$mem_limited"
    else
        estimate="$cpu_limited"
    fi
    [ "$estimate" -lt 1 ] && estimate=1
    echo "$estimate"
}

probe_target_capacity() {
    local count="$1"
    echo -e "${DIM}  Probing ${count} targets...${RESET}"
    if start_targets "$count"; then
        stop_targets >/dev/null 2>&1 || true
        return 0
    fi
    return 1
}

find_max_target_count() {
    require_podman
    local estimate low high mid
    estimate=$(estimate_auto_targets)
    local cpus mem_mb
    cpus=$(getconf _NPROCESSORS_ONLN 2>/dev/null || nproc)
    mem_mb=$(mem_available_mb)

    echo -e "${BOLD}${CYAN}Auto-sizing podman target fleet${RESET}"
    echo -e "${DIM}  Host CPUs:          ${cpus}${RESET}"
    echo -e "${DIM}  MemAvailable:      ${mem_mb} MiB${RESET}"
    echo -e "${DIM}  Reserve budget:    ${MEM_RESERVE_MB} MiB${RESET}"
    echo -e "${DIM}  Target budget:     ${TARGET_MEM_MB} MiB/container${RESET}"
    echo -e "${DIM}  CPU density cap:   ${TARGETS_PER_CPU} containers/core${RESET}"
    echo -e "${DIM}  Initial estimate:  ${estimate} targets${RESET}"

    low=1
    high="$estimate"
    while [ "$low" -lt "$high" ]; do
        mid=$(( (low + high + 1) / 2 ))
        if probe_target_capacity "$mid"; then
            low="$mid"
        else
            high=$(( mid - 1 ))
        fi
    done
    echo -e "${GREEN}✓ Auto-sized target count: ${low}${RESET}"
    echo ""
    AUTO_TARGET_COUNT="$low"
}

prepare_output_dir() {
    local mode="$1"
    local run_id timestamp out_dir
    timestamp=$(date -u +%Y%m%dT%H%M%SZ)
    mkdir -p "$SCRIPT_DIR/generated"
    run_id="${mode}-${timestamp}"
    out_dir="$SCRIPT_DIR/generated/${run_id}"
    mkdir -p "$out_dir"
    rm -f "$SCRIPT_DIR/generated/latest"
    ln -s "$out_dir" "$SCRIPT_DIR/generated/latest"
    LAST_OUTPUT_DIR="$out_dir"
}

# ─── Run the benchmark ───────────────────────────────────────────────
run_benchmark() {
    local count="$1"
    local hosts; hosts=$(target_hostlist "$count")
    local output_dir="$2"
    local output_mount; output_mount=$(mount_arg "$output_dir")

    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${CYAN}║  CommandGraph Benchmark — ${count} targets, 5 scenarios            ║${RESET}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
    echo ""

    $RUNTIME run --rm -it \
        --name "cg-bench-control-$$" \
        --network "$NETWORK" \
        -v "$output_mount" \
        -e "TARGET_HOSTS=$hosts" \
        -e "BENCH_OUTPUT_DIR=/workspace/generated" \
        -e "BENCH_KEEP_GRAPHS=1" \
        "$IMAGE_CONTROL" \
        -c "source /opt/cgr/demos/run-benchmark.sh"

    cat > "${output_dir}/README.md" <<EOF
# Generated benchmark graphs

This directory was generated by \`benchmarks/run-benchmark.sh\`.

- Target count: ${count}
- Hosts: ${hosts}
- Generated at: $(date -u +%Y-%m-%dT%H:%M:%SZ)

Inspect the highest-concurrency graph locally:

\`\`\`bash
python3 cgr.py serve ${output_dir}/bench-parN.cgr
python3 cgr.py visualize ${output_dir}/bench-parN.cgr -o ${output_dir}/bench-parN.html
\`\`\`

Other scenarios in this directory:

- \`bench-seq.cgr\`
- \`bench-par2.cgr\`
- \`bench-par4.cgr\`
- \`bench-parN.cgr\`
- \`bench-staged.cgr\`
EOF

    echo -e "${GREEN}✓ Generated graphs saved to ${output_dir}${RESET}"
    echo -e "${DIM}  Inspect with: python3 cgr.py serve ${output_dir}/bench-parN.cgr${RESET}"
    echo -e "${DIM}  Or visualize: python3 cgr.py visualize ${output_dir}/bench-parN.cgr -o ${output_dir}/bench-parN.html${RESET}"
}

# ─── Teardown ─────────────────────────────────────────────────────────
teardown() {
    echo -e "${DIM}Full teardown...${RESET}"
    stop_targets
    $RUNTIME network rm "$NETWORK" 2>/dev/null || true
    $RUNTIME rmi "$IMAGE_CONTROL" "$IMAGE_TARGET" 2>/dev/null || true
    rm -f "$SCRIPT_DIR/.ssh_pubkey"
    echo -e "${GREEN}✓ Cleaned up.${RESET}"
}

# ─── Main ─────────────────────────────────────────────────────────────
detect_runtime

ARG="${1:-8}"

case "$ARG" in
    teardown)
        teardown ;;
    shell)
        build_images
        prepare_output_dir "shell"
        start_targets "${2:-8}"
        local_hosts=$(target_hostlist "${2:-8}")
        output_mount=$(mount_arg "$LAST_OUTPUT_DIR")
        echo -e "${BOLD}Interactive shell (${2:-8} targets running)${RESET}"
        echo -e "${DIM}  Targets: ${local_hosts}${RESET}"
        echo -e "${DIM}  ssh deploy@cg-target-01   — reach a target${RESET}"
        echo -e "${DIM}  Generated graphs dir: ${LAST_OUTPUT_DIR}${RESET}"
        echo ""
        $RUNTIME run --rm -it \
            --name "cg-bench-shell" \
            --network "$NETWORK" \
            -v "$output_mount" \
            -e "TARGET_HOSTS=$local_hosts" \
            -e "BENCH_OUTPUT_DIR=/workspace/generated" \
            "$IMAGE_CONTROL"
        stop_targets ;;
    auto|max)
        build_images
        find_max_target_count
        NUM_TARGETS="$AUTO_TARGET_COUNT"
        prepare_output_dir "auto-${NUM_TARGETS}"
        start_targets "$NUM_TARGETS"
        run_benchmark "$NUM_TARGETS" "$LAST_OUTPUT_DIR"
        stop_targets
        echo "" ;;
    [0-9]*)
        NUM_TARGETS="$ARG"
        build_images
        prepare_output_dir "fixed-${NUM_TARGETS}"
        start_targets "$NUM_TARGETS"
        run_benchmark "$NUM_TARGETS" "$LAST_OUTPUT_DIR"
        stop_targets
        echo "" ;;
    *)
        echo "Usage: ./run-benchmark.sh [N|auto|max|teardown|shell]"
        echo "  N         Run benchmark against N targets (default: 8)"
        echo "  auto|max  Auto-size podman target count from host capacity"
        echo "  teardown  Remove all containers, network, images"
        echo "  shell     Interactive shell with targets running" ;;
esac
