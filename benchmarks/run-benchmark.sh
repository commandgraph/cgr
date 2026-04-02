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
        exit 1
    fi
    echo ""
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

# ─── Run the benchmark ───────────────────────────────────────────────
run_benchmark() {
    local count="$1"
    local hosts; hosts=$(target_hostlist "$count")

    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${CYAN}║  CommandGraph Benchmark — ${count} targets, 5 scenarios            ║${RESET}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
    echo ""

    $RUNTIME run --rm -it \
        --name "cg-bench-control-$$" \
        --network "$NETWORK" \
        -e "TARGET_HOSTS=$hosts" \
        "$IMAGE_CONTROL" \
        -c "source /opt/cgr/demos/run-benchmark.sh"
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
        start_targets "${2:-8}"
        local_hosts=$(target_hostlist "${2:-8}")
        echo -e "${BOLD}Interactive shell (${2:-8} targets running)${RESET}"
        echo -e "${DIM}  Targets: ${local_hosts}${RESET}"
        echo -e "${DIM}  ssh deploy@cg-target-01   — reach a target${RESET}"
        echo ""
        $RUNTIME run --rm -it \
            --name "cg-bench-shell" \
            --network "$NETWORK" \
            -e "TARGET_HOSTS=$local_hosts" \
            "$IMAGE_CONTROL"
        stop_targets ;;
    [0-9]*)
        NUM_TARGETS="$ARG"
        build_images
        start_targets "$NUM_TARGETS"
        run_benchmark "$NUM_TARGETS"
        stop_targets
        echo "" ;;
    *)
        echo "Usage: ./run-benchmark.sh [N|teardown|shell]"
        echo "  N         Run benchmark against N targets (default: 8)"
        echo "  teardown  Remove all containers, network, images"
        echo "  shell     Interactive shell with targets running" ;;
esac
