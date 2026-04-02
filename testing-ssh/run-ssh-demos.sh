#!/usr/bin/env bash
set -euo pipefail

IMAGE_CONTROL="cgr-ssh-control"
IMAGE_TARGET="cgr-ssh-target"
NETWORK="cgr-net"
TARGET_CONTAINER="cg-target"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'
BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

detect_runtime() {
    if command -v podman &>/dev/null; then RUNTIME=podman
    elif command -v docker &>/dev/null; then echo -e "${DIM}podman not found, using docker${RESET}"; RUNTIME=docker
    else echo -e "${RED}Error: neither podman nor docker found.${RESET}"; exit 1; fi
}

ensure_network() {
    if ! $RUNTIME network inspect "$NETWORK" >/dev/null 2>&1; then
        echo -e "${DIM}  Creating network: ${NETWORK}${RESET}"
        $RUNTIME network create "$NETWORK" >/dev/null 2>&1
    fi
}

build_images() {
    echo -e "${BOLD}${CYAN}Building SSH test images...${RESET}"
    local ctx; ctx=$(mktemp -d)

    # --- Control image ---
    echo -e "${DIM}  Building control node: ${IMAGE_CONTROL}${RESET}"
    cp "$PROJECT_DIR/cgr.py" "$ctx/"
    cp -r "$PROJECT_DIR/repo" "$ctx/"
    mkdir -p "$ctx/examples" "$ctx/demos"
    cp "$SCRIPT_DIR/examples/"*.cgr "$ctx/examples/"
    cp "$SCRIPT_DIR/demos/"*.sh "$ctx/demos/"
    cp "$SCRIPT_DIR/Containerfile.control" "$ctx/Containerfile"
    $RUNTIME build -t "$IMAGE_CONTROL" -f "$ctx/Containerfile" "$ctx" --quiet
    echo -e "${GREEN}  ✓ ${IMAGE_CONTROL}${RESET}"

    # Extract public key
    PUBKEY=$($RUNTIME run --rm "$IMAGE_CONTROL" -c "cat /opt/cgr/ssh_pubkey")
    echo "$PUBKEY" > "$SCRIPT_DIR/.ssh_pubkey"

    # --- Target image ---
    rm -rf "$ctx"/*
    echo -e "${DIM}  Building target node: ${IMAGE_TARGET}${RESET}"
    cp "$SCRIPT_DIR/Containerfile.target" "$ctx/Containerfile"
    cp "$SCRIPT_DIR/entrypoint-target.sh" "$ctx/"
    $RUNTIME build -t "$IMAGE_TARGET" -f "$ctx/Containerfile" "$ctx" --quiet
    echo -e "${GREEN}  ✓ ${IMAGE_TARGET}${RESET}"

    rm -rf "$ctx"
    echo -e "${GREEN}✓ Both images built.${RESET}"; echo ""
}

start_target() {
    ensure_network
    $RUNTIME rm -f "$TARGET_CONTAINER" 2>/dev/null || true

    if [ ! -f "$SCRIPT_DIR/.ssh_pubkey" ]; then
        echo -e "${RED}No SSH pubkey found. Run 'build' first.${RESET}"; exit 1
    fi
    local pubkey; pubkey=$(cat "$SCRIPT_DIR/.ssh_pubkey")

    echo -e "${DIM}Starting SSH target: ${TARGET_CONTAINER}${RESET}"
    $RUNTIME run -d --rm \
        --name "$TARGET_CONTAINER" \
        --network "$NETWORK" \
        --hostname target \
        --network-alias target \
        -e "AUTHORIZED_KEY=$pubkey" \
        "$IMAGE_TARGET" >/dev/null

    echo -e "${DIM}  Waiting for sshd...${RESET}"
    local ready=false
    for i in $(seq 1 20); do
        # Test if sshd is accepting connections by attempting a lightweight SSH
        if $RUNTIME run --rm --network "$NETWORK" "$IMAGE_CONTROL" \
            -c "ssh -o ConnectTimeout=2 deploy@target 'echo ok'" 2>/dev/null | grep -q ok; then
            ready=true; break
        fi
        sleep 1
    done

    if $ready; then
        echo -e "${GREEN}✓ Target running and SSH accepting connections${RESET}"
    else
        echo -e "${RED}✗ Cannot connect to target via SSH after 20s${RESET}"
        echo -e "${DIM}  Target logs:${RESET}"
        $RUNTIME logs "$TARGET_CONTAINER" 2>&1 | tail -10
        exit 1
    fi
}

stop_target() { $RUNTIME rm -f "$TARGET_CONTAINER" 2>/dev/null || true; }

run_demo() {
    local script="$1"
    $RUNTIME run --rm -it \
        --name "cg-control-$$" \
        --network "$NETWORK" \
        "$IMAGE_CONTROL" \
        -c "source /opt/cgr/demos/$script"
}

list_demos() {
    echo -e "${BOLD}Available SSH demos:${RESET}"
    echo ""
    echo -e "  ${CYAN}0${RESET}  Verify SSH connectivity between containers"
    echo -e "  ${CYAN}1${RESET}  Basic remote plan → apply → idempotent re-run"
    echo -e "  ${CYAN}2${RESET}  Crash recovery across the network"
    echo -e "  ${CYAN}3${RESET}  Parallel execution + sudo over SSH"
    echo -e "  ${CYAN}4${RESET}  Remote drift detection"
    echo ""
    echo -e "${DIM}Usage: ./run-ssh-demos.sh [NUMBER]${RESET}"
}

teardown() {
    echo -e "${DIM}Tearing down...${RESET}"
    $RUNTIME rm -f "$TARGET_CONTAINER" 2>/dev/null || true
    $RUNTIME network rm "$NETWORK" 2>/dev/null || true
    $RUNTIME rmi "$IMAGE_CONTROL" "$IMAGE_TARGET" 2>/dev/null || true
    rm -f "$SCRIPT_DIR/.ssh_pubkey"
    echo -e "${GREEN}✓ Cleaned up.${RESET}"
}

demo_scripts=(
    "00-ssh-verify.sh"
    "01-ssh-basic.sh"
    "02-ssh-crash-recovery.sh"
    "03-ssh-parallel.sh"
    "04-ssh-drift.sh"
)

detect_runtime

case "${1:-all}" in
    build) build_images ;;
    shell)
        build_images; start_target
        echo -e "${BOLD}Interactive control shell (target running in background)${RESET}"
        echo -e "${DIM}  ssh deploy@target    — reach the target${RESET}"
        echo -e "${DIM}  cgr --help  — the engine${RESET}"
        echo ""
        $RUNTIME run --rm -it --name "cg-control-shell" --network "$NETWORK" "$IMAGE_CONTROL"
        stop_target ;;
    list) list_demos ;;
    teardown) teardown ;;
    all)
        build_images; start_target
        echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}${CYAN}║  CommandGraph SSH Demo Suite — Running all 5 demos          ║${RESET}"
        echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
        echo ""
        for i in "${!demo_scripts[@]}"; do
            echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
            run_demo "${demo_scripts[$i]}"; echo ""
        done
        stop_target
        echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}${GREEN}║  All 5 SSH demos completed successfully.                    ║${RESET}"
        echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════════╝${RESET}" ;;
    [0-4])
        build_images; start_target
        run_demo "${demo_scripts[$1]}"
        stop_target ;;
    *) echo -e "${RED}Unknown argument: $1${RESET}"; echo ""; list_demos; exit 1 ;;
esac
