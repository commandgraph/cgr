#!/usr/bin/env bash
# Shared helpers for SSH demo scripts

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

banner() {
    echo ""
    echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
    echo -e "${BOLD}${CYAN}║  $1$(printf '%*s' $((58 - ${#1})) '')║${RESET}"
    echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
    echo ""
}

section() {
    echo ""
    echo -e "${BOLD}${YELLOW}── $1 ──${RESET}"
    echo ""
}

narrate() {
    echo -e "${DIM}  ▸ $1${RESET}"
}

success() {
    echo -e "${GREEN}  ✓ $1${RESET}"
}

fail_msg() {
    echo -e "${RED}  ✗ $1${RESET}"
}

run_cmd() {
    echo -e "${DIM}  \$ $1${RESET}"
    eval "$1"
    local rc=$?
    echo ""
    return $rc
}

# Run a command on the target via SSH
run_on_target() {
    echo -e "${DIM}  [target] \$ $1${RESET}"
    ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR deploy@target "$1"
    echo ""
}

wait_for_target() {
    echo -e "${DIM}  ▸ Waiting for SSH target to be reachable...${RESET}"
    for i in $(seq 1 30); do
        if ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=2 deploy@target "echo ok" >/dev/null 2>&1; then
            echo -e "${GREEN}  ✓ Target reachable via SSH${RESET}"
            return 0
        fi
        sleep 1
    done
    echo -e "${RED}  ✗ Target not reachable after 30s${RESET}"
    return 1
}

REPO=/opt/cgr/repo
EXAMPLES=/opt/cgr/examples
