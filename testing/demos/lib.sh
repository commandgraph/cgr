#!/usr/bin/env bash
# Shared helpers for demo scripts

# Colors
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

fail() {
    echo -e "${RED}  ✗ $1${RESET}"
}

pause() {
    local secs=${1:-1}
    sleep "$secs"
}

run_cmd() {
    echo -e "${DIM}  \$ $1${RESET}"
    eval "$1"
    local rc=$?
    echo ""
    return $rc
}

separator() {
    echo -e "${DIM}$(printf '─%.0s' {1..64})${RESET}"
}

REPO=/opt/cgr/repo
EXAMPLES=/opt/cgr/examples
