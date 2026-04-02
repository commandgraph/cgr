#!/usr/bin/env bash
# Shared helpers for benchmark scripts

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
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

wait_for_host() {
    local host="$1"
    for i in $(seq 1 20); do
        if ssh -o ConnectTimeout=2 "deploy@${host}" "echo ok" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    echo -e "${RED}  ✗ Cannot reach ${host}${RESET}"
    return 1
}

# Run a cgr apply, extract wall_clock_ms from the JSON report.
# Usage: time_run "label" /path/to/file.cgr
# Sets LAST_SECONDS and prints the result line.
LAST_SECONDS=0
time_run() {
    local label="$1"
    local file="$2"
    local report_file="${file}.report.json"
    rm -f "$report_file"

    cgr apply "$file" --no-color --report "$report_file" >/dev/null 2>&1
    local rc=$?

    if [ -f "$report_file" ]; then
        # Read wall_clock_ms from the engine's own report
        local ms
        ms=$(python3 -c "import json; print(json.load(open('${report_file}'))['wall_clock_ms'])" 2>/dev/null || echo "0")
        LAST_SECONDS=$(echo "scale=2; $ms / 1000" | bc)
        rm -f "$report_file"
    else
        LAST_SECONDS="0"
    fi

    if [ $rc -eq 0 ]; then
        echo -e "  ${GREEN}✓${RESET} ${label}: ${BOLD}${LAST_SECONDS}s${RESET}"
    else
        echo -e "  ${RED}✗${RESET} ${label}: ${BOLD}${LAST_SECONDS}s${RESET} (exit=$rc)"
    fi
    return $rc
}
