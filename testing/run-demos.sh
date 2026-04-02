#!/usr/bin/env bash
set -euo pipefail

# ─── CommandGraph Demo Runner ─────────────────────────────────────────
# Builds a Podman container with the engine pre-installed and runs
# interactive demos showing every feature.
#
# Usage:
#   ./run-demos.sh              # run all demos in sequence
#   ./run-demos.sh 3            # run only demo 3 (crash recovery)
#   ./run-demos.sh build        # just build the image
#   ./run-demos.sh shell        # drop into an interactive shell
#   ./run-demos.sh list         # list available demos
# ─────────────────────────────────────────────────────────────────────

IMAGE="cgr-test"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
DIM='\033[2m'
RESET='\033[0m'

# ─── Preflight checks ─────────────────────────────────────────────────
check_podman() {
    if command -v podman &>/dev/null; then
        RUNTIME=podman
    elif command -v docker &>/dev/null; then
        echo -e "${DIM}podman not found, falling back to docker${RESET}"
        RUNTIME=docker
    else
        echo -e "${RED}Error: neither podman nor docker found.${RESET}"
        echo "Install podman: https://podman.io/docs/installation"
        exit 1
    fi
}

# ─── Build the test image ─────────────────────────────────────────────
build_image() {
    echo -e "${BOLD}${CYAN}Building test image: ${IMAGE}${RESET}"
    echo -e "${DIM}  Context: ${SCRIPT_DIR}${RESET}"

    # Stage files into a build context
    local ctx
    ctx=$(mktemp -d)
    trap "rm -rf $ctx" EXIT

    # Engine
    cp "$PROJECT_DIR/cgr.py" "$ctx/"

    # Repo
    cp -r "$PROJECT_DIR/repo" "$ctx/"

    # Examples — use the testing scenarios
    mkdir -p "$ctx/examples"
    cp "$SCRIPT_DIR/examples/"*.cgr "$ctx/examples/"
    # Also copy the project examples for validation
    for f in "$PROJECT_DIR"/*.cg "$PROJECT_DIR"/*.cgr; do
        [ -f "$f" ] && cp "$f" "$ctx/examples/"
    done

    # Demos
    cp -r "$SCRIPT_DIR/demos" "$ctx/"

    # Containerfile
    cp "$SCRIPT_DIR/Containerfile" "$ctx/"

    $RUNTIME build -t "$IMAGE" -f "$ctx/Containerfile" "$ctx"
    echo -e "${GREEN}✓ Image built: ${IMAGE}${RESET}"
    echo ""
}

# ─── Run a demo inside the container ──────────────────────────────────
run_demo() {
    local script="$1"
    $RUNTIME run --rm -it \
        --name "cg-demo-$$" \
        "$IMAGE" \
        -c "source /opt/cgr/demos/$script"
}

# ─── List available demos ─────────────────────────────────────────────
list_demos() {
    echo -e "${BOLD}Available demos:${RESET}"
    echo ""
    echo -e "  ${CYAN}0${RESET}  Validate all example files (smoke test)"
    echo -e "  ${CYAN}1${RESET}  Basic plan → apply → idempotent re-run"
    echo -e "  ${CYAN}2${RESET}  Reusable template repository"
    echo -e "  ${CYAN}3${RESET}  Crash recovery & state tracking"
    echo -e "  ${CYAN}4${RESET}  First-class parallelism (parallel, each, stage)"
    echo -e "  ${CYAN}5${RESET}  Race cancellation"
    echo -e "  ${CYAN}6${RESET}  Drift detection"
    echo -e "  ${CYAN}7${RESET}  HTTP steps and collected reports"
    echo -e "  ${CYAN}8${RESET}  CLI tooling (fmt, lint, explain, why, convert)"
    echo -e "  ${CYAN}9${RESET}  Isolated state journals (--run-id, --state)"
    echo ""
    echo -e "${DIM}Usage: ./run-demos.sh [NUMBER]${RESET}"
}

# ─── Map demo number to script ────────────────────────────────────────
demo_scripts=(
    "00-validate.sh"
    "01-basic-execution.sh"
    "02-templates.sh"
    "03-state-resume.sh"
    "04-parallel.sh"
    "05-race.sh"
    "06-drift-detection.sh"
    "07-http-report.sh"
    "08-cli-tooling.sh"
    "09-state-isolation.sh"
)

# ─── Main ──────────────────────────────────────────────────────────────
check_podman

case "${1:-all}" in
    build)
        build_image
        ;;
    shell)
        build_image
        echo -e "${BOLD}Dropping into interactive shell...${RESET}"
        echo -e "${DIM}  Engine: cgr (in PATH)${RESET}"
        echo -e "${DIM}  Repo:   /opt/cgr/repo${RESET}"
        echo -e "${DIM}  Examples: /opt/cgr/examples${RESET}"
        echo ""
        $RUNTIME run --rm -it "$IMAGE"
        ;;
    list)
        list_demos
        ;;
    all)
        build_image
        echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}${CYAN}║  CommandGraph Demo Suite — Running all 10 demos             ║${RESET}"
        echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${RESET}"
        echo ""

        for i in "${!demo_scripts[@]}"; do
            echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
            run_demo "${demo_scripts[$i]}"
            echo ""
        done

        echo -e "${BOLD}${GREEN}╔══════════════════════════════════════════════════════════════╗${RESET}"
        echo -e "${BOLD}${GREEN}║  All 10 demos completed successfully.                       ║${RESET}"
        echo -e "${BOLD}${GREEN}╚══════════════════════════════════════════════════════════════╝${RESET}"
        ;;
    [0-9])
        build_image
        idx=$1
        run_demo "${demo_scripts[$idx]}"
        ;;
    *)
        echo -e "${RED}Unknown argument: $1${RESET}"
        echo ""
        list_demos
        exit 1
        ;;
esac
