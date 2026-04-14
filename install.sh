#!/usr/bin/env bash
# install.sh — CommandGraph (cgr) interactive installer
# Run from the repository root: ./install.sh

set -euo pipefail

# ── Terminal capability detection ─────────────────────────────────────────────

if [ -t 1 ] && [ "${TERM:-dumb}" != "dumb" ] && [ "${NO_COLOR:-}" = "" ]; then
    _RED='\033[0;31m'
    _GREEN='\033[0;32m'
    _YELLOW='\033[1;33m'
    _BLUE='\033[0;34m'
    _CYAN='\033[0;36m'
    _MAGENTA='\033[0;35m'
    _WHITE='\033[1;37m'
    _BOLD='\033[1m'
    _DIM='\033[2m'
    _RESET='\033[0m'
else
    _RED='' _GREEN='' _YELLOW='' _BLUE='' _CYAN='' _MAGENTA=''
    _WHITE='' _BOLD='' _DIM='' _RESET=''
fi

# ── Output helpers ────────────────────────────────────────────────────────────

nl()      { printf "\n"; }
info()    { printf "  ${_CYAN}→${_RESET}  %s\n" "$*"; }
ok()      { printf "  ${_GREEN}✓${_RESET}  ${_GREEN}%s${_RESET}\n" "$*"; }
warn()    { printf "  ${_YELLOW}⚠${_RESET}  ${_YELLOW}%s${_RESET}\n" "$*"; }
err()     { printf "  ${_RED}✗${_RESET}  ${_RED}%s${_RESET}\n" "$*" >&2; }
fatal()   { err "$*"; exit 1; }
dim()     { printf "  ${_DIM}%s${_RESET}\n" "$*"; }
section() { printf "\n${_BOLD}${_BLUE}▸${_RESET}${_BOLD} %s${_RESET}\n" "$*"; }
ask()     { printf "  ${_MAGENTA}?${_RESET}  ${_BOLD}%s${_RESET} " "$*"; }
hr()      { printf "  ${_DIM}%s${_RESET}\n" "────────────────────────────────────────────"; }

# Spinner — runs while a background PID is alive
spinner() {
    local pid=$1 msg=$2
    local frames=('⠋' '⠙' '⠹' '⠸' '⠼' '⠴' '⠦' '⠧' '⠇' '⠏')
    local i=0
    while kill -0 "$pid" 2>/dev/null; do
        printf "\r  ${_CYAN}%s${_RESET}  %s " "${frames[$((i % ${#frames[@]}))]}" "$msg"
        sleep 0.08
        i=$((i + 1))
    done
    printf "\r%-60s\r" " "   # clear line
}

# ── Banner ────────────────────────────────────────────────────────────────────

print_banner() {
    nl
    printf "${_CYAN}${_BOLD}  ╔══════════════════════════════════════════════╗${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ║${_RESET}                                              ${_CYAN}${_BOLD}║${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ║${_RESET}    ${_WHITE}${_BOLD}CommandGraph${_RESET}  ${_DIM}(cgr)${_RESET}  ${_CYAN}installer${_RESET}          ${_CYAN}${_BOLD}║${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ║${_RESET}    ${_DIM}DSL for declaring CLI command DAGs${_RESET}       ${_CYAN}${_BOLD}║${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ║${_RESET}                                              ${_CYAN}${_BOLD}║${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ╚══════════════════════════════════════════════╝${_RESET}\n"
    nl
}

# ── Locate source files ───────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CGR_SRC="${SCRIPT_DIR}/cgr.py"
REPO_SRC="${SCRIPT_DIR}/repo"

if [ ! -f "$CGR_SRC" ]; then
    fatal "cgr.py not found in ${SCRIPT_DIR}. Run this script from the CommandGraph repository root."
fi

# ── Python check ──────────────────────────────────────────────────────────────

find_python() {
    for candidate in python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            local ver
            ver=$("$candidate" -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>/dev/null)
            local major minor
            major="${ver%%.*}"; minor="${ver##*.}"
            if [ "$major" -ge 3 ] && [ "$minor" -ge 8 ] 2>/dev/null; then
                echo "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

# ── Version extraction ────────────────────────────────────────────────────────

get_cgr_version() {
    local py="${1:-python3}"
    "$py" "$CGR_SRC" version 2>/dev/null | head -1 | awk '{print $NF}' || echo "unknown"
}

# ── Menu rendering ────────────────────────────────────────────────────────────

print_menu() {
    local title="$1"; shift
    nl
    printf "  ${_BOLD}%s${_RESET}\n" "$title"
    hr
    local i=1
    for opt in "$@"; do
        printf "  ${_CYAN}${_BOLD}[%d]${_RESET}  %s\n" "$i" "$opt"
        i=$((i + 1))
    done
    hr
}

# Prompt user to pick a numbered option. Returns the chosen number in $REPLY.
pick() {
    local max=$1
    while true; do
        ask "Enter choice [1-${max}]:"
        read -r REPLY
        if printf '%s' "$REPLY" | grep -qE "^[0-9]+$" && [ "$REPLY" -ge 1 ] && [ "$REPLY" -le "$max" ]; then
            return 0
        fi
        warn "Invalid choice — enter a number between 1 and ${max}."
    done
}

# Yes/no prompt. Returns 0 for yes, 1 for no.
confirm() {
    local default="${2:-y}"
    local hint
    if [ "$default" = "y" ]; then hint="[Y/n]"; else hint="[y/N]"; fi
    while true; do
        ask "$1 ${hint}:"
        read -r REPLY
        REPLY="${REPLY:-$default}"
        case "$REPLY" in
            [Yy]*) return 0;;
            [Nn]*) return 1;;
            *)     warn "Please enter y or n.";;
        esac
    done
}

# ── Install location choice ───────────────────────────────────────────────────

choose_install_dir() {
    local user_bin="${HOME}/.local/bin"
    local system_bin="/usr/local/bin"

    print_menu "Where should cgr be installed?" \
        "${user_bin}  ${_DIM}(user — no sudo required)${_RESET}" \
        "${system_bin}  ${_DIM}(system-wide — requires sudo)${_RESET}" \
        "Custom path…"

    pick 3
    case "$REPLY" in
        1) INSTALL_DIR="$user_bin";   NEED_SUDO=0;;
        2) INSTALL_DIR="$system_bin"; NEED_SUDO=1;;
        3)
            ask "Enter install directory:"
            read -r INSTALL_DIR
            INSTALL_DIR="${INSTALL_DIR/#\~/$HOME}"  # expand leading ~
            # Guess whether sudo is needed
            if [ -w "$INSTALL_DIR" ] || [ -w "$(dirname "$INSTALL_DIR")" ]; then
                NEED_SUDO=0
            else
                NEED_SUDO=1
            fi
            ;;
    esac
}

# ── Repo (stdlib templates) install ──────────────────────────────────────────

choose_repo_dir() {
    INSTALL_REPO=0
    REPO_DEST=""

    if [ ! -d "$REPO_SRC" ]; then
        return 0
    fi

    nl
    if confirm "Install stdlib template repository? (44 reusable .cgr templates)" "y"; then
        INSTALL_REPO=1
        local default_repo="${HOME}/.cgr/repo"

        print_menu "Where should the repo be installed?" \
            "${default_repo}  ${_DIM}(recommended — available as cgr … --repo ~/.cgr/repo)${_RESET}" \
            "Custom path…"

        pick 2
        case "$REPLY" in
            1) REPO_DEST="$default_repo";;
            2)
                ask "Enter repo directory:"
                read -r REPO_DEST
                REPO_DEST="${REPO_DEST/#\~/$HOME}"
                ;;
        esac
    fi
}

# ── Pre-install summary ───────────────────────────────────────────────────────

print_summary() {
    nl
    section "Installation plan"
    hr
    info "cgr binary   →  ${_BOLD}${INSTALL_DIR}/cgr${_RESET}"
    info "Source file  →  ${_DIM}${CGR_SRC}${_RESET}"
    info "Python       →  ${_DIM}${PYTHON} (${PYTHON_VER})${_RESET}"
    info "cgr version  →  ${_DIM}${CGR_VER}${_RESET}"
    if [ "$INSTALL_REPO" = "1" ]; then
        info "Repo         →  ${_BOLD}${REPO_DEST}${_RESET}"
    fi
    if [ "$NEED_SUDO" = "1" ]; then
        warn "sudo required to write to ${INSTALL_DIR}"
    fi
    hr
}

# ── Do the install ────────────────────────────────────────────────────────────

do_install() {
    section "Installing"

    # Create install dir if needed (only for user dirs — system dirs should exist)
    if [ ! -d "$INSTALL_DIR" ]; then
        if [ "$NEED_SUDO" = "1" ]; then
            info "Creating ${INSTALL_DIR} …"
            sudo mkdir -p "$INSTALL_DIR"
        else
            info "Creating ${INSTALL_DIR} …"
            mkdir -p "$INSTALL_DIR"
        fi
        ok "Created ${INSTALL_DIR}"
    fi

    local dest="${INSTALL_DIR}/cgr"

    # Copy cgr.py → cgr with a spinner
    if [ "$NEED_SUDO" = "1" ]; then
        (sudo cp "$CGR_SRC" "$dest" && sudo chmod 755 "$dest") &
    else
        (cp "$CGR_SRC" "$dest" && chmod 755 "$dest") &
    fi
    local copy_pid=$!
    spinner "$copy_pid" "Copying cgr.py → ${dest} …"
    if ! wait "$copy_pid"; then
        fatal "Failed to copy cgr.py to ${dest}"
    fi
    ok "Installed  ${_BOLD}${dest}${_RESET}"

    # Install repo templates if requested
    if [ "$INSTALL_REPO" = "1" ] && [ -d "$REPO_SRC" ]; then
        mkdir -p "$REPO_DEST"
        (cp -r "${REPO_SRC}/." "${REPO_DEST}/") &
        local repo_pid=$!
        spinner "$repo_pid" "Copying stdlib templates → ${REPO_DEST} …"
        if ! wait "$repo_pid"; then
            fatal "Failed to copy repo to ${REPO_DEST}"
        fi
        local count
        count=$(find "$REPO_DEST" -name '*.cgr' | wc -l | tr -d ' ')
        ok "Installed  ${_BOLD}${REPO_DEST}${_RESET}  ${_DIM}(${count} templates)${_RESET}"
    fi
}

# ── PATH check ────────────────────────────────────────────────────────────────

check_path() {
    section "PATH check"
    if printf '%s' ":${PATH}:" | grep -q ":${INSTALL_DIR}:"; then
        ok "${INSTALL_DIR} is already in your PATH"
    else
        warn "${INSTALL_DIR} is not in your current PATH"
        nl
        local shell_rc=""
        case "${SHELL:-}" in
            */bash) shell_rc="${HOME}/.bashrc";;
            */zsh)  shell_rc="${HOME}/.zshrc";;
            */fish) shell_rc="${HOME}/.config/fish/config.fish";;
            *)      shell_rc="${HOME}/.profile";;
        esac
        dim "Add this line to ${shell_rc}:"
        nl
        printf "  ${_YELLOW}  export PATH=\"%s:\$PATH\"${_RESET}\n" "${INSTALL_DIR}"
        nl
        if confirm "Add it automatically to ${shell_rc}?" "y"; then
            local line="export PATH=\"${INSTALL_DIR}:\$PATH\""
            printf '\n# Added by CommandGraph installer\n%s\n' "$line" >> "$shell_rc"
            ok "Added to ${shell_rc}"
            warn "Restart your shell or run:  ${_BOLD}source ${shell_rc}${_RESET}"
        else
            warn "Skipped — add it manually before using cgr"
        fi
    fi
}

# ── Verify installation ───────────────────────────────────────────────────────

verify_install() {
    section "Verifying"
    local installed="${INSTALL_DIR}/cgr"

    if [ ! -x "$installed" ]; then
        fatal "${installed} is not executable. Something went wrong."
    fi

    local reported_ver
    if reported_ver=$("$PYTHON" "$installed" version 2>/dev/null | head -1); then
        ok "cgr runs correctly  ${_DIM}(${reported_ver})${_RESET}"
    else
        warn "cgr installed but could not run a version check"
    fi
}

# ── Post-install hints ────────────────────────────────────────────────────────

print_hints() {
    nl
    printf "${_CYAN}${_BOLD}  ╔══════════════════════════════════════════════╗${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ║${_RESET}  ${_GREEN}${_BOLD}Installation complete!${_RESET}                     ${_CYAN}${_BOLD}║${_RESET}\n"
    printf "${_CYAN}${_BOLD}  ╚══════════════════════════════════════════════╝${_RESET}\n"
    nl
    section "Quick start"
    hr
    dim "Validate a graph:"
    printf "  ${_CYAN}  cgr validate mysetup.cgr${_RESET}\n"
    nl
    dim "Preview what would run:"
    printf "  ${_CYAN}  cgr plan mysetup.cgr${_RESET}\n"
    nl
    dim "Execute:"
    printf "  ${_CYAN}  cgr apply mysetup.cgr${_RESET}\n"
    nl
    if [ "$INSTALL_REPO" = "1" ] && [ -n "$REPO_DEST" ]; then
        dim "Use stdlib templates:"
        printf "  ${_CYAN}  cgr apply mysetup.cgr --repo %s${_RESET}\n" "$REPO_DEST"
        nl
        dim "List available templates:"
        printf "  ${_CYAN}  cgr repo index --repo %s${_RESET}\n" "$REPO_DEST"
        nl
    fi
    dim "Open the web IDE:"
    printf "  ${_CYAN}  cgr serve mysetup.cgr${_RESET}\n"
    nl
    dim "Read the manual:"
    printf "  ${_CYAN}  cgr explain --help${_RESET}\n"
    hr
    nl
}

# ── Abort handler ─────────────────────────────────────────────────────────────

trap 'nl; warn "Installation cancelled."; exit 1' INT TERM

# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

print_banner

# Python pre-flight
section "Pre-flight checks"

if ! PYTHON=$(find_python); then
    fatal "Python 3.8 or later is required but was not found. Install Python 3 and try again."
fi

PYTHON_VER=$("$PYTHON" -c "import sys; print('.'.join(map(str, sys.version_info[:3])))")
CGR_VER=$(get_cgr_version "$PYTHON")

ok "Python  ${_DIM}${PYTHON}  ${PYTHON_VER}${_RESET}"
ok "Source  ${_DIM}${CGR_SRC}${_RESET}"
ok "Version ${_DIM}${CGR_VER}${_RESET}"

# Gather choices
choose_install_dir
choose_repo_dir
print_summary

nl
if ! confirm "Proceed with installation?" "y"; then
    nl
    info "Installation cancelled."
    exit 0
fi

do_install
check_path
verify_install
print_hints
