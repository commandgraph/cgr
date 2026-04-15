#!/usr/bin/env bash
# install.sh — CommandGraph (cgr) interactive installer
# Run from the repository root: ./install.sh

set -euo pipefail

# ── Terminal capability detection ─────────────────────────────────────────────

if [ -t 1 ] && [ "${TERM:-dumb}" != "dumb" ] && [ "${NO_COLOR:-}" = "" ]; then
    _ESC="$(printf '\033')"
    _RED="${_ESC}[0;31m"
    _GREEN="${_ESC}[0;32m"
    _YELLOW="${_ESC}[1;33m"
    _BLUE="${_ESC}[0;34m"
    _CYAN="${_ESC}[0;36m"
    _MAGENTA="${_ESC}[0;35m"
    _WHITE="${_ESC}[1;37m"
    _BOLD="${_ESC}[1m"
    _DIM="${_ESC}[2m"
    _RESET="${_ESC}[0m"
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
ACTION="install"

# ── Arguments ─────────────────────────────────────────────────────────────────

usage() {
    cat <<EOF
Usage: ./install.sh [install|uninstall]

Commands:
  install      Install cgr. This is the default.
  uninstall    Remove an installed cgr binary and optionally the stdlib repo.
EOF
}

parse_args() {
    case "${1:-install}" in
        install|--install) ACTION="install";;
        uninstall|--uninstall|remove|--remove) ACTION="uninstall";;
        -h|--help|help) usage; exit 0;;
        *) usage >&2; exit 2;;
    esac
}

# ── Python check ──────────────────────────────────────────────────────────────

find_python() {
    for candidate in python3 python; do
        if command -v "$candidate" >/dev/null 2>&1; then
            local ver
            ver=$("$candidate" -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>/dev/null)
            local major minor
            major="${ver%%.*}"; minor="${ver##*.}"
            if { [ "$major" -gt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -ge 8 ]; }; } 2>/dev/null; then
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

get_installed_cgr_version() {
    local path="$1"
    local line
    if [ -x "$path" ] && line=$("$path" version 2>/dev/null | head -1); then
        printf '%s' "$line" | awk '{print $NF}'
        return 0
    fi
    if line=$("$PYTHON" "$path" version 2>/dev/null | head -1); then
        printf '%s' "$line" | awk '{print $NF}'
        return 0
    fi
    printf 'unknown'
}

add_path_cgr_candidate() {
    local path="$1"
    [ -n "$path" ] || return 0
    [ "$path" != "$CGR_SRC" ] || return 0
    [ -x "$path" ] || return 0

    local item
    for item in "${CGR_PATH_CANDIDATES[@]:-}"; do
        [ "$item" != "$path" ] || return 0
    done
    CGR_PATH_CANDIDATES+=("$path")
}

find_path_cgrs() {
    CGR_PATH_CANDIDATES=()

    local old_ifs="$IFS"
    local dir
    IFS=:
    for dir in $PATH; do
        [ -n "$dir" ] || dir="."
        add_path_cgr_candidate "${dir}/cgr"
    done
    IFS="$old_ifs"
}

needs_sudo_for_path() {
    local path="$1"
    if [ -w "$path" ] || [ -w "$(dirname "$path")" ]; then
        return 1
    fi
    return 0
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

detect_existing_installations() {
    find_path_cgrs

    if [ "${#CGR_PATH_CANDIDATES[@]}" -eq 0 ]; then
        dim "No existing cgr command found in PATH"
        return 0
    fi

    section "Existing cgr installations"
    hr
    local i=1 path ver marker
    for path in "${CGR_PATH_CANDIDATES[@]}"; do
        ver=$(get_installed_cgr_version "$path")
        marker=""
        if [ "$i" -eq 1 ]; then
            marker=" ${_DIM}(PATH first)${_RESET}"
        fi
        info "${path}  ${_DIM}(version ${ver})${_RESET}${marker}"
        i=$((i + 1))
    done
    if [ "${#CGR_PATH_CANDIDATES[@]}" -gt 1 ]; then
        warn "Multiple cgr commands are visible in PATH. Your shell will run the first one above."
    fi
    hr
}

choose_install_action() {
    UPGRADE_EXISTING=0
    detect_existing_installations

    if [ "${#CGR_PATH_CANDIDATES[@]}" -gt 0 ]; then
        local first="${CGR_PATH_CANDIDATES[0]}"
        nl
        if confirm "Upgrade PATH-first cgr at ${first} to CommandGraph ${CGR_VER}?" "y"; then
            INSTALL_DIR="$(dirname "$first")"
            if needs_sudo_for_path "$first"; then
                NEED_SUDO=1
            else
                NEED_SUDO=0
            fi
            UPGRADE_EXISTING=1
            return 0
        fi
    fi

    choose_install_dir
}

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
    if [ "${UPGRADE_EXISTING:-0}" = "1" ]; then
        info "Upgrade      →  ${_BOLD}${INSTALL_DIR}/cgr${_RESET}"
    else
        info "cgr binary   →  ${_BOLD}${INSTALL_DIR}/cgr${_RESET}"
    fi
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

    if [ "${UPGRADE_EXISTING:-0}" = "1" ]; then
        ok "Upgraded PATH-first cgr to CommandGraph ${CGR_VER}"
    fi

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

# ── Uninstall helpers ────────────────────────────────────────────────────────

add_candidate() {
    local path="$1"
    [ -n "$path" ] || return 0
    [ "$path" != "$CGR_SRC" ] || return 0
    [ -e "$path" ] || return 0

    local item
    for item in "${UNINSTALL_CANDIDATES[@]:-}"; do
        [ "$item" != "$path" ] || return 0
    done
    UNINSTALL_CANDIDATES+=("$path")
}

find_uninstall_candidates() {
    UNINSTALL_CANDIDATES=()

    find_path_cgrs
    local path
    for path in "${CGR_PATH_CANDIDATES[@]}"; do
        add_candidate "$path"
    done
    add_candidate "${HOME}/.local/bin/cgr"
    add_candidate "/usr/local/bin/cgr"
}

choose_uninstall_target() {
    find_uninstall_candidates

    if [ "${#UNINSTALL_CANDIDATES[@]}" -gt 0 ]; then
        print_menu "Which cgr should be uninstalled?" "${UNINSTALL_CANDIDATES[@]}" "Custom path…"
        pick $((${#UNINSTALL_CANDIDATES[@]} + 1))
        if [ "$REPLY" -le "${#UNINSTALL_CANDIDATES[@]}" ]; then
            UNINSTALL_TARGET="${UNINSTALL_CANDIDATES[$((REPLY - 1))]}"
        else
            ask "Enter path to cgr:"
            read -r UNINSTALL_TARGET
            UNINSTALL_TARGET="${UNINSTALL_TARGET/#\~/$HOME}"
        fi
    else
        warn "No cgr binary was found in PATH, ~/.local/bin, or /usr/local/bin."
        ask "Enter path to cgr:"
        read -r UNINSTALL_TARGET
        UNINSTALL_TARGET="${UNINSTALL_TARGET/#\~/$HOME}"
    fi

    if [ -z "$UNINSTALL_TARGET" ]; then
        fatal "No uninstall target selected."
    fi
    if [ ! -e "$UNINSTALL_TARGET" ]; then
        fatal "${UNINSTALL_TARGET} does not exist."
    fi
    if [ "$UNINSTALL_TARGET" = "$CGR_SRC" ]; then
        fatal "Refusing to remove source file ${CGR_SRC}."
    fi

    if needs_sudo_for_path "$UNINSTALL_TARGET"; then
        NEED_SUDO=1
    else
        NEED_SUDO=0
    fi
    INSTALL_DIR="$(dirname "$UNINSTALL_TARGET")"
}

choose_uninstall_repo() {
    REMOVE_REPO=0
    REPO_DEST="${HOME}/.cgr/repo"

    if [ -d "$REPO_DEST" ]; then
        nl
        if confirm "Remove stdlib template repository at ${REPO_DEST}?" "n"; then
            REMOVE_REPO=1
        fi
    fi
}

print_uninstall_summary() {
    nl
    section "Uninstall plan"
    hr
    info "cgr binary   →  ${_BOLD}${UNINSTALL_TARGET}${_RESET}"
    if [ "$REMOVE_REPO" = "1" ]; then
        info "Repo         →  ${_BOLD}${REPO_DEST}${_RESET}"
    else
        dim "Repo cleanup skipped"
    fi
    dim "PATH entries added by this installer will be removed when found."
    if [ "$NEED_SUDO" = "1" ]; then
        warn "sudo required to remove ${UNINSTALL_TARGET}"
    fi
    hr
}

remove_installer_path_entries() {
    local rc_files=(
        "${HOME}/.bashrc"
        "${HOME}/.zshrc"
        "${HOME}/.profile"
        "${HOME}/.config/fish/config.fish"
    )
    local rc tmp removed=0
    local path_line="export PATH=\"${INSTALL_DIR}:\$PATH\""

    for rc in "${rc_files[@]}"; do
        [ -f "$rc" ] || continue
        if grep -Fq "$path_line" "$rc"; then
            tmp="${rc}.cgr-uninstall.$$"
            awk -v path_line="$path_line" '
                $0 == "# Added by CommandGraph installer" {
                    getline next_line
                    if (next_line == path_line) {
                        next
                    }
                    print
                    print next_line
                    next
                }
                $0 != path_line { print }
            ' "$rc" > "$tmp"
            mv "$tmp" "$rc"
            ok "Removed PATH entry from ${rc}"
            removed=1
        fi
    done

    if [ "$removed" = "0" ]; then
        dim "No installer-added PATH entries found"
    fi
}

do_uninstall() {
    section "Uninstalling"

    if [ "$NEED_SUDO" = "1" ]; then
        sudo rm -f "$UNINSTALL_TARGET"
    else
        rm -f "$UNINSTALL_TARGET"
    fi
    ok "Removed ${_BOLD}${UNINSTALL_TARGET}${_RESET}"

    if [ "$REMOVE_REPO" = "1" ]; then
        rm -rf "$REPO_DEST"
        ok "Removed ${_BOLD}${REPO_DEST}${_RESET}"
    fi

    remove_installer_path_entries
}

print_remaining_data_notice() {
    section "Remaining cgr data"

    find_path_cgrs
    if [ "${#CGR_PATH_CANDIDATES[@]}" -gt 0 ]; then
        warn "Other cgr commands are still reachable in PATH:"
        local path
        for path in "${CGR_PATH_CANDIDATES[@]}"; do
            dim "$path"
        done
    else
        ok "No other cgr command is currently reachable in PATH"
    fi

    nl
    dim "The uninstaller only removes the selected binary, optional default repo, and installer-added PATH entries."
    dim "It does not scan or delete graph project data."
    nl

    if [ -d "${HOME}/.cgr/repo" ]; then
        warn "Default stdlib repo still exists: ${HOME}/.cgr/repo"
    fi
    if [ -d "${HOME}/.cgr" ]; then
        warn "CommandGraph home still exists: ${HOME}/.cgr"
    fi
    if [ -d "${SCRIPT_DIR}/.state" ]; then
        warn "State directory exists in this checkout: ${SCRIPT_DIR}/.state"
    fi

    dim "Other possible locations:"
    dim "- Custom template repos passed with --repo"
    dim "- Per-graph state directories named .state/ in graph workspaces"
    dim "- Saved reports, DOT files, HTML visualizations, and secret vault files created by cgr commands"
    dim "- Manually added PATH entries or aliases in shell startup files"
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

parse_args "$@"
print_banner

if [ "$ACTION" = "install" ] && [ ! -f "$CGR_SRC" ]; then
    fatal "cgr.py not found in ${SCRIPT_DIR}. Run this script from the CommandGraph repository root."
fi

if [ "$ACTION" = "install" ]; then
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
    choose_install_action
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
else
    choose_uninstall_target
    choose_uninstall_repo
    print_uninstall_summary

    nl
    if ! confirm "Proceed with uninstall?" "n"; then
        nl
        info "Uninstall cancelled."
        exit 0
    fi

    do_uninstall
    nl
    ok "Uninstall complete."
    print_remaining_data_notice
fi
