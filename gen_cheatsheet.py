#!/usr/bin/env python3
"""Generate the CommandGraph single-page cheat sheet PDF."""

from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

OUTPUT = "/mnt/user-data/outputs/cgr-cheatsheet.pdf"
W, H = letter  # 612 x 792

# ─── Colors ────────────────────────────────────────────────────────────
BG        = HexColor("#FFFFFF")
FG        = HexColor("#1a1a1a")
MUTED     = HexColor("#5a5a5a")
ACCENT    = HexColor("#1B6B4A")  # teal-green
ACCENT_LT = HexColor("#E8F5EE")
BLUE      = HexColor("#185FA5")
BLUE_LT   = HexColor("#E6F1FB")
CORAL     = HexColor("#993C1D")
CORAL_LT  = HexColor("#FAECE7")
PURPLE    = HexColor("#534AB7")
PURPLE_LT = HexColor("#EEEDFE")
AMBER     = HexColor("#854F0B")
AMBER_LT  = HexColor("#FAEEDA")
GRAY_LT   = HexColor("#F4F3F0")
BORDER    = HexColor("#D0CFC9")

# ─── Fonts ─────────────────────────────────────────────────────────────
TITLE_SIZE    = 14
SECTION_SIZE  = 9
BODY_SIZE     = 7.2
CODE_SIZE     = 6.8
LABEL_SIZE    = 6.2
MONO          = "Courier"
MONO_B        = "Courier-Bold"
SANS          = "Helvetica"
SANS_B        = "Helvetica-Bold"

# ─── Layout ────────────────────────────────────────────────────────────
MARGIN   = 36
COL_GAP  = 16
COL_W    = (W - 2 * MARGIN - COL_GAP) / 2
LEFT_X   = MARGIN
RIGHT_X  = MARGIN + COL_W + COL_GAP


def rounded_rect(c, x, y, w, h, r=4, fill=None, stroke=None):
    """Draw a rounded rectangle."""
    c.saveState()
    if fill:
        c.setFillColor(fill)
    if stroke:
        c.setStrokeColor(stroke)
        c.setLineWidth(0.5)
    else:
        c.setStrokeColor(fill or GRAY_LT)
        c.setLineWidth(0)
    c.roundRect(x, y, w, h, r, fill=1 if fill else 0, stroke=1 if stroke else 0)
    c.restoreState()


def section_header(c, x, y, text, color=ACCENT, bg=ACCENT_LT, width=COL_W):
    """Draw a section header bar."""
    rounded_rect(c, x, y - 3, width, 14, r=3, fill=bg)
    c.setFont(SANS_B, SECTION_SIZE)
    c.setFillColor(color)
    c.drawString(x + 5, y, text)
    c.setFillColor(FG)
    return y - 17


def code_block(c, x, y, lines, width=COL_W, mono=True):
    """Draw a code block with background."""
    line_h = CODE_SIZE + 2.5
    block_h = len(lines) * line_h + 6
    rounded_rect(c, x, y - block_h + 4, width, block_h, r=3, fill=GRAY_LT)
    font = MONO if mono else SANS
    cy = y - 1
    for line in lines:
        c.setFont(font, CODE_SIZE)
        c.setFillColor(FG)
        # Syntax highlighting for keywords
        if mono and line.strip():
            _draw_highlighted(c, x + 5, cy, line)
        else:
            c.drawString(x + 5, cy, line)
        cy -= line_h
    c.setFillColor(FG)
    return y - block_h - 1


def _draw_highlighted(c, x, y, text):
    """Simple syntax highlighting for .cgr code."""
    keywords = {"first", "skip if", "run", "always run", "set", "using", "target",
                "verify", "parallel", "race", "each", "stage", "phase", "from",
                "as root", "retry", "if fails"}
    c.setFont(MONO, CODE_SIZE)
    # Simple: just draw the whole thing, color keywords
    parts = text.split()
    cursor = x
    for i, word in enumerate(parts):
        stripped = word.rstrip(":,")
        if stripped in keywords or word.startswith("$"):
            c.setFillColor(ACCENT)
        elif word.startswith("[") or word.startswith('"'):
            c.setFillColor(BLUE)
        elif word.startswith("${"):
            c.setFillColor(PURPLE)
        else:
            c.setFillColor(FG)
        c.drawString(cursor, y, word + " ")
        cursor += c.stringWidth(word + " ", MONO, CODE_SIZE)
    c.setFillColor(FG)


def label_line(c, x, y, label, desc, label_w=90, width=COL_W):
    """Draw a label: description line."""
    c.setFont(MONO_B, BODY_SIZE)
    c.setFillColor(ACCENT)
    c.drawString(x + 4, y, label)
    c.setFont(SANS, BODY_SIZE)
    c.setFillColor(MUTED)
    c.drawString(x + label_w, y, desc)
    c.setFillColor(FG)
    return y - (BODY_SIZE + 3)


def body_text(c, x, y, text, size=BODY_SIZE, color=FG):
    c.setFont(SANS, size)
    c.setFillColor(color)
    c.drawString(x + 4, y, text)
    c.setFillColor(FG)
    return y - (size + 3)


# ═══════════════════════════════════════════════════════════════════════
# BUILD THE CHEAT SHEET
# ═══════════════════════════════════════════════════════════════════════

c = canvas.Canvas(OUTPUT, pagesize=letter)

# ─── Title bar ─────────────────────────────────────────────────────────
rounded_rect(c, MARGIN, H - MARGIN - 28, W - 2*MARGIN, 28, r=5, fill=ACCENT)
c.setFont(SANS_B, TITLE_SIZE)
c.setFillColor(HexColor("#FFFFFF"))
c.drawString(MARGIN + 10, H - MARGIN - 22, "CommandGraph Cheat Sheet")
c.setFont(SANS, 8)
c.drawRightString(W - MARGIN - 10, H - MARGIN - 22, ".cgr readable format  ·  cgr  ·  Python 3.9+")
c.setFillColor(FG)

y_start = H - MARGIN - 48

# ═══════════════════════════════════════════════════════════════════════
# LEFT COLUMN
# ═══════════════════════════════════════════════════════════════════════
y = y_start

# ─── CLI Commands ──────────────────────────────────────────────────────
y = section_header(c, LEFT_X, y, "CLI COMMANDS", BLUE, BLUE_LT)
y = label_line(c, LEFT_X, y, "plan FILE", "Show execution order (add -v for commands)")
y = label_line(c, LEFT_X, y, "apply FILE", "Execute the graph (add -v for commands)")
y = label_line(c, LEFT_X, y, "apply FILE --dry-run", "Simulate without running anything")
y = label_line(c, LEFT_X, y, "apply FILE --no-resume", "Ignore state, run everything fresh")
y = label_line(c, LEFT_X, y, "validate FILE", "Check syntax and dependencies")
y = label_line(c, LEFT_X, y, "visualize FILE -o F.html", "Generate interactive HTML visualization")
y = label_line(c, LEFT_X, y, "dot FILE | dot -Tpng", "Graphviz DOT output")
y = label_line(c, LEFT_X, y, "repo index --repo DIR", "Scan and catalog templates")
y -= 3

# ─── State Commands ────────────────────────────────────────────────────
y = section_header(c, LEFT_X, y, "STATE COMMANDS", CORAL, CORAL_LT)
y = label_line(c, LEFT_X, y, "state show FILE", "Display state table: done/failed/pending")
y = label_line(c, LEFT_X, y, "state test FILE", "Re-run checks, detect drift, auto-fix")
y = label_line(c, LEFT_X, y, "state set FILE STEP done", "Force-mark a step as completed")
y = label_line(c, LEFT_X, y, "state set FILE STEP redo", "Force-mark a step for re-execution")
y = label_line(c, LEFT_X, y, "state drop FILE STEP", "Remove a step from state")
y = label_line(c, LEFT_X, y, "state reset FILE", "Delete the state file entirely")
y -= 3

# ─── File Structure ────────────────────────────────────────────────────
y = section_header(c, LEFT_X, y, ".cgr FILE STRUCTURE")
y = code_block(c, LEFT_X, y, [
    "--- Title ---",
    "",
    'set domain = "example.com"',
    "using apt/install_package, tls/certbot",
    "",
    'target "web-1" ssh deploy@10.0.1.5:',
    "",
    "  [step name] as root, timeout 2m:",
    "    first [dependency]",
    "    skip if $ test -f /etc/done",
    "    run    $ apt-get install -y nginx",
])
y -= 3

# ─── Step Keywords ─────────────────────────────────────────────────────
y = section_header(c, LEFT_X, y, "STEP BODY KEYWORDS")
y = label_line(c, LEFT_X, y, "first [name], [name]", "Dependencies (must complete first)", 108)
y = label_line(c, LEFT_X, y, "skip if $ cmd", "Skip if cmd exits 0 (idempotent)", 108)
y = label_line(c, LEFT_X, y, "run $ cmd", "The command to execute", 108)
y = label_line(c, LEFT_X, y, "always run $ cmd", "Run unconditionally (no skip)", 108)
y = label_line(c, LEFT_X, y, "env KEY = \"val\"", "Set environment variable", 108)
y = label_line(c, LEFT_X, y, 'when "expr == \'val\'"', "Conditional execution", 108)
y -= 3

# ─── Header Properties ────────────────────────────────────────────────
y = section_header(c, LEFT_X, y, "STEP HEADER PROPERTIES")
y = body_text(c, LEFT_X, y, 'Inline on the header line, comma-separated:', size=LABEL_SIZE, color=MUTED)
y = code_block(c, LEFT_X, y, [
    "[step] as root, timeout 3m, retry 2x wait 10s, if fails warn:",
])
y = label_line(c, LEFT_X, y, "as USER", "Run as this user (sudo)", 90)
y = label_line(c, LEFT_X, y, "timeout Ns / Nm", "Kill after N seconds/minutes (default 300s)", 90)
y = label_line(c, LEFT_X, y, "retry Nx wait Ns", "Retry N times, wait between", 90)
y = label_line(c, LEFT_X, y, "if fails stop|warn|ignore", "Failure policy (default: stop)", 90)
y -= 3

# ─── Templates ─────────────────────────────────────────────────────────
y = section_header(c, LEFT_X, y, "TEMPLATES")
y = code_block(c, LEFT_X, y, [
    "[install nginx] from apt/install_package:",
    '  name = "nginx"',
    "",
    "[get cert] from tls/certbot:",
    '  domain = "${domain}"',
])
y = body_text(c, LEFT_X, y, 'You name it in [brackets], reference by that exact name.', size=LABEL_SIZE, color=MUTED)
y -= 3

# ─── Common Check Recipes ──────────────────────────────────────────────
y = section_header(c, LEFT_X, y, "IDEMPOTENT CHECK RECIPES")
y = label_line(c, LEFT_X, y, "Package installed", "dpkg -l PKG | grep -q '^ii'", 90)
y = label_line(c, LEFT_X, y, "Command exists", "command -v BIN >/dev/null 2>&1", 90)
y = label_line(c, LEFT_X, y, "File exists", "test -f /path/to/file", 90)
y = label_line(c, LEFT_X, y, "Directory exists", "test -d /path/to/dir", 90)
y = label_line(c, LEFT_X, y, "Service running", "systemctl is-active SVC | grep -q active", 90)
y = label_line(c, LEFT_X, y, "Port open (ufw)", "ufw status | grep -q 'PORT/tcp.*ALLOW'", 90)
y = label_line(c, LEFT_X, y, "User exists", "id USERNAME >/dev/null 2>&1", 90)
y = label_line(c, LEFT_X, y, "Force always run", "false  (as the check command)", 90)


# ═══════════════════════════════════════════════════════════════════════
# RIGHT COLUMN
# ═══════════════════════════════════════════════════════════════════════
y = y_start

# ─── Parallel ──────────────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "PARALLEL — fork/join", PURPLE, PURPLE_LT)
y = code_block(c, RIGHT_X, y, [
    "parallel:",
    "  [branch a]: run $ cmd-a",
    "  [branch b]: run $ cmd-b",
    "",
    "parallel 3 at a time:",
    "  [deploy host-1]: run $ deploy.sh",
    "  [deploy host-2]: run $ deploy.sh",
    "  ...  # runs 3 concurrently, slides",
])
y = body_text(c, RIGHT_X, y, "Failure: if one fails wait for rest | stop all | ignore", size=LABEL_SIZE, color=MUTED)
y -= 3

# ─── Race ──────────────────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "RACE — first to succeed wins", PURPLE, PURPLE_LT)
y = code_block(c, RIGHT_X, y, [
    "race:",
    "  [try mirror a]: run $ curl -sf https://a.example/pkg",
    "  [try mirror b]: run $ curl -sf https://b.example/pkg",
    "  # first SUCCESS wins, rest cancelled",
])
y -= 3

# ─── Each ──────────────────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "EACH — parallel iteration", PURPLE, PURPLE_LT)
y = code_block(c, RIGHT_X, y, [
    'set servers = "web-1,web-2,web-3,web-4"',
    "",
    "each server in ${servers}, 2 at a time:",
    "  [deploy to ${server}]:",
    "    run $ ssh ${server} deploy.sh",
])
y -= 3

# ─── Stage/Phase ───────────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "STAGE / PHASE — rolling deploys", PURPLE, PURPLE_LT)
y = code_block(c, RIGHT_X, y, [
    'stage "production rollout":',
    '  phase "canary" 1 from ${servers}:',
    "    [deploy ${server}]: run $ activate.sh",
    '    verify "healthy": run $ curl http://${server}/health',
    "      retry 10x wait 3s",
    "",
    '  phase "50%" 50% from ${servers}:',
    "    each server, 2 at a time:",
    "      [deploy ${server}]: run $ activate.sh",
    "",
    '  phase "rest" remaining from ${servers}:',
    "    each server, 4 at a time:",
    "      [deploy ${server}]: run $ activate.sh",
])
y = body_text(c, RIGHT_X, y, "Count: N (exact), N% (percent), rest/remaining (all left)", size=LABEL_SIZE, color=MUTED)
y -= 3

# ─── State Tracking ───────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "STATE & CRASH RECOVERY", AMBER, AMBER_LT)
y = body_text(c, RIGHT_X, y, "State file: .state/FILE.cgr.state (JSON Lines, append-only, crash-safe)", size=BODY_SIZE, color=FG)
y -= 2
y = body_text(c, RIGHT_X, y, "Resume matrix — what happens on re-run:", size=LABEL_SIZE, color=MUTED)
y = label_line(c, RIGHT_X, y, "success / skip_check", "Skip (no SSH, instant)", 108)
y = label_line(c, RIGHT_X, y, "warned / failed", "Re-run", 108)
y = label_line(c, RIGHT_X, y, "not in state", "Run (never attempted)", 108)
y = label_line(c, RIGHT_X, y, "cancelled (race)", "Re-run (race retries)", 108)
y -= 3

# ─── Verify ───────────────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "VERIFY — smoke tests")
y = code_block(c, RIGHT_X, y, [
    'verify "HTTPS on ${domain}":',
    "  first [start nginx], [install curl]",
    "  run $ curl -sfk https://${domain}/",
    "  retry 3x wait 2s",
])
y = body_text(c, RIGHT_X, y, "Always runs (no skip if). Default: if fails warn.", size=LABEL_SIZE, color=MUTED)
y -= 3

# ─── Target Formats ───────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "TARGET (via SSH or local)")
y = code_block(c, RIGHT_X, y, [
    'target "web-1" ssh deploy@10.0.1.5:',
    'target "db" ssh root@10.0.2.3 port 5422:',
    'target "local" local:',
])
y -= 3

# ─── Stdlib Templates ─────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "STANDARD LIBRARY TEMPLATES")
y = label_line(c, RIGHT_X, y, "apt/install_package", "(name, version=\"latest\")", 108)
y = label_line(c, RIGHT_X, y, "apt/add_repo", "(repo_name, repo_line, key_url)", 108)
y = label_line(c, RIGHT_X, y, "firewall/allow_port", "(port, proto=\"tcp\")", 108)
y = label_line(c, RIGHT_X, y, "systemd/enable_service", "(service)", 108)
y = label_line(c, RIGHT_X, y, "tls/certbot", "(domain, email=\"admin@${domain}\")", 108)
y = label_line(c, RIGHT_X, y, "nginx/vhost", "(domain, port, doc_root, ssl)", 108)
y = label_line(c, RIGHT_X, y, "docker/compose_up", "(project_dir, compose_file)", 108)
y -= 3

# ─── Quick Patterns ───────────────────────────────────────────────────
y = section_header(c, RIGHT_X, y, "COMMON PATTERNS")
y = label_line(c, RIGHT_X, y, "Fan-out", "Steps with same first → parallel in wave", 108)
y = label_line(c, RIGHT_X, y, "Fan-in", "Step with first [a], [b], [c] → waits for all", 108)
y = label_line(c, RIGHT_X, y, "Sequential", "Chain: first [a] → first [b] → first [c]", 108)
y = label_line(c, RIGHT_X, y, "Nested child", "Indent child inside parent → implicit dep", 108)

# ─── Footer ────────────────────────────────────────────────────────────
c.setFont(SANS, 6)
c.setFillColor(MUTED)
c.drawString(MARGIN, 18, "CommandGraph DSL  ·  cgr  ·  Zero dependencies  ·  Python 3.9+")
c.drawRightString(W - MARGIN, 18, ".cg (structured) and .cgr (readable) formats  ·  All commands: cgr --help")

c.save()
print(f"✓ Cheat sheet written to {OUTPUT}")
