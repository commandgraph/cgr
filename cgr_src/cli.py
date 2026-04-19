"""CLI loading, argument parsing, and program entrypoint."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.lexer import *
from cgr_src.ast_nodes import *
from cgr_src.parser_cg import *
from cgr_src.parser_cgr import *
from cgr_src.repo import *
from cgr_src.resolver import *
from cgr_src.executor import *
from cgr_src.state import *
from cgr_src.commands import *
from cgr_src.dot import *
from cgr_src.visualize import *
from cgr_src.serve import *

def _parse_inventory(path: str) -> dict[str,str]:
    """Parse an Ansible-style INI inventory file into flat variables.

    Format:
        [group_name]
        hostname  host=ADDRESS  user=USER  port=PORT
        hostname2 host=ADDRESS2 user=USER2

        [group_name:vars]
        key = value

    Produces:
        group_name = "host1:user1@addr1,host2:user2@addr2"   (list for each)
        key = "value"                                         (group vars)
    """
    vs: dict[str,str] = {}
    groups: dict[str, list[dict[str,str]]] = {}  # group → list of host dicts
    current_group: str|None = None
    current_is_vars = False

    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or line.startswith(';'):
            continue

        # Section header: [group] or [group:vars]
        section_m = re.match(r'^\[([^\]:]+)(?::vars)?\]\s*$', line)
        if section_m:
            current_group = section_m.group(1)
            current_is_vars = ':vars]' in raw_line
            if current_group not in groups:
                groups[current_group] = []
            continue

        if current_group is None:
            continue

        if current_is_vars:
            # Group vars: key = value or key=value
            m = re.match(r'(\w+)\s*=\s*"?([^"]*?)"?\s*$', line)
            if m:
                vs[m.group(1)] = m.group(2)
            continue

        # Host line: name  key=value key=value ...
        parts = line.split()
        if not parts:
            continue
        hostname = parts[0]
        host_props: dict[str,str] = {'name': hostname}
        for part in parts[1:]:
            if '=' in part:
                k, v = part.split('=', 1)
                # Map ansible_host → host, ansible_user → user, ansible_port → port
                k = k.replace('ansible_', '')
                host_props[k.strip()] = v.strip().strip('"').strip("'")
        groups[current_group].append(host_props)

    # Build list variables for each group
    for group_name, hosts in groups.items():
        entries = []
        for h in hosts:
            name = h.get('name', '')
            user = h.get('user', '')
            host = h.get('host', name)  # default to hostname if no host= given
            port = h.get('port', '')
            if user:
                addr = f"{user}@{host}"
            else:
                addr = host
            if port and port != '22':
                addr += f":{port}"
            entries.append(f"{name}:{addr}")
        vs[group_name] = ",".join(entries)

    return vs


def _parse_vars_file(path: str) -> dict[str,str]:
    """Parse a .vars file: KEY = "value" or KEY = value, one per line."""
    vs: dict[str,str] = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        m = re.match(r'(\w+)\s*=\s*"([^"]*)"', line)
        if not m:
            m = re.match(r'(\w+)\s*=\s*(\S+)', line)
        if m: vs[m.group(1)] = m.group(2)
    return vs

def _parse_extra_vars(set_flags: list[str]|None, vars_file: str|None) -> dict[str,str]:
    """Build the extra_vars dict from --set flags and --vars-file."""
    extra: dict[str,str] = {}
    # Vars file first (lower precedence)
    if vars_file:
        p = Path(vars_file)
        if not p.exists():
            print(red(f"error: vars file not found: {vars_file}"), file=sys.stderr); sys.exit(1)
        extra.update(_parse_vars_file(vars_file))
    # CLI --set flags override vars file
    for s in (set_flags or []):
        if '=' not in s:
            print(red(f"error: --set requires KEY=VALUE format, got: {s}"), file=sys.stderr); sys.exit(1)
        k, v = s.split('=', 1)
        # Strip optional quotes from value
        v = v.strip().strip('"').strip("'")
        extra[k.strip()] = v
    return extra

def _auto_detect_graph() -> str:
    """Find a single .cgr or .cg file in the current directory."""
    candidates = sorted(Path(".").glob("*.cgr")) + sorted(Path(".").glob("*.cg"))
    if len(candidates) == 0:
        print(red("error: no .cgr or .cg file found in current directory."), file=sys.stderr)
        print("  Specify a file: cgr apply myfile.cgr", file=sys.stderr)
        sys.exit(1)
    if len(candidates) == 1:
        print(dim(f"  (auto-detected: {candidates[0]})"), file=sys.stderr)
        return str(candidates[0])
    # Multiple candidates — prefer .cgr over .cg, but if ambiguous, error
    cgr_files = [c for c in candidates if c.suffix == ".cgr"]
    if len(cgr_files) == 1:
        print(dim(f"  (auto-detected: {cgr_files[0]})"), file=sys.stderr)
        return str(cgr_files[0])
    print(red(f"error: multiple graph files found: {', '.join(str(c) for c in candidates)}"), file=sys.stderr)
    print("  Specify which one: cgr apply <file>", file=sys.stderr)
    sys.exit(1)


def _print_completion(shell: str):
    """Print shell completion script to stdout."""
    commands = "plan apply validate dot visualize state repo report serve check ping explain why convert fmt lint init secrets diff doctor version completion"
    if shell == "bash":
        print(textwrap.dedent(f'''\
            _cgr_completions() {{
                local cur prev commands
                cur="${{COMP_WORDS[COMP_CWORD]}}"
                prev="${{COMP_WORDS[COMP_CWORD-1]}}"
                commands="{commands}"

                if [ "$COMP_CWORD" -eq 1 ]; then
                    COMPREPLY=( $(compgen -W "$commands" -- "$cur") )
                    return
                fi

                case "$prev" in
                    --repo|--vars-file|--vault-pass-file|--log|-o|--output)
                        COMPREPLY=( $(compgen -f -- "$cur") )
                        return ;;
                    -i|--inventory)
                        COMPREPLY=( $(compgen -f -X '!*.ini' -- "$cur") )
                        return ;;
                    --tags|--skip-tags)
                        return ;;
                    --format)
                        COMPREPLY=( $(compgen -W "table json csv" -- "$cur") )
                        return ;;
                    state)
                        COMPREPLY=( $(compgen -W "show test reset set drop diff compact" -- "$cur") )
                        return ;;
                    repo)
                        COMPREPLY=( $(compgen -W "index" -- "$cur") )
                        return ;;
                    secrets)
                        COMPREPLY=( $(compgen -W "create edit view add rm" -- "$cur") )
                        return ;;
                    completion)
                        COMPREPLY=( $(compgen -W "bash zsh fish" -- "$cur") )
                        return ;;
                esac

                if [[ "$cur" == -* ]]; then
                    local opts="--repo --set --vars-file -v --verbose -i --inventory --help"
                    case "${{COMP_WORDS[1]}}" in
                        apply) opts="$opts --dry-run --parallel --progress --report --no-color --no-resume --no-state --start-from --timeout --log --on-complete --blast-radius --tags --skip-tags --vault-pass --vault-pass-ask --vault-pass-file --skip -A -K --ask-become-pass" ;;
                        plan) opts="$opts --tags --skip-tags --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        validate) opts="$opts -q --quiet --json --no-color --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        check) opts="$opts --json --parallel --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        dot|visualize|ping|why|explain|lint|diff) opts="$opts --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        visualize) opts="$opts -o --output --state" ;;
                        serve) opts="$opts --port --host --no-open" ;;
                        diff) opts="$opts --json" ;;
                        report) opts="$opts --format -o --output --keys --run-id" ;;
                    esac
                    COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
                    return
                fi

                COMPREPLY=( $(compgen -f -X '!*.cg?(r)' -- "$cur") )
            }}
            complete -F _cgr_completions cgr'''))
    elif shell == "zsh":
        print(textwrap.dedent(f'''\
            #compdef cgr
            _cgr() {{
                local -a commands
                commands=(
                    'plan:Show execution order'
                    'apply:Execute the graph'
                    'validate:Check syntax and dependencies'
                    'dot:Generate Graphviz DOT'
                    'visualize:Generate interactive HTML'
                    'state:Inspect/modify execution state'
                    'repo:Template repository operations'
                    'report:View collected outputs'
                    'serve:Start web IDE'
                    'check:Run checks for drift detection'
                    'ping:Verify SSH connectivity'
                    'explain:Show dependency chain'
                    'why:Show reverse dependency chain'
                    'convert:Convert .cg/.cgr'
                    'fmt:Auto-format .cgr file'
                    'lint:Check best practices'
                    'init:Scaffold new .cgr'
                    'secrets:Manage encrypted secrets'
                    'diff:Compare two graphs'
                    'doctor:Check environment'
                    'version:Show version info'
                    'completion:Generate shell completions'
                )
                _arguments '1:command:((${{commands}}))'  '*:file:_files -g "*.cg(r|)"'
            }}
            _cgr'''))
    elif shell == "fish":
        subcmds = commands.split()
        print("# Fish completions for cgr")
        for cmd in subcmds:
            print(f"complete -c cgr -n '__fish_use_subcommand' -a {cmd}")
        print("complete -c cgr -F  # file completion for arguments")


def _load(filepath, repo_dir=None, extra_vars=None, raise_on_error=False, inventory_files=None,
          vault_passphrase=None, vault_prompt=False, resolve_deferred_secrets=False):
    path=Path(filepath)
    if not path.exists():
        if raise_on_error: raise ValueError(f"not found: {path}")
        print(red(f"error: not found: {path}"),file=sys.stderr); sys.exit(1)
    source=path.read_text()
    include_base_dir = path.resolve().parent

    # Detect format by extension
    if path.suffix == ".cgr":
        try: ast=parse_cgr(source, str(path), include_base_dir)
        except CGRParseError as e:
            if raise_on_error: raise ValueError(e.msg) from e
            print(e.pretty(),file=sys.stderr); sys.exit(1)
    else:
        try: tokens=lex(source,str(path))
        except LexError as e:
            if raise_on_error: raise ValueError(e.msg) from e
            print(red(f"Lex error: {e.msg}"),file=sys.stderr)
            if e.src: print(dim(f"  {e.line:>4} │ ")+e.src,file=sys.stderr)
            sys.exit(1)
        try: ast=Parser(tokens,source,str(path),include_base_dir).parse()
        except ParseError as e:
            if raise_on_error: raise ValueError(e.msg) from e
            print(e.pretty(),file=sys.stderr); sys.exit(1)

    try:
        graph = resolve(ast, repo_dir=repo_dir, graph_file=str(path), extra_vars=extra_vars,
                        inventory_files=inventory_files, vault_passphrase=vault_passphrase,
                        vault_prompt=vault_prompt,
                        resolve_deferred_secrets=resolve_deferred_secrets)
        _validate_script_paths(graph)
        return graph
    except ResolveError as e:
        if raise_on_error: raise ValueError(str(e)) from e
        print(red(f"error: {e}"),file=sys.stderr); sys.exit(1)

def main():
    p=argparse.ArgumentParser(prog="cgr",
        description="cgr (CommandGraph) DSL v3 — Nested deps, templates, repository, state tracking",
        epilog="Examples:\n  cgr plan   infra.cgr --repo ./repo\n  cgr apply  infra.cgr --repo ./repo --dry-run\n  cgr state show infra.cgr\n  cgr state test infra.cgr --repo ./repo\n  cgr repo index --repo ./repo\n",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub=p.add_subparsers(dest="command",required=True)

    sp=sub.add_parser("plan",help="Show execution order without running anything")
    sp.add_argument("file",nargs="?",default=None,help="Graph file (.cg or .cgr) — auto-detected if omitted")
    sp.add_argument("--repo",help="Template repository directory")
    sp.add_argument("-v","--verbose",action="store_true",help="Show check/run commands for each step")
    sp.add_argument("--tags",default=None,metavar="T1,T2",help="Only include steps with these tags (comma-separated)")
    sp.add_argument("--skip-tags",default=None,metavar="T1,T2",help="Exclude steps with these tags (comma-separated)")
    sp.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sp.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sp.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    sp.add_argument("--allow-secret-cmd",action="store_true",
                    help="Resolve deferred secret cmd:/vault: variables during planning")
    _add_vault_pass_args(sp)

    sa=sub.add_parser("apply",help="Execute the graph")
    sa.add_argument("file",nargs="?",default=None,help="Graph file (.cg or .cgr) — auto-detected if omitted")
    sa.add_argument("--repo",help="Template repository directory")
    sa.add_argument("--dry-run",action="store_true",help="Resolve and plan without executing anything")
    sa.add_argument("--parallel",type=int,default=4,help="Max concurrent steps per wave (default: 4)")
    sa.add_argument("--progress",action="store_true",help="Show step start/progress updates during apply")
    sa.add_argument("--report",metavar="FILE",help="Write JSON execution report to FILE")
    sa.add_argument("--output",choices=["text","json"],default="text",help="Apply output format (default: text)")
    sa.add_argument("--no-color",action="store_true",help="Disable colored output")
    sa.add_argument("--no-resume","--no-state",action="store_true",dest="no_resume",help="Ignore state file, run everything fresh")
    sa.add_argument("--run-id",metavar="ID",help="Salt the default state file path for this apply run")
    sa.add_argument("--state",dest="apply_state",metavar="FILE",help="Use an explicit state file for this apply run")
    sa.add_argument("--start-from",metavar="STEP",help="Skip all steps before STEP (by name or slug)")
    sa.add_argument("--timeout",type=_parse_duration_str,default=None,metavar="DURATION",help="Abort entire run after DURATION (e.g. 300, 300s, 5m, 2h)")
    sa.add_argument("-v","--verbose",action="store_true",help="Show check/run commands and output for every step")
    sa.add_argument("--log",metavar="FILE",help="Write structured JSON Lines log to FILE")
    sa.add_argument("--on-complete",metavar="CMD",help="Run CMD after apply finishes (shell command)")
    sa.add_argument("-K","--ask-become-pass",action="store_true",help="Prompt for sudo password")
    sa.add_argument("--blast-radius",action="store_true",help="During --dry-run, run check clauses to show real blast radius")
    sa.add_argument("--check-mode",action="store_true",help="Run check clauses only — show what would execute without running (alias for --dry-run --blast-radius)")
    sa.add_argument("--tags",default=None,metavar="T1,T2",help="Only run steps with these tags (comma-separated)")
    sa.add_argument("--skip-tags",default=None,metavar="T1,T2",help="Skip steps with these tags (comma-separated)")
    _add_vault_pass_args(sa)
    sa.add_argument("--confirm",action="store_true",help="Pause and ask for confirmation before each wave")
    sa.add_argument("--skip",action="append",default=[],metavar="STEP",help="Skip a specific step (repeatable, by name or slug)")
    sa.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sa.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sa.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")

    sv=sub.add_parser("validate",help="Check syntax and dependency resolution")
    sv.add_argument("file",nargs="?",default=None,help="Graph file (.cg or .cgr) — auto-detected if omitted")
    sv.add_argument("--repo",help="Template repository directory")
    sv.add_argument("--no-color",action="store_true",help="Disable colored output")
    sv.add_argument("-q","--quiet",action="store_true",help="Exit 0/1 only, no output (for CI/CD)")
    sv.add_argument("--json",action="store_true",help="Machine-readable JSON output (for CI/CD)")
    sv.add_argument("--strict",action="store_true",help="Treat selected lint warnings as errors")
    sv.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sv.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sv.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    sv.add_argument("--allow-secret-cmd",action="store_true",
                    help="Resolve deferred secret cmd:/vault: variables during validation")
    _add_vault_pass_args(sv)

    sd=sub.add_parser("dot",help="Generate Graphviz DOT output")
    sd.add_argument("file",help="Graph file (.cg or .cgr)")
    sd.add_argument("--repo",help="Template repository directory")
    sd.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sd.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sd.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(sd)

    sw=sub.add_parser("visualize",help="Generate interactive HTML visualization")
    sw.add_argument("file",help="Graph file (.cg or .cgr)"); sw.add_argument("--repo",help="Template repository directory")
    sw.add_argument("-o","--output",default=None,help="Output HTML file (default: <input>.html)")
    sw.add_argument("--state",default=None,metavar="FILE",help="Overlay execution state from a .state file")
    sw.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sw.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sw.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(sw)

    ss=sub.add_parser("state",help="Inspect and modify execution state")
    ss.add_argument("action",choices=["show","test","reset","set","drop","diff","compact"])
    ss.add_argument("file")
    ss.add_argument("step",nargs="?",default=None,help="Step name (for set/drop) or second state file (for diff)")
    ss.add_argument("value",nargs="?",default=None,help="'done' or 'redo' (for set)")
    ss.add_argument("--repo",default=None)
    ss.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    ss.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    ss.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(ss)

    sr=sub.add_parser("repo"); sr.add_argument("action",choices=["index"]); sr.add_argument("--repo")

    rpt=sub.add_parser("report",help="View or export collected outputs from a run")
    rpt.add_argument("file",help="Graph file (.cg or .cgr)")
    rpt.add_argument("--format",choices=["table","json","csv"],default="table",help="Output format (default: table)")
    rpt.add_argument("-o","--output",metavar="FILE",help="Write report to file instead of stdout")
    rpt.add_argument("--keys",help="Comma-separated list of collect keys to include")
    rpt.add_argument("--run-id",metavar="ID",help="Read collected output for a specific apply run-id")

    sc=sub.add_parser("serve",help="Start the web IDE (editor + live visualization)")
    sc.add_argument("file",nargs="?",default=None,help="Graph file to open (.cg or .cgr)")
    sc.add_argument("--port",type=int,default=8420,help="Server port (default: 8420)")
    sc.add_argument("--host",default="127.0.0.1",help="Bind address (default: 127.0.0.1)")
    sc.add_argument("--unsafe-host",action="store_true",
                    help="Allow binding the IDE server to a non-loopback host")
    sc.add_argument("--allow-host",action="append",default=[],metavar="HOST",
                    help="Allow an additional Host header for reverse proxies (repeatable)")
    sc.add_argument("--repo",help="Template repository directory")
    sc.add_argument("--no-open",action="store_true",help="Don't auto-open browser (for headless/remote environments)")

    sch=sub.add_parser("check",help="Run check clauses to detect what needs to run (live drift detection)")
    sch.add_argument("file",help="Graph file (.cg or .cgr)")
    sch.add_argument("--repo",help="Template repository directory")
    sch.add_argument("-v","--verbose",action="store_true",help="Show stderr from failed checks")
    sch.add_argument("--json",action="store_true",help="Output results as JSON (for CI/CD)")
    sch.add_argument("--parallel",type=int,default=4,metavar="N",help="Max parallel checks per wave (default: 4)")
    sch.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sch.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sch.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(sch)

    spg=sub.add_parser("ping",help="Verify SSH connectivity to all targets")
    spg.add_argument("file",help="Graph file (.cg or .cgr)")
    spg.add_argument("--repo",help="Template repository directory")
    spg.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    spg.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    spg.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(spg)

    swy=sub.add_parser("why",help="Show what depends on a specific step (reverse of explain)")
    swy.add_argument("file",help="Graph file (.cg or .cgr)")
    swy.add_argument("step",help="Step name to query")
    swy.add_argument("--repo",help="Template repository directory")
    swy.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    swy.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    swy.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(swy)

    sex=sub.add_parser("explain",help="Show dependency chain for a specific step")
    sex.add_argument("file",help="Graph file (.cg or .cgr)")
    sex.add_argument("step",help="Step name to explain")
    sex.add_argument("--repo",help="Template repository directory")
    sex.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sex.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sex.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(sex)

    scv=sub.add_parser("convert",help="Convert between .cg and .cgr formats")
    scv.add_argument("file",help="Input file (.cg or .cgr)")
    scv.add_argument("-o","--output",default=None,help="Output file (default: same name, opposite extension)")

    sfm=sub.add_parser("fmt",help="Auto-format a .cgr file")
    sfm.add_argument("file",help=".cgr file to format")

    slt=sub.add_parser("lint",help="Check for best-practice issues")
    slt.add_argument("file",help="Graph file (.cg or .cgr)")
    slt.add_argument("--repo",help="Template repository directory")
    slt.add_argument("--strict",action="store_true",help="Treat selected lint warnings as errors")
    slt.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    slt.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    slt.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(slt)

    sin=sub.add_parser("init",help="Create a new .cgr scaffold file")
    sin.add_argument("file",nargs="?",default=None,help="Output file (default: setup.cgr)")

    ssc=sub.add_parser("secrets",help="Manage encrypted secrets files")
    ssc.add_argument("action",choices=["create","edit","view","add","rm","audit"])
    ssc.add_argument("file",help="Secrets file path")
    ssc.add_argument("key",nargs="?",default=None,help="Key name (for add/rm)")
    ssc.add_argument("value",nargs="?",default=None,help=argparse.SUPPRESS)
    _add_vault_pass_args(ssc)
    ssc.add_argument("--max-age",default=None,metavar="DAYS",help="Max secret age in days for audit (default: 90)")
    ssc.add_argument("--show-values", action="store_true",
                     help="Deprecated compatibility flag; `view` output is always masked")

    sdf=sub.add_parser("diff",help="Compare two graph files structurally")
    sdf.add_argument("file",help="First graph file (.cg or .cgr)")
    sdf.add_argument("file2",help="Second graph file (.cg or .cgr)")
    sdf.add_argument("--repo",help="Template repository directory")
    sdf.add_argument("--json",action="store_true",help="Machine-readable JSON output")
    sdf.add_argument("--set",action="append",default=[],metavar="KEY=VALUE",help="Override a variable (repeatable)")
    sdf.add_argument("--vars-file",default=None,metavar="FILE",help="Load variable overrides from a file")
    sdf.add_argument("-i","--inventory",action="append",default=[],metavar="FILE",help="Inventory file (repeatable)")
    _add_vault_pass_args(sdf)

    stt=sub.add_parser("test",help="Test templates: parse, resolve, and validate")
    stt.add_argument("template",nargs="?",default=None,help="Template path to test (e.g. apt/install_package). Tests all if omitted")
    stt.add_argument("--repo",help="Template repository directory")
    stt.add_argument("-v","--verbose",action="store_true",help="Show details for each template")

    sub.add_parser("doctor",help="Check environment for common issues")
    sub.add_parser("version",help="Show version information")

    # ── How ──────────────────────────────────────────────────────────
    shw=sub.add_parser("how",help="Show documentation and variables for a graph file")
    shw.add_argument("file",nargs="?",default=None,help="Graph file (.cgr or .cg)")

    # ── Shell completion generator ──────────────────────────────────────
    scmp=sub.add_parser("completion",help="Generate shell completion script")
    scmp.add_argument("shell",choices=["bash","zsh","fish"],help="Shell type")

    args=p.parse_args()
    if getattr(args,"no_color",False): global _COLOR; _COLOR=False

    if args.command=="completion":
        _print_completion(args.shell); return

    if args.command=="version":
        print(f"cgr (CommandGraph) {__version__}"); print(f"Python {sys.version.split()[0]}"); return

    if args.command=="doctor":
        cmd_doctor(); return

    if args.command=="test":
        cmd_test_template(repo_dir=getattr(args,"repo",None), template_path=args.template, verbose=args.verbose); return

    if args.command=="init":
        cmd_init(args.file); return

    # ── Auto-detect graph file if not provided ────────────────────────
    if hasattr(args, "file") and args.file is None and args.command not in ("serve", "init", "repo", "completion", "version", "doctor", "test", "how"):
        args.file = _auto_detect_graph()

    if args.command=="convert":
        cmd_convert(args.file, output_file=args.output); return

    if args.command=="diff":
        extra = _parse_extra_vars(getattr(args,"set",[]), getattr(args,"vars_file",None))
        inv_files = getattr(args,"inventory",[]) or []
        vault_passphrase, vault_prompt = _resolve_vault_pass_source(args)
        rc = cmd_diff(args.file, args.file2, repo_dir=getattr(args,"repo",None),
                      extra_vars=extra, inventory_files=inv_files,
                      json_output=getattr(args,"json",False),
                      vault_passphrase=vault_passphrase, vault_prompt=vault_prompt)
        sys.exit(rc)

    if args.command=="fmt":
        cmd_fmt(args.file); return

    if args.command=="how":
        cmd_how(args.file); return

    if args.command=="secrets":
        cmd_secrets(args.action, args.file, key=args.key, value=args.value, vault_args=args); return

    if args.command=="serve":
        cmd_serve(filepath=args.file, port=args.port, host=args.host,
                  repo_dir=getattr(args,"repo",None), no_open=args.no_open,
                  unsafe_host=getattr(args,"unsafe_host",False),
                  allow_hosts=getattr(args,"allow_host",[])); return

    if args.command=="report":
        fk = args.keys.split(",") if args.keys else None
        cmd_report(args.file, fmt=args.format, output_file=args.output,
                   filter_keys=fk, run_id=getattr(args, "run_id", None))
        return

    if args.command=="repo":
        cmd_repo_index(args.repo or DEFAULT_REPO); return

    if args.command=="state":
        # diff doesn't need graph loading — short-circuit
        if args.action=="diff":
            if not args.step:
                print(red("Usage: cgr state diff FILE1.state FILE2.state")); sys.exit(1)
            file_a = args.file if args.file.endswith(".state") else _state_path(args.file)
            file_b = args.step if args.step.endswith(".state") else _state_path(args.step)
            if not Path(file_a).exists():
                print(red(f"error: state file not found: {file_a}")); sys.exit(1)
            if not Path(file_b).exists():
                print(red(f"error: state file not found: {file_b}")); sys.exit(1)
            cmd_state_diff(file_a, file_b)
            return

        repo=getattr(args,"repo",None)
        graph=None
        vault_passphrase, vault_prompt = _resolve_vault_pass_source(args)
        extra = _parse_extra_vars(getattr(args,"set",[]), getattr(args,"vars_file",None))
        inv_files = getattr(args,"inventory",[]) or []
        need_graph_if_present = {"show", "reset", "set", "drop", "compact"}
        if args.action in need_graph_if_present and not Path(args.file).exists():
            graph = None
        else:
            try:
                graph=_load(args.file, repo_dir=repo, extra_vars=extra, inventory_files=inv_files,
                            vault_passphrase=vault_passphrase, vault_prompt=vault_prompt)
            except SystemExit:
                pass  # graph may not parse for show/reset

        if graph and graph.stateless and args.action in ("test", "show", "set", "drop", "compact"):
            print(red(f"error: 'state {args.action}' is not available for stateless graphs (set stateless = true)"))
            sys.exit(1)

        if args.action=="show":
            cmd_state_show(args.file, graph)
        elif args.action=="test":
            if not graph:
                print(red("error: graph file must be valid to run state test")); sys.exit(1)
            cmd_state_test(args.file, graph, repo_dir=repo)
        elif args.action=="reset":
            cmd_state_reset(args.file)
        elif args.action=="set":
            if not args.step or not args.value:
                print(red("Usage: cgr state set FILE STEP done|redo")); sys.exit(1)
            cmd_state_set(args.file, args.step, args.value, graph)
        elif args.action=="drop":
            if not args.step:
                print(red("Usage: cgr state drop FILE STEP")); sys.exit(1)
            cmd_state_drop(args.file, args.step, graph)
        elif args.action=="compact":
            sp = _state_path(args.file)
            if not Path(sp).exists():
                print(dim("  No state file to compact.")); return
            sf = StateFile(sp)
            removed = sf.compact()
            entries = len(sf.all_entries())
            print(green(f"  ✓ State compacted: {entries} entries, {removed} redundant line(s) removed."))
            print(f"  {dim(sp)}")
        return

    repo=getattr(args,"repo",None)
    extra = _parse_extra_vars(getattr(args,"set",[]), getattr(args,"vars_file",None))
    inv_files = getattr(args,"inventory",[]) or []
    vault_passphrase, vault_prompt = _resolve_vault_pass_source(args)
    resolve_deferred_secrets = (
        args.command == "apply"
        or (args.command in ("validate", "plan") and getattr(args, "allow_secret_cmd", False))
    )

    # For validate --json, catch parse/resolve errors and output JSON
    if args.command == "validate" and getattr(args, "json", False):
        try:
            graph = _load(args.file, repo_dir=repo, extra_vars=extra, raise_on_error=True,
                          inventory_files=inv_files, vault_passphrase=vault_passphrase,
                          vault_prompt=vault_prompt,
                          resolve_deferred_secrets=resolve_deferred_secrets)
        except ValueError as e:
            print(json.dumps({"valid": False, "file": args.file, "error": str(e)}, indent=2))
            sys.exit(1)
    else:
        graph=_load(args.file, repo_dir=repo, extra_vars=extra, inventory_files=inv_files,
                    vault_passphrase=vault_passphrase, vault_prompt=vault_prompt,
                    resolve_deferred_secrets=resolve_deferred_secrets)

    if args.command=="validate":
        if getattr(args,"strict",False):
            lint_warnings, lint_errors = _collect_lint_findings(graph, Path(args.file), strict=True)
            if getattr(args,"quiet",False):
                sys.exit(1 if lint_errors else 0)
        if getattr(args,"quiet",False):
            return  # _load already exited 1 on failure; reaching here means valid
        redacted_variables = _redact_variable_map(graph.variables, graph.sensitive_values)
        if getattr(args,"json",False):
            # Machine-readable JSON output
            tot=sum(len(w) for w in graph.waves)
            deduped=sum(1 for ids in graph.dedup_map.values() if len(ids)>1)
            xnode_deps = []
            for rid, res in graph.all_resources.items():
                for dep in res.needs:
                    if dep in graph.all_resources:
                        dep_res = graph.all_resources[dep]
                        if dep_res.node_name != res.node_name and not dep_res.is_barrier:
                            xnode_deps.append({"from": f"{res.node_name}.{res.short_name}",
                                               "to": f"{dep_res.node_name}.{dep_res.short_name}"})
            nodes_info = {}
            for nn, hn in graph.nodes.items():
                nodes_info[nn] = {
                    "via": hn.via_method,
                    "host": hn.via_props.get("host", ""),
                    "user": hn.via_props.get("user", ""),
                    "port": hn.via_props.get("port", "22"),
                    "resources": [r.short_name for r in hn.resources.values()],
                }
            result = {
                "valid": True,
                "file": args.file,
                "nodes": len(graph.nodes),
                "resources": len(graph.all_resources),
                "waves": len(graph.waves),
                "deduplications": deduped,
                "variables": redacted_variables,
                "node_details": nodes_info,
                "cross_node_deps": xnode_deps,
                "node_ordering": graph.node_ordering,
                "provenance": [
                    {"source": pv.source_file, "template": pv.template_name,
                     "version": pv.version,
                     "params": _redact_obj(pv.params_used, graph.sensitive_values)}
                    for pv in graph.provenance_log
                ],
                "wave_detail": [[rid for rid in w] for w in graph.waves],
            }
            if getattr(args,"strict",False):
                result["lint_errors"] = len(lint_errors)
            sp_path = Path(_state_path(args.file))
            if sp_path.exists():
                sf = StateFile(sp_path)
                result["state"] = sf.state_summary()
            print(json.dumps(result, indent=2))
            if getattr(args,"strict",False) and lint_errors:
                sys.exit(1)
            return
        tot=sum(len(w) for w in graph.waves)
        deduped=sum(1 for ids in graph.dedup_map.values() if len(ids)>1)
        print(green(f"✓ {args.file} is valid."))
        print(f"  {len(graph.nodes)} node(s), {len(graph.all_resources)} resource(s), {len(graph.waves)} wave(s)")
        if deduped: print(f"  {green(str(deduped))} deduplication(s) across templates")
        # Show variables with override annotations
        var_parts = []
        for k in sorted(redacted_variables.keys()):
            v = redacted_variables[k]
            tag = f" {yellow('(override)')}" if k in extra else ""
            var_parts.append(f"{k}={cyan(repr(v))}{tag}")
        print(f"  Variables: {', '.join(var_parts)}" if var_parts else f"  Variables: []")
        for nn,hn in graph.nodes.items():
            rn=[r.short_name for r in hn.resources.values()]
            print(f"  Node '{nn}': {', '.join(rn)}")
        # Show cross-node dependencies
        xnode_deps = []
        for rid, res in graph.all_resources.items():
            for dep in res.needs:
                if dep in graph.all_resources:
                    dep_res = graph.all_resources[dep]
                    if dep_res.node_name != res.node_name and not dep_res.is_barrier:
                        xnode_deps.append((res.node_name, res.short_name, dep_res.node_name, dep_res.short_name))
        if xnode_deps:
            print(f"\n  Cross-node dependencies:")
            for src_node, src_step, dst_node, dst_step in xnode_deps:
                print(f"    {cyan(src_node)}.{src_step} → {cyan(dst_node)}.{dst_step}")
        # Show node-level ordering
        if graph.node_ordering:
            print(f"\n  Node ordering:")
            for nn, deps in graph.node_ordering.items():
                print(f"    {cyan(nn)} after {', '.join(cyan(d) for d in deps)}")
        if graph.provenance_log:
            print(f"\n  Provenance:")
            for pv in graph.provenance_log:
                ver_tag = f" v{pv.version}" if pv.version else ""
                params = _redact_obj(pv.params_used, graph.sensitive_values)
                print(f"    ← {cyan(pv.source_file)}{ver_tag} ({pv.template_name}) params={params}")
        # Show collect keys
        ckeys = sorted(set(r.collect_key for r in graph.all_resources.values() if r.collect_key))
        if ckeys:
            print(f"\n  Collect keys: {', '.join(cyan(k) for k in ckeys)}")
        # Show tags
        all_tags = sorted(set(t for r in graph.all_resources.values() for t in r.tags))
        if all_tags:
            print(f"\n  Tags: {', '.join(cyan(t) for t in all_tags)}")
        sp_path = Path(_state_path(args.file))
        if sp_path.exists():
            sf = StateFile(sp_path)
            s = sf.state_summary()
            print(f"\n  State: {', '.join(f'{k}={v}' for k,v in sorted(s.items()))}")
        if getattr(args,"strict",False):
            lint_rc = cmd_lint(args.file, repo_dir=repo, extra_vars=extra, inventory_files=inv_files,
                               strict=True, vault_passphrase=vault_passphrase, vault_prompt=vault_prompt)
            sys.exit(1 if lint_rc else 0)
    elif args.command=="check":
        needs = cmd_check(graph, verbose=getattr(args,"verbose",False),
                          max_parallel=getattr(args,"parallel",4),
                          json_output=getattr(args,"json",False))
        sys.exit(1 if needs else 0)
    elif args.command=="ping":
        failures = cmd_ping(graph)
        sys.exit(1 if failures else 0)
    elif args.command=="explain":
        cmd_explain(graph, args.step)
    elif args.command=="why":
        cmd_why(graph, args.step)
    elif args.command=="lint":
        warnings = cmd_lint(args.file, repo_dir=repo, extra_vars=extra, inventory_files=inv_files,
                            strict=getattr(args,"strict",False),
                            vault_passphrase=vault_passphrase, vault_prompt=vault_prompt)
        sys.exit(1 if warnings else 0)
    elif args.command=="plan":
        _inc = set(args.tags.split(",")) if getattr(args,"tags",None) else None
        _exc = set(args.skip_tags.split(",")) if getattr(args,"skip_tags",None) else None
        cmd_plan(graph,verbose=args.verbose,include_tags=_inc,exclude_tags=_exc)
    elif args.command=="dot":  cmd_dot(graph)
    elif args.command=="visualize":
        out = args.output or (Path(args.file).stem + ".html")
        state_path = getattr(args, "state", None)
        if not state_path:
            # Auto-detect state file
            auto = _state_path(args.file)
            if Path(auto).exists(): state_path = auto
        cmd_visualize(graph, out, source_file=args.file, state_path=state_path)
    elif args.command=="apply":
        # Resolve become password
        become_pass = None
        if getattr(args, "ask_become_pass", False):
            import getpass
            become_pass = getpass.getpass("BECOME password: ")
        elif os.environ.get("CGR_BECOME_PASS"):
            become_pass = os.environ["CGR_BECOME_PASS"]
        # --check-mode is an alias for --dry-run --blast-radius
        if getattr(args, "check_mode", False):
            args.dry_run = True
            args.blast_radius = True
        _inc = set(args.tags.split(",")) if getattr(args,"tags",None) else None
        _exc = set(args.skip_tags.split(",")) if getattr(args,"skip_tags",None) else None
        apply_kwargs = dict(
            dry_run=args.dry_run, max_parallel=args.parallel,
            no_resume=getattr(args,"no_resume",False),
            verbose=getattr(args,"verbose",False),
            global_timeout=getattr(args,"timeout",None),
            start_from=getattr(args,"start_from",None),
            log_file=getattr(args,"log",None),
            on_complete=getattr(args,"on_complete",None),
            become_pass=become_pass,
            blast_radius=getattr(args,"blast_radius",False),
            include_tags=_inc, exclude_tags=_exc,
            skip_steps=getattr(args,"skip",[]) or [],
            confirm=getattr(args,"confirm",False),
            run_id=getattr(args,"run_id",None),
            state_path=getattr(args,"apply_state",None),
            progress_mode=getattr(args,"progress",False),
        )
        if getattr(args, "output", "text") == "json":
            with redirect_stdout(io.StringIO()):
                results, wall_ms = cmd_apply_stateful(graph, args.file, **apply_kwargs)
        else:
            results, wall_ms = cmd_apply_stateful(graph, args.file, **apply_kwargs)
        if args.report:
            rpt = _build_apply_report(graph, results, wall_ms, graph_file=args.file,
                                      state_path=getattr(args, "apply_state", None),
                                      run_id=getattr(args, "run_id", None))
            Path(args.report).write_text(json.dumps(rpt,indent=2))
            if getattr(args, "output", "text") != "json":
                print(dim(f"Report → {args.report}"))
        if getattr(args, "output", "text") == "json":
            rpt = _build_apply_report(graph, results, wall_ms, graph_file=args.file,
                                      state_path=getattr(args, "apply_state", None),
                                      run_id=getattr(args, "run_id", None))
            print(json.dumps(rpt, indent=2))
        sys.exit(1 if any(r.status in (Status.FAILED, Status.TIMEOUT) for r in results) else 0)

if __name__=="__main__": main()

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
