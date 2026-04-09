"""Template repository loading and version checks."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.lexer import *
from cgr_src.ast_nodes import *
from cgr_src.parser_cg import *
from cgr_src.parser_cgr import *

DEFAULT_REPO = os.path.expanduser("~/.commandgraph/repo")
_CGR_REPO_HOME = os.path.expanduser("~/.cgr/repo")

@dataclass
class Provenance:
    source_file: str        # e.g. "apt/install_package.cg"
    template_name: str      # e.g. "install_package"
    params_used: dict[str,str]
    used_by: list[str] = field(default_factory=list)  # call sites
    version: str|None = None  # template version if declared

def _find_template_file(repo_dirs: list[str], import_path: str) -> Path|None:
    """Search repo directories in order for a template file (.cgr preferred, .cg fallback)."""
    for rdir in repo_dirs:
        rd = Path(rdir)
        if not rd.exists(): continue
        # Prefer .cgr (readable), then .cg (structured)
        for ext in (".cgr", ".cg"):
            fp = rd / (import_path + ext)
            if fp.exists(): return fp
        # Try without extension
        fp = rd / import_path
        if fp.exists(): return fp
    return None

def load_template_from_repo(repo_dirs: list[str], import_path: str) -> ASTTemplate:
    """Load a template file (.cgr or .cg), searching repo_dirs in order."""
    filepath = _find_template_file(repo_dirs, import_path)
    if not filepath:
        searched = [str(Path(d) / (import_path + ".*")) for d in repo_dirs if Path(d).exists()]
        raise ResolveError(
            f"Template '{import_path}' not found in any repo.\n"
            f"  Searched:\n" + "\n".join(f"    • {s}" for s in searched) +
            (f"\n  (No repo directories exist)" if not searched else "")
        )
    source = filepath.read_text()
    # Use the appropriate parser based on extension
    if filepath.suffix == ".cgr":
        program = parse_cgr(source, str(filepath))
    else:
        tokens = lex(source, str(filepath))
        parser = Parser(tokens, source, str(filepath))
        program = parser.parse()
    if not program.templates:
        raise ResolveError(f"File '{filepath}' does not contain a template block.")
    tpl = program.templates[0]
    tpl.source_file = import_path
    return tpl

def _build_repo_search_path(explicit_repo: str|None = None, graph_file: str|None = None) -> list[str]:
    """
    Build the template repo search path (searched in order):
      1. Explicit --repo directory (if provided)
      2. CGR_REPO env var (or COMMANDGRAPH_REPO as backward-compat fallback)
      3. ./repo relative to the graph file's directory
      4. ./repo relative to CWD
      5. ~/.cgr/repo (new home dotfile)
      6. ~/.commandgraph/repo (legacy home dotfile)
    Duplicates are removed. Non-existent dirs are kept (logged later if needed).
    """
    dirs: list[str] = []
    seen: set[str] = set()
    def _add(d: str):
        r = str(Path(d).resolve())
        if r not in seen:
            seen.add(r); dirs.append(d)
    if explicit_repo:
        _add(explicit_repo)
    # Env var support: CGR_REPO takes precedence; COMMANDGRAPH_REPO is backward-compat
    env_repo = os.environ.get("CGR_REPO") or os.environ.get("COMMANDGRAPH_REPO")
    if env_repo:
        _add(env_repo)
    if graph_file:
        graph_dir = str(Path(graph_file).resolve().parent)
        _add(str(Path(graph_dir) / "repo"))
    _add(str(Path.cwd() / "repo"))
    _add(_CGR_REPO_HOME)
    _add(DEFAULT_REPO)
    return dirs

def cmd_repo_index(repo_dir: str):
    """Scan the repo and print a catalog of available templates."""
    repo = Path(repo_dir)
    if not repo.exists():
        print(red(f"Repo directory not found: {repo}"))
        sys.exit(1)
    templates = []
    seen_paths: set[str] = set()  # avoid double-counting .cg and .cgr of same template
    # Scan both .cgr (preferred) and .cg files
    for pattern in ("*.cgr", "*.cg"):
        for tpl_file in sorted(repo.rglob(pattern)):
            rel = tpl_file.relative_to(repo)
            tpl_path = str(rel.with_suffix(""))
            if tpl_path in seen_paths: continue  # already found as .cgr
            try:
                source = tpl_file.read_text()
                if tpl_file.suffix == ".cgr":
                    prog = parse_cgr(source, str(tpl_file))
                else:
                    tokens = lex(source, str(tpl_file))
                    prog = Parser(tokens, source, str(tpl_file)).parse()
                for t in prog.templates:
                    params = [(p.name, p.default) for p in t.params]
                    templates.append({
                        "path": tpl_path,
                        "name": t.name,
                        "params": params,
                        "description": t.description,
                        "file": str(rel),
                        "version": t.version,
                    })
                    seen_paths.add(tpl_path)
            except Exception as e:
                print(yellow(f"  ⚠ Skipping {rel}: {e}"))
    # Print catalog
    print(bold(f"\nCommandGraph Repository: {repo}"))
    print(bold("─" * 60))
    for t in templates:
        params_str = ", ".join(
            f"{n}" if d is None else f"{n}={d!r}" for n, d in t["params"]
        )
        fmt = dim(".cgr") if t["file"].endswith(".cgr") else dim(".cg")
        ver_str = f" {green('v' + t['version'])}" if t.get("version") else ""
        print(f"  {cyan(bold(t['path']))}{ver_str} {fmt}")
        print(f"    template {t['name']}({params_str})")
        if t["description"]:
            print(f"    {dim(t['description'])}")
        print()
    print(f"  {len(templates)} template(s) found.\n")
    # Write index
    idx_path = repo / "repo.index"
    with open(idx_path, "w") as f:
        json.dump(templates, f, indent=2)
    print(dim(f"  Index written to {idx_path}"))

# ── Template version checking ──────────────────────────────────────────────

def _parse_semver(s: str) -> tuple[int, ...]:
    """Parse a version string like '1.2.3' or '1.2' into a tuple of ints."""
    parts = s.strip().split(".")
    result = []
    for p in parts:
        try: result.append(int(p))
        except ValueError: result.append(0)
    while len(result) < 3: result.append(0)
    return tuple(result[:3])

def _check_version(op: str, required: str, actual: str) -> bool:
    """Check if `actual` version satisfies `op required`.
    Operators: = (exact), >= (minimum), ~> (compatible)."""
    req = _parse_semver(required)
    act = _parse_semver(actual)
    if op == "=":
        return act == req
    elif op == ">=":
        return act >= req
    elif op == "~>":
        # ~> X.Y   means >= X.Y.0, < (X+1).0.0
        # ~> X.Y.Z means >= X.Y.Z, < X.(Y+1).0
        parts = required.strip().split(".")
        if len(parts) <= 2:
            return act >= req and act[0] == req[0]
        else:
            return act >= req and act[0] == req[0] and act[1] == req[1]
    return False

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
