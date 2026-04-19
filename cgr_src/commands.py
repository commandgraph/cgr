"""Higher-level CLI commands: report, check, ping, explain, convert, fmt, lint, diff, testing, init, doctor, and secrets."""
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

def _load(filepath, repo_dir=None, extra_vars=None, raise_on_error=False, inventory_files=None,
          vault_passphrase=None, vault_prompt=False, resolve_deferred_secrets=False):
    path=Path(filepath)
    if not path.exists():
        if raise_on_error: raise ValueError(f"not found: {path}")
        print(red(f"error: not found: {path}"),file=sys.stderr); sys.exit(1)
    source=path.read_text()

    # Detect format by extension
    if path.suffix == ".cgr":
        try: ast=parse_cgr(source, str(path))
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
        try: ast=Parser(tokens,source,str(path)).parse()
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

# ── Report command ─────────────────────────────────────────────────────

def cmd_report(graph_file: str, *, fmt: str = "table", output_file: str|None = None,
               filter_keys: list[str]|None = None, run_id: str|None = None):
    """Generate a report from collected outputs."""
    op = _output_path(graph_file, run_id=run_id)
    if not Path(op).exists():
        print(red(f"error: No output file found at {op}"))
        hint = f"  Run 'cgr apply {graph_file}'"
        if run_id:
            hint += f" --run-id {run_id}"
        hint += " on a graph with 'collect' clauses first."
        print(dim(hint))
        sys.exit(1)

    of = OutputFile(op)
    entries = of.all_outputs()
    if filter_keys:
        entries = [e for e in entries if e.get("key") in filter_keys]

    if not entries:
        print(dim("  No collected outputs."))
        return

    if fmt == "json":
        out = json.dumps(entries, indent=2)
        if output_file:
            Path(output_file).write_text(out)
            print(dim(f"Report → {output_file}"))
        else:
            print(out)
        return

    if fmt == "csv":
        import csv, io
        buf = io.StringIO()
        # Determine all unique keys for columns
        all_keys = sorted(set(e.get("key", "") for e in entries))
        all_nodes = sorted(set(e.get("node", "") for e in entries))
        if len(all_nodes) > 1:
            # Multi-node: rows=nodes, columns=keys
            w = csv.writer(buf)
            w.writerow(["node"] + all_keys)
            node_data: dict[str, dict[str, str]] = {}
            for e in entries:
                nd = e.get("node", "")
                if nd not in node_data: node_data[nd] = {}
                node_data[nd][e.get("key", "")] = e.get("stdout", "").strip()
            for nd in all_nodes:
                row = [nd] + [node_data.get(nd, {}).get(k, "") for k in all_keys]
                w.writerow(row)
        else:
            # Single-node: rows=keys, columns=key,value
            w = csv.writer(buf)
            w.writerow(["key", "node", "value", "status", "ms"])
            for e in entries:
                rc = e.get("rc")
                status = "ok" if rc == 0 or rc is None else f"exit={rc}"
                w.writerow([e.get("key", ""), e.get("node", ""), e.get("stdout", "").strip(),
                           status, e.get("ms", 0)])
        out = buf.getvalue()
        if output_file:
            Path(output_file).write_text(out)
            print(dim(f"Report → {output_file}"))
        else:
            print(out, end="")
        return

    # Default: pretty table
    print()
    print(bold(f"{'═'*64}"))
    print(bold(f"  Report: {graph_file}"))
    mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(Path(op).stat().st_mtime))
    print(dim(f"  Collected: {mtime}  |  Entries: {len(entries)}"))
    print(bold(f"{'═'*64}"))
    print()

    for e in entries:
        key = e.get("key", "?")
        node = e.get("node", "?")
        rc = e.get("rc")
        ms = e.get("ms", 0)
        status = green("ok") if rc == 0 or rc is None else red(f"exit={rc}")
        stdout = e.get("stdout", "").rstrip()

        print(f"  {cyan(key)} {dim('on')} {bold(node)}  {status}  {dim(f'{ms}ms')}")
        if stdout:
            # Try to pretty-print as JSON
            try:
                parsed = json.loads(stdout)
                formatted = json.dumps(parsed, indent=2)
                for line in formatted.split("\n"):
                    print(f"    {line}")
            except (json.JSONDecodeError, ValueError):
                for line in stdout.split("\n"):
                    print(f"    {line}")
        print()

    print(bold("─" * 64))
    cmd = f"cgr report {graph_file}"
    if run_id:
        cmd += f" --run-id {run_id}"
    print(dim(f"  Export: {cmd} --format csv|json [-o FILE]"))
    print()


def _build_apply_report(graph: Graph, results: list[ExecResult], wall_ms: int,
                        *, graph_file: str, state_path: str | None = None,
                        run_id: str | None = None) -> dict:
    sp = state_path or _state_path(graph_file, run_id=run_id)
    state = StateFile(sp) if Path(sp).exists() else None
    wave_metrics = []
    run_metric = None
    if state:
        wave_metrics = [
            {
                "wave": w.wave,
                "wall_ms": w.wall_ms,
                "to_run": w.to_run,
                "skipped": w.skipped,
                "ts": w.ts,
            }
            for w in state.wave_metrics()
        ]
        rm = state.run_metric()
        if rm is not None:
            run_metric = {
                "wall_ms": rm.wall_ms,
                "total_results": rm.total_results,
                "bottleneck_id": rm.bottleneck_id,
                "bottleneck_ms": rm.bottleneck_ms,
                "ts": rm.ts,
            }
    report = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "version": __version__,
        "file": graph_file,
        "wall_clock_ms": wall_ms,
        "total_resources": len(results),
        "dedup": {h: ids for h, ids in graph.dedup_map.items() if len(ids) > 1},
        "provenance": [
            {"source": p.source_file, "template": p.template_name, "params": p.params_used}
            for p in graph.provenance_log
        ],
        "results": [
            {
                "id": r.resource_id,
                "status": r.status.value,
                "run_rc": r.run_rc,
                "check_rc": r.check_rc,
                "duration_ms": r.duration_ms,
                "attempts": r.attempts,
                "reason": r.reason,
            }
            for r in results
        ],
        "metrics": {
            "waves": wave_metrics,
            "run": run_metric or {"wall_ms": wall_ms},
        },
    }
    opath = _output_path(graph_file, run_id=run_id)
    if Path(opath).exists():
        of = OutputFile(opath)
        report["outputs"] = {
            f"{e.get('node','')}/{e.get('key','')}": e.get("stdout", "").strip()
            for e in of.all_outputs()
        }
    return report


# ── How command ───────────────────────────────────────────────────────

def _extract_file_header(source: str) -> tuple[str|None, str]:
    """Return (title, doc_body) from the leading --- / # comment block."""
    title = None
    doc_lines: list[str] = []
    for raw in source.splitlines():
        stripped = raw.strip()
        if not stripped:
            if doc_lines or title:
                doc_lines.append("")
            continue
        if stripped.startswith("---") and stripped.endswith("---") and title is None and not doc_lines:
            title = stripped[3:-3].strip()
            continue
        if stripped.startswith("#"):
            body = stripped[1:]
            if body and body[0] in (" ", "\t"):
                body = body[1:]
            doc_lines.append(body)
            continue
        break  # first non-comment, non-blank line ends the header
    while doc_lines and not doc_lines[-1]:
        doc_lines.pop()
    while doc_lines and not doc_lines[0]:
        doc_lines.pop(0)
    return title, "\n".join(doc_lines)


def _extract_set_vars(source: str) -> list[tuple[str, str]]:
    """Return [(name, raw_expr)] for top-level set declarations, preserving env() etc. un-evaluated."""
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw in source.splitlines():
        stripped = raw.strip()
        if stripped.startswith("#"):
            continue
        m = re.match(r'^set\s+(\w+)\s*=\s*(.+)', stripped)
        if m:
            name, expr = m.group(1), m.group(2).strip()
            if name == "stateless" or name in seen:
                continue
            result.append((name, expr))
            seen.add(name)
    return result


def _is_secret_set_expr(expr: str) -> bool:
    """Return True when a raw set expression declares a secret backend."""
    return bool(re.match(r'^secret\b', expr.strip()))


def _is_sensitive_default_name(name: str) -> bool:
    """Return True when a default variable name commonly denotes a secret."""
    normalized = re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
    return bool(re.search(
        r'(^|_)(secret|token|password|passwd|pwd|api_key|apikey|access_key|private_key|credential|credentials|auth|bearer)(_|$)',
        normalized,
    ))


def cmd_how(filepath: str):
    """Show header documentation and configurable variables for a graph file."""
    path = Path(filepath)
    if not path.exists():
        print(red(f"error: file not found: {filepath}")); sys.exit(1)

    source = path.read_text()
    title, doc_body = _extract_file_header(source)
    raw_vars = _extract_set_vars(source)

    # Best-effort parse to flag secrets
    secret_names: set[str] = set()
    try:
        ast = parse_cg(source, filepath) if filepath.endswith(".cg") else parse_cgr(source, filepath)
        secret_names = {v.name for v in ast.variables if v.is_secret}
    except Exception:
        pass

    print()

    if title:
        width = min(len(title), 60)
        print(f"  {bold(title)}")
        print(f"  {dim('─' * width)}")
        print()

    if doc_body:
        for line in doc_body.splitlines():
            print(f"  {line}" if line else "")
        print()
    elif not title:
        print(f"  {dim('(no documentation header)')}")
        print()

    if raw_vars:
        print(f"  {bold('Defaults')}  {dim('(override with --set NAME=...)')}")
        print()
        max_len = max(len(n) for n, _ in raw_vars)
        for name, expr in raw_vars:
            is_secret = name in secret_names or _is_secret_set_expr(expr) or _is_sensitive_default_name(name)
            display_expr = _masked_secret_display() if is_secret else expr
            secret_tag = f"  {dim('[secret]')}" if is_secret else ""
            print(f"    {cyan(name.ljust(max_len))}  {dim('=')}  {display_expr}{secret_tag}")
        print()


# ── Check command (live drift detection without prior run) ─────────────

def cmd_check(graph, *, max_parallel=4, verbose=False, json_output=False):
    """Run all check clauses against the live system to detect what needs to run."""
    total = sum(1 for r in graph.all_resources.values() if r.check and r.check.strip().lower() != "false")
    untestable = sum(1 for r in graph.all_resources.values() if not r.check or r.check.strip().lower() == "false")
    _s = graph.sensitive_values if hasattr(graph, 'sensitive_values') else set()

    if not json_output:
        print(); print(bold(f"{'═'*64}"))
        print(bold(f"  CommandGraph {cyan('CHECK')}"))
        for nn, hn in graph.nodes.items():
            if hn.via_method == "ssh":
                print(f"  Target: {cyan(nn)} → ssh {hn.via_props.get('user','?')}@{hn.via_props.get('host','?')}")
            else:
                print(f"  Target: {cyan(nn)} → local")
        print(f"  Testable: {total}  |  No check clause: {untestable}  |  Parallel: {max_parallel}")
        print(bold(f"{'═'*64}")); print()

    satisfied = 0; needs_run = 0; errors = 0; done = 0
    json_results = []  # for --json output

    def _check_one(rid):
        """Run a single check clause. Returns (rid, rc, stderr, error_msg)."""
        res = graph.all_resources[rid]
        node = graph.nodes.get(res.node_name)
        if not node:
            return (rid, None, "", "node not found")
        try:
            rc, stdout, stderr = _run_cmd(res.check, node, res, timeout=min(res.timeout, 15))
            return (rid, rc, stderr, None)
        except Exception as e:
            return (rid, None, "", str(e))

    for wi, wave in enumerate(graph.waves):
        checkable = [rid for rid in wave
                     if graph.all_resources[rid].check and graph.all_resources[rid].check.strip().lower() != "false"]
        not_checkable = [rid for rid in wave if rid not in checkable]

        # Print non-checkable resources
        for rid in not_checkable:
            done += 1
            res = graph.all_resources[rid]
            if not res.is_barrier:
                if not json_output:
                    print(f"  {dim(f'[{done}/{total+untestable}]')} {dim('— n/a ')} {bold(res.short_name)}  {dim('no check clause')}")
                json_results.append({"step": res.short_name, "node": res.node_name, "status": "no_check"})

        if not checkable:
            continue

        # Run checks in parallel within the wave
        with ThreadPoolExecutor(max_workers=min(max_parallel, len(checkable))) as pool:
            futs = {pool.submit(_check_one, rid): rid for rid in checkable}
            for f in as_completed(futs):
                rid = futs[f]
                rid_rc, rid_stderr, rid_err = f.result()[1], f.result()[2], f.result()[3]
                res = graph.all_resources[rid]
                done += 1
                if rid_err:
                    errors += 1
                    if not json_output:
                        print(f"  {dim(f'[{done}/{total+untestable}]')} {red('✗ err  ')} {bold(res.short_name)}  {red(rid_err[:60])}")
                    json_results.append({"step": res.short_name, "node": res.node_name, "status": "error", "error": rid_err})
                elif rid_rc == 0:
                    satisfied += 1
                    if not json_output:
                        print(f"  {dim(f'[{done}/{total+untestable}]')} {green('✓ ok   ')} {bold(res.short_name)}  {dim('check passes — already satisfied')}")
                    json_results.append({"step": res.short_name, "node": res.node_name, "status": "satisfied"})
                else:
                    needs_run += 1
                    if not json_output:
                        print(f"  {dim(f'[{done}/{total+untestable}]')} {yellow('△ needs')} {bold(res.short_name)}  {yellow('check failed — would need to run')}")
                        if verbose and rid_stderr.strip():
                            for line in rid_stderr.strip().splitlines()[:3]:
                                print(f"              {dim('│')} {_redact(line, _s)}")
                    json_results.append({"step": res.short_name, "node": res.node_name, "status": "needs_run"})

    if json_output:
        print(json.dumps({"satisfied": satisfied, "needs_run": needs_run,
                          "no_check": untestable, "errors": errors,
                          "results": json_results}, indent=2))
    else:
        print()
        print(bold(f"{'═'*64}"))
        print(bold("  Check Results"))
        print(f"  {'─'*44}")
        print(f"  {green(f'Already satisfied'):>39s} : {satisfied}")
        if needs_run:
            print(f"  {yellow(f'Needs to run'):>39s} : {needs_run}")
        if untestable:
            print(f"  {dim(f'No check clause'):>39s} : {untestable}")
        if errors:
            print(f"  {red(f'Errors'):>39s} : {errors}")
        if needs_run:
            print(f"\n  {yellow(bold(f'{needs_run} step(s) would need to run.'))}")
        else:
            print(f"\n  {green(bold('System is already in desired state.'))}")
        print(bold(f"{'═'*64}")); print()
    return needs_run


# ── Ping command (SSH connection pre-check) ───────────────────────────

def cmd_ping(graph):
    """Verify SSH connectivity to all targets in the graph."""
    ssh_nodes = [(nn, hn) for nn, hn in graph.nodes.items() if hn.via_method == "ssh"]
    local_nodes = [(nn, hn) for nn, hn in graph.nodes.items() if hn.via_method == "local"]

    if not ssh_nodes:
        print(f"  {dim('No SSH targets to ping.')}")
        if local_nodes:
            print(f"  {green('✓')} {len(local_nodes)} local target(s) — no connectivity check needed")
        return 0

    print(); print(bold(f"  Checking connectivity to {len(ssh_nodes)} SSH target(s)..."))
    print(bold("─" * 64))

    failures = 0
    for nn, hn in ssh_nodes:
        h = hn.via_props.get("host", "?")
        u = hn.via_props.get("user", "?")
        p = hn.via_props.get("port", "22")
        tgt = f"{u}@{h}" if u else h
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
               "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
               "-o", "LogLevel=ERROR", "-p", p, tgt, "echo ok"]
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            if proc.returncode == 0:
                print(f"  {green('✓ ok  ')} {cyan(nn)} → ssh {u}@{h}:{p}")
            else:
                failures += 1
                err_msg = proc.stderr.strip().splitlines()[0] if proc.stderr.strip() else f"exit={proc.returncode}"
                print(f"  {red('✗ fail')} {cyan(nn)} → ssh {u}@{h}:{p}")
                print(f"         {red(err_msg)}")
                # Diagnostic hints based on common error patterns
                err_lower = (proc.stderr or "").lower()
                if "permission denied" in err_lower or "publickey" in err_lower:
                    print(f"         {dim('Hint: Check SSH key. Try: ssh-copy-id ' + tgt)}")
                elif "connection refused" in err_lower:
                    print(f"         {dim('Hint: Is sshd running on ' + h + '? Try: ssh ' + tgt)}")
                elif "no route" in err_lower or "network is unreachable" in err_lower:
                    print(f"         {dim('Hint: Host unreachable. Check network/firewall and that ' + h + ' is correct.')}")
                elif "name or service not known" in err_lower or "could not resolve" in err_lower:
                    print(f"         {dim('Hint: DNS resolution failed for ' + h + '. Check hostname/IP.')}")
        except subprocess.TimeoutExpired:
            failures += 1
            print(f"  {red('✗ fail')} {cyan(nn)} → ssh {u}@{h}:{p}")
            print(f"         {red('Connection timed out (5s)')}")
            print(f"         {dim('Hint: Host may be down, firewalled, or wrong port. Try: ssh -p ' + p + ' ' + tgt)}")
        except FileNotFoundError:
            failures += 1
            print(f"  {red('✗ fail')} {cyan(nn)} → ssh {u}@{h}:{p}")
            print(f"         {red('ssh binary not found — install openssh-client')}")
            print(f"         {dim('Hint: apt-get install -y openssh-client')}")

    if local_nodes:
        for nn, hn in local_nodes:
            print(f"  {green('✓ ok  ')} {cyan(nn)} → local")

    print(bold("─" * 64))
    if failures:
        print(f"  {red(bold(f'{failures} target(s) unreachable.'))}")
    else:
        print(f"  {green(bold(f'All {len(ssh_nodes)} SSH target(s) reachable.'))}")
    print()
    return failures


# ── Explain command (dependency chain visualization) ──────────────────

def cmd_explain(graph, step_name):
    """Show the full dependency chain for a single step."""
    _s = graph.sensitive_values if hasattr(graph, 'sensitive_values') else set()
    # Find the resource
    slug = _slug(step_name)
    target_rid = None
    for rid, res in graph.all_resources.items():
        if res.short_name == slug or rid.endswith("." + slug) or rid == step_name:
            target_rid = rid; break
    if not target_rid:
        print(red(f"error: Step '{step_name}' not found."))
        print(f"  Available: {', '.join(r.short_name for r in graph.all_resources.values() if not r.is_barrier)}")
        sys.exit(1)

    res = graph.all_resources[target_rid]

    # Compute transitive dependencies (upstream)
    upstream = []
    def walk_up(rid, depth=0):
        r = graph.all_resources.get(rid)
        if not r: return
        for dep in r.needs:
            if dep in graph.all_resources:
                upstream.append((dep, depth + 1))
                walk_up(dep, depth + 1)
    walk_up(target_rid)

    # Compute transitive dependents (downstream)
    depnts = defaultdict(list)
    for rid2, res2 in graph.all_resources.items():
        for dep in res2.needs:
            depnts[dep].append(rid2)

    downstream = []
    def walk_down(rid, depth=0):
        for dep in depnts.get(rid, []):
            if dep in graph.all_resources:
                downstream.append((dep, depth + 1))
                walk_down(dep, depth + 1)
    walk_down(target_rid)

    # Find which wave
    target_wave = None
    for wi, wave in enumerate(graph.waves):
        if target_rid in wave:
            target_wave = wi + 1; break

    # Print
    print()
    print(bold(f"  Explain: [{step_name}]"))
    print(bold("─" * 64))

    # Identity
    print(f"  {dim('Resource ID:')} {target_rid}")
    print(f"  {dim('Node:')}        {cyan(res.node_name)}")
    print(f"  {dim('Wave:')}        {target_wave}")
    if res.description and res.description != res.short_name:
        print(f"  {dim('Description:')} {res.description}")
    if res.check and res.check.strip().lower() != "false":
        print(f"  {dim('Check:')}       {_redact(res.check, _s)[:70]}")
    if res.script_path:
        print(f"  {dim('Script:')}      {res.script_path[:70]}")
    if res.run:
        print(f"  {dim('Run:')}         {_redact(res.run, _s)[:70]}")
    if res.run_as:
        print(f"  {dim('Run as:')}      {res.run_as}")

    # Provenance
    if res.provenance:
        pv = res.provenance
        ver_tag = f" v{pv.version}" if pv.version else ""
        print(f"  {dim('Template:')}    {cyan(pv.source_file)}{ver_tag}")
        print(f"  {dim('Params:')}      {pv.params_used}")

    # Dedup
    if res.identity_hash in graph.dedup_map:
        sites = graph.dedup_map[res.identity_hash]
        if len(sites) > 1:
            others = [s.split(".")[-1] for s in sites if s != target_rid]
            print(f"  {dim('Shared with:')} {', '.join(others)}")

    # Upstream
    if upstream:
        seen = set()
        print(f"\n  {green('▲ Depends on')} ({len(set(r for r,_ in upstream))} steps):")
        for rid, depth in upstream:
            if rid in seen: continue
            seen.add(rid)
            r = graph.all_resources[rid]
            indent = "    " + "  " * depth
            label = f"{r.short_name}"
            if r.node_name != res.node_name:
                label = f"{cyan(r.node_name)}/{label}"
            if r.is_barrier:
                label = dim(label + " (barrier)")
            print(f"{indent}← {label}")
    else:
        print(f"\n  {green('▲ Depends on:')} {dim('nothing (root step)')}")

    # Downstream
    if downstream:
        seen = set()
        print(f"\n  {magenta('▼ Required by')} ({len(set(r for r,_ in downstream))} steps):")
        for rid, depth in downstream:
            if rid in seen: continue
            seen.add(rid)
            r = graph.all_resources[rid]
            indent = "    " + "  " * depth
            label = f"{r.short_name}"
            if r.node_name != res.node_name:
                label = f"{cyan(r.node_name)}/{label}"
            if r.is_barrier:
                label = dim(label + " (barrier)")
            print(f"{indent}→ {label}")
    else:
        print(f"\n  {magenta('▼ Required by:')} {dim('nothing (leaf step)')}")

    print()


def cmd_why(graph, step_name):
    """Show what depends on a step — the inverse of explain."""
    slug = _slug(step_name)
    target_rid = None
    for rid, res in graph.all_resources.items():
        if res.short_name == slug or rid.endswith("." + slug) or rid == step_name:
            target_rid = rid; break
    if not target_rid:
        print(red(f"error: Step '{step_name}' not found."))
        print(f"  Available: {', '.join(r.short_name for r in graph.all_resources.values() if not r.is_barrier)}")
        sys.exit(1)

    res = graph.all_resources[target_rid]

    # Build reverse dependency map
    depnts = defaultdict(list)
    for rid2, res2 in graph.all_resources.items():
        for dep in res2.needs:
            depnts[dep].append(rid2)

    # Walk downstream
    downstream = []
    visited = set()
    def walk_down(rid, depth=0):
        for dep in depnts.get(rid, []):
            if dep in visited or dep not in graph.all_resources: continue
            visited.add(dep)
            downstream.append((dep, depth + 1))
            walk_down(dep, depth + 1)
    walk_down(target_rid)

    # Direct dependents
    direct = depnts.get(target_rid, [])

    # Find which wave
    target_wave = None
    for wi, wave in enumerate(graph.waves):
        if target_rid in wave: target_wave = wi + 1; break

    print()
    print(bold(f"  Why: [{step_name}]"))
    print(bold("─" * 64))
    print(f"  {dim('Resource ID:')} {target_rid}")
    print(f"  {dim('Node:')}        {cyan(res.node_name)}")
    print(f"  {dim('Wave:')}        {target_wave}")

    if not downstream:
        print(f"\n  {dim('Nothing depends on this step. It is a leaf.')}")
        if res.is_verify:
            print(f"  {dim('(This is a verify block — expected to be a terminal step.)')}")
        print()
        return

    print(f"\n  {magenta('▼ Direct dependents')} ({len(direct)}):")
    for rid in direct:
        r = graph.all_resources.get(rid)
        if not r: continue
        label = r.short_name
        if r.node_name != res.node_name:
            label = f"{cyan(r.node_name)}/{label}"
        prov = f" ← {r.provenance.source_file}" if r.provenance else ""
        print(f"    → {bold(label)}{dim(prov)}")

    # Transitive (all downstream minus direct)
    transitive = [(rid, d) for rid, d in downstream if rid not in direct]
    if transitive:
        print(f"\n  {magenta('▼ Transitive dependents')} ({len(transitive)}):")
        for rid, depth in transitive:
            r = graph.all_resources.get(rid)
            if not r: continue
            indent = "    " + "  " * (depth - 1)
            label = r.short_name
            if r.node_name != res.node_name:
                label = f"{cyan(r.node_name)}/{label}"
            if r.is_barrier:
                label = dim(label + " (barrier)")
            print(f"{indent}→ {label}")

    print(f"\n  {dim(f'If you remove [{step_name}], {len(downstream)} step(s) would lose a dependency.')}")
    print()


# ── Convert command (.cg ↔ .cgr) ─────────────────────────────────────

def _emit_cgr(prog: ASTProgram) -> str:
    """Emit an ASTProgram as .cgr format text."""
    lines = []

    def _dq(value: str | None) -> str:
        return json.dumps("" if value is None else str(value))

    def _emit_cgr_http_lines(res: ASTResource, indent: int) -> list[str]:
        pfx = " " * indent
        body = [f"{pfx}{res.http_method.lower()} {_dq(res.http_url)}"]
        if res.http_auth:
            auth_type, auth_value = res.http_auth
            if auth_type == "bearer":
                body.append(f"{pfx}auth bearer {_dq(auth_value)}")
            elif auth_type == "basic":
                user, _, password = (auth_value or "").partition(":")
                body.append(f"{pfx}auth basic {_dq(user)} {_dq(password)}")
            elif auth_type == "header":
                hname, _, hval = (auth_value or "").partition(":")
                body.append(f"{pfx}auth header {_dq(hname)} = {_dq(hval)}")
        for hname, hval in res.http_headers or []:
            body.append(f"{pfx}header {_dq(hname)} = {_dq(hval)}")
        if res.http_body is not None:
            body.append(f"{pfx}body {res.http_body_type or 'text'} {_dq(res.http_body)}")
        if res.http_expect:
            body.append(f"{pfx}expect {res.http_expect}")
        return body

    def _emit_cgr_action_lines(res: ASTResource, indent: int) -> list[str]:
        pfx = " " * indent
        if res.script_path:
            return [f"{pfx}script {_dq(res.script_path)}"]
        if res.http_method and res.http_url:
            return _emit_cgr_http_lines(res, indent)
        if res.wait_kind and res.wait_target:
            return [f"{pfx}wait for {res.wait_kind} {_dq(res.wait_target)}"]
        if res.reduce_key:
            suffix = f" as {res.reduce_var}" if res.reduce_var else ""
            return [f"{pfx}reduce {_dq(res.reduce_key)}{suffix}"]
        if res.run:
            return [f"{pfx}run $ {res.run}"]
        return []

    def _emit_cgr_resource_body(res: ASTResource, lines: list[str], indent: int) -> None:
        pfx = " " * indent
        for dep in res.needs:
            lines.append(f"{pfx}first {_emit_cgr_dep_ref(dep)}")

        if res.check and res.check.strip().lower() == "false":
            if res.run:
                lines.append(f"{pfx}always run $ {res.run}")
            else:
                lines.extend(_emit_cgr_action_lines(res, indent))
        else:
            if res.check:
                lines.append(f"{pfx}skip if $ {res.check}")
            lines.extend(_emit_cgr_action_lines(res, indent))

        if res.subgraph_path:
            for k, v in res.subgraph_vars.items():
                lines.append(f"{pfx}{k} = {_dq(v)}")

        if res.when:
            lines.append(f"{pfx}when {_dq(res.when)}")
        for k, v in res.env.items():
            env_when = res.env_when.get(k)
            suffix = f' when {_dq(env_when)}' if env_when else ""
            lines.append(f"{pfx}env {k} = {_dq(v)}{suffix}")
        for flag_text, flag_when in res.flags:
            suffix = f' when {_dq(flag_when)}' if flag_when else ""
            lines.append(f"{pfx}flag {_dq(flag_text)}{suffix}")
        if res.until is not None:
            lines.append(f"{pfx}until {_dq(res.until)}")
        if res.collect_key:
            collect_suffix = ""
            if res.collect_var:
                collect_suffix = f" as {res.collect_var}"
            elif res.collect_format:
                collect_suffix = f" as {res.collect_format}"
            lines.append(f"{pfx}collect {_dq(res.collect_key)}{collect_suffix}")
        if res.tags:
            lines.append(f"{pfx}tags {', '.join(res.tags)}")
        for var, value in res.on_success_set or []:
            lines.append(f"{pfx}on success: set {var} = {_dq(value)}")
        for var, value in res.on_failure_set or []:
            lines.append(f"{pfx}on failure: set {var} = {_dq(value)}")

        if res.parallel_block:
            limit_str = f" {res.parallel_limit} at a time" if res.parallel_limit else ""
            policy_str = ""
            if res.parallel_fail_policy == "stop":
                policy_str = ", if one fails stop all"
            elif res.parallel_fail_policy == "ignore":
                policy_str = ", if one fails ignore"
            lines.append(f"{pfx}parallel{limit_str}{policy_str}:")
            for child in res.parallel_block:
                _emit_cgr_resource(child, lines, indent + 2)

        if res.race_block:
            into_str = f" into {res.race_into}" if res.race_into else ""
            lines.append(f"{pfx}race{into_str}:")
            for child in res.race_block:
                _emit_cgr_resource(child, lines, indent + 2)

        if res.each_var and res.each_body:
            limit_str = f", {res.each_limit} at a time" if res.each_limit else ""
            lines.append(f"{pfx}each {res.each_var} in {res.each_list_expr}{limit_str}:")
            _emit_cgr_resource(res.each_body, lines, indent + 2)

        for child in res.children:
            _emit_cgr_resource(child, lines, indent + 2)

    def _parse_target_each_prog(te: ASTTargetEach) -> ASTProgram:
        if te.body_format == "cgr":
            return parse_cgr(te.body_source, "<emit each>")
        tokens = list(te.body_tokens) + [Token(TT.EOF, "", te.line, 0)]
        parser = Parser(tokens, "", "<emit each>")
        return parser.parse()

    def _emit_cgr_target_each(te: ASTTargetEach, lines: list[str]) -> None:
        lines.append(f"each {', '.join(te.var_names)} in {te.list_expr}:")
        if te.body_format == "cgr":
            body = te.body_source.rstrip()
            if body:
                lines.extend(body.splitlines())
        else:
            sub_prog = _parse_target_each_prog(te)
            for node in sub_prog.nodes:
                _emit_cgr_node(node, lines, indent=2)
        lines.append("")

    def _emit_cgr_hook(label: str, res: ASTResource, lines: list[str]) -> None:
        lines.append(f"on {label}:")
        _emit_cgr_resource_body(res, lines, indent=2)
        lines.append("")

    def _emit_cgr_node(node: ASTNode, lines: list[str], indent: int = 0) -> None:
        pfx = " " * indent
        via = node.via
        if via.method == "ssh":
            u = via.props.get("user", "")
            h = via.props.get("host", "")
            p = via.props.get("port", "")
            tgt = f"{u}@{h}" if u else h
            if p and p != "22":
                tgt += f":{p}"
            after_str = ""
            if node.after_nodes:
                after_parts = []
                for an in node.after_nodes:
                    after_parts.append("each" if an == "__each__" else f'"{an}"')
                after_str = f", after {', '.join(after_parts)}"
            lines.append(f'{pfx}target "{node.name}" ssh {tgt}{after_str}:')
        else:
            after_str = ""
            if node.after_nodes:
                after_parts = []
                for an in node.after_nodes:
                    after_parts.append("each" if an == "__each__" else f'"{an}"')
                after_str = f", after {', '.join(after_parts)}"
            lines.append(f'{pfx}target "{node.name}" local{after_str}:')

        for res in node.resources:
            _emit_cgr_resource(res, lines, indent + 2)
        for inst in node.instantiations:
            human = getattr(inst, '_cgr_human_name', inst.template_name)
            lines.append(f'{pfx}  [{human}] from {inst.template_name}:')
            for k, v in inst.args.items():
                lines.append(f'{pfx}    {k} = {_dq(v)}')
            lines.append("")
        lines.append("")
    # Variables
    for v in prog.variables:
        lines.append(f'set {v.name} = "{v.value}"')
    if prog.variables:
        lines.append("")

    # Imports
    for u in prog.uses:
        ver = ""
        if u.version_op and u.version_req:
            ver = f" {u.version_op} {u.version_req}"
        lines.append(f'using {u.path}{ver}')
    if prog.uses:
        lines.append("")

    # Inventory
    for inv in prog.inventories:
        lines.append(f'inventory "{inv.path}"')
    if prog.inventories:
        lines.append("")

    # Secrets
    for sec in prog.secrets:
        lines.append(f'secrets "{sec.path}"')
    if prog.secrets:
        lines.append("")

    # Gather facts
    if prog.gather_facts:
        lines.append("gather facts")
        lines.append("")

    # Target-level each blocks
    for te in prog.target_each_blocks:
        _emit_cgr_target_each(te, lines)

    # Nodes
    for node in prog.nodes:
        _emit_cgr_node(node, lines)

    if prog.on_complete:
        _emit_cgr_hook("complete", prog.on_complete, lines)
    if prog.on_failure:
        _emit_cgr_hook("failure", prog.on_failure, lines)

    return "\n".join(lines)


def _emit_cgr_dep_ref(dep: str) -> str:
    """Render a dependency reference in .cgr syntax."""
    return "[" + "/".join(part.replace("_", " ") for part in dep.split(".")) + "]"


def _emit_duration_literal(seconds: int) -> str:
    """Render a duration in the shortest stable unit used by the parsers."""
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    if seconds % 60 == 0:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def _emit_cgr_retry_clause(res: ASTResource) -> str | None:
    if res.retries <= 0:
        return None
    if res.retry_backoff and (res.retry_backoff_max or res.retry_jitter_pct):
        clause = f"retry {res.retries}x backoff {_emit_duration_literal(res.retry_delay)}"
        if res.retry_backoff_max:
            clause += f"..{_emit_duration_literal(res.retry_backoff_max)}"
        if res.retry_jitter_pct:
            clause += f" jitter {res.retry_jitter_pct}%"
        return clause
    if res.retry_backoff:
        if res.retry_delay != 1:
            return f"retry {res.retries}x wait {_emit_duration_literal(res.retry_delay)} with backoff"
        return f"retry {res.retries}x with backoff"
    if res.retry_delay != 5:
        return f"retry {res.retries}x wait {_emit_duration_literal(res.retry_delay)}"
    return f"retry {res.retries}x"


def _emit_cg_retry_clause(res: ASTResource) -> str | None:
    if res.retries <= 0:
        return None
    parts = [f"retry {res.retries}"]
    if res.retry_delay != 5 or res.retry_backoff:
        parts.append(f"delay {_emit_duration_literal(res.retry_delay)}")
    if res.retry_backoff:
        parts.append("backoff")
    return " ".join(parts)


def _emit_cgr_resource(res: ASTResource, lines: list, indent: int = 2):
    """Emit a single ASTResource in .cgr format."""
    pfx = " " * indent
    desc = res.description or res.name
    def _dq(value: str | None) -> str:
        return json.dumps("" if value is None else str(value))
    header_parts = []
    if res.run_as:
        header_parts.append(f"as {res.run_as}")
    if res.timeout != 300 or res.timeout_reset_on_output:
        timeout_part = f"timeout {res.timeout}s"
        if res.timeout_reset_on_output:
            timeout_part += " reset on output"
        header_parts.append(timeout_part)
    retry_clause = _emit_cgr_retry_clause(res)
    if retry_clause:
        header_parts.append(retry_clause)
    if res.on_fail not in ("stop",):
        header_parts.append(f"if fails {res.on_fail}")
    header = ", ".join(header_parts)
    if header:
        header = " " + header

    if res.is_verify:
        lines.append(f'{pfx}verify "{desc}":')
        if res.needs:
            refs = " ".join(_emit_cgr_dep_ref(n) for n in res.needs)
            lines.append(f"{pfx}  first {refs}")
        if res.timeout != 30 or res.timeout_reset_on_output:
            timeout_line = f"{pfx}  timeout {_emit_duration_literal(res.timeout)}"
            if res.timeout_reset_on_output:
                timeout_line += " reset on output"
            lines.append(timeout_line)
        retry_clause = _emit_cgr_retry_clause(res)
        if retry_clause:
            lines.append(f"{pfx}  {retry_clause}")
        if res.script_path:
            lines.append(f'{pfx}  script {_dq(res.script_path)}')
        elif res.http_method and res.http_url:
            lines.append(f"{pfx}  {res.http_method.lower()} {_dq(res.http_url)}")
            if res.http_expect:
                lines.append(f"{pfx}  expect {res.http_expect}")
        elif res.wait_kind and res.wait_target:
            lines.append(f"{pfx}  wait for {res.wait_kind} {_dq(res.wait_target)}")
        elif res.reduce_key:
            suffix = f" as {res.reduce_var}" if res.reduce_var else ""
            lines.append(f'{pfx}  reduce {_dq(res.reduce_key)}{suffix}')
        elif res.run:
            lines.append(f"{pfx}  run $ {res.run}")
        lines.append("")
        return

    # Stage container: emit [resource name]: with stage inside
    if res.stage_name:
        lines.append(f"{pfx}[{desc}]{header}:")
        for dep in res.needs:
            lines.append(f"{pfx}  first {_emit_cgr_dep_ref(dep)}")
        lines.append(f'{pfx}  stage "{res.stage_name}":')
        if res.stage_phases:
            for phase in res.stage_phases:
                lines.append(f'{pfx}    phase "{phase.name}" {phase.count_expr} from {phase.from_list}:')
                if phase.each_limit:
                    # Emit each block inside phase for concurrency limit
                    lines.append(f'{pfx}      each server, {phase.each_limit} at a time:')
                    for child in phase.body:
                        _emit_cgr_resource(child, lines, indent + 8)
                else:
                    for child in phase.body:
                        _emit_cgr_resource(child, lines, indent + 6)
        if res.script_path:
            lines.append(f'{pfx}  script {_dq(res.script_path)}')
        elif res.run:
            lines.append(f"{pfx}  run $ {res.run}")
        lines.append("")
        return

    if res.subgraph_path:
        lines.append(f"{pfx}[{desc}] from {_dq(res.subgraph_path)}{header}:")
    else:
        lines.append(f"{pfx}[{desc}]{header}:")

    tmp_lines = []
    body_pfx = " " * (indent + 2)
    for dep in res.needs:
        tmp_lines.append(f"{body_pfx}first {_emit_cgr_dep_ref(dep)}")
    if res.check and res.check.strip().lower() == "false":
        if res.run:
            tmp_lines.append(f"{body_pfx}always run $ {res.run}")
        elif res.script_path:
            tmp_lines.append(f'{body_pfx}script {_dq(res.script_path)}')
        elif res.http_method and res.http_url:
            tmp_lines.append(f"{body_pfx}{res.http_method.lower()} {_dq(res.http_url)}")
            if res.http_auth:
                auth_type, auth_value = res.http_auth
                if auth_type == "bearer":
                    tmp_lines.append(f"{body_pfx}auth bearer {_dq(auth_value)}")
                elif auth_type == "basic":
                    user, _, password = (auth_value or "").partition(":")
                    tmp_lines.append(f"{body_pfx}auth basic {_dq(user)} {_dq(password)}")
                elif auth_type == "header":
                    hname, _, hval = (auth_value or "").partition(":")
                    tmp_lines.append(f"{body_pfx}auth header {_dq(hname)} = {_dq(hval)}")
            for hname, hval in res.http_headers or []:
                tmp_lines.append(f"{body_pfx}header {_dq(hname)} = {_dq(hval)}")
            if res.http_body is not None:
                tmp_lines.append(f"{body_pfx}body {res.http_body_type or 'text'} {_dq(res.http_body)}")
            if res.http_expect:
                tmp_lines.append(f"{body_pfx}expect {res.http_expect}")
        elif res.wait_kind and res.wait_target:
            tmp_lines.append(f"{body_pfx}wait for {res.wait_kind} {_dq(res.wait_target)}")
        elif res.reduce_key:
            suffix = f" as {res.reduce_var}" if res.reduce_var else ""
            tmp_lines.append(f'{body_pfx}reduce {_dq(res.reduce_key)}{suffix}')
    else:
        if res.check:
            tmp_lines.append(f"{body_pfx}skip if $ {res.check}")
        if res.script_path:
            tmp_lines.append(f'{body_pfx}script {_dq(res.script_path)}')
        elif res.http_method and res.http_url:
            tmp_lines.append(f"{body_pfx}{res.http_method.lower()} {_dq(res.http_url)}")
            if res.http_auth:
                auth_type, auth_value = res.http_auth
                if auth_type == "bearer":
                    tmp_lines.append(f"{body_pfx}auth bearer {_dq(auth_value)}")
                elif auth_type == "basic":
                    user, _, password = (auth_value or "").partition(":")
                    tmp_lines.append(f"{body_pfx}auth basic {_dq(user)} {_dq(password)}")
                elif auth_type == "header":
                    hname, _, hval = (auth_value or "").partition(":")
                    tmp_lines.append(f"{body_pfx}auth header {_dq(hname)} = {_dq(hval)}")
            for hname, hval in res.http_headers or []:
                tmp_lines.append(f"{body_pfx}header {_dq(hname)} = {_dq(hval)}")
            if res.http_body is not None:
                tmp_lines.append(f"{body_pfx}body {res.http_body_type or 'text'} {_dq(res.http_body)}")
            if res.http_expect:
                tmp_lines.append(f"{body_pfx}expect {res.http_expect}")
        elif res.wait_kind and res.wait_target:
            tmp_lines.append(f"{body_pfx}wait for {res.wait_kind} {_dq(res.wait_target)}")
        elif res.reduce_key:
            suffix = f" as {res.reduce_var}" if res.reduce_var else ""
            tmp_lines.append(f'{body_pfx}reduce {_dq(res.reduce_key)}{suffix}')
        elif res.run:
            tmp_lines.append(f"{body_pfx}run $ {res.run}")

    if res.subgraph_path:
        for k, v in res.subgraph_vars.items():
            tmp_lines.append(f"{body_pfx}{k} = {_dq(v)}")
    if res.when:
        tmp_lines.append(f"{body_pfx}when {_dq(res.when)}")
    for k, v in res.env.items():
        env_when = res.env_when.get(k)
        suffix = f' when {_dq(env_when)}' if env_when else ""
        tmp_lines.append(f"{body_pfx}env {k} = {_dq(v)}{suffix}")
    for flag_text, flag_when in res.flags:
        suffix = f' when {_dq(flag_when)}' if flag_when else ""
        tmp_lines.append(f"{body_pfx}flag {_dq(flag_text)}{suffix}")
    if res.until is not None:
        tmp_lines.append(f"{body_pfx}until {_dq(res.until)}")
    if res.collect_key:
        collect_suffix = ""
        if res.collect_var:
            collect_suffix = f" as {res.collect_var}"
        elif res.collect_format:
            collect_suffix = f" as {res.collect_format}"
        tmp_lines.append(f'{body_pfx}collect {_dq(res.collect_key)}{collect_suffix}')
    if res.tags:
        tmp_lines.append(f"{body_pfx}tags {', '.join(res.tags)}")
    for var, value in res.on_success_set or []:
        tmp_lines.append(f"{body_pfx}on success: set {var} = {_dq(value)}")
    for var, value in res.on_failure_set or []:
        tmp_lines.append(f"{body_pfx}on failure: set {var} = {_dq(value)}")

    if res.parallel_block:
        limit_str = f" {res.parallel_limit} at a time" if res.parallel_limit else ""
        policy_str = ""
        if res.parallel_fail_policy == "stop":
            policy_str = ", if one fails stop all"
        elif res.parallel_fail_policy == "ignore":
            policy_str = ", if one fails ignore"
        tmp_lines.append(f"{body_pfx}parallel{limit_str}{policy_str}:")
        for child in res.parallel_block:
            _emit_cgr_resource(child, tmp_lines, indent + 4)
    if res.race_block:
        into_str = f" into {res.race_into}" if res.race_into else ""
        tmp_lines.append(f"{body_pfx}race{into_str}:")
        for child in res.race_block:
            _emit_cgr_resource(child, tmp_lines, indent + 4)
    if res.each_var and res.each_body:
        limit_str = f", {res.each_limit} at a time" if res.each_limit else ""
        tmp_lines.append(f"{body_pfx}each {res.each_var} in {res.each_list_expr}{limit_str}:")
        _emit_cgr_resource(res.each_body, tmp_lines, indent + 4)
    for child in res.children:
        _emit_cgr_resource(child, tmp_lines, indent + 2)
    lines.extend(tmp_lines)

    lines.append("")


def _emit_cg(prog: ASTProgram) -> str:
    """Emit an ASTProgram as .cg format text."""
    lines = []

    def _dq(value: str | None) -> str:
        return json.dumps("" if value is None else str(value))

    def _emit_cg_http_lines(res: ASTResource, indent: int) -> list[str]:
        pfx = " " * indent
        body = [f"{pfx}{res.http_method.lower()} {_dq(res.http_url)}"]
        if res.http_auth:
            auth_type, auth_value = res.http_auth
            if auth_type == "bearer":
                body.append(f"{pfx}auth bearer {_dq(auth_value)}")
            elif auth_type == "basic":
                user, _, password = (auth_value or "").partition(":")
                body.append(f"{pfx}auth basic {_dq(user)} {_dq(password)}")
            elif auth_type == "header":
                hname, _, hval = (auth_value or "").partition(":")
                body.append(f"{pfx}auth header {_dq(hname)} = {_dq(hval)}")
        for hname, hval in res.http_headers or []:
            body.append(f"{pfx}header {_dq(hname)} = {_dq(hval)}")
        if res.http_body is not None:
            body.append(f"{pfx}body {res.http_body_type or 'text'} {_dq(res.http_body)}")
        if res.http_expect:
            body.append(f"{pfx}expect {_dq(res.http_expect)}")
        return body

    def _emit_cg_action_lines(res: ASTResource, indent: int) -> list[str]:
        pfx = " " * indent
        lines_local: list[str] = []
        if res.script_path:
            lines_local.append(f"{pfx}script {_dq(res.script_path)}")
        elif res.http_method and res.http_url:
            lines_local.extend(_emit_cg_http_lines(res, indent))
        elif res.wait_kind and res.wait_target:
            lines_local.append(f"{pfx}wait for {res.wait_kind} {_dq(res.wait_target)}")
        elif res.subgraph_path:
            lines_local.append(f"{pfx}from {_dq(res.subgraph_path)}")
            for k, v in res.subgraph_vars.items():
                lines_local.append(f"{pfx}{k} = {_dq(v)}")
        elif res.reduce_key:
            suffix = f" as {res.reduce_var}" if res.reduce_var else ""
            lines_local.append(f'{pfx}reduce {_dq(res.reduce_key)}{suffix}')
        elif res.run:
            lines_local.append(f"{pfx}run `{res.run}`")
        return lines_local

    def _parse_target_each_prog(te: ASTTargetEach) -> ASTProgram:
        if te.body_format == "cgr":
            return parse_cgr(te.body_source, "<emit each>")
        tokens = list(te.body_tokens) + [Token(TT.EOF, "", te.line, 0)]
        parser = Parser(tokens, "", "<emit each>")
        return parser.parse()

    def _emit_cg_hook(name: str, res: ASTResource, lines: list[str]) -> None:
        tmp_lines: list[str] = []
        _emit_cg_resource(res, tmp_lines, indent=0)
        lines.append(f"{name} {{")
        lines.extend(tmp_lines[1:-1])
        lines.append("}")
        lines.append("")

    def _emit_cg_target_each(te: ASTTargetEach, lines: list[str]) -> None:
        list_expr = te.list_expr
        if list_expr.startswith("${") and list_expr.endswith("}"):
            list_expr = list_expr[2:-1]
        lines.append(f"each {', '.join(te.var_names)} in {list_expr} {{")
        sub_prog = _parse_target_each_prog(te)
        for node in sub_prog.nodes:
            _emit_cg_node(node, lines, indent=2)
        lines.append("}")
        lines.append("")

    def _emit_cg_node(node: ASTNode, lines: list[str], indent: int = 0) -> None:
        pfx = " " * indent
        via = node.via
        lines.append(f'{pfx}node "{node.name}" {{')
        if via.method == "ssh":
            props = " ".join(f'{k} = {_dq(v)}' for k, v in via.props.items())
            lines.append(f"{pfx}  via ssh {props}".rstrip())
        else:
            lines.append(f"{pfx}  via local")
        if node.after_nodes:
            for an in node.after_nodes:
                lines.append(f"{pfx}  after each" if an == "__each__" else f'{pfx}  after "{an}"')
        lines.append("")
        for res in node.resources:
            _emit_cg_resource(res, lines, indent=indent + 2)
        for inst in node.instantiations:
            lines.append(f"{pfx}  {inst.template_name} {{")
            for k, v in inst.args.items():
                lines.append(f'{pfx}    {k} = {_dq(v)}')
            lines.append(f"{pfx}  }}")
        lines.append(f"{pfx}}}")
        lines.append("")
    for v in prog.variables:
        lines.append(f'var {v.name} = {_dq(v.value)}')
    if prog.variables:
        lines.append("")
    for u in prog.uses:
        ver = ""
        if u.version_op and u.version_req:
            if u.version_op == "=":
                ver = f' = "{u.version_req}"'
            elif u.version_op == ">=":
                ver = f' >= "{u.version_req}"'
            elif u.version_op == "~>":
                ver = f' ~> "{u.version_req}"'
        lines.append(f'use {_dq(u.path)}{ver}')
    if prog.uses:
        lines.append("")
    for inv in prog.inventories:
        lines.append(f'inventory {_dq(inv.path)}')
    if prog.inventories:
        lines.append("")
    if prog.gather_facts:
        lines.append("gather facts")
        lines.append("")
    for te in prog.target_each_blocks:
        _emit_cg_target_each(te, lines)
    for node in prog.nodes:
        _emit_cg_node(node, lines)
    if prog.on_complete:
        _emit_cg_hook("on_complete", prog.on_complete, lines)
    if prog.on_failure:
        _emit_cg_hook("on_failure", prog.on_failure, lines)
    return "\n".join(lines)


def _emit_cg_resource(res: ASTResource, lines: list, indent: int = 2):
    """Emit a single ASTResource in .cg format."""
    pfx = " " * indent
    def _dq(value: str | None) -> str:
        return json.dumps("" if value is None else str(value))
    def _emit_cg_http_lines(res: ASTResource, indent: int) -> list[str]:
        body_pfx = " " * indent
        body = [f"{body_pfx}{res.http_method.lower()} {_dq(res.http_url)}"]
        if res.http_auth:
            auth_type, auth_value = res.http_auth
            if auth_type == "bearer":
                body.append(f"{body_pfx}auth bearer {_dq(auth_value)}")
            elif auth_type == "basic":
                user, _, password = (auth_value or "").partition(":")
                body.append(f"{body_pfx}auth basic {_dq(user)} {_dq(password)}")
            elif auth_type == "header":
                hname, _, hval = (auth_value or "").partition(":")
                body.append(f"{body_pfx}auth header {_dq(hname)} = {_dq(hval)}")
        for hname, hval in res.http_headers or []:
            body.append(f"{body_pfx}header {_dq(hname)} = {_dq(hval)}")
        if res.http_body is not None:
            body.append(f"{body_pfx}body {res.http_body_type or 'text'} {_dq(res.http_body)}")
        if res.http_expect:
            body.append(f"{body_pfx}expect {_dq(res.http_expect)}")
        return body
    def _emit_cg_resource_body(res: ASTResource, lines: list[str], indent: int) -> None:
        body_pfx = " " * indent
        if res.needs:
            lines.append(f'{body_pfx}needs {", ".join(res.needs)}')
        if res.check:
            lines.append(f"{body_pfx}check `{res.check}`")
        if res.script_path:
            lines.append(f'{body_pfx}script {_dq(res.script_path)}')
        elif res.http_method and res.http_url:
            lines.extend(_emit_cg_http_lines(res, indent))
        elif res.wait_kind and res.wait_target:
            lines.append(f"{body_pfx}wait for {res.wait_kind} {_dq(res.wait_target)}")
        elif res.subgraph_path:
            lines.append(f'{body_pfx}from {_dq(res.subgraph_path)}')
            for k, v in res.subgraph_vars.items():
                lines.append(f'{body_pfx}{k} = {_dq(v)}')
        elif res.reduce_key:
            suffix = f" as {res.reduce_var}" if res.reduce_var else ""
            lines.append(f'{body_pfx}reduce {_dq(res.reduce_key)}{suffix}')
        elif res.run:
            lines.append(f"{body_pfx}run `{res.run}`")
        if res.run_as:
            lines.append(f"{body_pfx}as {res.run_as}")
        if res.timeout != 300 or res.timeout_reset_on_output:
            timeout_line = f"{body_pfx}timeout {res.timeout}"
            if res.timeout_reset_on_output:
                timeout_line += " reset on output"
            lines.append(timeout_line)
        retry_clause = _emit_cg_retry_clause(res)
        if retry_clause:
            lines.append(f"{body_pfx}{retry_clause}")
        if res.on_fail not in ("stop",):
            lines.append(f"{body_pfx}on_fail {res.on_fail}")
        if res.when:
            lines.append(f'{body_pfx}when {_dq(res.when)}')
        for k, v in res.env.items():
            env_when = res.env_when.get(k)
            suffix = f' when {_dq(env_when)}' if env_when else ""
            lines.append(f'{body_pfx}env {k} = {_dq(v)}{suffix}')
        for flag_text, flag_when in res.flags:
            suffix = f' when {_dq(flag_when)}' if flag_when else ""
            lines.append(f'{body_pfx}flag {_dq(flag_text)}{suffix}')
        if res.until is not None:
            lines.append(f'{body_pfx}until {_dq(res.until)}')
        if res.collect_key:
            collect_suffix = ""
            if res.collect_var:
                collect_suffix = f" as {res.collect_var}"
            elif res.collect_format:
                collect_suffix = f" as {res.collect_format}"
            lines.append(f'{body_pfx}collect {_dq(res.collect_key)}{collect_suffix}')
        if res.tags:
            lines.append(f'{body_pfx}tags {", ".join(res.tags)}')
        for var, value in res.on_success_set or []:
            lines.append(f'{body_pfx}on_success set {var} = {_dq(value)}')
        for var, value in res.on_failure_set or []:
            lines.append(f'{body_pfx}on_failure set {var} = {_dq(value)}')
        if res.parallel_block:
            limit_str = f" {res.parallel_limit}" if res.parallel_limit else ""
            fp = res.parallel_fail_policy
            policy_str = f", if_one_fails {fp}" if fp and fp != "wait" else ""
            lines.append(f"{body_pfx}parallel{limit_str}{policy_str} {{")
            for child in res.parallel_block:
                _emit_cg_resource(child, lines, indent + 4)
            lines.append(f"{body_pfx}}}")
        if res.race_block:
            into_str = f' into {_dq(res.race_into)}' if res.race_into else ""
            lines.append(f"{body_pfx}race{into_str} {{")
            for child in res.race_block:
                _emit_cg_resource(child, lines, indent + 4)
            lines.append(f"{body_pfx}}}")
        if res.each_var and res.each_body:
            list_expr = res.each_list_expr
            limit_str = f", {res.each_limit}" if res.each_limit else ""
            lines.append(f'{body_pfx}each {res.each_var} in {_dq(list_expr)}{limit_str} {{')
            _emit_cg_resource(res.each_body, lines, indent + 4)
            lines.append(f"{body_pfx}}}")
        if res.stage_name and res.stage_phases:
            lines.append(f'{body_pfx}stage {_dq(res.stage_name)} {{')
            for phase in res.stage_phases:
                lines.append(f'{body_pfx}  phase {_dq(phase.name)} {phase.count_expr} from {_dq(phase.from_list)} {{')
                if phase.each_limit:
                    lines.append(f'{body_pfx}    each server, {phase.each_limit} {{')
                    for child in phase.body:
                        _emit_cg_resource(child, lines, indent + 8)
                    lines.append(f"{body_pfx}    }}")
                else:
                    for child in phase.body:
                        _emit_cg_resource(child, lines, indent + 6)
                lines.append(f"{body_pfx}  }}")
            lines.append(f"{body_pfx}}}")
        for child in res.children:
            _emit_cg_resource(child, lines, indent + 2)
    rtype = "verify" if res.is_verify else "resource"
    if res.is_verify:
        lines.append(f'{pfx}verify "{res.description or res.name}" {{')
    else:
        lines.append(f"{pfx}resource {res.name} {{")
    if res.description and not res.is_verify:
        lines.append(f'{pfx}  description {_dq(res.description)}')
    _emit_cg_resource_body(res, lines, indent + 2)
    lines.append(f"{pfx}}}")


def _rewrite_cgr_named_instantiation_refs_for_cg(prog: ASTProgram, graph_file: str) -> None:
    """Rewrite .cgr human-named template step references to the .cg instance ids.

    Example: [install web packages] from apt/install_package becomes the .cg
    instance install_package_nginx_curl, so inline needs must be rewritten.
    """
    templates: dict[str, ASTTemplate] = {t.name: t for t in prog.templates}
    try:
        repo_dirs = _build_repo_search_path(graph_file=graph_file)
        for u in prog.uses:
            tpl = load_template_from_repo(repo_dirs, u.path)
            templates[u.alias or tpl.name] = tpl
    except Exception:
        # Best-effort only; if templates cannot be loaded we leave references as-is.
        return

    gvars = {v.name: v.value for v in prog.variables}

    def _instance_prefix(inst: ASTInstantiation) -> str | None:
        human_name = getattr(inst, "_cgr_human_name", None)
        if not human_name:
            return None
        tpl = templates.get(inst.template_name)
        if not tpl:
            return None
        param_vals = {}
        for p in tpl.params:
            if p.name in inst.args:
                param_vals[p.name] = _expand(inst.args[p.name], {**inst.args, **gvars, **param_vals}) or inst.args[p.name]
            elif p.default is not None:
                param_vals[p.name] = _expand(p.default, {**inst.args, **gvars, **param_vals}) or p.default
        if not tpl.params:
            return tpl.name
        primary_param = param_vals.get(tpl.params[0].name, "")
        safe_param = re.sub(r'[^a-zA-Z0-9_]', '_', primary_param)
        return f"{tpl.name}_{safe_param}" if safe_param else tpl.name

    def _rewrite_resource(res: ASTResource, mapping: dict[str, str]) -> None:
        res.needs = [mapping.get(dep, dep) for dep in res.needs]
        for child in res.children:
            _rewrite_resource(child, mapping)
        for child in res.parallel_block:
            _rewrite_resource(child, mapping)
        for child in res.race_block:
            _rewrite_resource(child, mapping)
        if res.each_body:
            _rewrite_resource(res.each_body, mapping)
        for phase in res.stage_phases:
            for child in phase.body:
                _rewrite_resource(child, mapping)

    for node in prog.nodes:
        mapping = {}
        for inst in node.instantiations:
            prefix = _instance_prefix(inst)
            human_name = getattr(inst, "_cgr_human_name", None)
            if prefix and human_name:
                mapping[_slug(human_name)] = prefix
        if not mapping:
            continue
        for res in node.resources:
            _rewrite_resource(res, mapping)


def _annotate_cg_instantiation_names_for_cgr(prog: ASTProgram, graph_file: str) -> None:
    """Annotate .cg template instances with readable .cgr step names.

    The emitted .cgr uses these labels in `[name] from template:` blocks, so
    they must line up with existing `.cg` needs references such as
    `install_package_nginx_curl`.
    """
    templates: dict[str, ASTTemplate] = {t.name: t for t in prog.templates}
    try:
        repo_dirs = _build_repo_search_path(graph_file=graph_file)
        for u in prog.uses:
            tpl = load_template_from_repo(repo_dirs, u.path)
            templates[u.alias or tpl.name] = tpl
    except Exception:
        pass

    gvars = {v.name: v.value for v in prog.variables}

    for node in prog.nodes:
        for inst in node.instantiations:
            tpl = templates.get(inst.template_name)
            if tpl and tpl.params:
                param_name = tpl.params[0].name
                raw_val = inst.args.get(param_name, "")
                if raw_val:
                    primary_val = _expand(raw_val, {**inst.args, **gvars}) or raw_val
                    safe_param = re.sub(r'[^a-zA-Z0-9_]', '_', primary_val)
                    prefix = f"{tpl.name}_{safe_param}" if safe_param else tpl.name
                else:
                    prefix = tpl.name
            else:
                prefix = inst.template_name
            inst._cgr_human_name = prefix.replace("_", " ")


def cmd_convert(filepath, output_file=None):
    """Convert between .cg and .cgr formats."""
    path = Path(filepath)
    if not path.exists():
        print(red(f"error: not found: {path}")); sys.exit(1)
    source = path.read_text()

    if path.suffix == ".cgr":
        # Parse as CGR, emit as .cg
        try:
            ast = parse_cgr(source, str(path))
        except CGRParseError as e:
            print(e.pretty(), file=sys.stderr); sys.exit(1)
        _rewrite_cgr_named_instantiation_refs_for_cg(ast, str(path))
        result = _emit_cg(ast)
        out_ext = ".cg"
    else:
        # Parse as .cg, emit as .cgr
        try:
            tokens = lex(source, str(path))
            ast = Parser(tokens, source, str(path)).parse()
        except (LexError, ParseError) as e:
            msg = e.pretty() if hasattr(e, 'pretty') else str(e)
            print(msg, file=sys.stderr); sys.exit(1)
        _annotate_cg_instantiation_names_for_cgr(ast, str(path))
        result = _emit_cgr(ast)
        out_ext = ".cgr"

    out_path = output_file or (str(path.with_suffix(out_ext)))
    Path(out_path).write_text(result)
    print(green(f"✓ Converted {filepath} → {out_path}"))


# ── Fmt command (auto-formatter) ──────────────────────────────────────

def cmd_fmt(filepath):
    """Auto-format a .cgr file: normalize indentation and style."""
    path = Path(filepath)
    if not path.exists():
        print(red(f"error: not found: {path}")); sys.exit(1)
    if path.suffix != ".cgr":
        print(red(f"error: cgr fmt only works on .cgr files")); sys.exit(1)
    source = path.read_text()
    try:
        ast = parse_cgr(source, str(path))
    except CGRParseError as e:
        print(e.pretty(), file=sys.stderr); sys.exit(1)

    result = _emit_cgr(ast)
    if result == source:
        print(dim(f"  {filepath} — already formatted"))
    else:
        path.write_text(result)
        print(green(f"✓ Formatted {filepath}"))


# ── Lint command (best-practice warnings) ─────────────────────────────

def _collect_lint_findings(graph, path, strict=False):
    warnings = []
    errors = []

    def add_warning(line, name, msg):
        warnings.append((line, name, msg))

    def add_error(line, name, msg):
        errors.append((line, name, msg))

    def _needs_script_suggestion(run_cmd: str) -> bool:
        return len(run_cmd) > 200 and bool(re.search(r'\||&&|\|\|', run_cmd))

    # Track variable references from the source text so lint can still see
    # ${var} usages after the resolver has expanded them away in graph commands.
    try:
        source_text = Path(path).read_text(encoding="utf-8")
    except OSError:
        source_text = ""
    used_vars = {m.group(1) for m in VAR_RE.finditer(source_text)}

    # Check each resource
    for rid, res in graph.all_resources.items():
        if res.is_barrier:
            continue
        run_cmd = res.run or ""
        allow_complex = strict and "cgr:allow-complex" in run_cmd

        # No check clause (non-idempotent)
        if (not res.check or res.check.strip().lower() == "false") and not res.is_verify:
            msg = "No check clause — step is not idempotent. Add 'skip if' for safe re-runs."
            if strict and not allow_complex:
                add_error(res.line, res.short_name, msg)
            else:
                add_warning(res.line, res.short_name, msg)

        # Missing description
        if not res.description or res.description == res.short_name:
            add_warning(res.line, res.short_name,
                "Missing or generic description. Add a descriptive name.")

        # Very long command (>200 chars)
        if run_cmd and len(run_cmd) > 200:
            if _needs_script_suggestion(run_cmd):
                msg = f"Run command is {len(run_cmd)} chars and contains shell control operators. Consider using the script keyword instead."
            else:
                msg = f"Run command is {len(run_cmd)} chars. Consider extracting to a script."
            if strict and not allow_complex:
                add_error(res.line, res.short_name, msg)
            else:
                add_warning(res.line, res.short_name, msg)

        # Default timeout (300s) on non-verify steps
        if res.timeout == 300 and res.run and not res.is_verify:
            # Only warn if the command looks like it could be long-running
            long_cmds = ("apt", "yum", "dnf", "pip", "npm", "docker", "make", "build", "compile")
            if any(lc in (res.run or "").lower() for lc in long_cmds):
                add_warning(res.line, res.short_name,
                    "Default 300s timeout on a potentially long-running command. Consider explicit 'timeout'.")

    # 'when' expressions referencing unknown variables (typo detection)
    for rid, res in graph.all_resources.items():
        if not res.when or res.is_barrier:
            continue
        # Extract bare identifiers from when expression (not quoted strings)
        when_expr = res.when.strip()
        for op in ("!=", "=="):
            if op in when_expr:
                parts = when_expr.split(op, 1)
                for part in parts:
                    token = part.strip().strip("'\"")
                    # Skip if it looks like a quoted literal, number, or known value
                    if part.strip().startswith(("'", '"')) or token in ("true", "false", "yes", "no", "0", "1", ""):
                        continue
                    if token not in graph.variables:
                        add_warning(res.line, res.short_name,
                            f"'when' expression references '{token}' which is not a known variable. "
                            f"If this is a literal value, quote it: '\"{token}\"'.")
                break
        else:
            # Truthy/falsy check (no operator)
            token = when_expr.lstrip("not ").strip().strip("'\"")
            if token and not when_expr.strip().startswith(("'", '"')) and token not in graph.variables:
                add_warning(res.line, res.short_name,
                    f"'when' expression references '{token}' which is not a known variable. "
                    f"If this is a literal value, quote it: '\"{token}\"'.")

    # 'each' loop steps writing to fixed paths (no loop variable in output)
    each_groups: dict[str, list[Resource]] = {}
    for rid, res in graph.all_resources.items():
        if res.parallel_group and "__each" in (res.parallel_group or ""):
            each_groups.setdefault(res.parallel_group, []).append(res)
    for gkey, items in each_groups.items():
        if len(items) < 2:
            continue
        # Check if run commands have output redirects to the same fixed path
        output_re = re.compile(r'(?:-o\s+|>\s*>?\s*)(\S+)')
        dest_counts: dict[str, int] = {}
        for item in items:
            for m in output_re.finditer(item.run or ""):
                dest = m.group(1)
                dest_counts[dest] = dest_counts.get(dest, 0) + 1
        for dest, count in dest_counts.items():
            if count > 1:
                parent_name = gkey.rsplit(".__each", 1)[0].rsplit(".", 1)[-1]
                add_warning(items[0].line, parent_name,
                    f"Each-loop branches write to the same path ({dest}). "
                    f"Include the loop variable in the path to avoid clobbering between concurrent iterations.")
                break

    # Duplicate collect keys on the same node
    collect_map: dict[str, list[tuple[str, int]]] = {}  # "node/key" → [(step_name, line)]
    for rid, res in graph.all_resources.items():
        if res.collect_key and not res.is_barrier:
            ck = f"{res.node_name}/{res.collect_key}"
            collect_map.setdefault(ck, []).append((res.short_name, res.line))
    for ck, steps in collect_map.items():
        if len(steps) > 1:
            node, key = ck.split("/", 1)
            step_names = ", ".join(s[0] for s in steps)
            add_warning(steps[0][1], key,
                f"Collect key '{key}' is used by multiple steps on node '{node}' ({step_names}). "
                f"Only the last to run will appear in reports.")

    # Unused variables
    for vname in graph.variables:
        if vname not in used_vars:
            # Check if used in template args or via props
            used_elsewhere = False
            for nn, hn in graph.nodes.items():
                for v in hn.via_props.values():
                    if f"${{{vname}}}" in v:
                        used_elsewhere = True
            if not used_elsewhere:
                add_warning(0, vname,
                    f"Variable '{vname}' is defined but may be unused.")

    # Race branches writing to the same destination without "race into"
    race_groups: dict[str, list[Resource]] = {}
    for rid, res in graph.all_resources.items():
        if res.is_race and res.parallel_group:
            race_groups.setdefault(res.parallel_group, []).append(res)
    for gkey, branches in race_groups.items():
        # Skip groups that already use race_into (safe by design)
        if any(b.race_into for b in branches):
            continue
        # Extract output destinations from run commands: -o FILE, > FILE, >> FILE
        output_re = re.compile(r'(?:-o\s+|>\s*>?\s*)(\S+)')
        all_dests: list[set[str]] = []
        for b in branches:
            dests = set(m.group(1) for m in output_re.finditer(b.run or ""))
            all_dests.append(dests)
        # Check for shared destinations across branches
        if len(all_dests) >= 2:
            shared = set.intersection(*all_dests) if all_dests else set()
            if shared:
                parent_name = gkey.rsplit(".__race", 1)[0].rsplit(".", 1)[-1]
                shared_str = ", ".join(sorted(shared))
                add_warning(branches[0].line, parent_name,
                    f"Race branches write to the same destination ({shared_str}). "
                    f"Use 'race into {shared_str}:' to avoid output clobbering between concurrent branches.")

    # Provisioning anti-patterns
    for rid, res in graph.all_resources.items():
        if res.is_barrier:
            continue
        run_cmd = res.run or ""
        allow_complex = strict and "cgr:allow-complex" in run_cmd

        # sed -i without a backup extension
        if re.search(r'\bsed\s+(-[a-z]*i(?!\.[a-z])[a-z]*\b|-[a-z]+\s+-i\b)', run_cmd):
            msg = "'sed -i' usage is brittle. Prefer 'line ... in FILE, replacing ...' instead."
            if strict and not allow_complex:
                add_error(res.line, res.short_name, msg)
            else:
                add_warning(res.line, res.short_name, msg)

        # echo/printf writing to /etc or system paths without using content keyword
        if re.search(r'\becho\b.*>\s*/(?:etc|usr|opt|srv)\b', run_cmd) or \
           re.search(r'\bprintf\b.*>\s*/(?:etc|usr|opt|srv)\b', run_cmd):
            msg = "Writing to a system path with echo/printf is not atomic. Use 'content > PATH:' instead."
            if strict and not allow_complex:
                add_error(res.line, res.short_name, msg)
            else:
                add_warning(res.line, res.short_name, msg)

        # printf/echo config generation anywhere
        if re.search(r'\b(?:printf|echo)\b.*>\s*\S+', run_cmd):
            add_warning(res.line, res.short_name,
                "Inline file generation in shell is hard to review and not crash-safe. "
                "Prefer 'content > PATH:' or a structured provisioning primitive.")

        # tee writing to system paths
        if re.search(r'\btee\b\s+/(?:etc|usr|opt|srv)\b', run_cmd):
            add_warning(res.line, res.short_name,
                "Writing to a system path with tee is not atomic. "
                "Use 'content > PATH:' for crash-safe atomic writes.")

        # chained sed -i on the same file
        if run_cmd.count("sed -i") >= 2:
            add_warning(res.line, res.short_name,
                "Multiple 'sed -i' edits in one shell command are brittle. "
                "Prefer 'line \"...\" in FILE, replacing \"REGEX\"'.")

        # local target manually calling ssh
        node = graph.nodes.get(res.node_name)
        if node and node.via_method == "local" and re.search(r'(^|\s)ssh\s', run_cmd):
            add_warning(res.line, res.short_name,
                "Local step shells out to ssh manually. Prefer SSH targets so steps stay visible in the DAG.")

        # conditional flag construction in shell
        if re.search(r'if\s+\[\s+-n\s+"?\$\{[^}]+\}"?\s*\].*FLAG=', run_cmd):
            add_warning(res.line, res.short_name,
                "Shell-built optional flags are fragile. Prefer 'flag \"...\" when \"...\"'.")

        # polling loop in shell
        if re.search(r'\bwhile\b.*\bsleep\b.*\bdone\b', run_cmd, re.DOTALL):
            add_warning(res.line, res.short_name,
                "Polling loop hidden inside shell. Prefer 'until \"VALUE\"' with 'retry'.")

        # cp over a system config without backup
        if re.search(r'\bcp\b(?!\s+-b)(?!\s+.*--backup)\s+\S+\s+/(?:etc|usr)\b', run_cmd):
            add_warning(res.line, res.short_name,
                "'cp' overwrites a system config without a backup flag. "
                "Use 'put SRC > DEST' (auto-backs up) or add '--backup=numbered'.")

    # Provisioning steps with writes but no validate clause
    for rid, res in graph.all_resources.items():
        if res.is_barrier:
            continue
        has_prov_write = bool(
            res.prov_content_inline is not None or res.prov_content_from or
            res.prov_lines or res.prov_put_src or
            res.prov_block_dest or res.prov_ini_dest or res.prov_json_dest
        )
        if has_prov_write and not res.prov_validate and not res.run and not res.script_path:
            # Only warn for steps touching known config file extensions
            dest = (res.prov_dest or res.prov_block_dest or res.prov_ini_dest or
                    res.prov_json_dest or "")
            if re.search(r'\.(conf|cfg|ini|json|yaml|yml|toml|env|service|timer)$', dest):
                add_warning(res.line, res.short_name,
                    f"Provisioning step writes to '{dest}' without a 'validate $' clause. "
                    f"Add 'validate $ <check-command>' to revert on failure.")

    # Templates without version constraints
    source = path.read_text()
    if path.suffix == ".cgr":
        for line in source.splitlines():
            if line.strip().startswith("using "):
                parts = line.strip()[5:].strip()
                for part in parts.split(","):
                    p = part.strip()
                    if p and not re.search(r'(>=|~>|=)\s*\d', p):
                        add_warning(0, p.strip('"').strip("'"),
                            f"Template '{p.strip()}' imported without version constraint. Consider 'using {p.strip()} >= 1.0'.")

    return warnings, errors


def cmd_lint(filepath, repo_dir=None, extra_vars=None, inventory_files=None, strict=False,
             vault_passphrase=None, vault_prompt=False):
    """Check for best-practice issues."""
    path = Path(filepath)
    if not path.exists():
        print(red(f"error: not found: {path}")); sys.exit(1)

    # First validate
    try:
        graph = _load(filepath, repo_dir=repo_dir, extra_vars=extra_vars,
                       raise_on_error=True, inventory_files=inventory_files,
                       vault_passphrase=vault_passphrase, vault_prompt=vault_prompt)
    except ValueError as e:
        print(red(f"error: {e}")); sys.exit(1)

    warnings, errors = _collect_lint_findings(graph, path, strict=strict)

    # Print results
    print()
    print(bold(f"  Lint: {filepath}"))
    print(bold("─" * 64))

    if strict and not errors and not warnings:
        print(f"  {green('✓ No warnings.')}")
    elif not strict and not warnings:
        print(f"  {green('✓ No warnings.')}")
    else:
        for line, name, msg in sorted(errors, key=lambda w: w[0]):
            loc = f"line {line}" if line else "global"
            print(f"  {red('error:')} {bold(name)} {dim(f'({loc})')}")
            print(f"    {msg}")
            print()
        for line, name, msg in sorted(warnings, key=lambda w: w[0]):
            loc = f"line {line}" if line else "global"
            print(f"  {yellow('⚠')} {bold(name)} {dim(f'({loc})')}")
            print(f"    {msg}")
            print()

    print(bold("─" * 64))
    if strict:
        print(f"  {len(errors)} error(s) found — use # cgr:allow-complex to suppress")
    else:
        print(f"  {len(warnings)} warning(s) found")
    print()
    return len(errors) if strict else len(warnings)

def _validate_script_paths(graph: Graph):
    def _check_res(res: Resource):
        if not res.script_path:
            return
        script_fs_path = _resolve_script_fs_path(res.script_path, graph.graph_file)
        if not os.path.isfile(script_fs_path):
            raise ResolveError(
                f"Script file not found for step '{res.short_name}': {res.script_path}\n"
                f"  Resolved to: {script_fs_path}"
            )
    for res in graph.all_resources.values():
        _check_res(res)
    if graph.on_complete:
        _check_res(graph.on_complete)
    if graph.on_failure:
        _check_res(graph.on_failure)


# ── Diff command (structural graph comparison) ───────────────────────

def cmd_diff(file_a, file_b, repo_dir=None, extra_vars=None, inventory_files=None, json_output=False,
             vault_passphrase=None, vault_prompt=False):
    """Compare two graph files structurally. Reports added/removed/changed resources and dependencies."""
    try:
        graph_a = _load(file_a, repo_dir=repo_dir, extra_vars=extra_vars, raise_on_error=True,
                        inventory_files=inventory_files, vault_passphrase=vault_passphrase,
                        vault_prompt=vault_prompt)
    except (ValueError, SystemExit) as e:
        print(red(f"error loading {file_a}: {e}")); sys.exit(1)
    try:
        graph_b = _load(file_b, repo_dir=repo_dir, extra_vars=extra_vars, raise_on_error=True,
                        inventory_files=inventory_files, vault_passphrase=vault_passphrase,
                        vault_prompt=vault_prompt)
    except (ValueError, SystemExit) as e:
        print(red(f"error loading {file_b}: {e}")); sys.exit(1)

    # Build resource maps keyed by a semantic identifier rather than the raw
    # resource id. This avoids false diffs when equivalent graphs generate
    # auto-named verify ids from different source line numbers.
    def _res_key(res):
        if res.is_verify and re.fullmatch(r"__verify_\d+", res.short_name or ""):
            desc = res.description or ""
            run = res.run or ""
            return f"{res.node_name}.__verify__::{desc}::{run}"
        return f"{res.node_name}.{res.short_name}"

    def _build_maps(graph):
        key_to_res = {}
        rid_to_key = {}
        for rid, res in graph.all_resources.items():
            if res.is_barrier:
                continue
            key = _res_key(res)
            key_to_res[key] = res
            rid_to_key[rid] = key
        return key_to_res, rid_to_key

    a_map, a_rid_to_key = _build_maps(graph_a)
    b_map, b_rid_to_key = _build_maps(graph_b)
    a_keys = set(a_map.keys()); b_keys = set(b_map.keys())

    added = sorted(b_keys - a_keys)
    removed = sorted(a_keys - b_keys)
    common = sorted(a_keys & b_keys)

    changed = []; deps_added = []; deps_removed = []; moved = []
    for key in common:
        ra, rb = a_map[key], b_map[key]
        diffs = []
        if ra.run != rb.run: diffs.append("command")
        if ra.check != rb.check: diffs.append("check")
        if ra.script_path != rb.script_path: diffs.append("script")
        if ra.run_as != rb.run_as: diffs.append("run_as")
        if (ra.http_method, ra.http_url, tuple(ra.http_headers), ra.http_body,
            ra.http_body_type, ra.http_auth, ra.http_expect) != (
            rb.http_method, rb.http_url, tuple(rb.http_headers), rb.http_body,
            rb.http_body_type, rb.http_auth, rb.http_expect):
            diffs.append("http")
        if (ra.wait_kind, ra.wait_target) != (rb.wait_kind, rb.wait_target):
            diffs.append("wait")
        if (ra.subgraph_path, tuple(sorted((ra.subgraph_vars or {}).items()))) != (
            rb.subgraph_path, tuple(sorted((rb.subgraph_vars or {}).items()))):
            diffs.append("subgraph")
        if (ra.collect_key, ra.collect_format, ra.collect_var) != (
            rb.collect_key, rb.collect_format, rb.collect_var):
            diffs.append("collect")
        if (ra.reduce_key, ra.reduce_var) != (rb.reduce_key, rb.reduce_var):
            diffs.append("reduce")
        if (tuple(ra.on_success_set), tuple(ra.on_failure_set)) != (
            tuple(rb.on_success_set), tuple(rb.on_failure_set)):
            diffs.append("bindings")
        if ra.when != rb.when: diffs.append("when")
        if ra.env != rb.env or ra.env_when != rb.env_when: diffs.append("env")
        if tuple(ra.flags) != tuple(rb.flags): diffs.append("flags")
        if ra.until != rb.until: diffs.append("until")
        if (ra.timeout, ra.timeout_reset_on_output) != (rb.timeout, rb.timeout_reset_on_output):
            diffs.append("timeout")
        if (ra.retries, ra.retry_delay, ra.retry_backoff, ra.retry_backoff_max, ra.retry_jitter_pct) != (
            rb.retries, rb.retry_delay, rb.retry_backoff, rb.retry_backoff_max, rb.retry_jitter_pct):
            diffs.append("retry")
        if tuple(ra.tags) != tuple(rb.tags): diffs.append("tags")
        if diffs:
            changed.append((key, diffs))
        # Dependency changes
        a_deps = {a_rid_to_key.get(dep, dep) for dep in ra.needs}
        b_deps = {b_rid_to_key.get(dep, dep) for dep in rb.needs}
        for d in b_deps - a_deps:
            deps_added.append((key, d.split(".")[-1] if "." in d else d))
        for d in a_deps - b_deps:
            deps_removed.append((key, d.split(".")[-1] if "." in d else d))

    # Wave movement
    a_wave_map = {}
    for wi, wave in enumerate(graph_a.waves):
        for rid in wave:
            r = graph_a.all_resources[rid]
            if not r.is_barrier:
                a_wave_map[a_rid_to_key[rid]] = wi + 1
    b_wave_map = {}
    for wi, wave in enumerate(graph_b.waves):
        for rid in wave:
            r = graph_b.all_resources[rid]
            if not r.is_barrier:
                b_wave_map[b_rid_to_key[rid]] = wi + 1
    for key in common:
        aw = a_wave_map.get(key, 0); bw = b_wave_map.get(key, 0)
        if aw != bw:
            moved.append((key, aw, bw))

    has_changes = added or removed or changed or deps_added or deps_removed or moved

    if json_output:
        result = {
            "file_a": file_a, "file_b": file_b,
            "resources_a": len(a_map), "resources_b": len(b_map),
            "waves_a": len(graph_a.waves), "waves_b": len(graph_b.waves),
            "added": [k.split(".", 1)[1] for k in added],
            "removed": [k.split(".", 1)[1] for k in removed],
            "changed": [{"resource": k.split(".", 1)[1], "fields": f} for k, f in changed],
            "deps_added": [{"resource": k.split(".", 1)[1], "dep": d} for k, d in deps_added],
            "deps_removed": [{"resource": k.split(".", 1)[1], "dep": d} for k, d in deps_removed],
            "moved": [{"resource": k.split(".", 1)[1], "wave_before": a, "wave_after": b} for k, a, b in moved],
            "identical": not has_changes,
        }
        print(json.dumps(result, indent=2))
        return 0 if not has_changes else 1

    # Human-readable output
    print(f"\n  {bold('cgr diff')} {dim(file_a)} → {dim(file_b)}")
    print(f"  {dim(f'{len(a_map)} resources, {len(graph_a.waves)} waves')} → {dim(f'{len(b_map)} resources, {len(graph_b.waves)} waves')}")
    print(bold("─" * 64))

    if not has_changes:
        print(green("  Graphs are structurally identical."))
        print(); return 0

    if added:
        for k in added:
            name = k.split(".", 1)[1]
            print(f"  {green('+ Added:  ')} {bold(name)}")
    if removed:
        for k in removed:
            name = k.split(".", 1)[1]
            print(f"  {red('- Removed:')} {bold(name)}")
    if changed:
        for k, fields in changed:
            name = k.split(".", 1)[1]
            print(f"  {yellow('~ Changed:')} {bold(name)} — {', '.join(fields)} differs")
    if deps_added:
        for k, dep in deps_added:
            name = k.split(".", 1)[1]
            print(f"  {cyan('+ Dep:   ')} {bold(name)} now depends on {dep}")
    if deps_removed:
        for k, dep in deps_removed:
            name = k.split(".", 1)[1]
            print(f"  {cyan('- Dep:   ')} {bold(name)} no longer depends on {dep}")
    if moved:
        for k, aw, bw in moved:
            name = k.split(".", 1)[1]
            print(f"  {dim('↔ Moved: ')} {bold(name)} wave {aw} → {bw}")

    print(bold("─" * 64))
    parts = []
    if added: parts.append(green(f"{len(added)} added"))
    if removed: parts.append(red(f"{len(removed)} removed"))
    if changed: parts.append(yellow(f"{len(changed)} changed"))
    if moved: parts.append(dim(f"{len(moved)} moved"))
    print(f"  {', '.join(parts)}")
    print()
    return 1 if has_changes else 0


# ── Template test command ─────────────────────────────────────────────

def cmd_test_template(repo_dir: str|None = None, template_path: str|None = None, verbose: bool = False):
    """Test templates: parse, resolve, and dry-run with mock parameter values."""
    repo_dirs = _build_repo_search_path(explicit_repo=repo_dir)
    existing = [d for d in repo_dirs if Path(d).is_dir()]
    if not existing:
        print(red("error: no template repo found")); sys.exit(1)

    # Discover templates to test
    templates: list[Path] = []
    if template_path:
        # Test a single template by path (e.g. "apt/install_package")
        fp = _find_template_file(repo_dirs, template_path)
        if fp:
            templates.append(fp)
        else:
            print(red(f"error: template '{template_path}' not found")); sys.exit(1)
    else:
        # Test all templates in all repos
        for rd in existing:
            templates.extend(sorted(Path(rd).rglob("*.cgr")))
            templates.extend(sorted(Path(rd).rglob("*.cg")))

    passed = 0; failed = 0; errors = []

    for tpl_path in templates:
        # Derive a short display name
        for rd in existing:
            try:
                rel = tpl_path.relative_to(rd)
                name = str(rel.with_suffix(""))
                break
            except ValueError:
                name = str(tpl_path)

        try:
            # Phase 1: Parse
            source = tpl_path.read_text()
            if tpl_path.suffix == ".cgr":
                prog = parse_cgr(source, str(tpl_path))
            else:
                tokens = lex(source, str(tpl_path))
                parser = Parser(tokens, source, str(tpl_path))
                prog = parser.parse()

            if not prog.templates:
                errors.append((name, "No template block found"))
                failed += 1
                if verbose:
                    print(f"  {red('✗')} {name}: no template block")
                continue

            tpl = prog.templates[0]

            # Phase 2: Resolve with mock params
            mock_args = {}
            for p in tpl.params:
                if p.default is not None:
                    mock_args[p.name] = p.default
                else:
                    mock_args[p.name] = f"__test_{p.name}__"

            # Build a minimal graph to test expansion
            collector: dict[str, Resource] = {}
            dedup_hashes: dict[str, str] = {}
            dedup_map: dict[str, list[str]] = defaultdict(list)
            prov: list[Provenance] = []
            tpl.source_file = name

            root_id = _expand_template(
                tpl, mock_args, "__test__", {},
                collector, dedup_hashes, dedup_map, prov,
            )

            # Phase 3: Verify expansion produced resources
            if not collector:
                errors.append((name, "Template expanded to zero resources"))
                failed += 1
                if verbose:
                    print(f"  {red('✗')} {name}: zero resources after expansion")
                continue

            # Phase 4: Check all resources have an executable action or descendants
            for rid, res in collector.items():
                if res.is_barrier:
                    continue
                if res.when and not _eval_when(res.when, {}):
                    continue
                has_descendants = any(other_id.startswith(rid + ".") for other_id in collector if other_id != rid)
                has_provisioning = bool(
                    res.prov_content_inline is not None or res.prov_content_from or
                    res.prov_lines or res.prov_put_src or
                    res.prov_block_dest or res.prov_ini_dest or
                    res.prov_json_dest or res.prov_asserts
                )
                has_action = bool(
                    res.run or res.script_path or res.http_method or
                    res.wait_kind or res.subgraph_path or res.reduce_key or
                    has_provisioning
                )
                if not has_action and not has_descendants:
                    errors.append((name, f"Resource '{res.short_name}' has no executable action"))
                    failed += 1
                    if verbose:
                        print(f"  {red('✗')} {name}: resource '{res.short_name}' missing executable action")
                    break
            else:
                passed += 1
                if verbose:
                    param_str = ", ".join(f"{p.name}{'='+repr(p.default) if p.default else ''}" for p in tpl.params)
                    print(f"  {green('✓')} {name}({param_str}) — {len(collector)} resource(s)")

        except Exception as e:
            errors.append((name, str(e)))
            failed += 1
            if verbose:
                print(f"  {red('✗')} {name}: {e}")

    # Summary
    print(bold(f"\n{'═'*50}"))
    print(bold("  cgr test — template validation"))
    print(bold(f"{'═'*50}\n"))
    total = passed + failed
    print(f"  {green(str(passed))} passed, {red(str(failed)) if failed else '0'} failed out of {total} template(s)\n")
    if errors and not verbose:
        print("  Failures:")
        for name, msg in errors:
            print(f"    {red('✗')} {name}: {msg}")
        print()
    if failed:
        sys.exit(1)


# ── Init command (scaffold new graph file) ────────────────────────────

def cmd_doctor():
    """Run environment diagnostics and print a checklist."""
    import shutil
    checks = []

    # Python version
    ver = sys.version.split()[0]
    major, minor = sys.version_info[:2]
    ok = (major, minor) >= (3, 9)
    checks.append((ok, f"Python {ver}", "Requires 3.9+" if not ok else ""))

    # SSH client
    ssh = shutil.which("ssh")
    checks.append((ssh is not None, "SSH client (ssh)", f"Found: {ssh}" if ssh else "Not found — install openssh-client"))
    if ssh:
        # Check ControlMaster support
        try:
            r = subprocess.run([ssh, "-o", "ControlMaster=auto", "-o", "ControlPath=/dev/null",
                                "-o", "BatchMode=yes", "localhost", "true"],
                               capture_output=True, text=True, timeout=3)
            # Any exit is fine — we just want to confirm the flag is accepted
            checks.append((True, "SSH ControlMaster", "Supported"))
        except (subprocess.TimeoutExpired, FileNotFoundError):
            checks.append((True, "SSH ControlMaster", "Could not verify (timeout)"))

    # sudo
    sudo = shutil.which("sudo")
    checks.append((sudo is not None, "sudo", f"Found: {sudo}" if sudo else "Not found — needed for run_as/as root"))

    # Repo directories
    repo_dirs = _build_repo_search_path()
    found_repo = False
    for rd in repo_dirs:
        if Path(rd).is_dir():
            count = sum(1 for _ in Path(rd).rglob("*.cgr"))
            checks.append((True, f"Template repo: {rd}", f"{count} template(s)"))
            found_repo = True
            break
    if not found_repo:
        checks.append((False, "Template repo", f"Not found in: {', '.join(repo_dirs[:3])}"))

    # Secrets key check
    secrets_key_env = os.environ.get("CGR_VAULT_PASS", "")
    checks.append((bool(secrets_key_env), "Vault passphrase (CGR_VAULT_PASS)", "Set" if secrets_key_env else "Not set (optional — for cgr secrets)"))

    # Graph files in CWD
    cwd_files = list(Path(".").glob("*.cgr")) + list(Path(".").glob("*.cg"))
    checks.append((len(cwd_files) > 0, f"Graph files in CWD", f"{len(cwd_files)} found" if cwd_files else "None — run cgr init to create one"))

    # Print results
    print(bold(f"\n{'═'*50}"))
    print(bold("  cgr doctor — environment check"))
    print(bold(f"{'═'*50}\n"))
    ok_count = sum(1 for ok, _, _ in checks if ok)
    for ok, label, detail in checks:
        icon = green("✓") if ok else (yellow("○") if "optional" in detail.lower() or "not set" in detail.lower() else red("✗"))
        print(f"  {icon} {label}")
        if detail:
            print(f"    {dim(detail)}")
    print(f"\n  {ok_count}/{len(checks)} checks passed")
    print(bold(f"{'═'*50}\n"))


def cmd_init(filepath=None):
    """Create a minimal .cgr scaffold file."""
    if filepath is None:
        filepath = "setup.cgr"
    path = Path(filepath)
    if path.exists():
        print(red(f"error: {filepath} already exists. Remove it first or choose a different name.")); sys.exit(1)

    scaffold = '''--- My Setup ---

# Variables — define reusable values
set app_name = "myapp"

# Uncomment to import templates from the repo:
# using apt/install_package

# Uncomment to use an inventory file:
# inventory "hosts.ini"

target "local" local:
  [check system]:
    run $ uname -a

  # Add more steps here:
  # [install dependencies]:
  #   skip if $ dpkg -l | grep -q mypackage
  #   run $ apt-get install -y mypackage

  # [configure app]:
  #   first [install dependencies]
  #   skip if $ test -f /etc/${app_name}/config.yml
  #   run $ mkdir -p /etc/${app_name} && cp config.yml /etc/${app_name}/

# SSH target example:
# target "webserver" ssh deploy@web-01:
#   [deploy code]:
#     run $ cd /opt/${app_name} && git pull
'''
    path.write_text(scaffold)
    print(green(f"✓ Created {filepath}"))
    print(f"  Edit it, then: cgr plan {filepath}")


# ── Secrets management ────────────────────────────────────────────────

_SECRETS_MAGIC = b"CGRSECRT"
_SECRETS_VERSION_V2 = 0x02

def _derive_keys(passphrase: str, salt: bytes) -> tuple[bytes, bytes]:
    """Derive separate AES and HMAC keys from passphrase + salt using PBKDF2."""
    key_material = hashlib.pbkdf2_hmac('sha256', passphrase.encode(), salt, 100000, dklen=64)
    return key_material[:32], key_material[32:]

def _derive_key(passphrase: str, salt: bytes) -> bytes:
    """Backward-compatible helper for callers that expect only the AES key."""
    enc_key, _ = _derive_keys(passphrase, salt)
    return enc_key


def _encrypt_data(data: bytes, passphrase: str) -> bytes:
    """Encrypt data with AES-256-CBC using PBKDF2 key derivation.
    Format v2: 1-byte version + 8-byte magic + 16-byte salt + 16-byte IV +
    ciphertext + 32-byte HMAC-SHA256."""
    salt = os.urandom(16)
    key, mac_key = _derive_keys(passphrase, salt)
    iv = os.urandom(16)
    # Use openssl for AES encryption (zero deps)
    try:
        proc = subprocess.run(
            ["openssl", "enc", "-aes-256-cbc", "-nosalt", "-K", key.hex(), "-iv", iv.hex()],
            input=data, capture_output=True, timeout=10)
        if proc.returncode != 0:
            raise RuntimeError(f"openssl enc failed: {proc.stderr.decode()[:200]}")
        ciphertext = proc.stdout
    except FileNotFoundError:
        raise RuntimeError("openssl not found. Install openssl to use encrypted secrets.")
    header = bytes([_SECRETS_VERSION_V2]) + _SECRETS_MAGIC + salt + iv
    tag = hmac.new(mac_key, header + ciphertext, hashlib.sha256).digest()
    return header + ciphertext + tag


def _decrypt_data(data: bytes, passphrase: str) -> bytes:
    """Decrypt data encrypted by _encrypt_data."""
    if len(data) < 8:
        raise ValueError("Not a valid .secrets file (bad magic header)")
    if data[0] == _SECRETS_VERSION_V2:
        if len(data) < 1 + len(_SECRETS_MAGIC) + 16 + 16 + 32:
            raise ValueError("Not a valid .secrets file (truncated v2 payload)")
        magic = data[1:9]
        if magic != _SECRETS_MAGIC:
            raise ValueError("Not a valid .secrets file (bad magic header)")
        salt = data[9:25]
        iv = data[25:41]
        ciphertext = data[41:-32]
        tag = data[-32:]
        key, mac_key = _derive_keys(passphrase, salt)
        expected_tag = hmac.new(mac_key, data[:-32], hashlib.sha256).digest()
        if not hmac.compare_digest(tag, expected_tag):
            raise ValueError("Decryption failed — integrity check failed")
    else:
        magic = data[:8]
        if magic != _SECRETS_MAGIC:
            raise ValueError("Not a valid .secrets file (bad magic header)")
        salt = data[8:24]
        iv = data[24:40]
        ciphertext = data[40:]
        key = _derive_key(passphrase, salt)
        warnings.warn(
            "secrets file uses legacy format without integrity protection; re-encrypt with `cgr secrets rekey`",
            DeprecationWarning,
            stacklevel=2,
        )
    try:
        proc = subprocess.run(
            ["openssl", "enc", "-aes-256-cbc", "-d", "-nosalt", "-K", key.hex(), "-iv", iv.hex()],
            input=ciphertext, capture_output=True, timeout=10)
        if proc.returncode != 0:
            raise ValueError("Decryption failed — wrong passphrase?")
        return proc.stdout
    except FileNotFoundError:
        raise RuntimeError("openssl not found. Install openssl to use encrypted secrets.")


def _resolve_vault_pass_source(args=None) -> tuple[str | None, bool]:
    """Return (passphrase, prompt_requested) from CLI flags or environment."""
    cli_pass = getattr(args, 'vault_pass', None) if args else None
    if cli_pass is not None:
        return cli_pass, False

    if args and getattr(args, 'vault_pass_ask', False):
        return None, True

    vault_pass_file = getattr(args, 'vault_pass_file', None) if args else None
    if vault_pass_file:
        vf = Path(vault_pass_file)
        if not vf.exists():
            print(red(f"error: vault pass file not found: {vf}")); sys.exit(1)
        return vf.read_text().strip(), False

    env_pass = os.environ.get('CGR_VAULT_PASS')
    if env_pass:
        return env_pass, False
    return None, False


def _get_vault_pass(args=None) -> str:
    """Resolve vault passphrase from CLI flags, env var, or TTY prompt."""
    passphrase, prompt_requested = _resolve_vault_pass_source(args)
    if passphrase:
        return passphrase
    if prompt_requested:
        import getpass
        return getpass.getpass("Vault passphrase: ")
    print(red("error: No vault passphrase. Use --vault-pass-ask (-A), --vault-pass PASS, --vault-pass-file, or CGR_VAULT_PASS env var."))
    sys.exit(1)


def _secure_delete_text_file(path_str: str):
    """Best-effort wipe of a temporary plaintext file before unlinking it."""
    try:
        fpath = Path(path_str)
        if not fpath.exists():
            return
        size = fpath.stat().st_size
        if size > 0:
            with fpath.open("r+b") as fh:
                fh.write(b"\x00" * size)
                fh.flush()
                os.fsync(fh.fileno())
        fpath.unlink()
    except Exception:
        try:
            Path(path_str).unlink(missing_ok=True)
        except Exception:
            pass


def _masked_secret_display() -> str:
    """Return a non-reversible display string for secret values."""
    return "<hidden>"


def _add_vault_pass_args(ap):
    """Add standard vault-pass CLI flags to a parser."""
    ap.add_argument("--vault-pass", default=None, metavar="PASS",
                    help="Vault passphrase for encrypted secrets")
    ap.add_argument("-A", "--vault-pass-ask", action="store_true",
                    help="Prompt for vault passphrase interactively")
    ap.add_argument("--vault-pass-file", default=None, metavar="FILE",
                    help="Read vault passphrase from FILE")


def _load_secrets(secrets_path: str, passphrase: str) -> dict[str, str]:
    """Load and decrypt a .secrets file, returning key-value pairs."""
    data = Path(secrets_path).read_bytes()
    plaintext = _decrypt_data(data, passphrase)
    result = {}
    for line in plaintext.decode().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        m = re.match(r'(\w+)\s*=\s*("(?:(?:\\.)|[^"\\])*"|\S+)', line)
        if m:
            raw_value = m.group(2)
            if raw_value.startswith('"'):
                try:
                    result[m.group(1)] = json.loads(raw_value)
                    continue
                except json.JSONDecodeError:
                    pass
            result[m.group(1)] = raw_value
    return result


def _read_secret_add_value(cli_value: str | None) -> str:
    if cli_value is not None:
        print(red("error: Refusing to read secret values from command-line arguments. Omit VALUE and enter it at the prompt, or pipe it on stdin."))
        sys.exit(1)
    if sys.stdin.isatty():
        import getpass
        return getpass.getpass("Secret value: ")
    return sys.stdin.read().rstrip("\r\n")


def _format_secret_assignment(key: str, secret_value: str, *, include_timestamp: bool = False) -> str:
    if not re.match(r'^\w+$', key):
        print(red("error: Secret key must contain only letters, numbers, and underscores."))
        sys.exit(1)
    if "\n" in secret_value or "\r" in secret_value:
        print(red("error: Secret values cannot contain newlines."))
        sys.exit(1)
    line = f"{key} = {json.dumps(secret_value)}"
    if include_timestamp:
        line += f'  # @updated {time.strftime("%Y-%m-%d")}'
    return line


def _write_encrypted_secrets(path: Path, plaintext: bytes, passphrase: str):
    encrypted = _encrypt_data(plaintext, passphrase)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        with os.fdopen(fd, "wb") as f:
            fd = None
            f.write(encrypted)
            f.flush()
            os.fsync(f.fileno())
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def cmd_secrets(action, filepath, key=None, value=None, vault_args=None):
    """Manage encrypted secrets files."""
    path = Path(filepath)

    if action == "create":
        if path.exists():
            print(red(f"error: {filepath} already exists.")); sys.exit(1)
        passphrase = _get_vault_pass(vault_args)
        # Confirm
        if sys.stdin.isatty():
            import getpass
            confirm = getpass.getpass("Confirm passphrase: ")
            if confirm != passphrase:
                print(red("error: Passphrases do not match.")); sys.exit(1)
        _write_encrypted_secrets(path, b"", passphrase)
        print(green(f"✓ Created encrypted secrets file: {filepath}"))

    elif action == "view":
        if not path.exists():
            print(red(f"error: {filepath} not found")); sys.exit(1)
        passphrase = _get_vault_pass(vault_args)
        show_values = bool(getattr(vault_args, "show_values", False))
        try:
            secrets = _load_secrets(filepath, passphrase)
            if show_values:
                print(yellow("  warning: plaintext secret display is disabled; showing masked values instead."))
            for k in secrets:
                print(f"  {cyan(k)}")
            if not secrets:
                print(dim("  (empty)"))
        except (ValueError, RuntimeError) as e:
            print(red(f"error: {e}")); sys.exit(1)

    elif action == "add":
        if not path.exists():
            print(red(f"error: {filepath} not found")); sys.exit(1)
        if not key:
            print(red("Usage: cgr secrets add FILE KEY")); sys.exit(1)
        secret_value = _read_secret_add_value(value)
        passphrase = _get_vault_pass(vault_args)
        try:
            data = path.read_bytes()
            plaintext = _decrypt_data(data, passphrase).decode()
        except (ValueError, RuntimeError) as e:
            print(red(f"error: {e}")); sys.exit(1)
        # Update or add the key
        updated = False
        new_lines = []
        for line in plaintext.splitlines():
            m = re.match(r'(\w+)\s*=', line.strip())
            if m and m.group(1) == key:
                new_lines.append(_format_secret_assignment(key, secret_value))
                updated = True
            else:
                new_lines.append(line)
        if not updated:
            new_lines.append(_format_secret_assignment(key, secret_value, include_timestamp=True))
        else:
            # Update the timestamp on the modified line
            new_lines = [re.sub(r'#\s*@updated\s+\S+', f'# @updated {time.strftime("%Y-%m-%d")}', l)
                         if l.strip().startswith(f'{key} ') else l for l in new_lines]
        _write_encrypted_secrets(path, "\n".join(new_lines).encode(), passphrase)
        print(green(f"✓ {'Updated' if updated else 'Added'} '{key}' in {filepath}"))

    elif action == "rm":
        if not path.exists():
            print(red(f"error: {filepath} not found")); sys.exit(1)
        if not key:
            print(red("Usage: cgr secrets rm FILE KEY")); sys.exit(1)
        passphrase = _get_vault_pass(vault_args)
        try:
            data = path.read_bytes()
            plaintext = _decrypt_data(data, passphrase).decode()
        except (ValueError, RuntimeError) as e:
            print(red(f"error: {e}")); sys.exit(1)
        new_lines = []
        removed = False
        for line in plaintext.splitlines():
            m = re.match(r'(\w+)\s*=', line.strip())
            if m and m.group(1) == key:
                removed = True
            else:
                new_lines.append(line)
        if not removed:
            print(yellow(f"  Key '{key}' not found in {filepath}")); return
        _write_encrypted_secrets(path, "\n".join(new_lines).encode(), passphrase)
        print(green(f"✓ Removed '{key}' from {filepath}"))

    elif action == "edit":
        if not path.exists():
            print(red(f"error: {filepath} not found")); sys.exit(1)
        passphrase = _get_vault_pass(vault_args)
        try:
            data = path.read_bytes()
            plaintext = _decrypt_data(data, passphrase).decode()
        except (ValueError, RuntimeError) as e:
            print(red(f"error: {e}")); sys.exit(1)
        editor = os.environ.get("EDITOR", "vi")
        import tempfile
        fd, tmp_path = tempfile.mkstemp(suffix='.txt')
        try:
            os.chmod(tmp_path, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as tmp:
                tmp.write(plaintext)
                tmp.flush()
                os.fsync(tmp.fileno())
        except Exception:
            try:
                os.close(fd)
            except OSError:
                pass
            _secure_delete_text_file(tmp_path)
            raise
        try:
            rc = subprocess.call([editor, tmp_path])
            if rc != 0:
                print(red(f"error: editor exited with {rc}")); return
            new_text = Path(tmp_path).read_text(encoding='utf-8')
            _write_encrypted_secrets(path, new_text.encode(), passphrase)
            print(green(f"✓ Secrets file updated: {filepath}"))
        finally:
            _secure_delete_text_file(tmp_path)

    elif action == "audit":
        if not path.exists():
            print(red(f"error: {filepath} not found")); sys.exit(1)
        passphrase = _get_vault_pass(vault_args)
        max_age_days = 90  # default rotation warning threshold
        if vault_args and getattr(vault_args, 'max_age', None):
            max_age_days = int(vault_args.max_age)
        try:
            data = path.read_bytes()
            plaintext = _decrypt_data(data, passphrase).decode()
        except (ValueError, RuntimeError) as e:
            print(red(f"error: {e}")); sys.exit(1)

        print(bold(f"\n{'═'*50}"))
        print(bold(f"  Secret rotation audit: {filepath}"))
        print(bold(f"  Max age: {max_age_days} days"))
        print(bold(f"{'═'*50}\n"))

        today = datetime.date.today()
        warnings_count = 0; total = 0
        for line in plaintext.splitlines():
            line_s = line.strip()
            if not line_s or line_s.startswith('#'):
                continue
            m = re.match(r'(\w+)\s*=', line_s)
            if not m:
                continue
            key_name = m.group(1)
            total += 1
            # Check for @updated annotation
            updated_m = re.search(r'#\s*@updated\s+(\d{4}-\d{2}-\d{2})', line_s)
            if updated_m:
                updated_date = datetime.date.fromisoformat(updated_m.group(1))
                age = (today - updated_date).days
                if age > max_age_days:
                    print(f"  {red('✗')} {bold(key_name):30s} {red(f'{age} days old')} (updated {updated_m.group(1)})")
                    warnings_count += 1
                else:
                    print(f"  {green('✓')} {bold(key_name):30s} {dim(f'{age} days old')} (updated {updated_m.group(1)})")
            else:
                print(f"  {yellow('○')} {bold(key_name):30s} {yellow('no rotation date')} — add '# @updated YYYY-MM-DD' annotation")
                warnings_count += 1

        print(f"\n  {total} secret(s), {warnings_count} warning(s)")
        if warnings_count:
            print(f"  {yellow('Rotate stale secrets and re-run audit.')}")
        else:
            print(f"  {green('All secrets are within rotation policy.')}")
        print()
        if warnings_count:
            sys.exit(1)

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
