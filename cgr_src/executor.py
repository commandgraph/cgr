"""Planning display, execution primitives, transport helpers, and apply logic."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.ast_nodes import *
from cgr_src.resolver import *

def cmd_plan(graph: Graph, *, verbose=False, include_tags=None, exclude_tags=None):
    print(); print(bold("CommandGraph Plan"))
    print(bold("─" * 64))
    for nn, hn in graph.nodes.items():
        if hn.via_method=="ssh":
            u=hn.via_props.get("user","?"); h=hn.via_props.get("host","?")
            p=hn.via_props.get("port","22")
            print(f"  Node {cyan(bold(nn))}  →  ssh {u}@{h}:{p}")
        else: print(f"  Node {cyan(bold(nn))}  →  local")
    # Dedup summary
    shared = {h: ids for h, ids in graph.dedup_map.items() if len(ids) > 1}
    if shared:
        dedup_count = sum(len(ids)-1 for ids in shared.values())
        print(f"  Deduplicated: {green(str(dedup_count))} resource(s) shared across call sites")
    if include_tags:
        print(f"  Tags: {', '.join(cyan(t) for t in sorted(include_tags))}")
    if exclude_tags:
        print(f"  Skip tags: {', '.join(yellow(t) for t in sorted(exclude_tags))}")
    if graph.stateless:
        print(f"  Mode: {dim('stateless (no .state file)')}")
    print(bold("─" * 64)); print()

    total = sum(len(w) for w in graph.waves)
    n_check = n_run = n_verify = n_tag_skip = 0

    current_phase: str|None = object()  # sentinel — never matches a real phase name
    for wi, wave in enumerate(graph.waves):
        print(dim(f"  ── Wave {wi+1} ({len(wave)} resources) ──"))
        for rid in wave:
            res = graph.all_resources[rid]
            # Print phase header on first appearance or phase transition
            if res.cgr_phase_name != current_phase:
                current_phase = res.cgr_phase_name
                if current_phase is not None:
                    print(f"    {cyan('Phase:')} {bold(current_phase)}")
            if _should_skip_tags(res, include_tags, exclude_tags):
                n_tag_skip += 1
                tag_str = ", ".join(res.tags) if res.tags else "untagged"
                print(f"    {dim('○ skip-tag')}  {bold(f'{res.short_name:26s}')}{dim(f'  [{tag_str}]')}")
                print()
                continue
            if res.is_verify:      sym = magenta("⧫ verify "); n_verify += 1
            elif res.check and res.check.strip().lower() != "false":
                                   sym = blue("◆ check→run"); n_check += 1
            else:                  sym = green("▶ run      "); n_run += 1

            needs_str = ""
            if res.needs:
                ns = []
                for d in res.needs:
                    if d in graph.all_resources:
                        dep_res = graph.all_resources[d]
                        if dep_res.node_name != res.node_name:
                            ns.append(cyan(f"{dep_res.node_name}/") + dep_res.short_name)
                        else:
                            ns.append(dep_res.short_name)
                    else:
                        ns.append(d.split(".")[-1])
                needs_str = dim(f"  needs [{', '.join(ns)}]")

            prov_str = ""
            if res.provenance:
                pv = res.provenance
                ver_tag = f" v{pv.version}" if pv.version else ""
                prov_str = dim(f"  ← {pv.source_file}{ver_tag}")

            print(f"    {sym}  {bold(f'{res.short_name:26s}')}{needs_str}{prov_str}")
            if res.description:
                print(f"                {dim(res.description)}")
            if res.script_path:
                print(f"                {cyan('[script]')} {dim(_redact(res.script_path, graph.sensitive_values))}")
            if res.flags:
                for flag in res.flags:
                    print(f"                {dim('+ ' + _redact(flag, graph.sensitive_values))}")

            # Show dedup info
            if res.identity_hash in graph.dedup_map:
                sites = graph.dedup_map[res.identity_hash]
                if len(sites) > 1:
                    others = [s.split(".")[-1] for s in sites if s != rid]
                    if others:
                        shared_str = ", ".join(others)
                        print(f"                {dim(f'(shared with: {shared_str})')}")

            if res.collect_key:
                ck = res.collect_key
                print(f"                {cyan('collect: ')}{cyan(repr(ck))}")

            if res.tags:
                print(f"                {dim('tags: ')}{dim(', '.join(res.tags))}")

            if res.http_method:
                print(f"                {magenta(res.http_method)} {dim(res.http_url or '')}")
                if res.http_expect:
                    print(f"                {dim('expect:')} {dim(res.http_expect)}")

            if verbose:
                _r = lambda t: _redact(t, graph.sensitive_values)
                if res.check: print(f"                {dim('check:')} {dim(_r(res.check)[:70])}")
                if res.http_method:
                    if res.http_auth:
                        print(f"                {dim('auth:  ')} {dim(res.http_auth[0])} ***")
                    for hname, hval in (res.http_headers or []):
                        print(f"                {dim('header:')} {dim(hname)}: {dim(_r(hval)[:50])}")
                    if res.http_body:
                        print(f"                {dim('body:  ')} {dim(_r(res.http_body)[:70])}")
                elif res.script_path:
                    print(f"                {dim('script:')} {dim(_r(res.script_path)[:70])}")
                elif res.run:
                    print(f"                {dim('run:  ')} {dim(_r(res.run)[:70])}")
                    for flag in res.flags:
                        print(f"                {dim('+     ')} {dim(_r(flag)[:70])}")
                if res.run_as: print(f"                {dim('as:   ')} {dim(res.run_as)}")
                if res.until is not None:
                    print(f"                {dim('until:')} {dim(repr(res.until))}")
            print()

    print(bold("─" * 64))
    parts=[]
    if n_check:    parts.append(blue(f"{n_check} to check/run"))
    if n_run:      parts.append(green(f"{n_run} to run"))
    if n_verify:   parts.append(magenta(f"{n_verify} to verify"))
    if n_tag_skip: parts.append(dim(f"{n_tag_skip} skipped by tag"))
    print(f"  Plan: {', '.join(parts)}  ({total} total)")
    print()

# ── Execution (unchanged core from v2) ──────────────────────────────────

def _sq(s): return "'" + s.replace("'", "'\\''") + "'"

# ── SSH Connection Pooling (ControlMaster) ─────────────────────────────
_ssh_control_dir: str|None = None
_become_pass: str|None = None  # sudo password (from --ask-become-pass or CGR_BECOME_PASS)
_rsync_available_cache: dict[str, bool] = {}  # per-node rsync availability cache ("user@host:port" -> bool)

def _ssh_pool_start(graph: Graph):
    """Open ControlMaster connections for all SSH hosts in the graph."""
    global _ssh_control_dir
    import tempfile
    _ssh_control_dir = tempfile.mkdtemp(prefix="cg_ssh_")
    hosts_started: set[str] = set()
    for nn, hn in graph.nodes.items():
        if hn.via_method != "ssh": continue
        h = hn.via_props.get("host", ""); u = hn.via_props.get("user", "")
        p = hn.via_props.get("port", "22")
        key = f"{u}@{h}:{p}"
        if key in hosts_started: continue
        tgt = f"{u}@{h}" if u else h
        sock = os.path.join(_ssh_control_dir, f"{u}_{h}_{p}")
        cmd = ["ssh", "-o", "BatchMode=yes", "-o", f"ControlPath={sock}",
               "-o", "ControlMaster=yes", "-o", "ControlPersist=120",
               "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=no",
               "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
               "-p", p, "-N", "-f", tgt]
        try:
            subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            hosts_started.add(key)
        except Exception:
            pass  # fall back to regular connections

def _ssh_pool_stop():
    """Close all ControlMaster connections and clean up."""
    global _ssh_control_dir
    if not _ssh_control_dir: return
    import shutil
    # Send exit to each control socket
    try:
        for sock in Path(_ssh_control_dir).glob("*"):
            try:
                subprocess.run(["ssh", "-o", f"ControlPath={sock}", "-O", "exit", "dummy"],
                               capture_output=True, timeout=5)
            except Exception:
                pass
    except Exception:
        pass
    try: shutil.rmtree(_ssh_control_dir, ignore_errors=True)
    except Exception: pass
    _ssh_control_dir = None

def _terminate_proc_tree(proc, sig=signal.SIGTERM):
    if proc is None:
        return
    try:
        os.killpg(proc.pid, sig)
    except Exception:
        try:
            proc.send_signal(sig)
        except Exception:
            pass


def _command_reads_tty(cmd) -> bool:
    if not isinstance(cmd, str):
        return False
    return bool(
        "/dev/tty" in cmd
        or "ssh-copy-id" in cmd
        or re.search(r"(^|[;&|({]\s*)read\s+(-[A-Za-z]*\s+)?", cmd)
    )


def _run_cmd(cmd, node, res, *, timeout, on_output=None, register_proc=None, unregister_proc=None, cancel_check=None):
    actual = cmd
    if node.via_method == "ssh":
        # For SSH: compare run_as against the SSH user, not the local user
        ssh_user = node.via_props.get("user", "")
        if res.run_as and res.run_as != ssh_user:
            actual = f"sudo -u {res.run_as} -- bash -c {_sq(cmd)}"
        h=node.via_props.get("host","localhost")
        p=node.via_props.get("port","22"); tgt=f"{ssh_user}@{h}" if ssh_user else h
        full=["ssh","-o","BatchMode=yes","-o","ConnectTimeout=10","-p",p,tgt,actual]
        # Use ControlMaster socket if available
        if _ssh_control_dir:
            sock = os.path.join(_ssh_control_dir, f"{ssh_user}_{h}_{p}")
            if os.path.exists(sock):
                full = ["ssh","-o","BatchMode=yes","-o",f"ControlPath={sock}","-p",p,tgt,actual]
        sh=False
    else:
        # For local: compare run_as against the local user
        if res.run_as and res.run_as != os.getenv("USER", "root"):
            actual = f"sudo -u {res.run_as} -- bash -c {_sq(cmd)}"
        full=actual; sh=True
    proc = None
    reset_on_output = getattr(res, "timeout_reset_on_output", False)
    stdin_restore = None
    stdin_pump_stop = None
    stdin_pump_thread = None
    try:
        stdin_data = None
        if _become_pass and res.run_as:
            # Pipe sudo password via stdin using sudo -S
            if isinstance(full, str):
                full = full.replace("sudo -u", "sudo -S -u")
            elif isinstance(full, list):
                full = [x.replace("sudo -u", "sudo -S -u") if "sudo" in x else x for x in full]
            stdin_data = (_become_pass + "\n").encode()
        interactive_tty = (
            node.via_method != "ssh"
            and stdin_data is None
            and (res.interactive or _command_reads_tty(full))
            and sys.stdin and sys.stdin.isatty()
            and sys.stdout and sys.stdout.isatty()
        )
        use_pty = bool(on_output) or interactive_tty
        if use_pty:
            master_fd, slave_fd = pty.openpty()
            echo_pty = on_output is None
            passthrough_stdin = echo_pty or interactive_tty
            def _pty_preexec():
                os.setsid()
                fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)
            try:
                proc = subprocess.Popen(
                    full,
                    shell=sh,
                    stdout=slave_fd,
                    stderr=slave_fd,
                    stdin=slave_fd if stdin_data is None else subprocess.PIPE,
                    text=False,
                    env={**os.environ, **res.env},
                    close_fds=True,
                    preexec_fn=_pty_preexec,
                )
            finally:
                os.close(slave_fd)
            if register_proc:
                register_proc(proc, input_fd=master_fd if stdin_data is None else None)
            os.set_blocking(master_fd, False)
            if stdin_data is not None and proc.stdin is not None:
                proc.stdin.write(stdin_data)
                proc.stdin.close()
            elif passthrough_stdin and sys.stdin and sys.stdin.isatty():
                stdin_state = {"fd": None, "attrs": None, "restored": False}
                stdin_state_lock = threading.Lock()
                stdin_pump_stop = threading.Event()

                def _restore_stdin():
                    with stdin_state_lock:
                        if stdin_state["restored"]:
                            return
                        stdin_fd = stdin_state["fd"]
                        stdin_attrs = stdin_state["attrs"]
                        stdin_state["restored"] = True
                    if stdin_fd is not None and stdin_attrs is not None:
                        try:
                            termios.tcsetattr(stdin_fd, termios.TCSANOW, stdin_attrs)
                        except termios.error:
                            pass

                stdin_restore = _restore_stdin

                def _pump_stdin():
                    try:
                        stdin_fd = sys.stdin.fileno()
                        stdin_attrs = termios.tcgetattr(stdin_fd)
                        with stdin_state_lock:
                            stdin_state["fd"] = stdin_fd
                            stdin_state["attrs"] = stdin_attrs
                        tty.setcbreak(stdin_fd)
                        cur_attrs = termios.tcgetattr(stdin_fd)
                        cur_attrs[3] &= ~termios.ECHO
                        termios.tcsetattr(stdin_fd, termios.TCSANOW, cur_attrs)
                        while not stdin_pump_stop.is_set() and proc.poll() is None:
                            ready, _, _ = select.select([sys.stdin], [], [], 0.1)
                            if not ready:
                                continue
                            chunk = os.read(stdin_fd, 1024)
                            if not chunk:
                                break
                            try:
                                os.write(master_fd, chunk)
                            except OSError:
                                break
                    except (OSError, ValueError, termios.error):
                        pass
                    finally:
                        _restore_stdin()
                stdin_pump_thread = threading.Thread(target=_pump_stdin, name="cgr-stdin-pump")
                stdin_pump_thread.start()

            out_chunks: list[str] = []
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            t_deadline = time.monotonic() + timeout
            try:
                while True:
                    if cancel_check and cancel_check():
                        _terminate_proc_tree(proc, signal.SIGTERM)
                        try:
                            proc.wait(timeout=1)
                        except Exception:
                            _terminate_proc_tree(proc, signal.SIGKILL)
                            proc.wait(timeout=1)
                        output = "".join(out_chunks).replace("\r\n", "\n").replace("\r", "\n")
                        return 130, output, "Cancelled"
                    remaining = t_deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(full, timeout)
                    ready, _, _ = select.select([master_fd], [], [], min(0.1, remaining))
                    if ready:
                        while True:
                            try:
                                data = os.read(master_fd, 4096)
                            except BlockingIOError:
                                break
                            except OSError:
                                data = b""
                            if not data:
                                break
                            text = decoder.decode(data)
                            if text:
                                out_chunks.append(text)
                                if reset_on_output:
                                    t_deadline = time.monotonic() + timeout
                                if on_output:
                                    on_output("stdout", text)
                                elif echo_pty and sys.stdout and sys.stdout.isatty():
                                    try:
                                        os.write(sys.stdout.fileno(), data)
                                    except OSError:
                                        pass
                    if proc.poll() is not None:
                        try:
                            while True:
                                data = os.read(master_fd, 4096)
                                if not data:
                                    break
                                text = decoder.decode(data)
                                if text:
                                    out_chunks.append(text)
                                    if reset_on_output:
                                        t_deadline = time.monotonic() + timeout
                                    if on_output:
                                        on_output("stdout", text)
                                    elif echo_pty and sys.stdout and sys.stdout.isatty():
                                        try:
                                            os.write(sys.stdout.fileno(), data)
                                        except OSError:
                                            pass
                        except (BlockingIOError, OSError):
                            pass
                        break
                tail = decoder.decode(b"", final=True)
                if tail:
                    out_chunks.append(tail)
                    if reset_on_output:
                        t_deadline = time.monotonic() + timeout
                    if on_output:
                        on_output("stdout", tail)
                proc.wait()
                output = "".join(out_chunks).replace("\r\n", "\n").replace("\r", "\n")
                return proc.returncode, output, ""
            finally:
                if stdin_pump_stop is not None:
                    stdin_pump_stop.set()
                if stdin_pump_thread is not None:
                    stdin_pump_thread.join(timeout=0.5)
                if stdin_restore is not None:
                    stdin_restore()
                try:
                    os.close(master_fd)
                except OSError:
                    pass
        proc = subprocess.Popen(
            full,
            shell=sh,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE if stdin_data is not None else None,
            text=False,
            env={**os.environ, **res.env},
            start_new_session=True,
        )
        if stdin_data is not None and proc.stdin is not None:
            proc.stdin.write(stdin_data)
            proc.stdin.close()
        if register_proc:
            register_proc(proc, input_fd=None)

        sel = selectors.DefaultSelector()
        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        decoders = {
            "stdout": codecs.getincrementaldecoder("utf-8")(errors="replace"),
            "stderr": codecs.getincrementaldecoder("utf-8")(errors="replace"),
        }
        if proc.stdout is not None:
            sel.register(proc.stdout, selectors.EVENT_READ, "stdout")
        if proc.stderr is not None:
            sel.register(proc.stderr, selectors.EVENT_READ, "stderr")

        t_deadline = time.monotonic() + timeout
        while sel.get_map():
            if cancel_check and cancel_check():
                _terminate_proc_tree(proc, signal.SIGTERM)
                try:
                    proc.wait(timeout=1)
                except Exception:
                    _terminate_proc_tree(proc, signal.SIGKILL)
                    proc.wait(timeout=1)
                return 130, "".join(stdout_chunks), "Cancelled"
            remaining = t_deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(full, timeout)
            events = sel.select(timeout=min(0.1, remaining))
            for key, _ in events:
                stream_name = key.data
                data = os.read(key.fileobj.fileno(), 4096)
                if not data:
                    sel.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                text = decoders[stream_name].decode(data)
                if not text:
                    continue
                if reset_on_output:
                    t_deadline = time.monotonic() + timeout
                if stream_name == "stdout":
                    stdout_chunks.append(text)
                else:
                    stderr_chunks.append(text)
                if on_output:
                    on_output(stream_name, text)
            if proc.poll() is not None and not events:
                break

        for stream_name, decoder in decoders.items():
            tail = decoder.decode(b"", final=True)
            if not tail:
                continue
            if reset_on_output:
                t_deadline = time.monotonic() + timeout
            if stream_name == "stdout":
                stdout_chunks.append(tail)
            else:
                stderr_chunks.append(tail)
            if on_output:
                on_output(stream_name, tail)

        proc.wait()
        return proc.returncode, "".join(stdout_chunks), "".join(stderr_chunks)
    except subprocess.TimeoutExpired:
        try:
            if proc is not None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except Exception:
                    proc.kill()
                proc.wait(timeout=1)
        except Exception:
            pass
        if reset_on_output:
            msg = f"Timed out after {timeout}s without output"
        else:
            msg = f"Timed out after {timeout}s"
        if node.via_method == "ssh":
            msg += " (note: the remote process may still be running on the target host)"
        return 124, "", msg
    except FileNotFoundError as e:
        # Build a diagnostic error message
        parts = [f"Command not found: {e.filename or '(unknown)'}"]
        if node.via_method == "ssh":
            h=node.via_props.get("host","?"); u=node.via_props.get("user","?")
            parts.append(f"  Target: ssh {u}@{h}")
            parts.append(f"  The 'ssh' binary was not found on the local machine.")
            parts.append(f"  Ensure openssh-client is installed: apt-get install -y openssh-client")
        else:
            # Local execution — figure out what failed
            if res.run_as and res.run_as != os.getenv("USER", "root"):
                parts.append(f"  Tried: sudo -u {res.run_as} -- bash -c '...'")
                parts.append(f"  The 'sudo' binary was not found. Install sudo or run as the target user directly.")
            else:
                parts.append(f"  Tried: bash -c '{cmd[:80]}{'...' if len(cmd) > 80 else ''}'")
                parts.append(f"  The shell could not find the command. Possible causes:")
                parts.append(f"    - The binary is not installed (try: which <command>)")
                parts.append(f"    - The binary is not in PATH (current PATH: {os.environ.get('PATH', '(not set)')[:120]})")
                parts.append(f"    - A typo in the command name")
        return 127, "", "\n".join(parts)
    finally:
        if proc is not None and unregister_proc:
            try:
                unregister_proc(proc)
            except Exception:
                pass

def _resolve_script_fs_path(script_path: str, graph_file: str|None) -> str:
    if os.path.isabs(script_path) or not graph_file:
        return script_path
    return os.path.normpath(os.path.join(os.path.dirname(graph_file), script_path))

def _expand_runtime_or_resolved(raw_value: str | None, resolved_value, vs: dict[str, str]):
    """Expand runtime vars when available; otherwise preserve the resolved value."""
    if raw_value is None:
        return resolved_value
    expanded = _expand(raw_value, vs, lenient=True)
    if isinstance(expanded, str) and VAR_RE.search(expanded):
        return resolved_value
    return expanded

def _runtime_resource_view(res: Resource, variables: dict[str, str] | None) -> Resource:
    """Return an execution-time view of a resource with runtime vars expanded."""
    vs = variables or {}
    runtime_env = {}
    source_env = res.raw_env or res.env
    for key, value in source_env.items():
        env_expr = res.env_when.get(key)
        if env_expr and not _eval_when(_expand(env_expr, vs, lenient=True), vs):
            continue
        runtime_env[key] = _expand_runtime_or_resolved(value, res.env.get(key, value), vs) or value
    raw_headers = res.raw_http_headers or res.http_headers
    runtime_http_headers = [
        (
            _expand_runtime_or_resolved(k, k, vs) or k,
            _expand_runtime_or_resolved(v, v, vs) or v,
        )
        for k, v in raw_headers
    ]
    runtime_http_auth = res.http_auth
    if res.raw_http_auth:
        runtime_http_auth = (
            res.raw_http_auth[0],
            _expand_runtime_or_resolved(res.raw_http_auth[1], res.http_auth[1] if res.http_auth else None, vs)
            if res.raw_http_auth[1] else None,
        )
    runtime_subgraph_vars = {
        key: _expand_runtime_or_resolved(raw_val, res.subgraph_vars.get(key), vs) or raw_val
        for key, raw_val in (res.raw_subgraph_vars or {}).items()
    }
    return replace(
        res,
        check=_expand_runtime_or_resolved(res.raw_check, res.check, vs),
        run=_expand_runtime_or_resolved(res.raw_run, res.run, vs),
        script_path=_expand_runtime_or_resolved(res.raw_script_path, res.script_path, vs),
        env=runtime_env,
        http_url=_expand_runtime_or_resolved(res.raw_http_url, res.http_url, vs),
        http_headers=runtime_http_headers,
        http_body=_expand_runtime_or_resolved(res.raw_http_body, res.http_body, vs),
        http_auth=runtime_http_auth,
        wait_target=_expand_runtime_or_resolved(res.raw_wait_target, res.wait_target, vs),
        subgraph_path=_expand_runtime_or_resolved(res.raw_subgraph_path, res.subgraph_path, vs),
        subgraph_vars=runtime_subgraph_vars,
    )

def _effective_run_cmd(res: Resource, graph_file: str|None = None) -> str:
    if res.run:
        return res.run
    if res.script_path:
        return shlex.quote(_resolve_script_fs_path(res.script_path, graph_file))
    return ""


def _normalize_webhook_path(path: str) -> str:
    if not path:
        return "/"
    return path if path.startswith("/") else "/" + path


class WebhookGateServer:
    """Lightweight local HTTP endpoint registry for wait-for-webhook steps."""

    def __init__(self, host: str = "127.0.0.1", port: int | None = None):
        import http.server

        self.host = host
        self.port = port or int(os.environ.get("CGR_WEBHOOK_PORT", "0") or "0")
        self._lock = threading.Lock()
        self._triggered: set[str] = set()
        self._waiters: dict[str, list[threading.Event]] = defaultdict(list)
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _handle(self):
                path = _normalize_webhook_path(self.path.split("?", 1)[0])
                outer.trigger(path)
                payload = json.dumps({"ok": True, "path": path}).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_GET(self):
                self._handle()

            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length:
                    self.rfile.read(length)
                self._handle()

            def log_message(self, fmt, *args):
                return

        self._server = http.server.ThreadingHTTPServer((self.host, self.port), Handler)
        self.port = int(self._server.server_port)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def endpoint(self, path: str) -> str:
        return f"http://{self.host}:{self.port}{_normalize_webhook_path(path)}"

    def trigger(self, path: str):
        path = _normalize_webhook_path(path)
        with self._lock:
            self._triggered.add(path)
            waiters = self._waiters.pop(path, [])
        for event in waiters:
            event.set()

    def wait(self, path: str, timeout: float, cancel_check=None) -> bool:
        path = _normalize_webhook_path(path)
        with self._lock:
            if path in self._triggered:
                return True
            event = threading.Event()
            self._waiters[path].append(event)
        deadline = time.monotonic() + timeout
        try:
            while True:
                if cancel_check and cancel_check():
                    return False
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                if event.wait(min(0.2, remaining)):
                    return True
        finally:
            with self._lock:
                waiters = self._waiters.get(path, [])
                if event in waiters:
                    waiters.remove(event)
                if not waiters:
                    self._waiters.pop(path, None)

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def _wait_for_file_path(path: str, node, res, timeout: int, cancel_check=None) -> tuple[int, str, str]:
    deadline = time.monotonic() + timeout
    while True:
        if cancel_check and cancel_check():
            return 130, "", "Cancelled"
        if node.via_method == "ssh":
            remaining = max(1, int(deadline - time.monotonic()))
            rc, _, stderr = _run_cmd(f"test -e {shlex.quote(path)}", node, res, timeout=min(5, remaining))
            if rc == 0:
                return 0, path, ""
            if time.monotonic() >= deadline:
                break
        else:
            if Path(path).exists():
                return 0, path, ""
            if time.monotonic() >= deadline:
                break
        time.sleep(0.25)
    return 124, "", f"Timed out after {timeout}s waiting for file {path}"


def _exec_subgraph_resource(res: Resource, *, graph_file: str | None, webhook_server: WebhookGateServer | None):
    subgraph_path = _resolve_script_fs_path(res.subgraph_path or "", graph_file)
    if not subgraph_path:
        raise ResolveError("Subgraph step is missing a graph path")
    subgraph = _load(subgraph_path, extra_vars=dict(res.subgraph_vars or {}), raise_on_error=True)
    buf = io.StringIO()
    with redirect_stdout(buf):
        results, wall_ms = cmd_apply_stateful(
            subgraph,
            subgraph_path,
            max_parallel=4,
            webhook_server=webhook_server,
        )
    failed = any(r.status in (Status.FAILED, Status.TIMEOUT) for r in results)
    return failed, wall_ms, buf.getvalue(), ""

def _check_http_expect(status_code: int, expect: str) -> bool:
    """Check if an HTTP status code matches an expect expression.
    Supports: '200', '200..299', '200, 201, 204'."""
    if not expect:
        return 200 <= status_code < 300  # default: any 2xx
    expect = expect.strip()
    # Range: 200..299
    if '..' in expect:
        parts = expect.split('..', 1)
        try:
            lo, hi = int(parts[0].strip()), int(parts[1].strip())
            return lo <= status_code <= hi
        except ValueError:
            return False
    # List: 200, 201, 204
    if ',' in expect:
        codes = [c.strip() for c in expect.split(',')]
        return str(status_code) in codes
    # Single: 200
    try:
        return status_code == int(expect)
    except ValueError:
        return False

def _exec_http_local(res):
    """Execute an HTTP request using urllib (for local targets). Returns (rc, stdout, stderr)."""
    import urllib.request, urllib.error, urllib.parse
    url = res.http_url
    method = res.http_method
    headers = dict(res.http_headers) if res.http_headers else {}
    # Auth
    if res.http_auth:
        auth_type, auth_val = res.http_auth
        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {auth_val}"
        elif auth_type == "basic":
            import base64
            headers["Authorization"] = "Basic " + base64.b64encode(auth_val.encode()).decode()
        elif auth_type == "header":
            # format: "HeaderName:value"
            hname, hval = auth_val.split(":", 1)
            headers[hname] = hval
    # Body
    data = None
    if res.http_body:
        if res.http_body_type == "json":
            headers.setdefault("Content-Type", "application/json")
            data = res.http_body.encode("utf-8")
        elif res.http_body_type == "form":
            headers.setdefault("Content-Type", "application/x-www-form-urlencoded")
            data = res.http_body.encode("utf-8")
        else:
            data = res.http_body.encode("utf-8")
    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=res.timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            status = resp.status
            if _check_http_expect(status, res.http_expect):
                return 0, body, f"HTTP {status}"
            else:
                return 1, body, f"HTTP {status} (expected {res.http_expect or '2xx'})"
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        if _check_http_expect(e.code, res.http_expect):
            return 0, body, f"HTTP {e.code}"
        return 1, body, f"HTTP {e.code} {e.reason} (expected {res.http_expect or '2xx'})"
    except urllib.error.URLError as e:
        return 1, "", f"Connection error: {e.reason}"
    except Exception as e:
        return 1, "", f"HTTP error: {e}"

def _exec_http_ssh(res, node):
    """Execute an HTTP request via curl over SSH (for remote targets). Returns (rc, stdout, stderr)."""
    parts = ["curl", "-s", "-S", "-w", r"\n%{http_code}", "-X", res.http_method]
    # Auth
    if res.http_auth:
        auth_type, auth_val = res.http_auth
        if auth_type == "bearer":
            parts.extend(["-H", f"Authorization: Bearer {auth_val}"])
        elif auth_type == "basic":
            parts.extend(["-u", auth_val])
        elif auth_type == "header":
            hname, hval = auth_val.split(":", 1)
            parts.extend(["-H", f"{hname}: {hval}"])
    # Headers
    for hname, hval in (res.http_headers or []):
        parts.extend(["-H", f"{hname}: {hval}"])
    # Body
    if res.http_body:
        if res.http_body_type == "json":
            parts.extend(["-H", "Content-Type: application/json"])
        elif res.http_body_type == "form":
            parts.extend(["-H", "Content-Type: application/x-www-form-urlencoded"])
        parts.extend(["-d", res.http_body])
    parts.extend(["--max-time", str(res.timeout)])
    parts.append(res.http_url)
    # Build curl command string
    cmd = " ".join(_sq(p) for p in parts)
    rc, stdout, stderr = _run_cmd(cmd, node, res, timeout=res.timeout + 5)
    if rc != 0:
        return rc, stdout, stderr
    # Parse status code from curl -w output (last line)
    lines = stdout.rstrip().rsplit("\n", 1)
    body = lines[0] if len(lines) > 1 else ""
    try:
        status_code = int(lines[-1].strip())
    except (ValueError, IndexError):
        return 1, stdout, "Could not parse HTTP status code from curl output"
    if _check_http_expect(status_code, res.http_expect):
        return 0, body, f"HTTP {status_code}"
    else:
        return 1, body, f"HTTP {status_code} (expected {res.http_expect or '2xx'})"

def _exec_prov_local(res, graph_file=None):
    """Execute provisioning operations locally. Returns (rc, stdout, stderr)."""
    import shutil
    backups: list[tuple[str, str]] = []  # (backup_path, original_dest)
    messages: list[str] = []

    def _backup(dest: str) -> str|None:
        """Backup dest to dest.cgr_bak if it exists. Returns backup path or None."""
        if os.path.exists(dest):
            bak = dest + ".cgr_bak"
            shutil.copy2(dest, bak)
            return bak
        return None

    def _atomic_write(dest: str, content: str) -> None:
        """Write content to dest atomically via a temp file."""
        os.makedirs(os.path.dirname(dest) if os.path.dirname(dest) else ".", exist_ok=True)
        tmp = dest + ".cgr_tmp"
        with open(tmp, "w") as f:
            f.write(content)
        os.replace(tmp, dest)

    def _revert_all() -> None:
        """Restore all backed-up files."""
        for bak, dest in reversed(backups):
            if bak and os.path.exists(bak):
                try:
                    os.replace(bak, dest)
                except OSError:
                    pass

    try:
        # ── assert keyword (run before any writes) ─────────────────────────
        if res.prov_asserts:
            for cmd, msg in res.prov_asserts:
                proc = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                                      timeout=min(res.timeout, 30), env={**os.environ, **res.env})
                if proc.returncode != 0:
                    return 1, "", msg or f"assertion failed: {cmd}"

        # ── content keyword ────────────────────────────────────────────────
        if res.prov_content_inline is not None or res.prov_content_from:
            dest = res.prov_dest
            if res.prov_content_from:
                src = res.prov_content_from
                # Resolve relative to graph file directory
                if not os.path.isabs(src) and graph_file:
                    src = os.path.join(os.path.dirname(graph_file), src)
                try:
                    raw = open(src).read()
                except FileNotFoundError:
                    return 1, "", f"content from: source file not found: {src}"
                content = VAR_RE.sub(lambda m: res.env.get(m.group(1), m.group(0)), raw)
            else:
                content = res.prov_content_inline
            bak = _backup(dest)
            if bak: backups.append((bak, dest))
            _atomic_write(dest, content + ("\n" if content and not content.endswith("\n") else ""))
            messages.append(f"wrote {dest}")

        # ── line keywords ──────────────────────────────────────────────────
        if res.prov_lines:
            dest = res.prov_dest
            bak = _backup(dest)
            if bak: backups.append((bak, dest))
            if os.path.exists(dest):
                with open(dest) as f:
                    file_lines = f.read().splitlines()
            else:
                file_lines = []
            for action, text, regex in res.prov_lines:
                if action == "present":
                    if text not in file_lines:
                        file_lines.append(text)
                        messages.append(f"appended line to {dest}")
                    else:
                        messages.append(f"line already present in {dest}")
                elif action == "absent":
                    before = len(file_lines)
                    file_lines = [l for l in file_lines if l != text]
                    if len(file_lines) < before:
                        messages.append(f"removed line from {dest}")
                    else:
                        messages.append(f"line already absent in {dest}")
                elif action == "replace":
                    replaced = False
                    for idx, l in enumerate(file_lines):
                        if re.search(regex, l):
                            file_lines[idx] = text
                            replaced = True
                            messages.append(f"replaced line in {dest}")
                            break
                    if not replaced:
                        file_lines.append(text)
                        messages.append(f"appended (no match for '{regex}') to {dest}")
            new_content = "\n".join(file_lines)
            if file_lines:
                new_content += "\n"
            _atomic_write(dest, new_content)

        # ── put keyword ────────────────────────────────────────────────────
        if res.prov_put_src:
            src = res.prov_put_src
            dest = res.prov_dest
            if not os.path.isabs(src) and graph_file:
                src = os.path.join(os.path.dirname(graph_file), src)
            if not os.path.exists(src):
                return 1, "", f"put: source file not found: {src}"
            bak = _backup(dest)
            if bak: backups.append((bak, dest))
            os.makedirs(os.path.dirname(dest) if os.path.dirname(dest) else ".", exist_ok=True)
            shutil.copy2(src, dest)
            if res.prov_put_mode:
                os.chmod(dest, int(res.prov_put_mode, 8))
            messages.append(f"copied {src} → {dest}")

        # ── block keyword ──────────────────────────────────────────────────
        if res.prov_block_dest:
            dest = res.prov_block_dest
            marker = res.prov_block_marker or "cgr:block"
            begin_marker = f"# {marker} begin"
            end_marker = f"# {marker} end"
            if res.prov_block_absent:
                # Remove the block
                if os.path.exists(dest):
                    with open(dest) as f:
                        file_text = f.read()
                    lines = file_text.splitlines()
                    new_lines = []
                    in_block = False
                    for l in lines:
                        if l.strip() == begin_marker:
                            in_block = True; continue
                        if l.strip() == end_marker:
                            in_block = False; continue
                        if not in_block:
                            new_lines.append(l)
                    bak = _backup(dest)
                    if bak: backups.append((bak, dest))
                    _atomic_write(dest, "\n".join(new_lines) + ("\n" if new_lines else ""))
                    messages.append(f"removed block '{marker}' from {dest}")
                else:
                    messages.append(f"block absent (file not found): {dest}")
            else:
                # Get block content
                if res.prov_block_from:
                    src = res.prov_block_from
                    if not os.path.isabs(src) and graph_file:
                        src = os.path.join(os.path.dirname(graph_file), src)
                    try:
                        block_content = open(src).read()
                    except FileNotFoundError:
                        return 1, "", f"block from: source file not found: {src}"
                    block_content = VAR_RE.sub(lambda m: res.env.get(m.group(1), m.group(0)), block_content)
                else:
                    block_content = res.prov_block_inline or ""
                new_block = f"{begin_marker}\n{block_content}"
                if block_content and not block_content.endswith("\n"):
                    new_block += "\n"
                new_block += f"{end_marker}\n"
                # Read or create file
                if os.path.exists(dest):
                    with open(dest) as f:
                        file_text = f.read()
                    lines = file_text.splitlines()
                    new_lines = []; in_block = False; inserted = False
                    for l in lines:
                        if l.strip() == begin_marker:
                            in_block = True
                            for bl in new_block.splitlines():
                                new_lines.append(bl)
                            inserted = True; continue
                        if l.strip() == end_marker:
                            in_block = False; continue
                        if not in_block:
                            new_lines.append(l)
                    if not inserted:
                        if new_lines and new_lines[-1].strip():
                            new_lines.append("")
                        new_lines.extend(new_block.rstrip("\n").splitlines())
                    bak = _backup(dest)
                    if bak: backups.append((bak, dest))
                    _atomic_write(dest, "\n".join(new_lines) + "\n")
                else:
                    os.makedirs(os.path.dirname(dest) if os.path.dirname(dest) else ".", exist_ok=True)
                    _atomic_write(dest, new_block)
                messages.append(f"wrote block '{marker}' in {dest}")

        # ── ini keyword ────────────────────────────────────────────────────
        if res.prov_ini_dest:
            import configparser
            dest = res.prov_ini_dest
            parser = configparser.RawConfigParser()
            parser.optionxform = str  # preserve key case
            if os.path.exists(dest):
                parser.read(dest)
            changed = False
            for section, key, val in res.prov_ini_ops:
                sec = section or "DEFAULT"
                if val == "absent":
                    if parser.has_option(sec, key):
                        parser.remove_option(sec, key)
                        if sec != "DEFAULT" and not parser.options(sec):
                            parser.remove_section(sec)
                        changed = True
                else:
                    if sec != "DEFAULT" and not parser.has_section(sec):
                        parser.add_section(sec)
                    current = parser.get(sec, key, fallback=None)
                    if current != val:
                        parser.set(sec, key, val)
                        changed = True
            if changed:
                bak = _backup(dest)
                if bak: backups.append((bak, dest))
                os.makedirs(os.path.dirname(dest) if os.path.dirname(dest) else ".", exist_ok=True)
                tmp = dest + ".cgr_tmp"
                with open(tmp, "w") as f:
                    parser.write(f)
                os.replace(tmp, dest)
                messages.append(f"updated ini {dest}")
            else:
                messages.append(f"ini {dest}: no changes needed")

        # ── json keyword ───────────────────────────────────────────────────
        if res.prov_json_dest:
            import json as _json
            dest = res.prov_json_dest
            if os.path.exists(dest):
                with open(dest) as f:
                    data = _json.load(f)
            else:
                data = {}
            changed = False
            for path, val in res.prov_json_ops:
                # Handle array append: key[] = "value"
                if path.endswith("[]"):
                    key = path[:-2]
                    parts = key.split(".")
                    obj = data
                    for p in parts[:-1]:
                        obj = obj.setdefault(p, {})
                    arr_key = parts[-1]
                    if not isinstance(obj.get(arr_key), list):
                        obj[arr_key] = []
                    if val not in obj[arr_key]:
                        obj[arr_key].append(val)
                        changed = True
                else:
                    parts = path.split(".")
                    obj = data
                    for p in parts[:-1]:
                        if p not in obj or not isinstance(obj[p], dict):
                            obj[p] = {}
                        obj = obj[p]
                    leaf = parts[-1]
                    if val == "absent":
                        if leaf in obj:
                            del obj[leaf]
                            changed = True
                    else:
                        if obj.get(leaf) != val:
                            obj[leaf] = val
                            changed = True
            if changed:
                bak = _backup(dest)
                if bak: backups.append((bak, dest))
                os.makedirs(os.path.dirname(dest) if os.path.dirname(dest) else ".", exist_ok=True)
                tmp = dest + ".cgr_tmp"
                with open(tmp, "w") as f:
                    _json.dump(data, f, indent=2)
                    f.write("\n")
                os.replace(tmp, dest)
                messages.append(f"updated json {dest}")
            else:
                messages.append(f"json {dest}: no changes needed")

        # ── validate keyword ───────────────────────────────────────────────
        if res.prov_validate:
            proc = subprocess.run(
                res.prov_validate, shell=True, capture_output=True, text=True,
                timeout=min(res.timeout, 60), env={**os.environ, **res.env}
            )
            if proc.returncode != 0:
                _revert_all()
                return 1, proc.stdout, f"validate failed (reverted): {proc.stderr}"
            messages.append("validate ok")

        # ── clean up backups ───────────────────────────────────────────────
        for bak, _ in backups:
            if bak and os.path.exists(bak):
                try: os.unlink(bak)
                except OSError: pass

        return 0, "\n".join(messages), ""

    except OSError as e:
        _revert_all()
        return 1, "", f"provisioning error: {e}"


def _exec_prov_ssh(res, node, graph_file=None):
    """Execute provisioning operations over SSH. Returns (rc, stdout, stderr)."""
    import base64
    import shutil
    messages: list[str] = []
    backups: list[str] = []  # list of dest paths that were backed up

    def _ssh_run(cmd: str) -> tuple[int, str, str]:
        return _run_cmd(cmd, node, res, timeout=res.timeout)

    def _ssh_backup(dest: str) -> int:
        """Backup remote dest to dest.cgr_bak. Returns rc."""
        rc, _, _ = _ssh_run(f"test -f {_sq(dest)} && cp -p {_sq(dest)} {_sq(dest + '.cgr_bak')} || true")
        if rc == 0:
            backups.append(dest)
        return rc

    def _ssh_revert() -> None:
        for dest in reversed(backups):
            bak = dest + ".cgr_bak"
            _ssh_run(f"test -f {_sq(bak)} && mv {_sq(bak)} {_sq(dest)} || true")

    try:
        # ── assert keyword (run before any writes) ─────────────────────────
        if res.prov_asserts:
            for cmd, msg in res.prov_asserts:
                rc, _, aerr = _ssh_run(cmd)
                if rc != 0:
                    return 1, "", msg or f"assertion failed: {cmd}"

        # ── content keyword ────────────────────────────────────────────────
        if res.prov_content_inline is not None or res.prov_content_from:
            dest = res.prov_dest
            if res.prov_content_from:
                src = res.prov_content_from
                if not os.path.isabs(src) and graph_file:
                    src = os.path.join(os.path.dirname(graph_file), src)
                try:
                    raw = open(src).read()
                except FileNotFoundError:
                    return 1, "", f"content from: source file not found: {src}"
                content = VAR_RE.sub(lambda m: res.env.get(m.group(1), m.group(0)), raw)
            else:
                content = res.prov_content_inline
            if content and not content.endswith("\n"):
                content += "\n"
            _ssh_backup(dest)
            encoded = base64.b64encode(content.encode()).decode()
            dir_cmd = f"mkdir -p {_sq(os.path.dirname(dest))}" if os.path.dirname(dest) else "true"
            write_cmd = f"{dir_cmd} && echo {_sq(encoded)} | base64 -d > {_sq(dest + '.cgr_tmp')} && mv {_sq(dest + '.cgr_tmp')} {_sq(dest)}"
            rc, out, err = _ssh_run(write_cmd)
            if rc != 0:
                _ssh_revert()
                return rc, out, f"content write failed: {err}"
            messages.append(f"wrote {dest}")

        # ── line keywords ──────────────────────────────────────────────────
        if res.prov_lines:
            dest = res.prov_dest
            _ssh_backup(dest)
            # Read current file content via SSH
            rc, current_content, _ = _ssh_run(f"cat {_sq(dest)} 2>/dev/null || true")
            file_lines = current_content.splitlines() if current_content else []
            for action, text, regex in res.prov_lines:
                if action == "present":
                    check_cmd = f"grep -qxF {_sq(text)} {_sq(dest)} 2>/dev/null"
                    rc, _, _ = _ssh_run(check_cmd)
                    if rc != 0:
                        _ssh_run(f"echo {_sq(text)} >> {_sq(dest)}")
                        messages.append(f"appended line to {dest}")
                    else:
                        messages.append(f"line already present in {dest}")
                elif action == "absent":
                    _ssh_run(f"grep -v -xF {_sq(text)} {_sq(dest)} > {_sq(dest + '.cgr_tmp')} && mv {_sq(dest + '.cgr_tmp')} {_sq(dest)} || true")
                    messages.append(f"ensured line absent in {dest}")
                elif action == "replace":
                    # Try sed replace; if no match, append
                    # Use python3 on remote for reliable regex handling
                    py_script = (
                        f"import re,sys; "
                        f"lines=open({repr(dest)}).read().splitlines(); "
                        f"replaced=False; "
                        f"result=[]; "
                        f"[result.append({repr(text)}) or result.__setitem__(-1,{repr(text)}) "
                        f"if not replaced and re.search({repr(regex)},l) else result.append(l) for l in lines]; "
                        f"replaced=any(re.search({repr(regex)},l) for l in lines); "
                        f"result=[{repr(text)} if re.search({repr(regex)},l) else l for l in lines]; "
                        f"result = result if replaced else result+[{repr(text)}]; "
                        f"open({repr(dest)},'w').write('\\n'.join(result)+'\\n')"
                    )
                    rc, _, err = _ssh_run(f"python3 -c {_sq(py_script)}")
                    if rc != 0:
                        _ssh_revert()
                        return 1, "", f"line replace failed: {err}"
                    messages.append(f"replaced/appended line in {dest}")

        # ── put keyword ────────────────────────────────────────────────────
        if res.prov_put_src:
            src = res.prov_put_src
            dest = res.prov_dest
            if not os.path.isabs(src) and graph_file:
                src = os.path.join(os.path.dirname(graph_file), src)
            if not os.path.exists(src):
                return 1, "", f"put: source file not found: {src}"
            _ssh_backup(dest)
            ssh_user = node.via_props.get("user", "")
            h = node.via_props.get("host", "localhost")
            p = node.via_props.get("port", "22")
            tgt = f"{ssh_user}@{h}" if ssh_user else h
            sock = None
            if _ssh_control_dir:
                _sock = os.path.join(_ssh_control_dir, f"{ssh_user}_{h}_{p}")
                if os.path.exists(_sock):
                    sock = _sock

            transfer = res.prov_put_transfer  # None, "rsync", or "scp"
            extra_opts = shlex.split(res.prov_put_transfer_opts) if res.prov_put_transfer_opts else []

            # Determine whether to use rsync
            use_rsync = False
            if transfer == "rsync":
                use_rsync = True
            elif transfer != "scp":
                # Auto-detect: check if rsync is available on the remote
                cache_key = f"{ssh_user}@{h}:{p}"
                if cache_key not in _rsync_available_cache:
                    rc_chk, _, _ = _ssh_run("which rsync")
                    _rsync_available_cache[cache_key] = (rc_chk == 0)
                use_rsync = _rsync_available_cache[cache_key]

            rsync_ok = False
            if use_rsync:
                ssh_parts = ["ssh", "-o", "BatchMode=yes"]
                if sock:
                    ssh_parts += ["-o", f"ControlPath={sock}"]
                ssh_parts += ["-p", p]
                ssh_e = " ".join(shlex.quote(x) for x in ssh_parts)
                rsync_cmd = ["rsync", "-az", "--partial"] + extra_opts + ["-e", ssh_e, src, f"{tgt}:{dest}"]
                proc = subprocess.run(rsync_cmd, capture_output=True, text=True, timeout=res.timeout)
                if proc.returncode != 0:
                    if transfer == "rsync":
                        _ssh_revert()
                        return 1, "", f"put (rsync) failed: {proc.stderr}"
                    # Auto-detect: fall back to scp
                    use_rsync = False
                else:
                    rsync_ok = True
                    if res.prov_put_mode:
                        rc, _, err = _ssh_run(f"chmod {res.prov_put_mode} {_sq(dest)}")
                        if rc != 0:
                            return 1, "", f"put chmod failed: {err}"
                    messages.append(f"copied {src} → {dest} (rsync)")

            if not use_rsync and not rsync_ok:
                scp_cmd = ["scp", "-o", "BatchMode=yes"]
                if sock:
                    scp_cmd += ["-o", f"ControlPath={sock}"]
                scp_cmd += extra_opts + ["-P", p, src, f"{tgt}:{dest}"]
                proc = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=res.timeout)
                if proc.returncode != 0:
                    _ssh_revert()
                    return 1, "", f"put (scp) failed: {proc.stderr}"
                if res.prov_put_mode:
                    rc, _, err = _ssh_run(f"chmod {res.prov_put_mode} {_sq(dest)}")
                    if rc != 0:
                        return 1, "", f"put chmod failed: {err}"
                messages.append(f"copied {src} → {dest}")

        # ── block keyword (SSH) ────────────────────────────────────────────
        if res.prov_block_dest:
            import base64 as _b64
            dest = res.prov_block_dest
            marker = res.prov_block_marker or "cgr:block"
            begin_marker = f"# {marker} begin"
            end_marker = f"# {marker} end"
            if res.prov_block_absent:
                py_script = (
                    f"import os; dest={repr(dest)}; bm={repr(begin_marker)}; em={repr(end_marker)};\n"
                    f"lines=open(dest).read().splitlines() if os.path.exists(dest) else [];\n"
                    f"r=[l for l in (lambda s: s)(lines) if not (l.strip()==bm or l.strip()==em or "
                    f"(lambda: [setattr(x,'skip',True) or True for x in [type('',(),{{'skip':False}})()]][0])()"
                    f")];\n"
                    f"open(dest,'w').write('\\n'.join(r)+'\\n') if lines else None"
                )
                # Simpler approach via sed-like shell
                remove_cmd = (
                    f"python3 -c \""
                    f"import os; dest='{dest}'; bm='{begin_marker}'; em='{end_marker}'; "
                    f"lines=open(dest).read().splitlines() if os.path.exists(dest) else []; "
                    f"in_b=False; r=[];"
                    f"[(in_b:=True) if l.strip()==bm else (r.append(l) if not in_b else None) if l.strip()!=em else setattr(type('',(),{{}})(),'x',(in_b:=False)) for l in lines];"
                    f"open(dest,'w').write('\\\\n'.join(r)+'\\\\n') if lines else None\""
                )
                # Use a simple awk-based approach instead for reliability
                awk_cmd = (
                    f"awk '/{begin_marker}/{{skip=1}} /{end_marker}/{{skip=0;next}} !skip' "
                    f"{_sq(dest)} > {_sq(dest + '.cgr_tmp')} && mv {_sq(dest + '.cgr_tmp')} {_sq(dest)} || true"
                )
                rc, _, err = _ssh_run(awk_cmd)
                if rc != 0:
                    _ssh_revert()
                    return 1, "", f"block remove failed: {err}"
                messages.append(f"removed block '{marker}' from {dest}")
            else:
                if res.prov_block_from:
                    src = res.prov_block_from
                    if not os.path.isabs(src) and graph_file:
                        src = os.path.join(os.path.dirname(graph_file), src)
                    try:
                        block_content = open(src).read()
                    except FileNotFoundError:
                        return 1, "", f"block from: source file not found: {src}"
                    block_content = VAR_RE.sub(lambda m: res.env.get(m.group(1), m.group(0)), block_content)
                else:
                    block_content = res.prov_block_inline or ""
                new_block = f"{begin_marker}\n{block_content}"
                if block_content and not block_content.endswith("\n"):
                    new_block += "\n"
                new_block += f"{end_marker}\n"
                _ssh_backup(dest)
                encoded = _b64.b64encode(new_block.encode()).decode()
                # Write new_block to a temp, then use awk to insert/replace in the dest
                write_block_cmd = (
                    f"echo {_sq(encoded)} | base64 -d > {_sq(dest + '.cgr_block')} && "
                    f"if [ -f {_sq(dest)} ] && grep -qF {_sq(begin_marker)} {_sq(dest)}; then "
                    f"  awk -v bm={_sq(begin_marker)} -v em={_sq(end_marker)} -v bf={_sq(dest + '.cgr_block')} "
                    f"  'BEGIN{{skip=0}} $0==bm{{skip=1; while((getline line<bf)>0) print line; next}} "
                    f"  $0==em{{skip=0; next}} !skip{{print}}' {_sq(dest)} > {_sq(dest + '.cgr_tmp')} && "
                    f"  mv {_sq(dest + '.cgr_tmp')} {_sq(dest)}; "
                    f"else "
                    f"  cat {_sq(dest + '.cgr_block')} >> {_sq(dest)} 2>/dev/null || cp {_sq(dest + '.cgr_block')} {_sq(dest)}; "
                    f"fi; "
                    f"rm -f {_sq(dest + '.cgr_block')}"
                )
                rc, _, err = _ssh_run(write_block_cmd)
                if rc != 0:
                    _ssh_revert()
                    return 1, "", f"block write failed: {err}"
                messages.append(f"wrote block '{marker}' in {dest}")

        # ── ini keyword (SSH) ──────────────────────────────────────────────
        if res.prov_ini_dest:
            import json as _json
            dest = res.prov_ini_dest
            _ssh_backup(dest)
            # Build python script to run on remote
            ops_json = _json.dumps(res.prov_ini_ops)
            py = (
                f"import configparser, json, os; "
                f"dest={repr(dest)}; ops=json.loads({repr(ops_json)}); "
                f"p=configparser.RawConfigParser(); p.optionxform=str; "
                f"p.read(dest) if os.path.exists(dest) else None; "
                f"changed=False; "
                f"[(p.add_section(sec) if sec and sec!='DEFAULT' and not p.has_section(sec) else None, "
                f" p.remove_option(sec or 'DEFAULT', key) if val=='absent' else None, "
                f" setattr(type('',(),{{}})(),'c',(p.set(sec or 'DEFAULT', key, val) or True)) if val!='absent' and p.get(sec or 'DEFAULT', key, fallback=None)!=val else None) "
                f"for sec,key,val in ops]; "
                f"os.makedirs(os.path.dirname(dest) or '.', exist_ok=True); "
                f"open(dest+'.cgr_tmp','w').write('') or p.write(open(dest+'.cgr_tmp','w')); "
                f"os.replace(dest+'.cgr_tmp', dest)"
            )
            rc, _, err = _ssh_run(f"python3 -c {_sq(py)}")
            if rc != 0:
                _ssh_revert()
                return 1, "", f"ini edit failed: {err}"
            messages.append(f"updated ini {dest}")

        # ── json keyword (SSH) ─────────────────────────────────────────────
        if res.prov_json_dest:
            import json as _json
            dest = res.prov_json_dest
            _ssh_backup(dest)
            ops_json = _json.dumps(res.prov_json_ops)
            py = (
                f"import json, os; "
                f"dest={repr(dest)}; ops=json.loads({repr(ops_json)}); "
                f"data=json.load(open(dest)) if os.path.exists(dest) else {{}}; "
                f"changed=False; "
                f"exec(open('/dev/stdin').read()) if False else None; "
                f"[( "
                f"  (lambda p,v: "
                f"    (lambda obj,k: obj[k].append(v) if v not in obj[k] else None)"
                f"    (__import__('functools').reduce(lambda d,k: d.setdefault(k,{{}}), p[:-1], data), p[-1]) "
                f"    if p[-1].endswith('[]') else "
                f"    (lambda obj,k: obj.__delitem__(k) if k in obj else None)"
                f"    (__import__('functools').reduce(lambda d,k: d.setdefault(k,{{}}), p[:-1], data), p[-1]) "
                f"    if v=='absent' else "
                f"    (lambda obj,k: obj.__setitem__(k,v))"
                f"    (__import__('functools').reduce(lambda d,k: d.setdefault(k,{{}}), p[:-1], data), p[-1])"
                f"  )(path.rstrip('[]').split('.'), val) "
                f") for path,val in ops]; "
                f"os.makedirs(os.path.dirname(dest) or '.', exist_ok=True); "
                f"json.dump(data, open(dest+'.cgr_tmp','w'), indent=2); "
                f"open(dest+'.cgr_tmp','a').write('\\n'); "
                f"os.replace(dest+'.cgr_tmp', dest)"
            )
            rc, _, err = _ssh_run(f"python3 -c {_sq(py)}")
            if rc != 0:
                _ssh_revert()
                return 1, "", f"json edit failed: {err}"
            messages.append(f"updated json {dest}")

        # ── validate keyword ───────────────────────────────────────────────
        if res.prov_validate:
            rc, vout, verr = _ssh_run(res.prov_validate)
            if rc != 0:
                _ssh_revert()
                return 1, vout, f"validate failed (reverted): {verr}"
            messages.append("validate ok")

        # ── clean up remote backups ────────────────────────────────────────
        for dest in backups:
            _ssh_run(f"rm -f {_sq(dest + '.cgr_bak')} 2>/dev/null || true")

        return 0, "\n".join(messages), ""

    except subprocess.TimeoutExpired:
        _ssh_revert()
        return 124, "", f"Timed out after {res.timeout}s during provisioning"
    except Exception as e:
        _ssh_revert()
        return 1, "", f"provisioning error: {e}"


def _eval_when(expr, variables=None):
    """Evaluate a when expression. Supports:
      - VAR == "value"  / VAR != "value"  (equality/inequality)
      - not VAR  (falsy: empty, false, 0, no, none, or unset)
      - VAR  (truthy: non-empty and not a falsy literal)
    Variables are resolved from the dict if provided."""
    if not expr: return True
    e = expr.strip()
    vs = variables or {}
    def _resolve(token):
        t = token.strip().strip("'\"")
        vm = re.match(r'^\$\{(\w+)\}$', t)
        if vm: t = vm.group(1)
        return vs.get(t, t)  # resolve variable name, fall back to literal
    for op in ("!=", "=="):
        if op in e:
            l, r = e.split(op, 1)
            lv = _resolve(l); rv = _resolve(r)
            return (lv == rv) if op == "==" else (lv != rv)
    # not VAR
    if e.startswith("not "):
        val = _resolve(e[4:])
        return val.lower() in ("", "false", "0", "no", "none")
    # Truthy check
    val = _resolve(e)
    return val.lower() not in ("", "false", "0", "no", "none")

def _last_nonempty_line(text: str) -> str|None:
    for line in reversed(text.splitlines()):
        if line.strip():
            return line.strip()
    return None

def exec_resource(res, node, *, dry_run=False, blast_radius=False, variables=None, graph_file=None,
                  accumulated_collected=None, on_output=None, register_proc=None, unregister_proc=None,
                  cancel_check=None, webhook_server: WebhookGateServer | None = None):
    t0=time.monotonic()
    res = _runtime_resource_view(res, variables)
    if not _eval_when(res.when, variables):
        return ExecResult(resource_id=res.id,status=Status.SKIP_WHEN,
                          duration_ms=int((time.monotonic()-t0)*1000),reason=f"when: {res.when!r} → false")
    # reduce "key": aggregate collected outputs and return
    if res.reduce_key:
        acc = accumulated_collected or {}
        texts = acc.get(res.reduce_key, [])
        aggregated = "\n".join(texts)
        vb = {}
        if res.reduce_var:
            vb[res.reduce_var] = aggregated
        if res.on_success_set:
            vb.update({v: val for v, val in res.on_success_set})
        if res.collect_var:
            vb[res.collect_var] = aggregated
        return ExecResult(resource_id=res.id, status=Status.SUCCESS,
                          run_rc=0, stdout=aggregated,
                          duration_ms=int((time.monotonic()-t0)*1000),
                          var_bindings=vb)
    check_rc=None
    if res.check and res.check.strip().lower()!="false":
        # dry-run always runs checks to show real blast radius (what would execute)
        check_rc,_,_=_run_cmd(res.check,node,res,timeout=min(res.timeout,30), register_proc=register_proc, unregister_proc=unregister_proc, cancel_check=cancel_check)
        if check_rc==0:
            return ExecResult(resource_id=res.id,status=Status.SKIP_CHECK,check_rc=0,
                              duration_ms=int((time.monotonic()-t0)*1000),reason="check passed — already satisfied")
    is_prov = bool(
        res.prov_dest or res.prov_lines or res.prov_put_src
        or res.prov_block_dest or res.prov_ini_dest or res.prov_json_dest
        or res.prov_asserts
    )
    is_wait = bool(res.wait_kind and res.wait_target)
    is_subgraph = bool(res.subgraph_path)
    if dry_run:
        is_http = bool(res.http_method and res.http_url)
        if is_http:
            parts = [f"dry-run: would {res.http_method} {res.http_url}"]
            if res.http_auth: parts.append(f"auth={res.http_auth[0]}")
            if res.http_headers: parts.append(f"headers={len(res.http_headers)}")
            if res.http_body_type: parts.append(f"body={res.http_body_type}")
            if res.http_expect: parts.append(f"expect={res.http_expect}")
            reason = " | ".join(parts)
        elif is_wait:
            label = f"dry-run: would wait for {res.wait_kind} {res.wait_target}"
            if res.wait_kind == "webhook" and webhook_server is not None:
                label += f" ({webhook_server.endpoint(res.wait_target)})"
            reason = label
        elif is_subgraph:
            vars_part = ", ".join(f"{k}={v}" for k, v in sorted((res.subgraph_vars or {}).items()))
            reason = f"dry-run: would apply subgraph {res.subgraph_path}" + (f" [{vars_part}]" if vars_part else "")
        elif is_prov:
            parts = [f"dry-run: would write {res.prov_dest}" if res.prov_dest else "dry-run: would provision"]
            if res.prov_put_src:
                xfer = f" via {res.prov_put_transfer}" if res.prov_put_transfer else ""
                parts.append(f"put {res.prov_put_src}{xfer}")
            if res.prov_lines: parts.append(f"{len(res.prov_lines)} line op(s)")
            if res.prov_block_dest: parts.append(f"block {res.prov_block_dest}")
            if res.prov_ini_dest: parts.append(f"ini {res.prov_ini_dest}")
            if res.prov_json_dest: parts.append(f"json {res.prov_json_dest}")
            if res.prov_asserts: parts.append(f"{len(res.prov_asserts)} assert(s)")
            if res.prov_validate: parts.append("with validate")
            reason = " | ".join(parts)
        else:
            action = "verify" if res.is_verify else ("run script" if res.script_path and not res.run else "run")
            reason = f"dry-run: would {action}"
        return ExecResult(resource_id=res.id,status=Status.PENDING,reason=reason)
    mx=1+res.retries; lr,lo,le=-1,"",""
    timed_out = False
    observed_value = None
    is_http = bool(res.http_method and res.http_url)
    effective_run_cmd = _effective_run_cmd(res, graph_file)
    for att in range(1,mx+1):
        if is_http:
            if node.via_method == "ssh":
                lr,lo,le=_exec_http_ssh(res, node)
            else:
                lr,lo,le=_exec_http_local(res)
        elif is_wait:
            if res.wait_kind == "webhook":
                if webhook_server is None:
                    lr, lo, le = 1, "", "Webhook gate server is not available"
                else:
                    if on_output:
                        on_output("stderr", f"waiting for webhook {webhook_server.endpoint(res.wait_target)}\n")
                    ok = webhook_server.wait(res.wait_target, res.timeout, cancel_check=cancel_check)
                    if ok:
                        lr, lo, le = 0, webhook_server.endpoint(res.wait_target), ""
                    else:
                        timed_out = not (cancel_check and cancel_check())
                        lr, lo, le = (124, "", f"Timed out after {res.timeout}s waiting for webhook {res.wait_target}") if timed_out else (130, "", "Cancelled")
            else:
                wait_path = _resolve_script_fs_path(res.wait_target or "", graph_file) if node.via_method == "local" else (res.wait_target or "")
                if on_output:
                    on_output("stderr", f"waiting for file {wait_path}\n")
                lr, lo, le = _wait_for_file_path(wait_path, node, res, res.timeout, cancel_check=cancel_check)
        elif is_subgraph:
            try:
                failed, _child_ms, child_stdout, child_stderr = _exec_subgraph_resource(
                    res, graph_file=graph_file, webhook_server=webhook_server)
                lr = 1 if failed else 0
                lo, le = child_stdout, child_stderr
            except Exception as e:
                lr, lo, le = 1, "", str(e)
        elif is_prov:
            if node.via_method == "ssh":
                lr,lo,le=_exec_prov_ssh(res, node, graph_file=graph_file)
            else:
                lr,lo,le=_exec_prov_local(res, graph_file=graph_file)
            # If provisioning succeeded and there's also a run command, execute it
            has_real_run = bool(effective_run_cmd) and not effective_run_cmd.startswith("__prov__ ")
            if lr == 0 and has_real_run:
                rr,ro,re_=_run_cmd(effective_run_cmd,node,res,timeout=res.timeout,on_output=on_output, register_proc=register_proc, unregister_proc=unregister_proc, cancel_check=cancel_check)
                lr=rr; lo=(lo+"\n"+ro).strip(); le=(le+"\n"+re_).strip()
        else:
            lr,lo,le=_run_cmd(effective_run_cmd,node,res,timeout=res.timeout,on_output=on_output, register_proc=register_proc, unregister_proc=unregister_proc, cancel_check=cancel_check)
        if cancel_check and cancel_check() and lr in (0, 130):
            return ExecResult(resource_id=res.id,status=Status.CANCELLED,run_rc=lr,check_rc=check_rc,
                stdout=lo,stderr=le or "Cancelled",duration_ms=int((time.monotonic()-t0)*1000),attempts=att,
                reason="cancelled")
        timed_out = (lr == 124 and "Timed out after" in le)
        if cancel_check and cancel_check() and lr != 0:
            return ExecResult(resource_id=res.id,status=Status.CANCELLED,run_rc=lr,check_rc=check_rc,
                stdout=lo,stderr=le or "Stopped by user",duration_ms=int((time.monotonic()-t0)*1000),attempts=att,
                reason="stopped by user")
        observed_value = _last_nonempty_line(lo or "")
        if lr==0 and res.until is not None:
            if observed_value == res.until:
                vb = {v: val for v, val in (res.on_success_set or [])}
                if res.collect_var and lo:
                    vb[res.collect_var] = lo.strip()
                return ExecResult(resource_id=res.id,status=Status.SUCCESS,run_rc=0,check_rc=check_rc,
                    stdout=lo,stderr=le,duration_ms=int((time.monotonic()-t0)*1000),attempts=att,
                    var_bindings=vb, observed_value=observed_value)
            le = (le + ("\n" if le else "") + f"until mismatch: expected {res.until!r}, got {observed_value!r}").strip()
            lr = 1
            if on_output and lo:
                on_output("stdout", lo if lo.endswith("\n") else lo + "\n")
        if lr==0:
            vb = {v: val for v, val in (res.on_success_set or [])}
            if res.collect_var and lo:
                vb[res.collect_var] = lo.strip()
            return ExecResult(resource_id=res.id,status=Status.SUCCESS,run_rc=0,check_rc=check_rc,
                stdout=lo,stderr=le,duration_ms=int((time.monotonic()-t0)*1000),attempts=att,
                var_bindings=vb, observed_value=observed_value)
        if att<mx:
            if res.retry_backoff:
                import random as _random
                delay = res.retry_delay * (2 ** (att - 1))
                if res.retry_backoff_max > 0:
                    delay = min(delay, res.retry_backoff_max)
                if res.retry_jitter_pct > 0:
                    jitter = delay * res.retry_jitter_pct / 100.0
                    delay = delay + _random.uniform(-jitter, jitter)
                time.sleep(max(0.0, delay))
            else:
                time.sleep(res.retry_delay)
    # Distinguish timeout from failure for state tracking (timeout retries on resume)
    fail_vb = {v: val for v, val in (res.on_failure_set or [])}
    if timed_out:
        return ExecResult(resource_id=res.id,status=Status.TIMEOUT,run_rc=lr,check_rc=check_rc,
                          stdout=lo,stderr=le,duration_ms=int((time.monotonic()-t0)*1000),attempts=mx,
                          var_bindings=fail_vb, observed_value=observed_value)
    st=Status.WARNED if res.on_fail==OnFail.WARN else (Status.SUCCESS if res.on_fail==OnFail.IGNORE else Status.FAILED)
    return ExecResult(resource_id=res.id,status=st,run_rc=lr,check_rc=check_rc,stdout=lo,stderr=le,
                      duration_ms=int((time.monotonic()-t0)*1000),attempts=mx,var_bindings=fail_vb,
                      observed_value=observed_value)

def cmd_apply(graph, *, dry_run=False, max_parallel=4, progress_mode=False, webhook_server: WebhookGateServer | None = None):
    total=sum(len(w) for w in graph.waves)
    print(); print(bold(f"{'═'*64}"))
    mode=yellow("DRY-RUN") if dry_run else green("APPLY")
    print(bold(f"  CommandGraph {mode}"))
    for nn,hn in graph.nodes.items():
        if hn.via_method=="ssh":
            print(f"  Target: {cyan(nn)} → ssh {hn.via_props.get('user','?')}@{hn.via_props.get('host','?')}")
        else: print(f"  Target: {cyan(nn)} → local")
    shared=sum(1 for ids in graph.dedup_map.values() if len(ids)>1)
    if shared: print(f"  Deduplication: {shared} shared resource(s)")
    print(f"  Resources: {total}  |  Max concurrent steps per wave: {max_parallel}  |  Mode: stateless")
    print(bold(f"{'═'*64}")); print()
    done=0; started=0; results=[]; hard_fail=False
    start_lock = threading.Lock()
    live_progress = _LiveApplyProgress(enabled=_stdout_supports_live_updates())
    emit_start_updates = progress_mode and not live_progress.enabled
    inline_updates = emit_start_updates and _COLOR and max_parallel == 1
    runtime_collected: dict = {}  # key → list[str] for collect/reduce
    live_progress.start()
    try:
        for wi,wave in enumerate(graph.waves):
            if hard_fail: break
            live_progress.set_wave(wi + 1, len(graph.waves), len(wave))
            live_progress.before_print()
            print(dim(f"── Wave {wi+1}/{len(graph.waves)} ({done}/{total} done, {len(wave)} to run) ──"))
            def _run_with_start(rid):
                nonlocal started
                res = graph.all_resources[rid]
                node = graph.nodes[res.node_name]
                runtime_res = _runtime_resource_view(res, graph.variables)
                command_needs_terminal = (
                    live_progress.enabled
                    and node.via_method != "ssh"
                    and (res.interactive or _command_reads_tty(_effective_run_cmd(runtime_res, graph.graph_file)))
                    and sys.stdin and sys.stdin.isatty()
                    and sys.stdout and sys.stdout.isatty()
                )
                if live_progress.enabled:
                    live_progress.step_started(rid, res.short_name, res.timeout, res.timeout_reset_on_output)
                elif emit_start_updates:
                    with start_lock:
                        started += 1
                        _print_exec_start(started, total, res, graph, inline=inline_updates)
                on_output = None
                if live_progress.enabled and not command_needs_terminal:
                    sensitive = graph.sensitive_values if hasattr(graph, "sensitive_values") else set()
                    def on_output(stream_name, text, _rid=rid, _name=res.short_name, _s=sensitive):
                        if text:
                            live_progress.note_activity(_rid)
                        if stream_name == "stdout":
                            live_progress.stream_stdout(_rid, _name, text, sensitive=_s)
                if command_needs_terminal:
                    live_progress.suspend()
                try:
                    return exec_resource(
                        res,
                        node,
                        dry_run=dry_run,
                        variables=graph.variables,
                        graph_file=graph.graph_file,
                        accumulated_collected=runtime_collected,
                        on_output=on_output,
                        webhook_server=webhook_server,
                    )
                finally:
                    if command_needs_terminal:
                        live_progress.resume()
            with ThreadPoolExecutor(max_workers=min(max_parallel,len(wave))) as pool:
                futs={pool.submit(_run_with_start, rid):rid for rid in wave}
                for f in as_completed(futs):
                    rid=futs[f]; r=f.result(); results.append(r); done+=1
                    res=graph.all_resources[rid]
                    live_progress.step_finished(rid)
                    live_progress.before_print()
                    _print_exec(done,total,res,r,graph,inline=inline_updates)
                    if r.var_bindings:
                        graph.variables.update(r.var_bindings)
                    if res.collect_key and r.status == Status.SUCCESS:
                        runtime_collected.setdefault(res.collect_key, []).append(r.stdout)
                    if r.status in (Status.FAILED, Status.TIMEOUT): hard_fail=True
    finally:
        live_progress.stop()
    _print_summary(results)
    # ── Execute on_complete / on_failure hooks ───────────────────────────
    has_failure = any(r.status in (Status.FAILED, Status.TIMEOUT) for r in results)
    hook = graph.on_failure if has_failure else graph.on_complete
    if hook and not dry_run:
        hook_node = HostNode(name="__hook", via_method="local", via_props={})
        print(dim(f"\n── Hook: {'on_failure' if has_failure else 'on_complete'} ──"))
        hr = exec_resource(hook, hook_node, variables=graph.variables)
        results.append(hr)
        _print_exec(len(results), len(results), hook, hr, graph)
    return results

def _print_exec(idx,total,res,r,graph,verbose=False,indent="",inline=False):
    pfx = indent  # extra indent for grouped resources
    ctr=dim(f"[{idx}/{total}]")
    m={
        Status.SKIP_CHECK:(green("✓ skip "),dim(r.reason)),
        Status.SKIP_WHEN: (dim("○ skip "),dim(r.reason)),
        Status.SUCCESS:   (green("✓ ok   "),dim(f"{r.duration_ms}ms"+( f" ({r.attempts}x)" if r.attempts>1 else ""))),
        Status.WARNED:    (yellow("⚠ warn "),yellow(f"exit={r.run_rc}")),
        Status.FAILED:    (red("✗ fail "),red(f"exit={r.run_rc}")),
        Status.PENDING:   (blue("… plan "),dim(r.reason)),
        Status.CANCELLED: (dim("○ race "),dim(r.reason)),
        Status.TIMEOUT:   (red("⏱ time "),red(f"timed out after {res.timeout}s" + (" without output" if res.timeout_reset_on_output else ""))),
    }
    sym,detail=m.get(r.status,("?",""))
    tag=magenta(" verify") if res.is_verify else ""
    prov=dim(f" ← {res.provenance.source_file}") if res.provenance else ""
    head = f"{pfx}  {ctr} {sym} {bold(res.short_name)}{tag}{prov}"
    if inline:
        inline_detail = ""
        if detail and r.status not in (Status.SUCCESS, Status.SKIP_CHECK, Status.SKIP_WHEN, Status.PENDING):
            inline_detail = f" {detail}"
        sys.stdout.write("\r\033[2K" + head + inline_detail + "\n")
        sys.stdout.flush()
    else:
        print(head)
    pad = pfx + "              "
    if not inline and res.description: print(f"{pad}{dim(res.description)}")
    if detail and not (inline and r.status in (Status.SUCCESS, Status.SKIP_CHECK, Status.SKIP_WHEN, Status.PENDING)):
        print(f"{pad}{detail}")
    _s = graph.sensitive_values if hasattr(graph, 'sensitive_values') else set()
    if res.until is not None:
        want = repr(res.until)
        raw = repr(r.observed_value) if r.observed_value is not None else "None"
        got = _redact(raw, _s)
        print(f"{pad}{dim(f'until: {want} (last={got})')}")
    # Show commands: always on -v, or automatically on failure/warning
    show_commands = verbose or r.status in (Status.FAILED, Status.WARNED)
    if show_commands:
        via = ""
        node = graph.nodes.get(res.node_name)
        if node and node.via_method == "ssh":
            u=node.via_props.get('user',''); h=node.via_props.get('host','')
            p=node.via_props.get('port','22')
            via = f"ssh {u}@{h}:{p} → " if u else f"ssh {h}:{p} → "
        if res.run_as:
            sudo_pfx = f"sudo -u {res.run_as} → "
        else:
            sudo_pfx = ""
        if res.check and res.check.strip().lower() != "false":
            check_trunc = _redact(res.check, _s)[:80] + ("…" if len(res.check) > 80 else "")
            if r.status == Status.SKIP_CHECK:
                print(f"{pad}{dim('check:')} {dim(via + sudo_pfx)}{dim(check_trunc)} {green('→ 0')}")
            else:
                rc_str = str(r.check_rc) if r.check_rc is not None else "≠0"
                print(f"{pad}{dim('check:')} {dim(via + sudo_pfx)}{dim(check_trunc)} {dim(f'→ {rc_str}')}")
        if r.status not in (Status.SKIP_CHECK, Status.SKIP_WHEN, Status.CANCELLED):
            display_cmd = _effective_run_cmd(res, graph.graph_file)
            run_label = "script:" if res.script_path and not res.run else "run:  "
            run_trunc = _redact(display_cmd, _s)[:80] + ("…" if len(display_cmd) > 80 else "")
            rc_color = green if r.run_rc == 0 else red
            rc_str = str(r.run_rc) if r.run_rc is not None else "—"
            print(f"{pad}{dim(run_label)} {dim(via + sudo_pfx)}{run_trunc} {rc_color(f'→ {rc_str}')}")
        if r.stdout and r.stdout.strip():
            stdout_redacted = _redact(r.stdout, _s)
            for line in stdout_redacted.strip().splitlines():
                print(f"{pad}{dim('│')} {line.rstrip()}")
    # Show shared info
    if res.identity_hash in graph.dedup_map:
        sites=graph.dedup_map[res.identity_hash]
        if len(sites)>1:
            others=[s.split(".")[-1] for s in sites if s!=res.id]
            if others: print(f"{pad}{dim(f'(shared with {len(others)} call site(s))')}")
    # Stderr: always show on failure/warning (not gated on verbose)
    if r.status in (Status.FAILED,Status.WARNED) and r.stderr.strip():
        stderr_redacted = _redact(r.stderr, _s)
        for line in stderr_redacted.strip().splitlines():
            print(f"{pad}{red('│')} {line}")
    if not inline:
        print()

def _print_exec_start(idx, total, res, graph, indent="", inline=False):
    pfx = indent
    ctr = dim(f"[{idx}/{total}]")
    tag = magenta(" verify") if res.is_verify else ""
    prov = dim(f" ← {res.provenance.source_file}") if res.provenance else ""
    head = f"{pfx}  {ctr} {blue('▶ run  ')} {bold(res.short_name)}{tag}{prov}"
    if inline:
        sys.stdout.write(head)
        sys.stdout.flush()
        return
    print(head)
    pad = pfx + "              "
    if res.description:
        print(f"{pad}{dim(res.description)}")
    node = graph.nodes.get(res.node_name)
    if node:
        if node.via_method == "ssh":
            user = node.via_props.get("user", "")
            host = node.via_props.get("host", "?")
            port = node.via_props.get("port", "22")
            target = f"ssh {user}@{host}:{port}" if user else f"ssh {host}:{port}"
        else:
            target = "local"
        print(f"{pad}{dim(f'executing on {target}')}")
    print()

def _stdout_supports_live_updates() -> bool:
    try:
        return bool(sys.stdout.isatty())
    except Exception:
        return False

class _LiveApplyProgress:
    def __init__(self, *, enabled: bool, stream=None, interval: float = 0.2):
        self.enabled = enabled
        self.stream = stream or sys.stdout
        self.interval = interval
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._spin = 0
        self._wave_label = ""
        self._running: dict[str, tuple[str, float, float, int | None, bool]] = {}
        self._stdout_buffers: dict[str, str] = {}
        self._stdout_meta: dict[str, tuple[str, set[str]]] = {}
        self._stdout_cr_pending: dict[str, bool] = {}
        self._stdout_cr_line: dict[str, bool] = {}
        self._stdout_deferred_cr_line: dict[str, str] = {}
        self._stdout_cursor: dict[str, int] = {}
        self._stdout_live_preview: dict[str, str] = {}
        self._suspended = False

    def start(self):
        if not self.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="cgr-live-progress", daemon=True)
        self._thread.start()

    def stop(self):
        if not self.enabled:
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=0.5)
        self.clear()

    def clear(self):
        if not self.enabled:
            return
        with self._lock:
            self._write("\r\033[2K")

    def suspend(self):
        if not self.enabled:
            return
        with self._lock:
            self._suspended = True
            self._write("\r\033[2K")

    def resume(self):
        if not self.enabled:
            return
        with self._lock:
            self._suspended = False

    def set_wave(self, current: int, total: int, to_run: int):
        if not self.enabled:
            return
        with self._lock:
            self._wave_label = f"Wave {current}/{total}"
            if to_run <= 0:
                self._running.clear()

    def step_started(self, rid: str, short_name: str, timeout: int | None = None, timeout_reset_on_output: bool = False):
        if not self.enabled:
            return
        with self._lock:
            now = time.monotonic()
            self._running[rid] = (short_name, now, now, timeout, timeout_reset_on_output)
            self._stdout_buffers[rid] = ""
            self._stdout_meta[rid] = ("", set())
            self._stdout_cr_pending[rid] = False
            self._stdout_cr_line[rid] = False
            self._stdout_deferred_cr_line[rid] = ""
            self._stdout_cursor[rid] = 0
            self._stdout_live_preview[rid] = ""

    def note_activity(self, rid: str):
        if not self.enabled:
            return
        with self._lock:
            current = self._running.get(rid)
            if current is None:
                return
            short_name, started_at, _, timeout, timeout_reset_on_output = current
            self._running[rid] = (short_name, started_at, time.monotonic(), timeout, timeout_reset_on_output)

    def step_finished(self, rid: str):
        if not self.enabled:
            return
        with self._lock:
            self._flush_stdout_buffer(rid)
            self._running.pop(rid, None)
            self._stdout_buffers.pop(rid, None)
            self._stdout_meta.pop(rid, None)
            self._stdout_cr_pending.pop(rid, None)
            self._stdout_cr_line.pop(rid, None)
            self._stdout_deferred_cr_line.pop(rid, None)
            self._stdout_cursor.pop(rid, None)
            self._stdout_live_preview.pop(rid, None)

    def before_print(self):
        self.clear()

    def stream_stdout(self, rid: str, short_name: str, text: str, *, indent: str = "", sensitive: set[str] | None = None):
        if not self.enabled or not text:
            return
        with self._lock:
            current = self._running.get(rid)
            if current is not None:
                self._running[rid] = (current[0], current[1], time.monotonic(), current[3], current[4])
            sensitive_set = set(sensitive or set())
            self._stdout_meta[rid] = (indent, sensitive_set)
            buf = self._stdout_buffers.get(rid, "")
            cursor = self._stdout_cursor.get(rid, len(buf))
            text = text.replace("\r\n", "\n")
            for ch in text:
                if ch == "\n":
                    if buf:
                        self._stdout_live_preview[rid] = _redact(buf.rstrip(), sensitive_set)
                        self._emit_stdout_line(short_name, buf, indent=indent, sensitive=sensitive_set)
                    buf = ""
                    cursor = 0
                elif ch == "\r":
                    cursor = 0
                    if buf:
                        self._stdout_live_preview[rid] = _redact(buf.rstrip(), sensitive_set)
                else:
                    if cursor < len(buf):
                        buf = buf[:cursor] + ch + buf[cursor + 1:]
                    else:
                        buf += ch
                    cursor += 1
            if buf:
                self._stdout_live_preview[rid] = _redact(buf.rstrip(), sensitive_set)
            else:
                self._stdout_live_preview[rid] = ""
            self._stdout_buffers[rid] = buf
            self._stdout_cursor[rid] = cursor
            self._stdout_deferred_cr_line[rid] = buf

    def _write(self, text: str):
        self.stream.write(text)
        self.stream.flush()

    def _emit_stdout_line(self, short_name: str, line: str, *, indent: str = "", sensitive: set[str] | None = None):
        redacted = _redact(line.rstrip(), sensitive or set())
        prefix = f"{indent}  {dim('│ out  ')} {bold(short_name)} {dim('|')}"
        self._write("\r\033[2K")
        self._write(f"{prefix} {redacted}\n")
        self._write(self._render())

    def _flush_stdout_buffer(self, rid: str):
        buf = self._stdout_buffers.get(rid, "")
        if not buf:
            buf = self._stdout_deferred_cr_line.get(rid, "")
        if not buf:
            return
        short_name = self._running.get(rid, (rid, 0.0, 0.0, None, False))[0]
        indent, sensitive = self._stdout_meta.get(rid, ("", set()))
        self._emit_stdout_line(short_name, buf, indent=indent, sensitive=sensitive)
        self._stdout_buffers[rid] = ""
        self._stdout_deferred_cr_line[rid] = ""
        self._stdout_cr_line[rid] = False

    def _terminal_columns(self) -> int:
        try:
            return max(20, os.get_terminal_size(self.stream.fileno()).columns)
        except Exception:
            return 120

    def _truncate_visible(self, text: str, width: int) -> str:
        if len(_strip_ansi(text)) <= width:
            return text
        if width <= 1:
            return ""
        return _strip_ansi(text)[:max(0, width - 3)].rstrip() + "..."

    def _render(self) -> str:
        spinner = "|/-\\"[self._spin % 4]
        if not self._running:
            label = self._wave_label or "Preparing apply"
            return f"\r\033[2K  {blue(spinner)} {dim(label + '...')}"
        running_items = list(self._running.items())
        items = [item for _, item in running_items]
        now = time.monotonic()
        shown_parts = []
        for _, (name, _, last_activity_at, timeout, reset_on_output) in running_items[:2]:
            part = name
            if timeout:
                if reset_on_output:
                    remaining = max(0, int(timeout - max(0.0, now - last_activity_at) + 0.999999))
                    part += f" (idle timeout {timeout}s, counter {remaining}s)"
                else:
                    part += f" (timeout {timeout}s)"
            shown_parts.append(part)
        shown = ", ".join(shown_parts)
        if len(items) > 2:
            shown += f" +{len(items) - 2} more"
        elapsed = int(max(now - started_at for _, started_at, _, _, _ in items))
        label = self._wave_label or "Apply"
        preview = ""
        for rid, (name, _, _, _, _) in running_items:
            latest = self._stdout_live_preview.get(rid, "").strip()
            if latest:
                preview = f" | {name}: {latest}"
                break
        label_text = f"{label}: running {shown} ({elapsed}s){preview}"
        label_text = self._truncate_visible(label_text, self._terminal_columns() - 4)
        return f"\r\033[2K  {blue(spinner)} {dim(label_text)}"

    def _run(self):
        while not self._stop.wait(self.interval):
            with self._lock:
                if self._suspended:
                    continue
                self._spin += 1
                self._write(self._render())

def _print_summary(results):
    c=defaultdict(int)
    for r in results: c[r.status]+=1
    tms=sum(r.duration_ms for r in results)
    print(bold(f"{'═'*64}")); print(bold("  Summary")); print(f"  {'─'*44}")
    for st,fn in [(Status.SUCCESS,lambda:green("Succeeded")),
                  (Status.SKIP_CHECK,lambda:green("Skipped (check)")),
                  (Status.SKIP_WHEN,lambda:dim("Skipped (when)")),
                  (Status.WARNED,lambda:yellow("Warned")),
                  (Status.FAILED,lambda:red("Failed")),
                  (Status.PENDING,lambda:blue("Planned")),
                  (Status.CANCELLED,lambda:dim("Cancelled (race)")),
                  (Status.TIMEOUT,lambda:red("Timed out"))]:
        if c[st]: print(f"  {fn():>39s} : {c[st]}")
    print(f"  {'Total time':>39s} : {tms}ms")
    fail=any(r.status in (Status.FAILED, Status.TIMEOUT) for r in results)
    print(f"\n  {red(bold('RESULT: FAILED')) if fail else green(bold('RESULT: OK'))}")
    print(bold(f"{'═'*64}")); print()

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
