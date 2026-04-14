"""Persistent state journals, output journals, and stateful apply entrypoints."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.resolver import *
from cgr_src.executor import *

@dataclass
class StateEntry:
    id: str; status: str; rc: int|None; ms: int; ts: str
    wave: int; check_rc: int|None; manual: bool = False
    observed_value: str|None = None
    var_bindings: dict[str, str] = field(default_factory=dict)


@dataclass
class WaveMetric:
    wave: int
    wall_ms: int
    ts: str
    to_run: int = 0
    skipped: int = 0


@dataclass
class RunMetric:
    wall_ms: int
    ts: str
    total_results: int = 0
    bottleneck_id: str | None = None
    bottleneck_ms: int = 0

class StateFile:
    """Append-only JSON Lines state journal with crash-safe writes."""

    def __init__(self, path: str|Path):
        self.path = Path(path)
        self.lock_path = Path(f"{self.path}.lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, StateEntry] = {}  # latest entry per resource id
        self._wave_metrics: dict[int, WaveMetric] = {}
        self._run_metric: RunMetric | None = None
        self._lock_pid: int|None = None
        self._lock_fh = None
        if self.path.exists():
            self._load()

    def _load(self):
        """Load state from the JSONL file. Tolerates truncated last line."""
        for line_no, raw in enumerate(self.path.read_text().splitlines(), 1):
            raw = raw.strip()
            if not raw: continue
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                print(yellow(f"  warning: skipping malformed state line {line_no}"), file=sys.stderr)
                continue
            if d.get("_lock"):
                self._lock_pid = d.get("pid")
                continue
            if d.get("_unlock"):
                self._lock_pid = None
                continue
            if d.get("_wave"):
                wave = int(d.get("wave", 0) or 0)
                self._wave_metrics[wave] = WaveMetric(
                    wave=wave,
                    wall_ms=int(d.get("wall_ms", 0) or 0),
                    ts=d.get("ts", ""),
                    to_run=int(d.get("to_run", 0) or 0),
                    skipped=int(d.get("skipped", 0) or 0),
                )
                continue
            if d.get("_run"):
                self._run_metric = RunMetric(
                    wall_ms=int(d.get("wall_ms", 0) or 0),
                    ts=d.get("ts", ""),
                    total_results=int(d.get("total_results", 0) or 0),
                    bottleneck_id=d.get("bottleneck_id"),
                    bottleneck_ms=int(d.get("bottleneck_ms", 0) or 0),
                )
                continue
            if d.get("_drop"):
                resource_id = d.get("id")
                if resource_id:
                    self._entries.pop(resource_id, None)
                continue
            if "id" not in d or "status" not in d:
                print(yellow(f"  warning: skipping incomplete state line {line_no}"), file=sys.stderr)
                continue
            se = StateEntry(
                id=d["id"], status=d["status"], rc=d.get("rc"),
                ms=d.get("ms", 0), ts=d.get("ts", ""),
                wave=d.get("wave", 0), check_rc=d.get("check_rc"),
                manual=d.get("manual", False), observed_value=d.get("observed_value"),
                var_bindings=d.get("var_bindings") or {})
            self._entries[se.id] = se

    def is_locked(self) -> tuple[bool, int|None]:
        """Check if another run is active. Returns (locked, pid)."""
        if self._lock_fh is not None:
            return True, self._lock_pid or os.getpid()
        if self.lock_path.exists():
            try:
                with open(self.lock_path, "r") as f:
                    pid_text = f.read().strip()
                pid = int(pid_text) if pid_text else None
            except (OSError, ValueError):
                pid = None
            if pid is not None:
                try:
                    with open(self.lock_path, "a+") as lock_fh:
                        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
                except OSError:
                    return True, pid
        if self._lock_pid is None: return False, None
        # Check if PID is still alive
        try:
            os.kill(self._lock_pid, 0)
            return True, self._lock_pid
        except (OSError, ProcessLookupError):
            return False, self._lock_pid  # stale lock

    def acquire_lock(self):
        """Acquire the OS lock and write a journal sentinel."""
        if self._lock_fh is not None:
            return
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fh = open(self.lock_path, "a+")
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            lock_fh.seek(0)
            pid_text = lock_fh.read().strip()
            lock_fh.close()
            pid = int(pid_text) if pid_text.isdigit() else None
            raise RuntimeError(f"state file is locked by PID {pid}" if pid else "state file is locked")
        lock_fh.seek(0)
        lock_fh.truncate()
        lock_fh.write(str(os.getpid()))
        lock_fh.flush()
        os.fsync(lock_fh.fileno())
        self._lock_fh = lock_fh
        self._lock_pid = os.getpid()
        self._append({"_lock": True, "pid": os.getpid(), "ts": _now()})

    def release_lock(self):
        """Write an unlock sentinel and release the OS lock."""
        if self._lock_fh is None:
            self._lock_pid = None
            return
        self._append({"_unlock": True, "pid": os.getpid(), "ts": _now()})
        try:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._lock_fh.close()
            self._lock_fh = None
        self._lock_pid = None

    class _LockContext:
        def __init__(self, state_file: "StateFile"):
            self.state_file = state_file

        def __enter__(self):
            self.state_file.acquire_lock()
            return self.state_file

        def __exit__(self, exc_type, exc, tb):
            self.state_file.release_lock()
            return False

    def lock(self):
        """Return a context manager that holds the state lock."""
        return self._LockContext(self)

    def get(self, resource_id: str) -> StateEntry|None:
        return self._entries.get(resource_id)

    def all_entries(self) -> dict[str, StateEntry]:
        return dict(self._entries)

    def wave_metrics(self) -> list[WaveMetric]:
        return [self._wave_metrics[k] for k in sorted(self._wave_metrics)]

    def run_metric(self) -> RunMetric | None:
        return self._run_metric

    def should_skip(self, resource_id: str) -> bool:
        """Return True if this resource was completed and should be skipped on resume."""
        e = self._entries.get(resource_id)
        if e is None: return False
        return e.status in ("success", "skip_check", "skip_when")

    def record(self, result: ExecResult, wave: int, sensitive: set[str]|None = None):
        """Append a result to the state journal. Crash-safe (fsync)."""
        redacted_ov = _redact(result.observed_value, sensitive or set()) if result.observed_value else result.observed_value
        redacted_vb = _redact_obj(result.var_bindings or {}, sensitive or set())
        entry = StateEntry(
            id=result.resource_id, status=result.status.value,
            rc=result.run_rc, ms=result.duration_ms, ts=_now(),
            wave=wave, check_rc=result.check_rc, observed_value=redacted_ov,
            var_bindings=redacted_vb or {})
        self._entries[result.resource_id] = entry
        self._append({
            "id": entry.id, "status": entry.status, "rc": entry.rc,
            "ms": entry.ms, "ts": entry.ts, "wave": entry.wave,
            "check_rc": entry.check_rc, "observed_value": entry.observed_value,
            "var_bindings": entry.var_bindings})

    def set_manual(self, resource_id: str, status: str):
        """Manually set a resource's state (done or redo)."""
        entry = StateEntry(
            id=resource_id, status=status, rc=0 if status == "success" else None,
            ms=0, ts=_now(), wave=0, check_rc=None, manual=True)
        self._entries[resource_id] = entry
        self._append({
            "id": entry.id, "status": entry.status, "rc": entry.rc,
            "ms": entry.ms, "ts": entry.ts, "wave": entry.wave,
            "check_rc": entry.check_rc, "manual": True})

    def drop(self, resource_id: str):
        """Remove a resource from state (append a tombstone)."""
        self._entries.pop(resource_id, None)
        self._append({"id": resource_id, "_drop": True, "ts": _now()})

    def record_wave(self, wave: int, wall_ms: int, *, to_run: int = 0, skipped: int = 0):
        metric = WaveMetric(wave=wave, wall_ms=wall_ms, ts=_now(), to_run=to_run, skipped=skipped)
        self._wave_metrics[wave] = metric
        self._append({
            "_wave": True,
            "wave": metric.wave,
            "wall_ms": metric.wall_ms,
            "to_run": metric.to_run,
            "skipped": metric.skipped,
            "ts": metric.ts,
        })

    def record_run(self, wall_ms: int, *, total_results: int = 0,
                   bottleneck_id: str | None = None, bottleneck_ms: int = 0):
        metric = RunMetric(
            wall_ms=wall_ms,
            ts=_now(),
            total_results=total_results,
            bottleneck_id=bottleneck_id,
            bottleneck_ms=bottleneck_ms,
        )
        self._run_metric = metric
        self._append({
            "_run": True,
            "wall_ms": metric.wall_ms,
            "total_results": metric.total_results,
            "bottleneck_id": metric.bottleneck_id,
            "bottleneck_ms": metric.bottleneck_ms,
            "ts": metric.ts,
        })

    def _append(self, obj: dict):
        """Append a JSON line and fsync."""
        with open(self.path, "a") as f:
            f.write(json.dumps(obj, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def reset(self):
        """Delete the state file entirely."""
        if self.path.exists():
            self.path.unlink()
        if self.lock_path.exists():
            self.lock_path.unlink()
        self._entries.clear()
        self._lock_pid = None

    def compact(self) -> int:
        """Compact the state file to only contain latest entry per resource.
        Returns number of lines removed."""
        if not self.path.exists(): return 0
        old_lines = sum(1 for _ in self.path.read_text().splitlines() if _.strip())
        # Write latest entries only
        with open(self.path, "w") as f:
            for rid, entry in sorted(self._entries.items()):
                f.write(json.dumps({
                    "id": entry.id, "status": entry.status, "rc": entry.rc,
                    "ms": entry.ms, "ts": entry.ts, "wave": entry.wave,
                    "check_rc": entry.check_rc,
                    **({"manual": True} if entry.manual else {})
                }, separators=(",", ":")) + "\n")
            for wave in self.wave_metrics():
                f.write(json.dumps({
                    "_wave": True, "wave": wave.wave, "wall_ms": wave.wall_ms,
                    "to_run": wave.to_run, "skipped": wave.skipped, "ts": wave.ts,
                }, separators=(",", ":")) + "\n")
            if self._run_metric is not None:
                f.write(json.dumps({
                    "_run": True,
                    "wall_ms": self._run_metric.wall_ms,
                    "total_results": self._run_metric.total_results,
                    "bottleneck_id": self._run_metric.bottleneck_id,
                    "bottleneck_ms": self._run_metric.bottleneck_ms,
                    "ts": self._run_metric.ts,
                }, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        new_lines = len(self._entries) + len(self._wave_metrics) + (1 if self._run_metric is not None else 0)
        return old_lines - new_lines

    def state_summary(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for e in self._entries.values():
            counts[e.status] += 1
        return dict(counts)


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _sanitize_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", run_id).strip("._-") or "run"


def _state_path(graph_file: str, run_id: str | None = None) -> str:
    cwd = Path.cwd().resolve()
    graph_path = Path(graph_file).expanduser()
    if not graph_path.is_absolute():
        graph_path = (cwd / graph_path).resolve()
    else:
        graph_path = graph_path.resolve()
    try:
        rel = graph_path.relative_to(cwd)
    except ValueError:
        rel = Path("_external", *graph_path.parts[1:]) if graph_path.is_absolute() else Path("_external", graph_path.name)
    suffix = f".{_sanitize_run_id(run_id)}" if run_id else ""
    return str(cwd / ".state" / rel.parent / f"{rel.name}{suffix}.state")

def _output_path(graph_file: str, run_id: str | None = None) -> str:
    suffix = f".{_sanitize_run_id(run_id)}" if run_id else ""
    return graph_file + suffix + ".output"


class OutputFile:
    """Append-only JSONL file for collected command outputs."""

    MAX_SIZE = 1024 * 1024  # 1MB default per entry

    def __init__(self, path: str|Path):
        self.path = Path(path)
        self._entries: dict[str, dict] = {}  # latest entry per key+node
        if self.path.exists():
            self._load()

    def _load(self):
        for raw in self.path.read_text().splitlines():
            raw = raw.strip()
            if not raw: continue
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                continue
            k = f"{d.get('node','')}/{d.get('key','')}"
            self._entries[k] = d

    def record(self, resource_id: str, key: str, node: str,
               stdout: str, stderr: str, rc: int|None, ms: int,
               sensitive: set[str]|None = None):
        """Append a collected output entry. Crash-safe (fsync)."""
        stdout = _redact(stdout, sensitive) if sensitive else stdout
        stderr = _redact(stderr, sensitive) if sensitive else stderr
        stdout_t = stdout[:self.MAX_SIZE]
        if len(stdout) > self.MAX_SIZE:
            stdout_t += f"\n[truncated at {self.MAX_SIZE // 1024}KB]"
        entry = {"id": resource_id, "key": key, "node": node,
                 "stdout": stdout_t, "stderr": stderr[:self.MAX_SIZE],
                 "rc": rc, "ms": ms, "ts": _now()}
        self._entries[f"{node}/{key}"] = entry
        with open(self.path, "a") as f:
            f.write(json.dumps(entry, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def all_outputs(self) -> list[dict]:
        """Return all collected outputs (latest per key+node)."""
        return list(self._entries.values())

    def get_by_key(self, key: str) -> list[dict]:
        """Return all outputs matching a key (across nodes)."""
        return [d for d in self._entries.values() if d.get("key") == key]

    def keys(self) -> list[str]:
        """Return unique collect keys."""
        return sorted(set(d.get("key", "") for d in self._entries.values()))

    def reset(self):
        """Delete the output file entirely."""
        if self.path.exists():
            self.path.unlink()
        self._entries.clear()


# ── State-aware apply ──────────────────────────────────────────────────

def _describe_group(group_key: str, count: int, limit: int|None, is_race: bool) -> tuple[str, str, str]:
    """Generate a human-readable banner for a parallel group.
    Returns (type_tag, label, info_string)."""
    # Extract the parent step name and construct type from the group key
    # e.g. "deploy.configure_everything.__parallel" → ("configure_everything", "parallel")
    parts = group_key.split(".")
    construct = parts[-1] if parts else ""
    # Walk backwards to find the parent step name (skip __ prefixed parts)
    parent = ""
    for p in reversed(parts[:-1]):
        if not p.startswith("__"):
            parent = p; break

    # Clean up parent name
    parent_label = parent.replace("_", " ") if parent else "group"

    if is_race or "__race" in construct:
        return ("⚡ race", parent_label, f"({count} branches, first wins)")
    elif "__each" in construct:
        lim = f", {limit} at a time" if limit else ""
        return ("↔ each", parent_label, f"({count} items{lim})")
    elif "__phase" in construct:
        phase_num = construct.replace("__phase_", "")
        lim = f", {limit} at a time" if limit else ""
        return ("◆ phase", parent_label, f"(phase {phase_num}, {count} items{lim})")
    elif "__parallel" in construct:
        lim = f", {limit} at a time" if limit and limit < count else ""
        return ("║ parallel", parent_label, f"({count} branches{lim})")
    else:
        return ("║ group", parent_label, f"({count} resources)")


def cmd_apply_stateful(graph, graph_file, *, dry_run=False, max_parallel=4, no_resume=False, verbose=False, global_timeout=None, start_from=None, log_file=None, on_complete=None, become_pass=None, blast_radius=False, include_tags=None, exclude_tags=None, skip_steps=None, confirm=False, run_id=None, state_path=None, progress_mode=False, webhook_server: WebhookGateServer | None = None):
    """Execute the graph with persistent state tracking for crash recovery.
    Returns (results, wall_clock_ms)."""
    wall_t0 = time.monotonic()
    deadline = wall_t0 + global_timeout if global_timeout is not None else None

    # Resolve start_from to a set of resource IDs to skip
    skip_before: set[str] = set()
    if start_from:
        slug = _slug(start_from)
        # Find the target resource
        target_rid = None
        for rid, res in graph.all_resources.items():
            if res.short_name == slug or rid.endswith("." + slug) or rid == start_from:
                target_rid = rid; break
        if not target_rid:
            print(red(f"error: --start-from '{start_from}' not found."))
            print(f"  Available: {', '.join(r.short_name for r in graph.all_resources.values())}")
            sys.exit(1)
        # Find which wave the target is in
        target_wave = None
        for wi, wave in enumerate(graph.waves):
            if target_rid in wave: target_wave = wi; break
        if target_wave is not None:
            # Skip all resources in waves before the target wave
            for wi in range(target_wave):
                skip_before.update(graph.waves[wi])
    # Resolve --skip steps to resource IDs
    skip_rids: set[str] = set()
    for skip_name in (skip_steps or []):
        slug = _slug(skip_name)
        found = False
        for rid, res in graph.all_resources.items():
            if res.short_name == slug or rid.endswith("." + slug) or rid == skip_name:
                skip_rids.add(rid); found = True; break
        if not found:
            print(yellow(f"  warning: --skip '{skip_name}' not found, ignoring."))

    sp = state_path or _state_path(graph_file, run_id=run_id)
    effective_stateless = graph.stateless and not state_path
    if graph.stateless and state_path:
        print(yellow(f"  warning: --state FILE overrides 'set stateless = true'; state will be persisted."))
    state = StateFile(sp) if not no_resume and not effective_stateless else None

    # Output collector for steps with collect clauses
    has_collect = any(r.collect_key for r in graph.all_resources.values())
    output = OutputFile(_output_path(graph_file, run_id=run_id)) if has_collect and not dry_run else None

    total = sum(len(w) for w in graph.waves)

    # Count how many will be skipped from state
    skip_from_state = 0
    if state and not no_resume:
        for rid in graph.all_resources:
            if state.should_skip(rid): skip_from_state += 1

    # --start-from: warn about skipped unsatisfied dependencies
    if start_from and skip_before:
        unsatisfied = []
        for rid in skip_before:
            res = graph.all_resources.get(rid)
            if res and not res.is_barrier and not (state and state.should_skip(rid)):
                unsatisfied.append(res.short_name)
        if unsatisfied:
            print(yellow(f"  ⚠ --start-from is skipping {len(unsatisfied)} step(s) that have NOT previously succeeded:"))
            for name in unsatisfied[:10]:
                print(f"    - {name}")
            if len(unsatisfied) > 10:
                print(f"    ... and {len(unsatisfied) - 10} more")
            print(f"    If '{start_from}' depends on these, it may fail.")

    # Header
    print(); print(bold(f"{'═'*64}"))
    mode = yellow("DRY-RUN") if dry_run else green("APPLY")
    print(bold(f"  CommandGraph {mode}"))
    for nn, hn in graph.nodes.items():
        if hn.via_method == "ssh":
            print(f"  Target: {cyan(nn)} → ssh {hn.via_props.get('user','?')}@{hn.via_props.get('host','?')}")
        else:
            print(f"  Target: {cyan(nn)} → local")
    hdr = f"  Resources: {total}  |  Max concurrent steps per wave: {max_parallel}"
    if global_timeout is not None: hdr += f"  |  Timeout: {global_timeout}s"
    if start_from: hdr += f"  |  Start: {start_from}"
    print(hdr)
    if skip_from_state:
        print(f"  Resuming: {green(str(skip_from_state))} already done, {total - skip_from_state} remaining")
        if skip_from_state >= total:
            print(f"  {dim('All steps are already marked complete in the state file.')}")
            print(f"  {dim('Use --no-resume to ignore the state file and run everything fresh.')}")
    if effective_stateless:
        print(f"  State: {dim('(stateless — no disk persistence)')}")
    else:
        print(f"  State: {dim(sp)}")
    if log_file:
        print(f"  Log: {dim(log_file)}")
    print(bold(f"{'═'*64}")); print()

    # Dry-run diff: compare current graph against state file
    if dry_run and state and Path(sp).exists():
        state_ids = set(state.all_entries().keys())
        graph_ids = set(rid for rid, r in graph.all_resources.items() if not r.is_barrier)
        new_steps = graph_ids - state_ids
        removed_steps = state_ids - graph_ids
        if new_steps or removed_steps:
            print(bold("  Changes since last run:"))
            for rid in sorted(new_steps):
                r = graph.all_resources.get(rid)
                print(f"    {green('+ new')}     {bold(r.short_name) if r else rid}")
            for rid in sorted(removed_steps):
                print(f"    {red('- removed')} {bold(rid.split('.')[-1])}")
            print()

    # Open log file for structured logging
    log_fh = None
    if log_file and not dry_run:
        log_fh = open(log_file, "a")
        log_fh.write(json.dumps({"type": "start", "file": graph_file, "ts": _now(),
                                  "resources": total, "version": __version__}) + "\n")
        log_fh.flush(); os.fsync(log_fh.fileno())

    # Store become_pass for _run_cmd access
    global _become_pass
    _become_pass = become_pass

    lock_cm = state.lock() if state and not dry_run else nullcontext()

    # Start SSH connection pool (ControlMaster) for all SSH targets
    has_ssh = any(hn.via_method == "ssh" for hn in graph.nodes.values())
    if has_ssh and not dry_run:
        _ssh_pool_start(graph)

    done = 0; started = 0; results = []; hard_fail = False
    start_lock = threading.Lock()
    live_progress = _LiveApplyProgress(enabled=_stdout_supports_live_updates())
    runtime_collected: dict = {}  # key → list[str] for collect/reduce
    owned_webhook_server = None
    if webhook_server is None and any(r.wait_kind == "webhook" for r in graph.all_resources.values()):
        webhook_server = owned_webhook_server = WebhookGateServer()
    try:
        live_progress.start()
        with lock_cm:
            for wi, wave in enumerate(graph.waves):
                wave_t0 = time.monotonic()
                if hard_fail: break
                # Check global timeout before starting a new wave
                if deadline and time.monotonic() > deadline:
                    elapsed = int(time.monotonic() - wall_t0)
                    print(red(f"\n  ⏱ Global timeout reached ({global_timeout}s) after {elapsed}s — aborting."))
                    hard_fail = True; break

                # Partition wave into skip (from state, --start-from, or tags) and execute
                to_skip_start = []; to_skip_state = []; to_run = []; to_skip_tag = []; to_skip_manual = []
                for rid in wave:
                    if rid in skip_before:
                        to_skip_start.append(rid)
                    elif state and state.should_skip(rid):
                        to_skip_state.append(rid)
                    elif rid in skip_rids:
                        to_skip_manual.append(rid)
                    elif _should_skip_tags(graph.all_resources[rid], include_tags, exclude_tags):
                        to_skip_tag.append(rid)
                    else:
                        to_run.append(rid)
                live_progress.set_wave(wi + 1, len(graph.waves), len(to_run))

                skip_info = []
                if to_skip_start: skip_info.append(f"{len(to_skip_start)} before start")
                if to_skip_state: skip_info.append(f"{len(to_skip_state)} from state")
                if to_skip_manual: skip_info.append(f"{len(to_skip_manual)} skipped")
                if to_skip_tag: skip_info.append(f"{len(to_skip_tag)} by tag")
                if skip_info:
                    live_progress.before_print()
                    print(dim(f"── Wave {wi+1}/{len(graph.waves)} ({', '.join(skip_info)}, {len(to_run)} to run) ──"))
                    for rid in to_skip_start:
                        done += 1
                        res = graph.all_resources[rid]
                        print(f"  {dim(f'[{done}/{total}]')} {yellow('○ skip ')} {bold(res.short_name)}")
                        print(f"              {dim('--start-from')}")
                        print()
                        results.append(ExecResult(resource_id=rid, status=Status.SKIP_WHEN,
                                                 duration_ms=0, reason="skipped before --start-from"))
                    for rid in to_skip_state:
                        done += 1
                        res = graph.all_resources[rid]
                        se = state.get(rid) if state else None
                        if se and se.var_bindings:
                            graph.variables.update(se.var_bindings)
                        ms_str = f"{se.ms}ms" if se else ""
                        print(f"  {dim(f'[{done}/{total}]')} {green('✓ state')} {bold(res.short_name)}")
                        print(f"              {dim(f'completed previously {ms_str}')}")
                        print()
                        # Add a synthetic result for the summary
                        results.append(ExecResult(resource_id=rid, status=Status.SKIP_CHECK,
                                                 duration_ms=0, reason="from state"))
                    for rid in to_skip_manual:
                        done += 1
                        res = graph.all_resources[rid]
                        print(f"  {dim(f'[{done}/{total}]')} {yellow('○ skip ')} {bold(res.short_name)}")
                        print(f"              {dim('--skip flag')}")
                        print()
                        results.append(ExecResult(resource_id=rid, status=Status.SKIP_WHEN,
                                                 duration_ms=0, reason="skipped by --skip flag"))
                        if state and not dry_run:
                            state.record(ExecResult(resource_id=rid, status=Status.SKIP_WHEN,
                                                    duration_ms=0, reason="manual skip"), wi + 1,
                                         sensitive=graph.sensitive_values)
                    for rid in to_skip_tag:
                        done += 1
                        res = graph.all_resources[rid]
                        print(f"  {dim(f'[{done}/{total}]')} {dim('○ tag  ')} {bold(res.short_name)}")
                        print()
                        results.append(ExecResult(resource_id=rid, status=Status.SKIP_WHEN,
                                                 duration_ms=0, reason="skipped by tag filter"))
                elif to_run:
                    live_progress.before_print()
                    print(dim(f"── Wave {wi+1}/{len(graph.waves)} ({len(to_run)} resources) ──"))

                if not to_run:
                    live_progress.set_wave(wi + 1, len(graph.waves), 0)
                    wave_wall_ms = int((time.monotonic() - wave_t0) * 1000)
                    if state and not dry_run:
                        state.record_wave(wi + 1, wave_wall_ms, to_run=0,
                                          skipped=len(to_skip_start) + len(to_skip_state) + len(to_skip_manual) + len(to_skip_tag))
                    continue

                # Interactive confirmation before each wave
                if confirm and not dry_run and to_run:
                    names = [graph.all_resources[rid].short_name for rid in to_run[:5]]
                    extra = f" ... and {len(to_run)-5} more" if len(to_run) > 5 else ""
                    print(f"\n  {yellow('▶')} Wave {wi+1}/{len(graph.waves)} will run {len(to_run)} step(s): {', '.join(names)}{extra}")
                    try:
                        ans = input(f"  Proceed? [y/N/q] ").strip().lower()
                    except (EOFError, KeyboardInterrupt):
                        ans = "q"
                    if ans in ("q", "quit"):
                        print(dim("  Aborted by user.")); hard_fail = True; break
                    if ans not in ("y", "yes"):
                        print(dim("  Skipping wave.")); continue

                # Group resources by parallel_group for per-group concurrency
                groups: dict[str|None, list[str]] = defaultdict(list)
                for rid in to_run:
                    res = graph.all_resources[rid]
                    groups[res.parallel_group].append(rid)

                for group_key, group_rids in groups.items():
                    if not group_rids: continue

                    # Determine concurrency for this group
                    sample = graph.all_resources[group_rids[0]]
                    limit = sample.concurrency_limit or max_parallel
                    is_race = sample.is_race

                    # Print group banner for named parallel groups
                    if group_key is not None:
                        live_progress.before_print()
                        gtype, glabel, ginfo = _describe_group(group_key, len(group_rids), limit, is_race)
                        print(f"  {cyan(gtype)} {bold(glabel)} {dim(ginfo)}")

                    gi = "    " if group_key is not None else ""  # group indent

                    emit_start_updates = progress_mode and not live_progress.enabled
                    inline_updates = emit_start_updates and _COLOR and limit == 1

                    def _run_with_start(rid, *, cancel_check=None, register_proc=None, unregister_proc=None):
                        nonlocal started
                        res = graph.all_resources[rid]
                        node = graph.nodes[res.node_name]
                        runtime_res = _runtime_resource_view(res, graph.variables)
                        command_needs_terminal = (
                            live_progress.enabled
                            and node.via_method != "ssh"
                            and _command_reads_tty(_effective_run_cmd(runtime_res, graph_file))
                            and sys.stdin and sys.stdin.isatty()
                            and sys.stdout and sys.stdout.isatty()
                        )
                        if live_progress.enabled:
                            live_progress.step_started(rid, res.short_name, res.timeout, res.timeout_reset_on_output)
                        elif emit_start_updates:
                            with start_lock:
                                started += 1
                                _print_exec_start(started, total, res, graph, indent=gi, inline=inline_updates)
                        on_output = None
                        if live_progress.enabled and not command_needs_terminal:
                            sensitive = graph.sensitive_values if hasattr(graph, "sensitive_values") else set()
                            def on_output(stream_name, text, _rid=rid, _name=res.short_name, _gi=gi, _s=sensitive):
                                if text:
                                    live_progress.note_activity(_rid)
                                if stream_name == "stdout":
                                    live_progress.stream_stdout(_rid, _name, text, indent=_gi, sensitive=_s)
                        if command_needs_terminal:
                            live_progress.suspend()
                        try:
                            return exec_resource(
                                res,
                                node,
                                dry_run=dry_run,
                                blast_radius=blast_radius,
                                variables=graph.variables,
                                accumulated_collected=runtime_collected,
                                on_output=on_output,
                                cancel_check=cancel_check,
                                register_proc=register_proc,
                                unregister_proc=unregister_proc,
                                graph_file=graph_file,
                                webhook_server=webhook_server,
                            )
                        finally:
                            if command_needs_terminal:
                                live_progress.resume()

                    if is_race:
                        # Race mode: first success wins, all others become cancelled
                        race_won = False
                        race_results: list[tuple] = []  # (rid, ExecResult, Resource)
                        race_cancel = threading.Event()
                        # Detect race_into destination (all branches share the same dest)
                        race_dest = sample.race_into  # None if plain race:
                        with ThreadPoolExecutor(max_workers=min(limit, len(group_rids))) as pool:
                            futs = {pool.submit(_run_with_start, rid, cancel_check=race_cancel.is_set): rid
                                    for rid in group_rids}
                            for f in as_completed(futs):
                                rid = futs[f]; r = f.result(); done += 1
                                if r.var_bindings:
                                    graph.variables.update(r.var_bindings)
                                res = graph.all_resources[rid]
                                live_progress.step_finished(rid)
                                race_results.append((rid, r, res))
                                if r.status == Status.SUCCESS and not race_won:
                                    race_won = True
                                    race_cancel.set()
                                    # Cancel remaining futures
                                    for other_f in futs:
                                        if other_f != f and not other_f.done():
                                            other_f.cancel()
                        # Post-process: winner gets recorded as-is, all others as CANCELLED
                        winner_found = False
                        winner_branch_idx = None
                        for rid, r, res in race_results:
                            if r.status == Status.SUCCESS and not winner_found:
                                winner_found = True
                                winner_branch_idx = res.race_branch_index
                                results.append(r)
                                if res.collect_key:
                                    runtime_collected.setdefault(res.collect_key, []).append(r.stdout)
                                live_progress.before_print()
                                _print_exec(done - len(race_results) + race_results.index((rid, r, res)) + 1, total, res, r, graph, verbose=verbose, indent=gi, inline=inline_updates)
                                if state and not dry_run: state.record(r, wi + 1, sensitive=graph.sensitive_values)
                                if output and res.collect_key:
                                    output.record(rid, res.collect_key, res.node_name,
                                                  r.stdout, r.stderr, r.run_rc, r.duration_ms,
                                                  sensitive=graph.sensitive_values)
                            else:
                                reason = "race: another branch won" if winner_found else f"race: lost (exit={r.run_rc})"
                                cr = ExecResult(resource_id=rid, status=Status.CANCELLED, run_rc=r.run_rc,
                                                stdout=r.stdout, stderr=r.stderr, duration_ms=r.duration_ms,
                                                attempts=r.attempts, reason=reason)
                                results.append(cr)
                                idx = done - len(race_results) + race_results.index((rid, r, res)) + 1
                                live_progress.before_print()
                                print(f"{gi}  {dim(f'[{idx}/{total}]')} {dim('○ race ')} {bold(res.short_name)}")
                                display_reason = "cancelled — another branch won" if winner_found else f"lost — exit={r.run_rc}"
                                print(f"{gi}              {dim(display_reason)}")
                                print()
                                if state and not dry_run: state.record(cr, wi + 1, sensitive=graph.sensitive_values)
                        # race into: rename winner's temp to final dest, clean up losers
                        if race_dest and not dry_run:
                            branch_count = len(group_rids)
                            if winner_found and winner_branch_idx is not None:
                                winner_tmp = f"{race_dest}.race.{winner_branch_idx}"
                                try:
                                    if os.path.exists(winner_tmp):
                                        os.replace(winner_tmp, race_dest)
                                except OSError:
                                    pass  # best-effort
                            # Clean up all temp files (including loser branches)
                            for bi in range(branch_count):
                                tmp = f"{race_dest}.race.{bi}"
                                try:
                                    if os.path.exists(tmp):
                                        os.remove(tmp)
                                except OSError:
                                    pass  # best-effort cleanup
                        if not winner_found:
                            hard_fail = True  # ALL branches failed — race is lost
                    else:
                        # Normal or parallel mode: respect concurrency limit
                        with ThreadPoolExecutor(max_workers=min(limit, len(group_rids))) as pool:
                            futs = {pool.submit(_run_with_start, rid): rid
                                    for rid in group_rids}
                            for f in as_completed(futs):
                                rid = futs[f]; r = f.result(); results.append(r); done += 1
                                res = graph.all_resources[rid]
                                live_progress.step_finished(rid)
                                live_progress.before_print()
                                _print_exec(done, total, res, r, graph, verbose=verbose, indent=gi, inline=inline_updates)
                                if r.var_bindings:
                                    graph.variables.update(r.var_bindings)
                                if res.collect_key and r.status == Status.SUCCESS:
                                    runtime_collected.setdefault(res.collect_key, []).append(r.stdout)
                                if state and not dry_run: state.record(r, wi + 1, sensitive=graph.sensitive_values)
                                if log_fh:
                                    log_fh.write(json.dumps({"type":"step","id":rid,"name":res.short_name,
                                        "status":r.status.value,"rc":r.run_rc,"ms":r.duration_ms,
                                        "wave":wi+1,"ts":_now()}) + "\n")
                                    log_fh.flush(); os.fsync(log_fh.fileno())
                                if output and res.collect_key and r.status in (Status.SUCCESS, Status.FAILED, Status.WARNED):
                                    output.record(rid, res.collect_key, res.node_name,
                                                  r.stdout, r.stderr, r.run_rc, r.duration_ms,
                                                  sensitive=graph.sensitive_values)
                                if r.status in (Status.FAILED, Status.TIMEOUT):
                                    if group_key is None:
                                        # Ungrouped resource: use its own on_fail policy
                                        if res.on_fail == OnFail.STOP or r.status == Status.TIMEOUT:
                                            hard_fail = True
                                        # WARN and IGNORE don't hard_fail
                                    else:
                                        # Grouped resource: use the group's failure policy
                                        fp = sample.group_fail_policy
                                        if fp == "stop":
                                            hard_fail = True
                                            for other_f in futs:
                                                if not other_f.done(): other_f.cancel()
                                        elif fp == "ignore":
                                            pass
                                        # "wait" = default, continue all
                wave_wall_ms = int((time.monotonic() - wave_t0) * 1000)
                live_progress.set_wave(wi + 1, len(graph.waves), 0)
                if state and not dry_run:
                    state.record_wave(wi + 1, wave_wall_ms, to_run=len(to_run), skipped=len(to_skip_start) + len(to_skip_state) + len(to_skip_manual) + len(to_skip_tag))
    except RuntimeError as e:
        live_progress.before_print()
        print(red(f"error: {e}"))
        print(f"  If the process crashed, run: cgr state reset {graph_file}")
        sys.exit(1)
    finally:
        live_progress.stop()
        # Always release lock and close SSH pool, even on crash/interrupt
        _become_pass = None
        if has_ssh and not dry_run:
            _ssh_pool_stop()
        if owned_webhook_server is not None:
            owned_webhook_server.stop()

    _print_summary(results)
    if output and output.all_outputs():
        op = _output_path(graph_file, run_id=run_id)
        n = len(output.all_outputs())
        print(f"  {cyan('📋 Collected')} {n} output(s) → {dim(op)}")
        report_cmd = f"cgr report {graph_file}"
        if run_id:
            report_cmd += f" --run-id {shlex.quote(run_id)}"
        print(f"  View with: {report_cmd}")
        print()
    wall_ms = int((time.monotonic() - wall_t0) * 1000)
    if state and not dry_run:
        step_metrics = {
            r.resource_id: r.duration_ms
            for r in results
            if r.resource_id in graph.all_resources and not graph.all_resources[r.resource_id].is_barrier
        }
        bottleneck_id = max(step_metrics, key=step_metrics.get) if step_metrics else None
        state.record_run(wall_ms, total_results=len(results), bottleneck_id=bottleneck_id,
                         bottleneck_ms=step_metrics.get(bottleneck_id, 0) if bottleneck_id else 0)

    # Finalize log
    if log_fh:
        failed_count = sum(1 for r in results if r.status in (Status.FAILED, Status.TIMEOUT))
        log_fh.write(json.dumps({"type":"done","ts":_now(),"wall_ms":wall_ms,
                                  "total":len(results),"failed":failed_count}) + "\n")
        log_fh.flush(); os.fsync(log_fh.fileno())
        log_fh.close()
        print(f"  {dim(f'Log → {log_file}')}")

    # on-complete hook
    if on_complete:
        failed_count = sum(1 for r in results if r.status in (Status.FAILED, Status.TIMEOUT))
        env_extra = {
            **os.environ,
            "CGR_STATUS": "failed" if failed_count else "ok",
            "CGR_TOTAL": str(len(results)),
            "CGR_FAILED": str(failed_count),
            "CGR_FILE": graph_file,
            "CGR_WALL_MS": str(wall_ms),
        }
        try:
            subprocess.run(on_complete, shell=True, env=env_extra, timeout=60)
        except Exception as e:
            print(yellow(f"  warning: --on-complete failed: {e}"))

    return results, wall_ms


# ── State CLI commands ─────────────────────────────────────────────────

def cmd_state_show(graph_file: str, graph: Graph|None = None):
    """Display the state table for a graph."""
    sp = _state_path(graph_file)
    state = StateFile(sp)
    entries = state.all_entries()

    if not entries:
        print(dim(f"  No state file found at {sp}"))
        print(dim(f"  Run 'cgr apply {graph_file}' to create one."))
        return

    # Get all resource IDs from graph if available
    all_ids = set(graph.all_resources.keys()) if graph else set(entries.keys())

    print()
    print(bold(f"State: {sp}"))
    mtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(Path(sp).stat().st_mtime))
    print(dim(f"  Last modified: {mtime}"))
    print(bold("─" * 64))

    sym_map = {
        "success": green("✓ done   "),
        "skip_check": green("✓ skip   "),
        "skip_when": dim("○ skip   "),
        "warned": yellow("⚠ warned "),
        "failed": red("✗ failed "),
        "cancelled": dim("○ race   "),
    }

    done_count = fail_count = pending_count = 0

    # Show entries for known resources in wave order
    if graph:
        shown = set()
        for wave in graph.waves:
            for rid in wave:
                shown.add(rid)
                e = entries.get(rid)
                res = graph.all_resources.get(rid)
                short = res.short_name if res else rid.split(".")[-1]
                if e:
                    sym = sym_map.get(e.status, dim("? unknown"))
                    ms = f"{e.ms}ms" if e.ms else "—"
                    extra = ""
                    if e.manual: extra = dim(" (manual)")
                    if e.status == "failed": extra = red(f" exit={e.rc}")
                    if e.observed_value is not None and e.status in ("failed", "timeout", "warned"):
                        extra += dim(f" last={e.observed_value!r}")
                    print(f"  {sym} {bold(f'{short:28s}')} {dim(f'{ms:>8s}')}  {dim(e.ts)}{extra}")
                    if e.status in ("success", "skip_check", "skip_when"): done_count += 1
                    elif e.status == "failed": fail_count += 1
                else:
                    print(f"  {dim('· pending ')} {bold(f'{short:28s}')} {dim('—'):>8s}  {dim('(never ran)')}")
                    pending_count += 1
        # Orphaned entries (in state but not in graph)
        for rid, e in entries.items():
            if rid not in shown:
                rid_short = rid.split('.')[-1]
                print(f"  {dim('? orphan  ')} {dim(f'{rid_short:28s}')} {dim('—'):>8s}  {dim('not in current graph')}")
    else:
        for rid, e in sorted(entries.items()):
            sym = sym_map.get(e.status, dim("? unknown"))
            ms = f"{e.ms}ms" if e.ms else "—"
            short = rid.split(".")[-1]
            print(f"  {sym} {bold(f'{short:28s}')} {dim(f'{ms:>8s}')}  {dim(e.ts)}")
            if e.status in ("success", "skip_check", "skip_when"): done_count += 1
            elif e.status == "failed": fail_count += 1

    print(bold("─" * 64))
    parts = []
    if done_count: parts.append(green(f"Done: {done_count}"))
    if fail_count: parts.append(red(f"Failed: {fail_count}"))
    if pending_count: parts.append(dim(f"Pending: {pending_count}"))
    print(f"  {' | '.join(parts)}")
    run_metric = state.run_metric()
    if run_metric is not None:
        bottleneck = ""
        if run_metric.bottleneck_id:
            bottleneck = f"  |  Bottleneck: {run_metric.bottleneck_id.split('.')[-1]} ({run_metric.bottleneck_ms}ms)"
        print(f"  {dim(f'Last run wall time: {run_metric.wall_ms}ms')}{dim(bottleneck)}")

    # Show resume hint
    if fail_count or pending_count:
        print(f"  {dim('Re-run will resume from the first failed/pending step.')}")
    print()


def cmd_state_test(graph_file: str, graph: Graph, repo_dir: str|None = None):
    """Re-run check commands against live system to verify state accuracy."""
    sp = _state_path(graph_file)
    state = StateFile(sp)
    entries = state.all_entries()
    _s = graph.sensitive_values if hasattr(graph, 'sensitive_values') else set()

    if not entries:
        print(dim("  No state to test.")); return

    print()
    print(bold("Testing state against live system..."))
    print(bold("─" * 64))

    valid = 0; drifted = 0; untestable = 0

    for rid, entry in sorted(entries.items()):
        if entry.status not in ("success", "skip_check"): continue
        res = graph.all_resources.get(rid)
        if not res:
            print(f"  {dim('? skip')}  {dim(rid.split('.')[-1]):28s}  {dim('not in current graph')}")
            continue
        if not res.check or res.check.strip().lower() == "false":
            untestable += 1
            print(f"  {dim('— n/a ')}  {bold(f'{res.short_name:28s}')}  {dim('no check command')}")
            continue

        node = graph.nodes.get(res.node_name)
        if not node: continue

        rc, _, stderr = _run_cmd(res.check, node, res, timeout=min(res.timeout, 15))
        if rc == 0:
            valid += 1
            print(f"  {green('✓ valid')}  {bold(f'{res.short_name:28s}')}  {dim('check still passes')}")
        else:
            drifted += 1
            print(f"  {red('✗ drift')}  {bold(f'{res.short_name:28s}')}  {red('check FAILED — marking for re-run')}")
            if stderr.strip():
                for line in stderr.strip().splitlines()[:3]:
                    print(f"              {red('│')} {_redact(line, _s)}")
            state.set_manual(rid, "failed")

    print(bold("─" * 64))
    parts = [green(f"Valid: {valid}")]
    if drifted: parts.append(red(f"Drifted: {drifted}"))
    if untestable: parts.append(dim(f"Untestable: {untestable}"))
    print(f"  {' | '.join(parts)}")
    if drifted:
        print(f"  {yellow(f'{drifted} resource(s) auto-marked for re-run.')}")
    print()


def cmd_state_set(graph_file: str, step_name: str, action: str, graph: Graph|None = None):
    """Manually set a resource's state."""
    sp = _state_path(graph_file)
    state = StateFile(sp)

    # Resolve step name to resource ID
    slug = re.sub(r'[^a-zA-Z0-9]+', '_', step_name.strip()).strip('_').lower()
    rid = None
    if graph:
        for r in graph.all_resources:
            if r.endswith(f".{slug}") or graph.all_resources[r].short_name == slug:
                rid = r; break
    if not rid:
        # Try to find in state entries
        for r in state.all_entries():
            if r.endswith(f".{slug}") or r.split(".")[-1] == slug:
                rid = r; break
    if not rid:
        # Last resort: construct it
        print(yellow(f"  warning: Could not resolve '{step_name}' to a known resource."))
        print(f"  Known resources: {[r.split('.')[-1] for r in (graph.all_resources if graph else state.all_entries())]}")
        return

    if action == "done":
        state.set_manual(rid, "success")
        print(green(f"  ✓ Marked [{step_name}] as done."))
    elif action == "redo":
        state.set_manual(rid, "failed")
        print(yellow(f"  ↻ Marked [{step_name}] for re-run."))
    else:
        print(red(f"  Unknown action '{action}'. Use 'done' or 'redo'."))


def cmd_state_drop(graph_file: str, step_name: str, graph: Graph|None = None):
    """Remove a resource from state entirely."""
    sp = _state_path(graph_file)
    state = StateFile(sp)

    slug = re.sub(r'[^a-zA-Z0-9]+', '_', step_name.strip()).strip('_').lower()
    rid = None
    for r in state.all_entries():
        if r.endswith(f".{slug}") or r.split(".")[-1] == slug:
            rid = r; break
    if not rid:
        print(yellow(f"  '{step_name}' not found in state.")); return

    state.drop(rid)
    print(dim(f"  Dropped [{step_name}] from state."))


def cmd_state_reset(graph_file: str):
    """Delete the state file."""
    sp = _state_path(graph_file)
    state = StateFile(sp)
    state.reset()
    print(green(f"  ✓ State file deleted: {sp}"))


def cmd_state_diff(file_a: str, file_b: str):
    """Compare two state files and show differences."""
    sa = StateFile(file_a)
    sb = StateFile(file_b)
    ea = sa.all_entries()
    eb = sb.all_entries()
    all_ids = sorted(set(list(ea.keys()) + list(eb.keys())))

    if not all_ids:
        print(dim("  Both state files are empty.")); return

    label_a = Path(file_a).name
    label_b = Path(file_b).name

    print(bold(f"\nState diff: {label_a} ↔ {label_b}"))
    print(bold("─" * 64))

    added = []; removed = []; changed = []; same = 0
    for rid in all_ids:
        a = ea.get(rid)
        b = eb.get(rid)
        short = rid.split(".")[-1]
        if a and not b:
            removed.append((short, a))
        elif b and not a:
            added.append((short, b))
        elif a and b and a.status != b.status:
            changed.append((short, a, b))
        else:
            same += 1

    if added:
        print(f"\n  {green(f'+ Added in {label_b}')} ({len(added)})")
        for short, e in added:
            print(f"    {green('+')} {bold(short):30s} {e.status:12s} {e.ms}ms")
    if removed:
        print(f"\n  {red(f'- Only in {label_a}')} ({len(removed)})")
        for short, e in removed:
            print(f"    {red('-')} {bold(short):30s} {e.status:12s} {e.ms}ms")
    if changed:
        print(f"\n  {yellow('~ Changed')} ({len(changed)})")
        for short, a, b in changed:
            print(f"    {yellow('~')} {bold(short):30s} {a.status} → {b.status}  ({a.ms}ms → {b.ms}ms)")
    if not added and not removed and not changed:
        print(f"\n  {dim('No differences.')}")

    print(f"\n  {dim(f'Summary: {len(added)} added, {len(removed)} removed, {len(changed)} changed, {same} unchanged')}")
    print()

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
