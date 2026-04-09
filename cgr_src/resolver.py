"""AST expansion, dependency resolution, graph construction, and wave planning."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.ast_nodes import *
from cgr_src.repo import *

import platform as _platform
import multiprocessing as _mp

def _gather_facts() -> dict[str, str]:
    """Gather local system facts into a dictionary of variables.
    These are available as ${fact_name} in graph files with 'gather facts'."""
    facts: dict[str, str] = {}
    # OS identification
    uname = os.uname()
    facts["os_name"] = uname.sysname.lower()       # linux, darwin, freebsd
    facts["os_release"] = uname.release              # 5.15.0-91-generic
    facts["os_machine"] = uname.machine              # x86_64, aarch64, arm64
    facts["hostname"] = uname.nodename
    facts["arch"] = _platform.machine()              # x86_64, aarch64

    # OS family detection
    os_family = "unknown"
    if os.path.exists("/etc/os-release"):
        try:
            osr = Path("/etc/os-release").read_text()
            for line in osr.splitlines():
                if line.startswith("ID_LIKE="):
                    val = line.split("=", 1)[1].strip().strip('"').lower()
                    if "debian" in val or "ubuntu" in val:
                        os_family = "debian"
                    elif "rhel" in val or "fedora" in val or "centos" in val:
                        os_family = "redhat"
                    elif "suse" in val:
                        os_family = "suse"
                    elif "arch" in val:
                        os_family = "arch"
                    else:
                        os_family = val.split()[0]
                    break
                elif line.startswith("ID="):
                    val = line.split("=", 1)[1].strip().strip('"').lower()
                    if val in ("debian", "ubuntu", "raspbian", "linuxmint", "pop"):
                        os_family = "debian"
                    elif val in ("rhel", "centos", "fedora", "rocky", "alma", "amzn"):
                        os_family = "redhat"
                    elif val in ("opensuse", "sles"):
                        os_family = "suse"
                    elif val == "arch":
                        os_family = "arch"
                    elif val == "alpine":
                        os_family = "alpine"
                    else:
                        os_family = val
        except OSError:
            pass
    elif uname.sysname == "Darwin":
        os_family = "darwin"
    facts["os_family"] = os_family

    # Distro name & version
    if os.path.exists("/etc/os-release"):
        try:
            osr = Path("/etc/os-release").read_text()
            for line in osr.splitlines():
                if line.startswith("PRETTY_NAME="):
                    facts["os_pretty"] = line.split("=", 1)[1].strip().strip('"')
                elif line.startswith("VERSION_ID="):
                    facts["os_version"] = line.split("=", 1)[1].strip().strip('"')
                elif line.startswith("ID="):
                    facts["os_id"] = line.split("=", 1)[1].strip().strip('"')
        except OSError:
            pass
    if "os_version" not in facts:
        facts["os_version"] = _platform.version()
    if "os_id" not in facts:
        facts["os_id"] = uname.sysname.lower()

    # CPU
    try:
        facts["cpu_count"] = str(_mp.cpu_count() or 1)
    except Exception:
        facts["cpu_count"] = "1"

    # Memory (Linux only — /proc/meminfo)
    if os.path.exists("/proc/meminfo"):
        try:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    facts["memory_mb"] = str(kb // 1024)
                    break
        except (OSError, ValueError):
            pass
    if "memory_mb" not in facts:
        facts["memory_mb"] = "0"

    # Python version (useful for tooling steps)
    facts["python_version"] = _platform.python_version()

    # Current user
    facts["current_user"] = os.environ.get("USER", os.environ.get("LOGNAME", "unknown"))

    return facts


def _resolve_deferred_secret_var(var: ASTVar) -> str:
    if var.secret_cmd:
        try:
            result = subprocess.run(var.secret_cmd, shell=True, capture_output=True, text=True, timeout=30)
            if result.returncode != 0:
                raise ResolveError(
                    f"Secret cmd '{var.secret_cmd}' exited {result.returncode} (stderr suppressed)\n"
                    f"  Referenced at line {var.line}"
                )
            return result.stdout.strip()
        except subprocess.TimeoutExpired as exc:
            raise ResolveError(
                f"Secret cmd '{var.secret_cmd}' timed out after 30s\n"
                f"  Referenced at line {var.line}"
            ) from exc

    if var.secret_vault:
        vault_addr = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
        vault_token = os.environ.get("VAULT_TOKEN", "")
        if not vault_token:
            raise ResolveError(
                f"VAULT_TOKEN environment variable is not set\n"
                f"  Referenced at line {var.line}"
            )
        vault_path = var.secret_vault
        vault_field = "value"
        if "#" in vault_path:
            vault_path, vault_field = vault_path.rsplit("#", 1)
        url = f"{vault_addr.rstrip('/')}/v1/{vault_path}"
        try:
            import urllib.request as _ureq
            req = _ureq.Request(url, headers={"X-Vault-Token": vault_token})
            with _ureq.urlopen(req, timeout=10) as resp:
                import json as _json
                data = _json.loads(resp.read())
            secret_data = data.get("data", {})
            if "data" in secret_data:
                secret_data = secret_data["data"]
            if vault_field not in secret_data:
                available = ", ".join(secret_data.keys())
                raise ResolveError(
                    f"Vault secret '{vault_path}' has no field '{vault_field}'. Available: {available}\n"
                    f"  Referenced at line {var.line}"
                )
            return str(secret_data[vault_field])
        except ResolveError:
            raise
        except Exception as exc:
            raise ResolveError(
                f"Vault read failed for '{vault_path}': {exc}\n"
                f"  Referenced at line {var.line}"
            ) from exc

    return var.value or ""


def _materialize_ast_variables(variables: list[ASTVar], *, resolve_deferred_secrets: bool = False,
                               skip_names: set[str] | None = None) -> tuple[dict[str, str], set[str]]:
    values: dict[str, str] = {}
    sensitive_values: set[str] = set()
    skip_names = skip_names or set()
    for var in variables:
        if var.name in skip_names:
            continue
        val = var.value or ""
        if var.secret_cmd or var.secret_vault:
            if resolve_deferred_secrets:
                val = _resolve_deferred_secret_var(var)
            else:
                val = _DEFERRED_SECRET_CMD if var.secret_cmd else _DEFERRED_SECRET_VAULT
        values[var.name] = val
        if var.is_secret and val and not val.startswith("<deferred:"):
            sensitive_values.add(val)
    return values, sensitive_values


class ResolveError(Exception): pass

class OnFail(Enum): STOP="stop"; WARN="warn"; IGNORE="ignore"
class Status(Enum):
    PENDING="pending"; SKIP_CHECK="skip_check"; SKIP_WHEN="skip_when"
    SUCCESS="success"; FAILED="failed"; WARNED="warned"; CANCELLED="cancelled"; TIMEOUT="timeout"

@dataclass
class Resource:
    id: str; short_name: str; node_name: str; description: str
    needs: list[str]; check: str|None; run: str; run_as: str|None
    timeout: int; retries: int; retry_delay: int; retry_backoff: bool; on_fail: OnFail
    when: str|None; env: dict[str,str]; is_verify: bool; line: int
    timeout_reset_on_output: bool = False
    script_path: str|None = None
    raw_check: str|None = None
    raw_run: str|None = None
    raw_script_path: str|None = None
    raw_http_url: str|None = None
    raw_http_headers: list[tuple[str, str]] = field(default_factory=list)
    raw_http_body: str|None = None
    raw_http_auth: tuple[str, str|None]|None = None
    raw_env: dict[str, str] = field(default_factory=dict)
    source_line: int = 0  # line in the user's .cgr/.cg file (not template-internal)
    provenance: Provenance|None = None
    identity_hash: str = ""
    # Parallel execution metadata (set by resolver during expansion)
    parallel_group: str|None = None       # group ID — resources in same group run together
    concurrency_limit: int|None = None    # max concurrent within this group (None = unlimited)
    is_race: bool = False                 # if True, first-to-succeed cancels rest
    race_into: str|None = None            # final destination path — engine manages temp files
    race_branch_index: int|None = None    # branch ordinal within the race (0, 1, 2...)
    is_barrier: bool = False              # synthetic join node (no run command, just a dep target)
    group_fail_policy: str = "wait"       # wait|stop|ignore — how to handle failures in group
    collect_key: str|None = None          # if set, stdout is collected under this key
    collect_format: str|None = None       # "json" for pretty-printed JSON output
    tags: list[str] = field(default_factory=list)  # step tags for --tags/--skip-tags filtering
    # HTTP request fields (resolved from ASTResource)
    http_method: str|None = None
    http_url: str|None = None
    http_headers: list[tuple[str,str]] = field(default_factory=list)
    http_body: str|None = None
    http_body_type: str|None = None
    http_auth: tuple[str,str|None]|None = None
    http_expect: str|None = None
    wait_kind: str|None = None
    wait_target: str|None = None
    raw_wait_target: str|None = None
    subgraph_path: str|None = None
    raw_subgraph_path: str|None = None
    subgraph_vars: dict[str, str] = field(default_factory=dict)
    raw_subgraph_vars: dict[str, str] = field(default_factory=dict)
    # Provisioning operation fields (Phase 1)
    prov_content_inline: str|None = None
    prov_content_from: str|None = None
    prov_dest: str|None = None
    prov_lines: list[tuple] = field(default_factory=list)
    prov_put_src: str|None = None
    prov_put_mode: str|None = None
    prov_put_transfer: str|None = None
    prov_put_transfer_opts: str|None = None
    prov_validate: str|None = None
    # Provisioning operation fields (Phase 2)
    prov_block_dest: str|None = None
    prov_block_marker: str|None = None
    prov_block_inline: str|None = None
    prov_block_from: str|None = None
    prov_block_absent: bool = False
    prov_ini_dest: str|None = None
    prov_ini_ops: list[tuple] = field(default_factory=list)
    prov_json_dest: str|None = None
    prov_json_ops: list[tuple] = field(default_factory=list)
    prov_asserts: list[tuple] = field(default_factory=list)
    # Backoff/jitter fields
    retry_backoff_max: int = 0
    retry_jitter_pct: int = 0
    # On-success/failure variable bindings
    on_success_set: list[tuple] = field(default_factory=list)
    on_failure_set: list[tuple] = field(default_factory=list)
    # Collect-to-variable and reduce
    collect_var: str|None = None
    reduce_key: str|None = None
    reduce_var: str|None = None
    # Conditional command fragments / polling
    flags: list[str] = field(default_factory=list)
    env_when: dict[str, str] = field(default_factory=dict)
    until: str|None = None

@dataclass
class HostNode:
    name: str; via_method: str; via_props: dict[str,str]
    resources: dict[str, Resource]

@dataclass
class Graph:
    nodes: dict[str, HostNode]; all_resources: dict[str, Resource]
    waves: list[list[str]]; variables: dict[str,str]
    dedup_map: dict[str, list[str]]  # identity_hash → [resource_ids]
    provenance_log: list[Provenance]
    node_ordering: dict[str, list[str]] = field(default_factory=dict)  # node → after_nodes
    sensitive_values: set[str] = field(default_factory=set)  # secret values to redact from output
    on_complete: Resource|None = None   # hook: runs after successful apply
    on_failure: Resource|None = None    # hook: runs after failed apply
    graph_file: str|None = None         # path to the source .cgr/.cg file (for relative put/content from)

@dataclass
class ExecResult:
    resource_id: str; status: Status; check_rc: int|None=None
    run_rc: int|None=None; stdout: str=""; stderr: str=""
    duration_ms: int=0; attempts: int=0; reason: str=""
    var_bindings: dict = field(default_factory=dict)  # variables to bind in caller after exec
    observed_value: str|None = None

VAR_RE = re.compile(r"\$\{(\w+)\}")
ENV_RE = re.compile(r"\$\{ENV:([^}]+)\}")


def _parse_cgr_dep_ref(ref: str) -> str:
    """Parse a bracket dependency reference into a dotted resource id.

    In .cgr, `/` separates path segments inside a dependency reference. That
    lets the syntax represent both flat cross-node refs like `[db/setup schema]`
    and nested refs like `[db/parent/child]` or `[parent/child]`.
    """
    if "/" in ref:
        return ".".join(_slug(part) for part in ref.split("/") if part.strip())
    return _slug(ref)

def _edit_distance(a: str, b: str) -> int:
    """Levenshtein edit distance between two strings."""
    if len(a) < len(b): return _edit_distance(b, a)
    if not b: return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (0 if ca == cb else 1)))
        prev = curr
    return prev[-1]

def _expand(text: str|None, vs: dict[str,str], *, lenient: bool = False) -> str|None:
    """Expand ${var} and ${ENV:VAR} references.
    lenient=True: leave undefined ${var} as-is (for when expressions referencing runtime vars)."""
    if text is None: return None
    # First pass: expand ${ENV:NAME} from os.environ (error if unset)
    def _env_sub(m):
        name = m.group(1)
        val = os.environ.get(name)
        if val is None:
            raise ResolveError(f"Environment variable '{name}' is not set (referenced as ${{ENV:{name}}}). "
                               f"Set it or use 'set {name.lower()} = env(\"{name}\")' for optional defaults.")
        return val
    text = ENV_RE.sub(_env_sub, text)
    # Second pass: expand ${name} from the variable dict
    def r(m):
        k=m.group(1)
        if k not in vs:
            if lenient: return m.group(0)  # preserve ${varname} for runtime evaluation
            raise ResolveError(f"Undefined variable '${{{k}}}'. Known: {sorted(vs)}")
        return vs[k]
    return VAR_RE.sub(r, text)

def _expand_shell_fragment(text: str|None, vs: dict[str, str]) -> str|None:
    """Expand variables for a shell argument fragment, quoting values safely."""
    if text is None:
        return None
    def _env_sub(m):
        name = m.group(1)
        val = os.environ.get(name)
        if val is None:
            raise ResolveError(f"Environment variable '{name}' is not set (referenced as ${{ENV:{name}}})")
        return shlex.quote(val)
    text = ENV_RE.sub(_env_sub, text)
    def _var_sub(m):
        key = m.group(1)
        if key not in vs:
            raise ResolveError(f"Undefined variable '${{{key}}}'. Known: {sorted(vs)}")
        return shlex.quote(vs[key])
    return VAR_RE.sub(_var_sub, text)

def _redact(text: str|None, sensitive: set[str]) -> str|None:
    """Replace any occurrence of sensitive values with ***REDACTED***."""
    if text is None or not sensitive:
        return text
    result = text
    for sv in sorted(sensitive, key=len, reverse=True):
        if sv and sv in result:
            result = result.replace(sv, "***REDACTED***")
    return result


def _redact_variable_map(variables: dict[str, str], sensitive: set[str]) -> dict[str, str]:
    """Return a copy of variables with secret values redacted for display."""
    return {k: (_redact(v, sensitive) if isinstance(v, str) else v) for k, v in variables.items()}


def _redact_obj(obj, sensitive: set[str]):
    """Recursively redact sensitive substrings from strings inside nested JSON-like data."""
    if not sensitive:
        return obj
    if isinstance(obj, str):
        return _redact(obj, sensitive)
    if isinstance(obj, list):
        return [_redact_obj(v, sensitive) for v in obj]
    if isinstance(obj, tuple):
        return tuple(_redact_obj(v, sensitive) for v in obj)
    if isinstance(obj, dict):
        return {k: _redact_obj(v, sensitive) for k, v in obj.items()}
    return obj

def _should_skip_tags(res: Resource, include_tags: set[str]|None, exclude_tags: set[str]|None) -> bool:
    """Return True if this resource should be skipped based on tag filters.
    Barrier and verify steps are never tag-filtered (they're structural)."""
    if res.is_barrier or res.is_verify:
        return False
    if not include_tags and not exclude_tags:
        return False
    res_tags = set(res.tags)
    if include_tags:
        # Must have at least one matching tag (untagged steps are skipped)
        if not res_tags & include_tags:
            return True
    if exclude_tags:
        # Skip if any excluded tag matches
        if res_tags & exclude_tags:
            return True
    return False

def _identity(check: str|None, run: str, run_as: str|None, node_name: str = "", prov_dest: str|None = None) -> str:
    """Compute a content-based identity hash for deduplication.
    Includes node_name so identical commands on different hosts are never deduped."""
    blob = f"{node_name}|{check or ''}|{run}|{run_as or ''}|{prov_dest or ''}"
    return hashlib.sha256(blob.encode()).hexdigest()[:16]

def _flatten_ast_resource(
    ast_res: ASTResource,
    node_name: str,
    prefix: str,
    vs: dict[str,str],
    provenance: Provenance|None,
    collector: dict[str, Resource],
    dedup_hashes: dict[str, str],
    dedup_map: dict[str, list[str]],
    source_line: int = 0,
    known_nodes: set[str]|None = None,
    templates: dict[str, ASTTemplate]|None = None,
    provenance_log: list[Provenance]|None = None,
) -> str:
    """
    Recursively flatten a (possibly nested) ASTResource into the collector.
    Expands parallel/race/each/stage into gate → fork → join patterns.
    Returns the resource ID of the root (which is the join/barrier).
    """
    rid = f"{node_name}.{prefix}"
    has_construct = bool(ast_res.parallel_block or ast_res.race_block or
                         ast_res.each_body or ast_res.stage_phases)

    # ── Resolve parent's explicit dependencies first ────────────────
    parent_needs_raw = []
    for dep_name in ast_res.needs:
        # Cross-node reference: if dep starts with a known node name that isn't ours,
        # it's already fully qualified — pass through unchanged
        if known_nodes and '.' in dep_name:
            cross_node = dep_name.split('.')[0]
            if cross_node in known_nodes and cross_node != node_name:
                parent_needs_raw.append(dep_name)
                continue
        scoped = f"{node_name}.{prefix}.{dep_name}"
        if scoped in collector:
            parent_needs_raw.append(scoped)
        else:
            parent_prefix = ".".join(prefix.split(".")[:-1]) if "." in prefix else ""
            sibling = f"{node_name}.{parent_prefix}.{dep_name}" if parent_prefix else f"{node_name}.{dep_name}"
            parent_needs_raw.append(sibling)

    # ── If we have constructs, create a gate node for parent deps ───
    gate_id = None
    if has_construct and parent_needs_raw:
        gate_id = f"{node_name}.{prefix}.__gate"
        gate = Resource(
            id=gate_id, short_name=f"{ast_res.name}__gate",
            node_name=node_name, description=f"gate for {ast_res.description or ast_res.name}",
            needs=list(parent_needs_raw), check=None, run="true", run_as=None,
            timeout=5, retries=0, retry_delay=0, retry_backoff=False, on_fail=OnFail.STOP,
            when=None, env={}, is_verify=False, line=ast_res.line,
            is_barrier=True, identity_hash=_identity(None, f"gate_{rid}", None))
        collector[gate_id] = gate
        dedup_map[gate.identity_hash] = [gate_id]

    # ── Flatten regular children ────────────────────────────────────
    child_ids = []
    for child in ast_res.children:
        if child.inner_template_name:
            if not templates or child.inner_template_name not in templates:
                raise ResolveError(
                    f"Unknown template '{child.inner_template_name}' at line {child.line}.\n"
                    f"  Available: {sorted(templates.keys()) if templates else []}\n"
                    f"  Did you forget a 'use' statement?"
                )
            child_id = _expand_template(
                templates[child.inner_template_name],
                {k: _expand(v, vs) or v for k, v in child.inner_template_args.items()},
                node_name, vs, collector, dedup_hashes, dedup_map,
                provenance_log if provenance_log is not None else [],
                human_name=child.description, source_line=source_line or child.line,
                known_nodes=known_nodes, templates=templates,
            )
            if provenance and child_id in collector and collector[child_id].provenance:
                collector[child_id].provenance.used_by.append(prefix)
            if child_id in collector:
                for dep in child.needs:
                    resolved_dep = dep
                    if known_nodes and '.' in dep:
                        cross_node = dep.split('.')[0]
                        if cross_node in known_nodes and cross_node != node_name:
                            resolved_dep = dep
                        else:
                            scoped = f"{node_name}.{prefix}.{dep}"
                            if scoped in collector:
                                resolved_dep = scoped
                            else:
                                parent_prefix = ".".join(prefix.split(".")[:-1]) if "." in prefix else ""
                                resolved_dep = f"{node_name}.{parent_prefix}.{dep}" if parent_prefix else f"{node_name}.{dep}"
                    else:
                        scoped = f"{node_name}.{prefix}.{dep}"
                        if scoped in collector:
                            resolved_dep = scoped
                        else:
                            parent_prefix = ".".join(prefix.split(".")[:-1]) if "." in prefix else ""
                            resolved_dep = f"{node_name}.{parent_prefix}.{dep}" if parent_prefix else f"{node_name}.{dep}"
                    if resolved_dep not in collector[child_id].needs:
                        collector[child_id].needs.append(resolved_dep)
        else:
            child_prefix = f"{prefix}.{child.name}"
            child_id = _flatten_ast_resource(
                child, node_name, child_prefix, vs, provenance,
                collector, dedup_hashes, dedup_map, source_line=source_line,
                known_nodes=known_nodes, templates=templates,
                provenance_log=provenance_log)
        child_ids.append(child_id)
        # Wire child to gate if present
        if gate_id and child_id in collector:
            if gate_id not in collector[child_id].needs:
                collector[child_id].needs.append(gate_id)

    def _wire_gate(res_id):
        """Ensure a resource depends on the gate node."""
        if gate_id and res_id in collector:
            if gate_id not in collector[res_id].needs:
                collector[res_id].needs.append(gate_id)

    # ── Expand parallel block ───────────────────────────────────────
    if ast_res.parallel_block:
        group_id = f"{rid}.__parallel"
        for pchild in ast_res.parallel_block:
            pc_prefix = f"{prefix}.{pchild.name}"
            pc_id = _flatten_ast_resource(pchild, node_name, pc_prefix, vs, provenance,
                                          collector, dedup_hashes, dedup_map, source_line=source_line,
                                          known_nodes=known_nodes, templates=templates,
                                          provenance_log=provenance_log)
            child_ids.append(pc_id)
            _wire_gate(pc_id)
            if pc_id in collector:
                collector[pc_id].parallel_group = group_id
                collector[pc_id].concurrency_limit = ast_res.parallel_limit
                collector[pc_id].group_fail_policy = ast_res.parallel_fail_policy

    # ── Expand race block ───────────────────────────────────────────
    if ast_res.race_block:
        group_id = f"{rid}.__race"
        race_dest = _expand(ast_res.race_into, vs) if ast_res.race_into else None
        for bi, rchild in enumerate(ast_res.race_block):
            # If race_into is set, inject _race_out variable for this branch
            branch_vs = vs
            if race_dest:
                branch_vs = {**vs, "_race_out": f"{race_dest}.race.{bi}"}
            rc_prefix = f"{prefix}.{rchild.name}"
            rc_id = _flatten_ast_resource(rchild, node_name, rc_prefix, branch_vs, provenance,
                                          collector, dedup_hashes, dedup_map, source_line=source_line,
                                          known_nodes=known_nodes, templates=templates,
                                          provenance_log=provenance_log)
            child_ids.append(rc_id)
            _wire_gate(rc_id)
            if rc_id in collector:
                collector[rc_id].parallel_group = group_id
                collector[rc_id].is_race = True
                if race_dest:
                    collector[rc_id].race_into = race_dest
                    collector[rc_id].race_branch_index = bi

    # ── Expand each loop ────────────────────────────────────────────
    if ast_res.each_var and ast_res.each_list_expr and ast_res.each_body:
        list_val = _expand(ast_res.each_list_expr, vs) or ""
        items = [x.strip() for x in list_val.split(",") if x.strip()]
        group_id = f"{rid}.__each"
        body = ast_res.each_body
        for item in items:
            each_vs = {**vs, ast_res.each_var: item}
            safe_item = re.sub(r'[^a-zA-Z0-9_]', '_', item).strip('_').lower()
            # Always append item suffix to guarantee unique names
            base_name = re.sub(r'[^a-zA-Z0-9_]', '_', body.name).strip('_').lower()
            item_name = f"{base_name}_{safe_item}"
            expanded = ASTResource(
                name=item_name, description=_expand(body.description, each_vs) or body.description,
                needs=body.needs, check=body.check, run=body.run,
                run_as=body.run_as, timeout=body.timeout, retries=body.retries,
                retry_delay=body.retry_delay, retry_backoff=body.retry_backoff, on_fail=body.on_fail,
                when=body.when, env=body.env, is_verify=body.is_verify,
                line=body.line, timeout_reset_on_output=body.timeout_reset_on_output,
                script_path=body.script_path,
                collect_key=body.collect_key, tags=list(body.tags), children=body.children,
                collect_format=body.collect_format, retry_backoff_max=body.retry_backoff_max,
                retry_jitter_pct=body.retry_jitter_pct, on_success_set=list(body.on_success_set),
                on_failure_set=list(body.on_failure_set), collect_var=body.collect_var,
                reduce_key=body.reduce_key, reduce_var=body.reduce_var,
                inner_template_name=body.inner_template_name,
                inner_template_args=dict(body.inner_template_args), flags=list(body.flags),
                env_when=dict(body.env_when), until=body.until,
                wait_kind=body.wait_kind, wait_target=body.wait_target,
                subgraph_path=body.subgraph_path, subgraph_vars=dict(body.subgraph_vars))
            ec_prefix = f"{prefix}.{item_name}"
            ec_id = _flatten_ast_resource(expanded, node_name, ec_prefix, each_vs, provenance,
                                          collector, dedup_hashes, dedup_map, source_line=source_line,
                                          known_nodes=known_nodes, templates=templates,
                                          provenance_log=provenance_log)
            child_ids.append(ec_id)
            _wire_gate(ec_id)
            if ec_id in collector:
                collector[ec_id].parallel_group = group_id
                raw_lim = ast_res.each_limit
                if isinstance(raw_lim, str):
                    expanded_lim = _expand(raw_lim, vs)
                    try:
                        raw_lim = int(expanded_lim)
                    except ValueError:
                        raise ResolveError(
                            f"each concurrency limit: '{raw_lim}' expanded to "
                            f"'{expanded_lim}', which is not a valid integer.")
                collector[ec_id].concurrency_limit = raw_lim

    # ── Expand stage/phase ──────────────────────────────────────────
    if ast_res.stage_phases:
        all_items: list[str] = []
        used_items: list[str] = []
        prev_barrier_id: str|None = gate_id  # first phase gates on parent deps
        if ast_res.stage_phases:
            list_val = _expand(ast_res.stage_phases[0].from_list, vs) or ""
            all_items = [x.strip() for x in list_val.split(",") if x.strip()]
        for pi, phase in enumerate(ast_res.stage_phases):
            count = phase.count_expr
            if count in ("rest", "remaining"):
                phase_items = [x for x in all_items if x not in used_items]
            elif count.endswith("%"):
                n = max(1, len(all_items) * int(count.rstrip("%")) // 100)
                phase_items = [x for x in all_items if x not in used_items][:n]
            else:
                try: n = int(count)
                except ValueError: n = 1
                phase_items = [x for x in all_items if x not in used_items][:n]
            used_items.extend(phase_items)
            phase_group = f"{rid}.__phase_{pi}"
            phase_child_ids = []
            for item in phase_items:
                each_vs = {**vs, "server": item}
                for pbody in phase.body:
                    safe_item = re.sub(r'[^a-zA-Z0-9_]', '_', item).strip('_').lower()
                    if pbody.is_verify:
                        v_needs = [c.split(".")[-1] for c in phase_child_ids]
                        v_res = ASTResource(name=pbody.name,
                            description=_expand(pbody.description, each_vs) or pbody.description,
                            needs=v_needs, check=pbody.check, run=pbody.run,
                            run_as=pbody.run_as, timeout=pbody.timeout, retries=pbody.retries,
                            retry_delay=pbody.retry_delay, retry_backoff=pbody.retry_backoff, on_fail=pbody.on_fail,
                            when=pbody.when, env=pbody.env, is_verify=True,
                            line=pbody.line, timeout_reset_on_output=pbody.timeout_reset_on_output,
                            script_path=pbody.script_path,
                            collect_key=pbody.collect_key, tags=list(pbody.tags), children=[],
                            collect_format=pbody.collect_format, retry_backoff_max=pbody.retry_backoff_max,
                            retry_jitter_pct=pbody.retry_jitter_pct, on_success_set=list(pbody.on_success_set),
                            on_failure_set=list(pbody.on_failure_set), collect_var=pbody.collect_var,
                            reduce_key=pbody.reduce_key, reduce_var=pbody.reduce_var,
                            inner_template_name=pbody.inner_template_name,
                            inner_template_args=dict(pbody.inner_template_args), flags=list(pbody.flags),
                            env_when=dict(pbody.env_when), until=pbody.until,
                            wait_kind=pbody.wait_kind, wait_target=pbody.wait_target,
                            subgraph_path=pbody.subgraph_path, subgraph_vars=dict(pbody.subgraph_vars))
                        v_prefix = f"{prefix}.__phase_{pi}.{pbody.name}"
                        v_id = _flatten_ast_resource(v_res, node_name, v_prefix,
                            each_vs, provenance, collector, dedup_hashes, dedup_map, source_line=source_line,
                            known_nodes=known_nodes, templates=templates,
                            provenance_log=provenance_log)
                        phase_child_ids.append(v_id)
                    else:
                        base_name = re.sub(r'[^a-zA-Z0-9_]', '_', pbody.name).strip('_').lower()
                        item_name = f"{base_name}_{safe_item}"
                        expanded = ASTResource(name=item_name,
                            description=_expand(pbody.description, each_vs) or pbody.description,
                            needs=pbody.needs, check=pbody.check, run=pbody.run,
                            run_as=pbody.run_as, timeout=pbody.timeout, retries=pbody.retries,
                            retry_delay=pbody.retry_delay, retry_backoff=pbody.retry_backoff, on_fail=pbody.on_fail,
                            when=pbody.when, env=pbody.env, is_verify=False,
                            line=pbody.line, timeout_reset_on_output=pbody.timeout_reset_on_output,
                            script_path=pbody.script_path, collect_key=pbody.collect_key,
                            tags=list(pbody.tags), children=pbody.children,
                            collect_format=pbody.collect_format, retry_backoff_max=pbody.retry_backoff_max,
                            retry_jitter_pct=pbody.retry_jitter_pct, on_success_set=list(pbody.on_success_set),
                            on_failure_set=list(pbody.on_failure_set), collect_var=pbody.collect_var,
                            reduce_key=pbody.reduce_key, reduce_var=pbody.reduce_var,
                            inner_template_name=pbody.inner_template_name,
                            inner_template_args=dict(pbody.inner_template_args), flags=list(pbody.flags),
                            env_when=dict(pbody.env_when), until=pbody.until,
                            wait_kind=pbody.wait_kind, wait_target=pbody.wait_target,
                            subgraph_path=pbody.subgraph_path, subgraph_vars=dict(pbody.subgraph_vars))
                        ec_prefix = f"{prefix}.__phase_{pi}.{item_name}"
                        ec_id = _flatten_ast_resource(expanded, node_name, ec_prefix,
                            each_vs, provenance, collector, dedup_hashes, dedup_map, source_line=source_line,
                            known_nodes=known_nodes, templates=templates,
                            provenance_log=provenance_log)
                        phase_child_ids.append(ec_id)
                        if ec_id in collector:
                            collector[ec_id].parallel_group = phase_group
                            raw_phase_lim = phase.each_limit
                            if isinstance(raw_phase_lim, str):
                                expanded_phase_lim = _expand(raw_phase_lim, vs)
                                try:
                                    raw_phase_lim = int(expanded_phase_lim)
                                except ValueError:
                                    raise ResolveError(
                                        f"each concurrency limit: '{raw_phase_lim}' expanded to "
                                        f"'{expanded_phase_lim}', which is not a valid integer.")
                            collector[ec_id].concurrency_limit = raw_phase_lim
                            # Wire to previous phase barrier
                            if prev_barrier_id and prev_barrier_id not in collector[ec_id].needs:
                                collector[ec_id].needs.append(prev_barrier_id)
            # Phase barrier
            barrier_id = f"{node_name}.{prefix}.__phase_{pi}_barrier"
            barrier_needs = list(phase_child_ids)
            if prev_barrier_id and prev_barrier_id not in barrier_needs:
                barrier_needs.append(prev_barrier_id)
            barrier = Resource(
                id=barrier_id, short_name=f"phase_{phase.name}",
                node_name=node_name, description=f"Phase: {phase.name} ({len(phase_items)} items)",
                needs=barrier_needs, check=None, run="true", run_as=None,
                timeout=10, retries=0, retry_delay=0, retry_backoff=False, on_fail=OnFail.STOP,
                when=None, env={}, is_verify=False, line=phase.line,
                is_barrier=True, identity_hash=_identity(None, f"barrier_{phase_group}_{pi}", None))
            collector[barrier_id] = barrier
            dedup_map[barrier.identity_hash] = [barrier_id]
            prev_barrier_id = barrier_id
            child_ids.append(barrier_id)

    # ── Create the root resource (barrier/join for constructs) ──────
    exp_check = _expand(ast_res.check, vs)
    exp_run   = _expand(ast_res.run, vs) or ""
    exp_script_path = _expand(ast_res.script_path, vs) if ast_res.script_path else None
    exp_desc  = _expand(ast_res.description, vs) or ""
    resolved_flags: list[str] = []
    for flag_text, flag_when in ast_res.flags:
        if flag_when:
            flag_expr = _expand(flag_when, vs, lenient=True)
            if not _eval_when(flag_expr, vs):
                continue
        resolved_flags.append(_expand_shell_fragment(flag_text, vs) or "")
    if resolved_flags:
        exp_run = (exp_run + " " + " ".join(f for f in resolved_flags if f)).strip()
    effective_run_key = exp_run or ""
    if exp_script_path and not effective_run_key:
        effective_run_key = f"__script__ {exp_script_path}"
    # For HTTP steps, synthesize run early so identity hash is unique per URL/method
    if ast_res.http_method and not effective_run_key:
        exp_http_url_early = _expand(ast_res.http_url, vs) if ast_res.http_url else ""
        exp_run = f"{ast_res.http_method} {exp_http_url_early}"
        effective_run_key = exp_run
    exp_wait_target = _expand(ast_res.wait_target, vs) if ast_res.wait_target else None
    if ast_res.wait_kind and not effective_run_key:
        effective_run_key = f"__wait__ {ast_res.wait_kind} {exp_wait_target or ''}".strip()
    exp_subgraph_path = _expand(ast_res.subgraph_path, vs) if ast_res.subgraph_path else None
    exp_subgraph_vars = {k: _expand(v, vs) or v for k, v in (ast_res.subgraph_vars or {}).items()}
    if exp_subgraph_path and not effective_run_key:
        subgraph_sig = ",".join(f"{k}={exp_subgraph_vars[k]}" for k in sorted(exp_subgraph_vars))
        effective_run_key = f"__subgraph__ {exp_subgraph_path} {subgraph_sig}".strip()
    # Expand provisioning fields
    exp_prov_content_inline = _expand(ast_res.prov_content_inline, vs) if ast_res.prov_content_inline is not None else ast_res.prov_content_inline
    exp_prov_content_from = _expand(ast_res.prov_content_from, vs) if ast_res.prov_content_from else None
    exp_prov_dest = _expand(ast_res.prov_dest, vs) if ast_res.prov_dest else None
    exp_prov_lines = [(_expand(action, vs), _expand(text, vs), regex) for action, text, regex in ast_res.prov_lines] if ast_res.prov_lines else []
    exp_prov_put_src = _expand(ast_res.prov_put_src, vs) if ast_res.prov_put_src else None
    exp_prov_put_transfer_opts = _expand(ast_res.prov_put_transfer_opts, vs) if ast_res.prov_put_transfer_opts else None
    exp_prov_validate = _expand(ast_res.prov_validate, vs) if ast_res.prov_validate else None
    # Phase 2 provisioning field expansion
    exp_prov_block_dest = _expand(ast_res.prov_block_dest, vs) if ast_res.prov_block_dest else None
    exp_prov_block_marker = _expand(ast_res.prov_block_marker, vs) if ast_res.prov_block_marker else None
    exp_prov_block_inline = _expand(ast_res.prov_block_inline, vs) if ast_res.prov_block_inline is not None else ast_res.prov_block_inline
    exp_prov_block_from = _expand(ast_res.prov_block_from, vs) if ast_res.prov_block_from else None
    exp_prov_ini_dest = _expand(ast_res.prov_ini_dest, vs) if ast_res.prov_ini_dest else None
    exp_prov_ini_ops = [(sec, _expand(key, vs), _expand(val, vs) if isinstance(val, str) else val)
                        for sec, key, val in ast_res.prov_ini_ops] if ast_res.prov_ini_ops else []
    exp_prov_json_dest = _expand(ast_res.prov_json_dest, vs) if ast_res.prov_json_dest else None
    exp_prov_json_ops = [(_expand(path, vs), _expand(val, vs) if isinstance(val, str) else val)
                         for path, val in ast_res.prov_json_ops] if ast_res.prov_json_ops else []
    exp_prov_asserts = [(_expand(cmd, vs), _expand(msg, vs) if msg else None)
                        for cmd, msg in ast_res.prov_asserts] if ast_res.prov_asserts else []
    # For provisioning steps without run, synthesize run from dest for unique identity hash
    prov_key = exp_prov_dest or exp_prov_block_dest or exp_prov_ini_dest or exp_prov_json_dest
    if prov_key and not effective_run_key:
        effective_run_key = f"__prov__ {prov_key}"
    # For reduce steps without run, synthesize a run key for identity hash
    if ast_res.reduce_key and not effective_run_key:
        effective_run_key = f"__reduce__ {ast_res.reduce_key}"
    ihash = _identity(exp_check, effective_run_key, ast_res.run_as, node_name, exp_prov_dest)

    # Dedup (skip for constructs)
    if ihash in dedup_hashes and not has_construct:
        existing_id = dedup_hashes[ihash]
        dedup_map[ihash].append(rid)
        if existing_id in collector and provenance:
            if collector[existing_id].provenance:
                collector[existing_id].provenance.used_by.append(prefix)
        return existing_id

    # Build final needs: explicit + gate + children
    resolved_needs = []
    if not has_construct:
        # Regular resource: keep explicit needs
        resolved_needs = list(parent_needs_raw)
    else:
        # Construct parent: depends on gate (if any) and all construct children
        if gate_id: resolved_needs.append(gate_id)

    for cid in child_ids:
        if cid not in resolved_needs:
            resolved_needs.append(cid)

    is_barrier_node = not effective_run_key and has_construct
    if is_barrier_node:
        effective_run_key = "true"

    exp_collect = _expand(ast_res.collect_key, vs) if ast_res.collect_key else None
    # Expand HTTP fields
    exp_http_url = _expand(ast_res.http_url, vs) if ast_res.http_url else None
    exp_http_headers = [(_expand(k, vs), _expand(v, vs)) for k, v in ast_res.http_headers] if ast_res.http_headers else []
    exp_http_body = _expand(ast_res.http_body, vs) if ast_res.http_body else None
    exp_http_auth = None
    if ast_res.http_auth:
        exp_http_auth = (ast_res.http_auth[0], _expand(ast_res.http_auth[1], vs) if ast_res.http_auth[1] else None)
    exp_http_expect = ast_res.http_expect  # no variable expansion needed for status codes
    resolved_env = {}
    for key, value in ast_res.env.items():
        env_expr = ast_res.env_when.get(key)
        if env_expr:
            exp_env_expr = _expand(env_expr, vs, lenient=True)
            if not _eval_when(exp_env_expr, vs):
                continue
        resolved_env[key] = _expand(value, vs) or value
    res = Resource(
        id=rid, short_name=ast_res.name, node_name=node_name,
        description=exp_desc, needs=resolved_needs,
        check=exp_check, run=exp_run, run_as=ast_res.run_as,
        timeout=ast_res.timeout, retries=ast_res.retries,
        retry_delay=ast_res.retry_delay, retry_backoff=ast_res.retry_backoff, on_fail=OnFail(ast_res.on_fail),
        when=_expand(ast_res.when, vs, lenient=True), env=resolved_env,
        is_verify=ast_res.is_verify, line=ast_res.line,
        timeout_reset_on_output=ast_res.timeout_reset_on_output,
        script_path=exp_script_path,
        raw_check=ast_res.check, raw_run=ast_res.run, raw_script_path=ast_res.script_path,
        raw_http_url=ast_res.http_url, raw_http_headers=list(ast_res.http_headers),
        raw_http_body=ast_res.http_body, raw_http_auth=ast_res.http_auth,
        raw_env=dict(ast_res.env),
        source_line=source_line or ast_res.line,
        provenance=provenance, identity_hash=ihash, is_barrier=is_barrier_node,
        collect_key=exp_collect, collect_format=ast_res.collect_format,
        tags=list(ast_res.tags),
        http_method=ast_res.http_method, http_url=exp_http_url,
        http_headers=exp_http_headers, http_body=exp_http_body,
        http_body_type=ast_res.http_body_type,
        http_auth=exp_http_auth, http_expect=exp_http_expect,
        wait_kind=ast_res.wait_kind, wait_target=exp_wait_target, raw_wait_target=ast_res.wait_target,
        subgraph_path=exp_subgraph_path, raw_subgraph_path=ast_res.subgraph_path,
        subgraph_vars=exp_subgraph_vars, raw_subgraph_vars=dict(ast_res.subgraph_vars),
        prov_content_inline=exp_prov_content_inline, prov_content_from=exp_prov_content_from,
        prov_dest=exp_prov_dest, prov_lines=exp_prov_lines,
        prov_put_src=exp_prov_put_src, prov_put_mode=ast_res.prov_put_mode,
        prov_put_transfer=ast_res.prov_put_transfer, prov_put_transfer_opts=exp_prov_put_transfer_opts,
        prov_validate=exp_prov_validate,
        prov_block_dest=exp_prov_block_dest, prov_block_marker=exp_prov_block_marker,
        prov_block_inline=exp_prov_block_inline, prov_block_from=exp_prov_block_from,
        prov_block_absent=ast_res.prov_block_absent,
        prov_ini_dest=exp_prov_ini_dest, prov_ini_ops=exp_prov_ini_ops,
        prov_json_dest=exp_prov_json_dest, prov_json_ops=exp_prov_json_ops,
        prov_asserts=exp_prov_asserts,
        retry_backoff_max=ast_res.retry_backoff_max, retry_jitter_pct=ast_res.retry_jitter_pct,
        on_success_set=list(ast_res.on_success_set), on_failure_set=list(ast_res.on_failure_set),
        collect_var=ast_res.collect_var,
        reduce_key=ast_res.reduce_key, reduce_var=ast_res.reduce_var,
        flags=resolved_flags, env_when=dict(ast_res.env_when), until=ast_res.until,
    )
    collector[rid] = res
    dedup_hashes[ihash] = rid
    dedup_map[ihash].append(rid)
    return rid

def _expand_template(
    template: ASTTemplate,
    args: dict[str,str],
    node_name: str,
    global_vars: dict[str,str],
    collector: dict[str, Resource],
    dedup_hashes: dict[str, str],
    dedup_map: dict[str, list[str]],
    provenance_log: list[Provenance],
    human_name: str | None = None,
    source_line: int = 0,
    known_nodes: set[str]|None = None,
    templates: dict[str, ASTTemplate]|None = None,
) -> str:
    """Expand a template instantiation into resources. Returns the root resource ID."""
    # Build the variable scope: global vars + template params
    param_vals = {}
    known_params = {p.name for p in template.params}
    # Check for unknown parameters
    unknown = [k for k in args if k not in known_params]
    if unknown:
        avail = ", ".join(p.name + (f'="{p.default}"' if p.default else " (required)") for p in template.params)
        hints = []
        for u in unknown:
            # Suggest close matches (edit distance ≤ 2 or substring match)
            suggestions = []
            for kp in known_params:
                if u in kp or kp in u:
                    suggestions.append(kp)
                elif _edit_distance(u, kp) <= 2:
                    suggestions.append(kp)
            if suggestions:
                hints.append(f"  '{u}' — did you mean: {', '.join(suggestions)}?")
        hint_str = "\n" + "\n".join(hints) if hints else ""
        raise ResolveError(
            f"Template '{template.name}' does not accept parameter(s): {', '.join(unknown)}\n"
            f"  Available: {avail}{hint_str}"
        )
    for p in template.params:
        if p.name in args:
            param_vals[p.name] = args[p.name]
        elif p.default is not None:
            param_vals[p.name] = _expand(p.default, {**global_vars, **args}) or p.default
        else:
            avail = ", ".join(
                f"{pp.name}={pp.default!r}" if pp.default else f"{pp.name} (required)"
                for pp in template.params
            )
            provided = ", ".join(f'{k}="{v}"' for k, v in args.items()) or "(none)"
            raise ResolveError(
                f"Template '{template.name}' requires parameter '{p.name}' which was not provided.\n"
                f"  Expected: {avail}\n"
                f"  Provided: {provided}"
            )

    vs = {**global_vars, **param_vals}

    # Generate a unique prefix from template name + primary param
    # e.g. install_package with name=nginx → install_package_nginx
    # OR: if a human_name was given (.cgr format), use its slug
    if human_name:
        prefix = _slug(human_name) if callable(_slug) else re.sub(r'[^a-zA-Z0-9]+', '_', human_name).strip('_').lower()
    else:
        primary_param = param_vals.get(template.params[0].name, "") if template.params else ""
        safe_param = re.sub(r'[^a-zA-Z0-9_]', '_', primary_param)
        prefix = f"{template.name}_{safe_param}" if safe_param else template.name

    prov = Provenance(
        source_file=template.source_file,
        template_name=template.name,
        params_used=param_vals,
        version=template.version,
    )
    provenance_log.append(prov)

    root_id = _flatten_ast_resource(
        template.body, node_name, prefix, vs, prov,
        collector, dedup_hashes, dedup_map, source_line=source_line,
        known_nodes=known_nodes, templates=templates,
        provenance_log=provenance_log,
    )
    return root_id


def resolve(prog: ASTProgram, repo_dir: str|None = None, graph_file: str|None = None,
            extra_vars: dict[str,str]|None = None, inventory_files: list[str]|None = None,
            vault_passphrase: str | None = None, vault_prompt: bool = False,
            resolve_deferred_secrets: bool = False) -> Graph:
    """Full resolution: imports → expansion → dedup → DAG."""

    # ── Load inventory files (lowest precedence) ─────────────────────────
    inv_vars: dict[str,str] = {}

    # Inventory files from the graph (inventory "file.ini")
    for inv in prog.inventories:
        inv_path = Path(inv.path)
        # Resolve relative to graph file directory
        if not inv_path.is_absolute() and graph_file:
            inv_path = Path(graph_file).parent / inv_path
        if not inv_path.exists():
            raise ResolveError(
                f"Inventory file not found: {inv.path}\n"
                f"  Referenced at line {inv.line}\n"
                f"  Searched: {inv_path}")
        inv_vars.update(_parse_inventory(str(inv_path)))

    # CLI --inventory files (override graph-declared inventory)
    for inv_file in (inventory_files or []):
        inv_path = Path(inv_file)
        if not inv_path.exists():
            raise ResolveError(f"Inventory file not found: {inv_file}")
        inv_vars.update(_parse_inventory(str(inv_path)))

    # ── Gather local facts (lowest precedence) ───────────────────────
    fact_vars: dict[str, str] = {}
    if prog.gather_facts:
        fact_vars = _gather_facts()

    # ── Global variables ────────────────────────────────────────────────
    # Precedence: facts < inventory < graph set < --vars-file < secrets < --set
    gvars = dict(fact_vars)   # start with facts (lowest)
    gvars.update(inv_vars)    # inventory overrides facts
    # Apply overrides (CLI --set and --vars-file take precedence, but before secrets)
    overrides = extra_vars or {}
    graph_vars, sensitive_values = _materialize_ast_variables(
        prog.variables,
        resolve_deferred_secrets=resolve_deferred_secrets,
        skip_names=set(overrides.keys()),
    )
    gvars.update(graph_vars)  # graph set overrides

    # Load secrets (between --vars-file and --set in precedence)
    resolved_vault_pass = vault_passphrase if vault_passphrase is not None else os.environ.get('CGR_VAULT_PASS', '')
    for sec in prog.secrets:
        sec_path = Path(sec.path)
        if not sec_path.is_absolute() and graph_file:
            sec_path = Path(graph_file).parent / sec_path
        if not sec_path.exists():
            raise ResolveError(
                f"Secrets file not found: {sec.path}\n"
                f"  Referenced at line {sec.line}\n"
                f"  Searched: {sec_path}")
        if not resolved_vault_pass and vault_prompt and sys.stdin.isatty():
            import getpass
            resolved_vault_pass = getpass.getpass("Vault passphrase: ")
        if not resolved_vault_pass:
            raise ResolveError(
                f"Secrets file '{sec.path}' requires a vault passphrase.\n"
                f"  Use --vault-pass-ask (-A), --vault-pass, --vault-pass-file, or CGR_VAULT_PASS env var.")
        try:
            secret_vars = _load_secrets(str(sec_path), resolved_vault_pass)
            for v in secret_vars.values():
                if v:  # don't redact empty strings
                    sensitive_values.add(v)
            gvars.update(secret_vars)
        except (ValueError, RuntimeError) as e:
            raise ResolveError(f"Failed to load secrets '{sec.path}': {e}")

    gvars.update(overrides)

    # ── Build repo search path ───────────────────────────────────────────
    repo_dirs = _build_repo_search_path(explicit_repo=repo_dir, graph_file=graph_file)

    # ── Load imported templates ─────────────────────────────────────────
    templates: dict[str, ASTTemplate] = {}

    # Templates defined inline
    for t in prog.templates:
        templates[t.name] = t

    # Templates imported via `use`
    for u in prog.uses:
        tpl = load_template_from_repo(repo_dirs, u.path)
        # Version constraint checking
        if u.version_op and u.version_req:
            if not tpl.version:
                raise ResolveError(
                    f"Template '{u.path}' does not declare a version,\n"
                    f"  but line {u.line} requires {u.version_op} {u.version_req}.\n"
                    f"  Add 'version X.Y.Z' to the template file to fix this.")
            if not _check_version(u.version_op, u.version_req, tpl.version):
                raise ResolveError(
                    f"Version mismatch for template '{u.path}':\n"
                    f"  Required: {u.version_op} {u.version_req} (line {u.line})\n"
                    f"  Found:    {tpl.version} ({tpl.source_file})")
        alias = u.alias or tpl.name
        templates[alias] = tpl

    # ── Expand top-level target each blocks ─────────────────────────────
    each_generated_names: list[str] = []
    for te in prog.target_each_blocks:
        list_val = _expand(te.list_expr, gvars) or ""
        items = [x.strip() for x in list_val.split(",") if x.strip()]
        for item in items:
            parts = item.split(":", 1)
            sub = {}
            if len(te.var_names) >= 2 and len(parts) == 2:
                sub[te.var_names[0]] = parts[0].strip()
                sub[te.var_names[1]] = parts[1].strip()
            elif len(te.var_names) == 1:
                sub[te.var_names[0]] = item
            else:
                sub[te.var_names[0]] = parts[0].strip() if parts else item
                if len(te.var_names) > 1:
                    sub[te.var_names[1]] = parts[1].strip() if len(parts) > 1 else item

            if te.body_format == "cgr":
                # Substitute variables in body source and parse as CGR
                body = te.body_source
                for vname, vval in sub.items():
                    body = body.replace(f"${{{vname}}}", vval)
                # Also substitute global vars
                for vname, vval in gvars.items():
                    body = body.replace(f"${{{vname}}}", vval)
                sub_prog = parse_cgr(body, filename="<each>")
                for node in sub_prog.nodes:
                    prog.nodes.append(node)
                    each_generated_names.append(node.name)
            elif te.body_format == "cg":
                # Substitute in tokens and parse
                expanded_tokens = []
                for t in te.body_tokens:
                    val = t.value
                    for vname, vval in sub.items():
                        val = val.replace(f"${{{vname}}}", vval)
                    for vname, vval in gvars.items():
                        val = val.replace(f"${{{vname}}}", vval)
                    expanded_tokens.append(Token(t.type, val, t.line, t.col))
                expanded_tokens.append(Token(TT.EOF, "", te.line, 0))
                sub_parser = Parser.__new__(Parser)
                sub_parser.tokens = expanded_tokens
                sub_parser.pos = 0
                sub_parser.source_lines = []
                sub_parser.filename = "<each>"
                if sub_parser._at_id("node"):
                    node = sub_parser._p_node()
                    prog.nodes.append(node)
                    each_generated_names.append(node.name)

    # Resolve `after each` sentinel → actual generated target names
    for an in prog.nodes:
        if "__each__" in an.after_nodes:
            an.after_nodes.remove("__each__")
            an.after_nodes.extend(each_generated_names)

    # ── Process nodes ───────────────────────────────────────────────────
    all_res: dict[str, Resource] = {}
    nodes: dict[str, HostNode] = {}
    dedup_hashes: dict[str, str] = {}  # identity_hash → first resource_id
    dedup_map: dict[str, list[str]] = defaultdict(list)
    provenance_log: list[Provenance] = []
    all_node_names: set[str] = {an.name for an in prog.nodes}

    for an in prog.nodes:
        nn = an.name
        via_props = {k: _expand(v, gvars) or v for k, v in an.via.props.items()}
        hn = HostNode(name=nn, via_method=an.via.method, via_props=via_props, resources={})

        # Expand template instantiations
        for inst in an.instantiations:
            if inst.template_name not in templates:
                raise ResolveError(
                    f"Unknown template '{inst.template_name}' at line {inst.line}.\n"
                    f"  Available: {sorted(templates.keys())}\n"
                    f"  Did you forget a 'use' statement?"
                )
            _expand_template(
                templates[inst.template_name],
                {k: _expand(v, gvars) or v for k, v in inst.args.items()},
                nn, gvars, all_res, dedup_hashes, dedup_map, provenance_log,
                human_name=getattr(inst, '_cgr_human_name', None),
                source_line=inst.line,
                known_nodes=all_node_names, templates=templates,
            )

        # Process inline resources (from groups + top-level)
        all_ast: list[ASTResource] = list(an.resources)
        for g in an.groups:
            all_ast.extend(g.resources)

        for ar in all_ast:
            _flatten_ast_resource(
                ar, nn, ar.name, gvars, None,
                all_res, dedup_hashes, dedup_map, source_line=ar.line,
                known_nodes=all_node_names, templates=templates,
                provenance_log=provenance_log,
            )

        for rid, res in all_res.items():
            if res.node_name == nn:
                hn.resources[rid] = res
        nodes[nn] = hn

    # ── Wire node-level `after` ordering ─────────────────────────────────
    # For each node with after_nodes, create a barrier that depends on all
    # leaf resources in the predecessor node. All root resources in this
    # node then depend on the barrier.
    node_after: dict[str, list[str]] = {}
    for an in prog.nodes:
        if an.after_nodes:
            node_after[an.name] = list(an.after_nodes)

    for target_nn, pred_names in node_after.items():
        for pred_nn in pred_names:
            if pred_nn not in nodes:
                raise ResolveError(
                    f"Node '{target_nn}' declares 'after \"{pred_nn}\"' but node '{pred_nn}' doesn't exist.\n"
                    f"  Known nodes: {sorted(nodes.keys())}")
            # Find leaf resources (out-degree=0 within predecessor node)
            pred_rids = set(nodes[pred_nn].resources.keys())
            depended_on = set()
            for rid, res in all_res.items():
                for dep in res.needs:
                    if dep in pred_rids:
                        depended_on.add(dep)
            leaves = pred_rids - depended_on
            if not leaves:
                leaves = pred_rids  # fallback: depend on everything

            # Create barrier node
            barrier_id = f"{pred_nn}.__node_barrier_for_{target_nn}"
            barrier = Resource(
                id=barrier_id, short_name=f"__barrier_{pred_nn}",
                node_name=pred_nn,
                description=f"node barrier: {target_nn} waits for {pred_nn}",
                needs=sorted(leaves), check=None, run="true", run_as=None,
                timeout=5, retries=0, retry_delay=0, retry_backoff=False, on_fail=OnFail.STOP,
                when=None, env={}, is_verify=False, line=0,
                is_barrier=True, identity_hash=_identity(None, f"node_barrier_{pred_nn}_{target_nn}", None, pred_nn))
            all_res[barrier_id] = barrier
            nodes[pred_nn].resources[barrier_id] = barrier

            # Find root resources (in-degree=0 within target node)
            target_rids = set(nodes[target_nn].resources.keys())
            has_inbound = set()
            for rid in target_rids:
                for dep in all_res[rid].needs:
                    if dep in target_rids:
                        has_inbound.add(rid)
            roots = target_rids - has_inbound
            if not roots:
                roots = target_rids  # fallback

            # Wire all roots in target node to depend on the barrier
            for root_rid in roots:
                if barrier_id not in all_res[root_rid].needs:
                    all_res[root_rid].needs.append(barrier_id)

    # ── Resolve dangling `needs` references ─────────────────────────────
    # Some needs references may be shorthand. Try to resolve them.
    known_ids = set(all_res.keys())
    for rid, res in all_res.items():
        resolved_needs = []
        seen_needs = set()
        for dep in res.needs:
            if dep in known_ids:
                if dep not in seen_needs:
                    resolved_needs.append(dep)
                    seen_needs.add(dep)
            else:
                # Check if this is an explicit cross-node reference:
                # dep like "other_node.step" where other_node != res.node_name
                is_cross_node = False
                if '.' in dep:
                    node_prefix = dep.split('.')[0]
                    if node_prefix in nodes and node_prefix != res.node_name:
                        is_cross_node = True
                        # Explicit cross-node ref: find matching resource in that node
                        suffix = dep[len(node_prefix)+1:]
                        dep_tail = suffix.split('.')[-1]
                        candidates = [k for k in known_ids
                                     if k.startswith(node_prefix + ".") and
                                     (k == dep or k.endswith(f".{dep_tail}"))]
                        if len(candidates) == 1:
                            dep_id = candidates[0]
                            if dep_id not in seen_needs:
                                resolved_needs.append(dep_id)
                                seen_needs.add(dep_id)
                        elif len(candidates) > 1:
                            # Prefer exact suffix match
                            exact = [c for c in candidates if c.endswith(f".{suffix}")]
                            dep_id = exact[0] if exact else candidates[0]
                            if dep_id not in seen_needs:
                                resolved_needs.append(dep_id)
                                seen_needs.add(dep_id)
                        else:
                            raise ResolveError(
                                f"Resource '{rid}' (line {res.line}) needs '{dep}' "
                                f"which doesn't exist in node '{node_prefix}'.\n"
                                f"  Resources in '{node_prefix}':\n" +
                                "\n".join(f"    • {k}" for k in sorted(k2 for k2 in known_ids if k2.startswith(node_prefix + ".")))
                            )
                        continue

                if is_cross_node:
                    continue  # already handled above

                # Try fuzzy matching within same node first
                dep_suffix = dep.split('.')[-1]
                same_node_candidates = [k for k in known_ids
                                       if k.startswith(res.node_name + ".") and k.endswith(f".{dep_suffix}")]
                if len(same_node_candidates) == 1:
                    dep_id = same_node_candidates[0]
                    if dep_id not in seen_needs:
                        resolved_needs.append(dep_id)
                        seen_needs.add(dep_id)
                elif len(same_node_candidates) > 1:
                    dep_id = same_node_candidates[0]
                    if dep_id not in seen_needs:
                        resolved_needs.append(dep_id)
                        seen_needs.add(dep_id)
                else:
                    # No same-node match — check other nodes for a helpful error
                    all_candidates = [k for k in known_ids if k.endswith(f".{dep_suffix}")]
                    if all_candidates and len(nodes) > 1:
                        other_candidates = [c for c in all_candidates if not c.startswith(res.node_name + ".")]
                        if other_candidates:
                            suggestions = ", ".join(f"[{c.split('.')[0]}/{dep_suffix}]" for c in other_candidates[:3])
                            raise ResolveError(
                                f"Resource '{rid}' (line {res.line}) needs '{dep}' "
                                f"which doesn't exist in node '{res.node_name}'.\n"
                                f"  Found in other node(s): {suggestions}\n"
                                f"  Use cross-node syntax: first [{other_candidates[0].split('.')[0]}/{dep_suffix}]"
                            )
                    raise ResolveError(
                        f"Resource '{rid}' (line {res.line}) needs '{dep}' which doesn't exist.\n"
                        f"  Known resources:\n" +
                        "\n".join(f"    • {k}" for k in sorted(known_ids))
                    )
        res.needs = resolved_needs

    # ── Collect HTTP auth tokens for redaction ──────────────────────────
    for rid, res in all_res.items():
        if res.http_auth and res.http_auth[1]:
            sensitive_values.add(res.http_auth[1])

    # ── Topological sort ────────────────────────────────────────────────
    waves = _topo_waves(all_res)

    # ── Resolve on_complete/on_failure hooks ──────────────────────────────
    def _resolve_hook(ast_hook, hook_name):
        if not ast_hook: return None
        exp = lambda t: _expand(t, gvars)
        exp_url = exp(ast_hook.http_url) if ast_hook.http_url else None
        exp_run = exp(ast_hook.run) if ast_hook.run else None
        exp_script_path = exp(ast_hook.script_path) if ast_hook.script_path else None
        if ast_hook.http_method and not exp_run:
            exp_run = f"{ast_hook.http_method} {exp_url or ''}"
        exp_headers = [(exp(h), exp(v)) for h, v in (ast_hook.http_headers or [])]
        exp_auth = None
        if ast_hook.http_auth:
            exp_auth = (ast_hook.http_auth[0], exp(ast_hook.http_auth[1]) if ast_hook.http_auth[1] else None)
            if exp_auth[1]: sensitive_values.add(exp_auth[1])
        return Resource(id=hook_name, short_name=hook_name,
            node_name="__hook", run=exp_run or "", check=None, run_as=None,
            timeout=ast_hook.timeout, retries=ast_hook.retries,
            retry_delay=ast_hook.retry_delay, retry_backoff=False, on_fail=OnFail.WARN,
            needs=[], description=f"Hook: {hook_name}", is_verify=False,
            when=None, env={}, line=0, script_path=exp_script_path,
            http_method=ast_hook.http_method, http_url=exp_url,
            http_headers=exp_headers, http_body=exp(ast_hook.http_body) if ast_hook.http_body else None,
            http_body_type=ast_hook.http_body_type, http_auth=exp_auth,
            http_expect=ast_hook.http_expect, collect_key=ast_hook.collect_key)

    resolved_on_complete = _resolve_hook(prog.on_complete, "__on_complete")
    resolved_on_failure = _resolve_hook(prog.on_failure, "__on_failure")

    return Graph(
        nodes=nodes, all_resources=all_res, waves=waves,
        variables=gvars, dedup_map=dict(dedup_map),
        provenance_log=provenance_log,
        node_ordering=node_after,
        sensitive_values=sensitive_values,
        on_complete=resolved_on_complete,
        on_failure=resolved_on_failure,
        graph_file=graph_file,
    )


def _find_cycle_path(remaining: set[str], resources: dict[str, Resource]) -> list[str]|None:
    """Find the shortest cycle path among remaining (in-cycle) resources via DFS."""
    for start in sorted(remaining):
        visited = set()
        stack = [(start, [start])]
        while stack:
            node, path = stack.pop()
            for dep in resources[node].needs:
                if dep not in remaining: continue
                if dep == start and len(path) > 1:
                    return path  # found cycle back to start
                if dep not in visited:
                    visited.add(dep)
                    stack.append((dep, path + [dep]))
    return None


def _topo_waves(resources: dict[str, Resource]) -> list[list[str]]:
    adj: dict[str, list[str]] = defaultdict(list)
    in_deg = {rid: 0 for rid in resources}
    for rid, res in resources.items():
        for dep in res.needs:
            if dep in in_deg:  # guard against resolved-away dedup refs
                adj[dep].append(rid)
                in_deg[rid] += 1
    q: deque[str] = deque(sorted(rid for rid, d in in_deg.items() if d == 0))
    waves: list[list[str]] = []; visited = 0
    while q:
        wave = sorted(q); q.clear(); waves.append(wave); visited += len(wave)
        for rid in wave:
            for child in sorted(adj[rid]):
                in_deg[child] -= 1
                if in_deg[child] == 0: q.append(child)
    if visited != len(resources):
        remaining = set(rid for rid, d in in_deg.items() if d > 0)
        # Extract exact cycle path via DFS
        cycle_path = _find_cycle_path(remaining, resources)
        if cycle_path:
            cycle_str = " → ".join(cycle_path + [cycle_path[0]])
            msg = f"Dependency cycle detected:\n  {cycle_str}\n\n"
        else:
            msg = "Dependency cycle detected among:\n"
        msg += "Involved resources:\n"
        msg += "\n".join(f"  • {r} → needs {[n for n in resources[r].needs if n in remaining]}" for r in sorted(remaining)[:8])
        if len(remaining) > 8:
            msg += f"\n  ... and {len(remaining) - 8} more"
        raise ResolveError(msg)
    return waves

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
