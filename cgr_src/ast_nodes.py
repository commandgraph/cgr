"""All AST dataclasses shared by both graph syntaxes and later phases."""
from __future__ import annotations
from cgr_src.common import *

@dataclass
class ASTVar:
    name: str
    value: str | None
    line: int
    is_secret: bool = False
    secret_cmd: str | None = None
    secret_vault: str | None = None
@dataclass
class ASTVia:       method: str; props: dict[str,str]; line: int
@dataclass
class ASTParam:     name: str; default: str | None; line: int

@dataclass
class ASTResource:
    name: str; description: str; needs: list[str]
    check: str|None; run: str|None; run_as: str|None
    timeout: int; retries: int; retry_delay: int; retry_backoff: bool
    on_fail: str; when: str|None; env: dict[str,str]
    is_verify: bool; line: int
    timeout_reset_on_output: bool = False
    script_path: str|None = None
    collect_key: str|None = None  # output collection key
    collect_format: str|None = None  # "json" for pretty-printed JSON output
    tags: list[str] = field(default_factory=list)  # step tags for filtering
    children: list[ASTResource] = field(default_factory=list)
    # HTTP request fields (populated by parser for get/post/put/patch/delete steps)
    http_method: str|None = None     # GET, POST, PUT, PATCH, DELETE
    http_url: str|None = None        # URL (variable-expanded at resolve time)
    http_headers: list[tuple[str,str]] = field(default_factory=list)  # [(name, value), ...]
    http_body: str|None = None       # request body (JSON or form-encoded)
    http_body_type: str|None = None  # "json" or "form"
    http_auth: tuple[str,str|None]|None = None  # ("bearer", token) or ("basic", "user:pass")
    http_expect: str|None = None     # expected status codes: "200", "200..299", "200, 201"
    wait_kind: str|None = None       # "webhook" or "file"
    wait_target: str|None = None
    subgraph_path: str|None = None
    subgraph_vars: dict[str, str] = field(default_factory=dict)
    # Parallel constructs (populated by parser, expanded by resolver)
    parallel_block: list[ASTResource] = field(default_factory=list)
    parallel_limit: int|None = None
    parallel_fail_policy: str = "wait"  # wait|stop|ignore
    race_block: list[ASTResource] = field(default_factory=list)
    race_into: str|None = None  # "race into EXPR:" — final destination path for winner
    each_var: str|None = None
    each_list_expr: str|None = None
    each_limit: int|str|None = None   # int for literal, str for ${var} expression
    each_body: ASTResource|None = None
    stage_name: str|None = None
    stage_phases: list[Any] = field(default_factory=list)  # list of ASTPhase
    # Provisioning operation fields (Phase 1)
    prov_content_inline: str|None = None    # inline content body text
    prov_content_from: str|None = None      # source file path for 'content from FILE > PATH'
    prov_dest: str|None = None              # destination path for content/line/put
    prov_lines: list[tuple] = field(default_factory=list)  # (action, text, replacing_regex|None)
    prov_put_src: str|None = None           # source path for 'put SRC > DEST'
    prov_put_mode: str|None = None          # octal mode string e.g. "0755"
    prov_put_transfer: str|None = None      # "rsync" or "scp" (None = auto-detect)
    prov_put_transfer_opts: str|None = None # raw flags passed to transfer tool
    prov_validate: str|None = None          # command for 'validate $ CMD'
    # Provisioning operation fields (Phase 2)
    prov_block_dest: str|None = None        # destination file for block keyword
    prov_block_marker: str|None = None      # marker string for block keyword
    prov_block_inline: str|None = None      # inline block body text
    prov_block_from: str|None = None        # source file for 'block from FILE'
    prov_block_absent: bool = False         # if True, remove the block
    prov_ini_dest: str|None = None          # destination file for ini keyword
    prov_ini_ops: list[tuple] = field(default_factory=list)  # (section|None, key, value)  value="absent" to remove
    prov_json_dest: str|None = None         # destination file for json keyword
    prov_json_ops: list[tuple] = field(default_factory=list)  # (path, value)  value="absent" to remove
    prov_asserts: list[tuple] = field(default_factory=list)  # (cmd, message|None)
    # Backoff/jitter fields (for retry Nx backoff Xs syntax)
    retry_backoff_max: int = 0   # cap in seconds (0 = no cap)
    retry_jitter_pct: int = 0    # jitter percentage (0 = none)
    interactive: bool = False    # force terminal-interactive mode (PTY + stdin forwarding)
    # On-success/failure variable bindings
    on_success_set: list[tuple] = field(default_factory=list)  # [(var, val), ...]
    on_failure_set: list[tuple] = field(default_factory=list)
    # Collect-to-variable and reduce
    collect_var: str|None = None
    reduce_key: str|None = None
    reduce_var: str|None = None
    inner_template_name: str|None = None
    inner_template_args: dict[str,str] = field(default_factory=dict)
    # Conditional command fragments / polling
    flags: list[tuple[str, str|None]] = field(default_factory=list)  # [(flag_text, when_expr|None)]
    env_when: dict[str, str] = field(default_factory=dict)  # KEY -> when_expr
    until: str|None = None
    cgr_phase_name: str|None = None  # set when step is inside a phase "name" when "COND": block

@dataclass
class ASTPhase:
    name: str; count_expr: str; from_list: str
    body: list[ASTResource]; each_limit: int|str|None
    line: int

@dataclass
class ASTTemplate:
    name: str; params: list[ASTParam]; description: str
    body: ASTResource   # the template root resource (may have children)
    source_file: str; line: int
    version: str|None = None  # semantic version "X.Y.Z"

@dataclass
class ASTUse:
    path: str; alias: str|None; line: int
    version_op: str|None = None   # "=", ">=", "~>"
    version_req: str|None = None  # "1.2.0"

@dataclass
class ASTInstantiation:
    template_name: str; args: dict[str,str]; line: int

@dataclass
class ASTGroup:
    name: str; run_as: str|None; on_fail: str|None
    timeout: int|None; resources: list[ASTResource]; line: int
    timeout_reset_on_output: bool|None = None

@dataclass
class ASTNode:
    name: str; via: ASTVia
    resources: list[ASTResource]; groups: list[ASTGroup]
    instantiations: list[ASTInstantiation]; line: int
    after_nodes: list[str] = field(default_factory=list)  # node-level ordering

@dataclass
class ASTInventory:
    """Inventory file reference."""
    path: str; line: int

@dataclass
class ASTSecrets:
    """Encrypted secrets file reference."""
    path: str; line: int

@dataclass
class ASTTargetEach:
    """Top-level each block that stamps out target blocks."""
    var_names: list[str]   # e.g. ["name", "addr"]
    list_expr: str          # e.g. "${hosts}"
    body_source: str        # raw source text of the body (target block)
    body_format: str        # "cgr" or "cg"
    line: int
    # For .cg parser: captured tokens
    body_tokens: list[Any] = field(default_factory=list)

@dataclass
class ASTGatherFacts:
    """Directive to gather system facts into variables."""
    line: int

@dataclass
class ASTProgram:
    variables: list[ASTVar]; uses: list[ASTUse]
    templates: list[ASTTemplate]; nodes: list[ASTNode]
    target_each_blocks: list[ASTTargetEach] = field(default_factory=list)
    inventories: list[ASTInventory] = field(default_factory=list)
    secrets: list[ASTSecrets] = field(default_factory=list)
    gather_facts: bool = False
    stateless: bool = False
    on_complete: ASTResource|None = None   # hook: runs after successful apply
    on_failure: ASTResource|None = None    # hook: runs after failed apply


_DEFERRED_SECRET_CMD = "<deferred:cmd>"
_DEFERRED_SECRET_VAULT = "<deferred:vault>"

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
