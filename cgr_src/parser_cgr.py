"""Indentation-based .cgr parser."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.ast_nodes import *

class CGRParseError(Exception):
    def __init__(self, msg, line_no=0, line_text="", filename=""):
        self.msg=msg; self.line_no=line_no; self.line_text=line_text; self.filename=filename
        super().__init__(msg)
    def pretty(self):
        parts=[red(f"error: {self.msg}")]
        if self.line_no:
            loc=f"  at {self.filename}:{self.line_no}" if self.filename else f"  at line {self.line_no}"
            parts[0]+=dim(loc)
            if self.line_text:
                parts.append(dim(f"  {self.line_no:>4} │ ")+self.line_text)
        return "\n".join(parts)

def _slug(name: str) -> str:
    """Convert a human step name to a valid identifier slug."""
    return re.sub(r'[^a-zA-Z0-9]+', '_', name.strip()).strip('_').lower()

def _parse_cgr_dep_ref(ref: str) -> str:
    """Parse a bracket dependency reference into a dotted resource id."""
    if "/" in ref:
        return ".".join(_slug(part) for part in ref.split("/") if part.strip())
    return _slug(ref)

def parse_cgr(source: str, filename: str = "", _include_base_dir: Path | None = None) -> ASTProgram:
    """Parse a .cgr (human-readable) file into the same AST as the .cg parser."""
    lines_raw = source.split("\n")
    # Pre-process: strip comments, track original line numbers
    lines: list[tuple[int, int, str]] = []  # (line_no, indent, content)
    raw_block_indent: int | None = None
    for i, raw in enumerate(lines_raw, 1):
        raw_rstripped = raw.rstrip()
        raw_indent = len(raw_rstripped) - len(raw_rstripped.lstrip())
        if raw_block_indent is not None:
            if raw_rstripped.strip() and raw_indent > raw_block_indent:
                lines.append((i, raw_indent, raw_rstripped.strip()))
                continue
            if raw_rstripped.strip() and raw_indent <= raw_block_indent:
                raw_block_indent = None
        # Comment stripping: find first unquoted # that is not inside a $ command.
        # Rules: # after "$ " is shell; # inside a double-quoted string is literal.
        dollar_pos = raw.find("$ ")
        in_quote = False
        hash_pos = -1
        for ci, ch in enumerate(raw):
            if ch == '"':
                in_quote = not in_quote
            elif ch == '#' and not in_quote:
                if dollar_pos >= 0 and ci > dollar_pos:
                    break  # # is inside shell command — do not strip
                hash_pos = ci
                break
        if hash_pos >= 0:
            stripped = raw[:hash_pos].rstrip()
        else:
            stripped = raw.rstrip()
        if not stripped.strip():
            continue
        indent = len(stripped) - len(stripped.lstrip())
        line_text = stripped.strip()
        lines.append((i, indent, line_text))
        if re.match(r'content\s*>\s*(\S+)\s*:\s*$', line_text) or re.match(r'block\s+in\s+(\S+)\s*,\s*marker\s+"([^"]*)"\s*:\s*$', line_text):
            raw_block_indent = indent

    variables: list[ASTVar] = []
    uses: list[ASTUse] = []
    nodes: list[ASTNode] = []
    templates: list[ASTTemplate] = []
    target_each_blocks: list[ASTTargetEach] = []
    inventories: list[ASTInventory] = []
    secrets_list: list[ASTSecrets] = []
    gather_facts = False
    stateless = False
    pos = 0

    def peek():
        return lines[pos] if pos < len(lines) else None

    def advance():
        nonlocal pos; r = lines[pos]; pos += 1; return r

    def err(msg, ln=0, lt=""):
        return CGRParseError(msg, ln, lt, filename)

    # ── Top-level parsing ──────────────────────────────────────────────
    while pos < len(lines):
        ln, indent, text = peek()

        # --- Title --- (skip)
        if text.startswith("---") and text.endswith("---"):
            advance(); continue

        # set var = "value" or set var = env("ENV_NAME") or set var = secret "env:VAR" / secret "file:PATH"
        if text.startswith("set "):
            advance()
            # set stateless = true — disable state file persistence for this graph
            if re.match(r'set\s+stateless\s*=\s*true\b', text):
                stateless = True; continue
            if re.match(r'set\s+stateless\s*=\s*false\b', text):
                stateless = False; continue
            # secret "env:VAR_NAME" — read from environment variable (redacted in output)
            sec_env_m = re.match(r'set\s+(\w+)\s*=\s*secret\s+"env:([^"]+)"', text)
            if sec_env_m:
                var_name = sec_env_m.group(1)
                env_key = sec_env_m.group(2)
                val = os.environ.get(env_key)
                if val is None:
                    raise err(f"Secret env var '{env_key}' is not set", ln, text)
                variables.append(ASTVar(name=var_name, value=val, line=ln, is_secret=True))
                continue
            # secret "file:PATH" — read from file (redacted in output)
            sec_file_m = re.match(r'set\s+(\w+)\s*=\s*secret\s+"file:([^"]+)"', text)
            if sec_file_m:
                var_name = sec_file_m.group(1)
                file_path = sec_file_m.group(2)
                try:
                    val = Path(file_path).read_text().strip()
                except FileNotFoundError:
                    raise err(f"Secret file not found: {file_path}", ln, text)
                variables.append(ASTVar(name=var_name, value=val, line=ln, is_secret=True))
                continue
            # secret "cmd:COMMAND" — run a command, capture stdout (redacted in output)
            sec_cmd_m = re.match(r'set\s+(\w+)\s*=\s*secret\s+"cmd:([^"]+)"', text)
            if sec_cmd_m:
                var_name = sec_cmd_m.group(1)
                cmd_str = sec_cmd_m.group(2)
                variables.append(ASTVar(
                    name=var_name,
                    value=_DEFERRED_SECRET_CMD,
                    line=ln,
                    is_secret=True,
                    secret_cmd=cmd_str,
                ))
                continue
            # secret "vault:SECRET_PATH" — read from HashiCorp Vault via HTTP API (redacted in output)
            sec_vault_m = re.match(r'set\s+(\w+)\s*=\s*secret\s+"vault:([^"]+)"', text)
            if sec_vault_m:
                var_name = sec_vault_m.group(1)
                vault_path = sec_vault_m.group(2)
                variables.append(ASTVar(
                    name=var_name,
                    value=_DEFERRED_SECRET_VAULT,
                    line=ln,
                    is_secret=True,
                    secret_vault=vault_path,
                ))
                continue
            # env() function: set var = env("NAME") or env("NAME", "default")
            env_m = re.match(r'set\s+(\w+)\s*=\s*env\(\s*"([^"]+)"(?:\s*,\s*"([^"]*)")?\s*\)', text)
            if env_m:
                var_name = env_m.group(1)
                env_key = env_m.group(2)
                env_default = env_m.group(3)
                val = os.environ.get(env_key)
                if val is None:
                    if env_default is not None:
                        val = env_default
                    else:
                        raise err(f"Environment variable '{env_key}' is not set and no default provided", ln, text)
                variables.append(ASTVar(name=var_name, value=val, line=ln))
                continue
            m = re.match(r'set\s+(\w+)\s*=\s*"([^"]*)"', text)
            if not m:
                m = re.match(r'set\s+(\w+)\s*=\s*(\S+)', text)
            if not m:
                raise err(f"Invalid set statement", ln, text)
            variables.append(ASTVar(name=m.group(1), value=m.group(2), line=ln))
            continue

        # include "other_file.cgr" — merge another graph file's contents
        if text.startswith("include "):
            advance()
            inc_m = re.match(r'include\s+"([^"]+)"', text)
            if not inc_m:
                raise err(f"Invalid include statement (expected: include \"path.cgr\")", ln, text)
            inc_path = inc_m.group(1)
            try:
                resolved_include, inc_source = _read_include_file(
                    inc_path,
                    filename,
                    _include_base_dir,
                )
            except ValueError as exc:
                raise err(str(exc), ln, text)
            except FileNotFoundError as exc:
                raise err(f"Included file not found: {exc}", ln, text)
            inc_ast = parse_cgr(inc_source, str(resolved_include), resolved_include.parent)
            # Merge: variables (don't overwrite existing), uses, nodes
            existing_var_names = {v.name for v in variables}
            for v in inc_ast.variables:
                if v.name not in existing_var_names:
                    variables.append(v)
            uses.extend(inc_ast.uses)
            nodes.extend(inc_ast.nodes)
            templates.extend(inc_ast.templates)
            target_each_blocks.extend(inc_ast.target_each_blocks)
            inventories.extend(inc_ast.inventories)
            continue

        # using template1, template2, ...
        if text.startswith("using"):
            advance()
            rest = text[5:].strip()
            # Collect continuation lines (indented, no keyword)
            while pos < len(lines) and lines[pos][1] > indent:
                rest += " " + lines[pos][2]; advance()
            # Parse comma-separated template paths with optional version constraints
            # e.g. "apt/install_package >= 1.0, firewall/allow_port ~> 2.0"
            for part in rest.split(","):
                p = part.strip().strip('"').strip("'")
                if not p: continue
                ver_m = re.match(r'^([\w/]+)\s*(>=|~>|=)\s*"?(\d+\.\d+(?:\.\d+)?)"?\s*$', p)
                if ver_m:
                    uses.append(ASTUse(path=ver_m.group(1), alias=None, line=ln,
                                       version_op=ver_m.group(2), version_req=ver_m.group(3)))
                else:
                    uses.append(ASTUse(path=p, alias=None, line=ln))
            continue

        # inventory "file.ini"
        if text.startswith("inventory "):
            advance()
            inv_m = re.match(r'inventory\s+"([^"]+)"', text)
            if not inv_m:
                raise err(f"Invalid inventory statement. Expected: inventory \"file.ini\"", ln, text)
            inventories.append(ASTInventory(path=inv_m.group(1), line=ln))
            continue

        # gather facts
        if text == "gather facts":
            advance()
            gather_facts = True
            continue

        # secrets "file.secrets"
        if text.startswith("secrets "):
            advance()
            sec_m = re.match(r'secrets\s+"([^"]+)"', text)
            if not sec_m:
                raise err(f"Invalid secrets statement. Expected: secrets \"file.secrets\"", ln, text)
            secrets_list.append(ASTSecrets(path=sec_m.group(1), line=ln))
            continue

        # template name(params):
        if text.startswith("template "):
            tpl = _parse_cgr_template(lines, pos, err, filename)
            templates.append(tpl)
            advance()  # template line
            while pos < len(lines) and lines[pos][1] > indent:
                pos += 1
            continue

        # target "name" ssh user@host:  OR  target "name" local:
        if text.startswith("target "):
            node = _parse_cgr_target(lines, pos, err)
            nodes.append(node)
            # skip past this target block
            advance()  # target line
            target_indent = indent
            while pos < len(lines) and lines[pos][1] > target_indent:
                pos += 1
            continue

        # each name, addr in ${hosts}:  (top-level target loop)
        each_m = re.match(r'each\s+(\w+)(?:\s*,\s*(\w+))?\s+in\s+(\$\{[^}]+\})\s*:\s*$', text)
        if each_m:
            each_vars = [each_m.group(1)]
            if each_m.group(2): each_vars.append(each_m.group(2))
            each_list_expr = each_m.group(3)
            each_base_indent = indent
            each_line = ln
            advance()  # each line

            # Collect the body lines of the each block (indented under it)
            each_body_lines = []
            while pos < len(lines) and lines[pos][1] > each_base_indent:
                each_body_lines.append(lines[pos])
                pos += 1

            # Reconstruct body source for deferred expansion in resolve()
            body_source_parts = []
            for bln, bindent, btext in each_body_lines:
                body_source_parts.append(("  " * bindent if bindent else "") + btext)
            body_source = "\n".join(body_source_parts)

            target_each_blocks.append(ASTTargetEach(
                var_names=each_vars, list_expr=each_list_expr,
                body_source=body_source, body_format="cgr",
                line=each_line))
            continue

        # on complete: / on failure: hooks
        if text.startswith("on "):
            hook_m = re.match(r'on\s+(complete|failure)\s*:', text)
            if hook_m:
                hook_type = hook_m.group(1)
                hook_indent = indent
                advance()
                hook_body = []
                while pos < len(lines) and lines[pos][1] > hook_indent:
                    hook_body.append(lines[pos]); advance()
                hook_res = _parse_cgr_step(f"__on_{hook_type}", "", hook_body, ln, err)
                if hook_type == "complete":
                    on_complete_hook = hook_res
                else:
                    on_failure_hook = hook_res
                continue

        raise err(f"Expected 'set', 'using', 'inventory', 'secrets', 'gather facts', 'target', 'template', 'each', 'on complete', 'on failure', or '---', got: {text[:30]}", ln, text)

    return ASTProgram(variables=variables, uses=uses, templates=templates, nodes=nodes,
                      target_each_blocks=target_each_blocks, inventories=inventories,
                      secrets=secrets_list, gather_facts=gather_facts, stateless=stateless,
                      on_complete=on_complete_hook if 'on_complete_hook' in dir() else None,
                      on_failure=on_failure_hook if 'on_failure_hook' in dir() else None)


def _parse_cgr_template(lines, start_pos, err, filename="") -> ASTTemplate:
    """Parse a template block in .cgr format into an ASTTemplate."""
    ln, base_indent, text = lines[start_pos]

    # Parse: template name(param1, param2 = "default"):
    m = re.match(r'template\s+(\w+)\s*\(([^)]*)\)\s*:\s*$', text)
    if not m:
        raise err(f"Invalid template header. Expected: template name(params):", ln, text)
    tpl_name = m.group(1)
    params_str = m.group(2).strip()

    # Parse parameters
    params: list[ASTParam] = []
    if params_str:
        for part in params_str.split(","):
            part = part.strip()
            pm = re.match(r'(\w+)\s*=\s*"([^"]*)"', part)
            if pm:
                params.append(ASTParam(name=pm.group(1), default=pm.group(2), line=ln))
            else:
                params.append(ASTParam(name=part.strip(), default=None, line=ln))

    # Collect body lines
    body_lines = []
    p = start_pos + 1
    while p < len(lines) and lines[p][1] > base_indent:
        body_lines.append(lines[p]); p += 1

    # Parse the template body as a step (the template root is a resource)
    # Extract description and version if present
    description = ""
    tpl_version = None
    root_body = []
    for bln, bindent, btext in body_lines:
        desc_m = re.match(r'description\s+"([^"]*)"', btext)
        ver_m = re.match(r'version\s+(\d+\.\d+(?:\.\d+)?)\s*$', btext)
        if desc_m:
            description = desc_m.group(1)
        elif ver_m:
            tpl_version = ver_m.group(1)
        else:
            root_body.append((bln, bindent, btext))

    # Parse the body using the existing step parser
    body_res = _parse_cgr_step(tpl_name, "", root_body, ln, err, allow_no_run_with_children=True)
    if description:
        body_res.description = description

    return ASTTemplate(
        name=tpl_name, params=params, description=description,
        body=body_res, source_file=filename, line=ln,
        version=tpl_version)


def _parse_cgr_template_child_instantiation(
    child_name: str,
    tpl_path: str,
    child_body: list,
    ln: int,
) -> ASTResource:
    """Parse a nested template composition step inside a template body."""
    needs: list[str] = []
    args: dict[str, str] = {}

    for _, _, btext in child_body:
        if btext.startswith("needs ") or btext.startswith("first "):
            refs = re.findall(r'\[([^\]]+)\]', btext)
            for ref in refs:
                needs.append(_parse_cgr_dep_ref(ref))
            continue

        am = re.match(r'(\w+)\s*=\s*"([^"]*)"', btext)
        if am:
            args[am.group(1)] = am.group(2)
            continue

        am = re.match(r'(\w+)\s*=\s*(\S+)', btext)
        if am:
            args[am.group(1)] = am.group(2)

    return ASTResource(
        name=_slug(child_name), description=child_name, needs=needs,
        check=None, run=None, run_as=None, timeout=None,
        retries=0, retry_delay=5, retry_backoff=False, on_fail="stop",
        when=None, env={}, is_verify=False, line=ln,
        timeout_reset_on_output=False,
        inner_template_name=tpl_path.split("/")[-1],
        inner_template_args=args,
    )


def _parse_target_body_items(body_lines, err, phase_name=None, phase_when=None):
    """Parse a list of body lines (from a target or a phase block) into resources + instantiations.

    phase_name / phase_when: if set, inject cgr_phase_name and when into every parsed resource.
    Raises an error if a nested phase "..." when "...": block is found inside a phase block.
    """
    resources: list[ASTResource] = []
    instantiations: list[ASTInstantiation] = []
    i = 0

    while i < len(body_lines):
        bln, bindent, btext = body_lines[i]

        # phase "name" when "COND":  — conditional group block (not nestable)
        phase_m = re.match(r'phase\s+"([^"]+)"\s+when\s+"([^"]+)":\s*$', btext)
        if phase_m:
            if phase_name is not None:
                raise err(f"phase blocks cannot be nested (inside phase '{phase_name}')", bln, btext)
            pname = phase_m.group(1)
            pwhen = phase_m.group(2)
            phase_body = []
            j = i + 1
            while j < len(body_lines) and body_lines[j][1] > bindent:
                phase_body.append(body_lines[j]); j += 1
            sub_resources, sub_insts = _parse_target_body_items(phase_body, err,
                                                                 phase_name=pname, phase_when=pwhen)
            resources.extend(sub_resources)
            instantiations.extend(sub_insts)
            i = j; continue

        # [step name] ... :  or  [step name]: run $ ...
        step_spec = _parse_cgr_step_line(btext)
        if step_spec:
            step_name, step_header, inline_step = step_spec
            # Collect body lines for this step
            step_body = []
            j = i + 1
            while j < len(body_lines) and body_lines[j][1] > bindent:
                step_body.append(body_lines[j]); j += 1
            if inline_step is not None:
                step_body = [(bln, bindent + 2, inline_step)] + step_body

            # Check if it's a template step (has "from")
            from_m = re.search(r'from\s+([\w/]+)', step_header)
            if from_m:
                # Template instantiation
                tpl_path = from_m.group(1)
                tpl_alias = tpl_path.split("/")[-1]
                args = {}
                for sln, sind, stxt in step_body:
                    am = re.match(r'(\w+)\s*=\s*"([^"]*)"', stxt)
                    if am: args[am.group(1)] = am.group(2)
                    else:
                        am = re.match(r'(\w+)\s*=\s*(\S+)', stxt)
                        if am: args[am.group(1)] = am.group(2)
                inst = ASTInstantiation(template_name=tpl_alias, args=args, line=bln)
                inst._cgr_human_name = step_name
                instantiations.append(inst)
            else:
                # Inline resource — inject phase attributes
                res = _parse_cgr_step(step_name, step_header, step_body, bln, err)
                if phase_name is not None:
                    res.cgr_phase_name = phase_name
                    if res.when is None:
                        res.when = phase_when
                resources.append(res)
            i = j; continue

        # Common typo: step header missing trailing colon.
        if re.match(r'\[[^\]]+\]', btext):
            raise err("Invalid step header. Expected '[step name]:'", bln, btext)

        # verify "description":
        verify_m = re.match(r'verify\s+"([^"]+)":\s*$', btext)
        if verify_m:
            vdesc = verify_m.group(1)
            vbody = []
            j = i + 1
            while j < len(body_lines) and body_lines[j][1] > bindent:
                vbody.append(body_lines[j]); j += 1
            res = _parse_cgr_verify(vdesc, vbody, bln, err)
            if phase_name is not None:
                res.cgr_phase_name = phase_name
                if res.when is None:
                    res.when = phase_when
            resources.append(res)
            i = j; continue

        # stage "name": at target level — wraps in a synthetic step
        stage_tm = re.match(r'stage\s+"([^"]+)"\s*:\s*$', btext)
        if stage_tm:
            stage_body = []
            j = i + 1
            while j < len(body_lines) and body_lines[j][1] > bindent:
                stage_body.append(body_lines[j]); j += 1
            sname = stage_tm.group(1)
            res = _parse_cgr_step(sname, "", [(bln, bindent, btext)] + stage_body, bln, err)
            if phase_name is not None:
                res.cgr_phase_name = phase_name
                if res.when is None:
                    res.when = phase_when
            resources.append(res)
            i = j; continue

        i += 1  # skip unrecognised lines

    return resources, instantiations


def _parse_cgr_target(lines, start_pos, err) -> ASTNode:
    """Parse a target block and all its steps."""
    ln, base_indent, text = lines[start_pos]

    # Parse: target "name" ssh user@host port N:  or  target "name" local:
    # Optionally: target "name" ssh user@host, after "dep1", "dep2":
    m = re.match(r'target\s+"([^"]+)"\s+(.*?):\s*$', text)
    if not m:
        raise err(f"Invalid target line. Expected: target \"name\" ssh user@host:", ln, text)
    name = m.group(1)
    rest = m.group(2).strip()

    # Extract after "node" clauses from rest
    after_nodes = []
    after_m = re.search(r',\s*after\s+(.+)$', rest)
    if after_m:
        after_text = after_m.group(1)
        # Check for "after each" — sentinel expanded by resolver
        if re.search(r'\beach\b', after_text):
            after_nodes.append("__each__")
            # Also pick up any explicit quoted names alongside "each"
            after_nodes.extend(re.findall(r'"([^"]+)"', after_text))
        else:
            after_nodes = re.findall(r'"([^"]+)"', after_text)
        rest = rest[:after_m.start()].strip()

    via_text = rest

    # Parse via clause
    if via_text.startswith("ssh"):
        via_rest = via_text[3:].strip()
        props = {}
        # ssh user@host:port  or  ssh user@host port N
        sm = re.match(r'([\w${}\-]+)@([\w.${\}\-]+)(?::(\d+))?(?:\s+port\s+(\d+))?', via_rest)
        if sm:
            props["user"] = sm.group(1)
            props["host"] = sm.group(2)
            if sm.group(3): props["port"] = sm.group(3)      # user@host:port
            elif sm.group(4): props["port"] = sm.group(4)    # user@host port N
        else:
            # ssh host
            sm = re.match(r'([\w.\-]+)', via_rest)
            if sm: props["host"] = sm.group(1)
        via = ASTVia(method="ssh", props=props, line=ln)
    elif via_text.startswith("local"):
        via = ASTVia(method="local", props={}, line=ln)
    else:
        raise err(f"Expected 'ssh' or 'local' in target line", ln, text)

    # Collect all lines belonging to this target
    body_lines = []
    p = start_pos + 1
    while p < len(lines) and lines[p][1] > base_indent:
        body_lines.append(lines[p]); p += 1

    resources, instantiations = _parse_target_body_items(body_lines, err)
    return ASTNode(name=name, via=via, resources=resources, groups=[],
                   instantiations=instantiations, line=ln,
                   after_nodes=after_nodes)


def _parse_cgr_header_props(header: str) -> dict:
    """Parse step header properties: as root, timeout 2m, retry 2x wait 10s, if fails warn"""
    props: dict[str, Any] = {}
    # as USER
    m = re.search(r'\bas\s+(\w+)', header)
    if m: props["run_as"] = m.group(1)
    # timeout Ns, Nm, or Nh
    timeout_prop = _parse_timeout_text(header)
    if timeout_prop is not None:
        props["timeout"], props["timeout_reset_on_output"] = timeout_prop
    # retry Nx wait Ns / retry Nx every Ns / retry Nx with backoff
    m = re.search(r'\bretry\s+(\d+)x(?:\s+(?:wait|every)\s+(\d+)(s|m|h)?)?(?:\s+with\s+backoff)?', header)
    if m:
        props["retries"] = int(m.group(1))
        if m.group(2):
            d = _duration_to_secs(int(m.group(2)), m.group(3))
            props["retry_delay"] = d
        if "with backoff" in header:
            props["retry_backoff"] = True
            if "retry_delay" not in props:
                props["retry_delay"] = 1  # default base delay for backoff
    # if fails stop|warn|ignore
    m = re.search(r'\bif\s+fails?\s+(stop|warn|ignore)', header)
    if m: props["on_fail"] = m.group(1)
    return props


_CGR_CONTINUATION_STOPWORDS = (
    "needs ", "first ", "skip if ", "run ", "script ", "always run ", "env ",
    "when ", "collect ", "flag ", "until ", "parallel", "race", "each ", "stage ",
    "as ", "timeout ", "retry ", "if fails", "wait for ",
    "content ", "line ", "put ", "validate ",
    "block ", "ini ", "json ", "assert ",
    "on success:", "on success :", "on failure:", "on failure :",
    "reduce ",
    "run:", "always run:", "skip if:",
)
_CGR_STEP_RE = re.compile(r'\[[^\]]+\].*:\s*$')

def _is_cgr_keyword(s: str) -> bool:
    """Return True if a continuation line should stop multi-line collection.
    Stops on step keywords AND on actual child-step markers [name]: but NOT
    on shell test expressions like [ -f "..." ] which don't end with ':'."""
    if s.startswith(_CGR_CONTINUATION_STOPWORDS):
        return True
    # [step name]: pattern — ends with ':' after the closing ']'
    if s.startswith("[") and _CGR_STEP_RE.match(s):
        return True
    return False


_CGR_SHELL_CONTROL_WORD_RE = re.compile(
    r"^\s*(if|then|else|elif\b.*|fi|for\b.*|while\b.*|until\b.*|do|done|case\b.*|esac|select\b.*|\{|\})\s*(?:#.*)?$"
)


def _cgr_command_summary(command: str) -> str:
    summary = " ".join(command.split())
    if len(summary) > 240:
        summary = summary[:237] + "..."
    return summary or "(empty command)"


def _can_instrument_cgr_command_block(commands: list[str]) -> bool:
    """Only instrument simple command lists; avoid changing shell compound syntax."""
    for command in commands:
        if "<<" in command:
            return False
        for line in command.splitlines():
            if _CGR_SHELL_CONTROL_WORD_RE.match(line):
                return False
    return True


def _instrument_cgr_command_block(commands: list[str]) -> str:
    preamble = [
        "__cgr_cmd='before first command'",
        "__cgr_line=0",
        "trap '__cgr_rc=$?; if [ \"$__cgr_rc\" -ne 0 ]; then printf \"%s\\n\" \"CommandGraph: run block failed at command $__cgr_line: $__cgr_cmd\" >&2; fi' EXIT",
        "set -e",
    ]
    body: list[str] = []
    for idx, command in enumerate(commands, 1):
        body.append(f"__cgr_line={idx}; __cgr_cmd={shlex.quote(_cgr_command_summary(command))}")
        body.append(command)
    return "\n".join(preamble + body)


def _join_cgr_command_block(lines: list[str]) -> str:
    """Join run: block lines into a fail-fast script, preserving shell continuations."""
    commands: list[str] = []
    current: list[str] = []
    for line in lines:
        current.append(line)
        if line.endswith("\\"):
            continue
        commands.append("\n".join(current))
        current = []
    if current:
        commands.append("\n".join(current))
    if _can_instrument_cgr_command_block(commands):
        return _instrument_cgr_command_block(commands)
    return "set -e\n" + "\n".join(commands)


def _parse_cgr_step_line(text: str):
    """Parse either a block step header or a one-line step body."""
    block_m = re.match(r'\[([^\]]+)\](.*):\s*$', text)
    if block_m:
        return block_m.group(1), block_m.group(2).strip(), None
    inline_m = re.match(r'\[([^\]]+)\]\s*:\s*(.+)\s*$', text)
    if inline_m:
        return inline_m.group(1), "", inline_m.group(2).strip()
    return None


def _parse_cgr_stringish(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        return text[1:-1]
    return text


def _parse_cgr_from_target(header: str) -> str | None:
    m = re.search(r'\bfrom\s+("([^"]+)"|\'([^\']+)\'|(\S+))', header)
    if not m:
        return None
    return m.group(2) or m.group(3) or m.group(4)


def _parse_cgr_step(name: str, header: str, body: list, ln: int, err,
                    allow_no_run_with_children: bool = False) -> ASTResource:
    """Parse a [step name] block into an ASTResource."""
    hp = _parse_cgr_header_props(header)
    from_target = _parse_cgr_from_target(header)
    slug = _slug(name)
    needs: list[str] = []
    check: str | None = None
    run_cmd: str | None = None
    script_path: str | None = None
    env: dict[str, str] = {}
    env_when: dict[str, str] = {}
    when: str | None = None
    collect_key: str | None = None
    collect_format: str | None = None
    tags: list[str] = []
    children: list[ASTResource] = []
    always_run = False
    # HTTP request fields
    http_method: str|None = None; http_url: str|None = None
    http_headers: list[tuple[str,str]] = []; http_body: str|None = None
    http_body_type: str|None = None; http_auth: tuple|None = None
    http_expect: str|None = None
    wait_kind: str|None = None
    wait_target: str|None = None
    subgraph_path: str|None = None
    subgraph_vars: dict[str, str] = {}
    parallel_block: list[ASTResource] = []
    parallel_limit: int|None = None
    parallel_fail_policy: str = "wait"
    race_block: list[ASTResource] = []
    race_into: str|None = None
    each_var: str|None = None; each_list_expr: str|None = None
    each_limit: int|str|None = None; each_body: ASTResource|None = None
    stage_name: str|None = None; stage_phases: list = []
    # Provisioning fields (Phase 1)
    prov_content_inline: str|None = None; prov_content_from: str|None = None
    prov_dest: str|None = None; prov_lines: list[tuple] = []
    prov_put_src: str|None = None; prov_put_mode: str|None = None
    prov_put_transfer: str|None = None; prov_put_transfer_opts: str|None = None
    prov_validate: str|None = None
    # New feature fields
    retry_backoff_max: int = 0
    retry_jitter_pct: int = 0
    on_success_set: list[tuple] = []
    on_failure_set: list[tuple] = []
    collect_var: str|None = None
    reduce_key: str|None = None
    reduce_var: str|None = None
    flags: list[tuple[str, str|None]] = []
    until: str|None = None
    interactive: bool = False
    # Provisioning fields (Phase 2)
    prov_block_dest: str|None = None; prov_block_marker: str|None = None
    prov_block_inline: str|None = None; prov_block_from: str|None = None
    prov_block_absent: bool = False
    prov_ini_dest: str|None = None; prov_ini_ops: list[tuple] = []
    prov_json_dest: str|None = None; prov_json_ops: list[tuple] = []
    prov_asserts: list[tuple] = []

    if from_target and from_target.endswith((".cgr", ".cg")):
        subgraph_path = from_target

    i = 0
    while i < len(body):
        bln, bindent, btext = body[i]

        # Nested step: [child name] ... :  or  [child name]: run $ ...
        child_spec = _parse_cgr_step_line(btext)
        if child_spec:
            child_name, child_header, inline_child = child_spec
            child_body = []
            j = i + 1
            while j < len(body) and body[j][1] > bindent:
                child_body.append(body[j]); j += 1
            if inline_child is not None:
                child_body = [(bln, bindent + 2, inline_child)] + child_body
            from_m = _parse_cgr_from_target(child_header)
            if from_m:
                if from_m.endswith((".cgr", ".cg")):
                    child = _parse_cgr_step(child_name, child_header, child_body, bln, err)
                else:
                    child = _parse_cgr_template_child_instantiation(
                        child_name, from_m, child_body, bln)
            else:
                child = _parse_cgr_step(child_name, child_header, child_body, bln, err)
            children.append(child)
            i = j; continue

        # needs [name], [name]  (also accepts 'first' as primary keyword)
        # Cross-node: first [node/step name]  →  node.slugged_step_name
        if btext.startswith("needs ") or btext.startswith("first "):
            refs = re.findall(r'\[([^\]]+)\]', btext)
            for ref in refs:
                needs.append(_parse_cgr_dep_ref(ref))
            i += 1; continue

        # skip if:  (multiline block form — lines joined with ' && ')
        if re.match(r'skip if\s*:\s*$', btext):
            j = i + 1
            cond_lines = []
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                cond_lines.append(body[j][2]); j += 1
            if not cond_lines:
                raise err(f"Step [{name}] has an empty 'skip if:' block", ln)
            check = " && ".join(cond_lines)
            i = j; continue

        # skip if $ command  (supports multi-line continuation via indentation)
        if btext.startswith("skip if "):
            cmd = btext[8:].strip()
            if cmd.startswith("$ "): cmd = cmd[2:]
            check = cmd
            j = i + 1
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                check += "\n" + body[j][2]; j += 1
            i = j; continue

        # always run:  (multiline block form — lines joined with ' && ', fail-fast)
        if re.match(r'always run\s*:\s*$', btext):
            j = i + 1
            cmd_lines = []
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                cmd_lines.append(body[j][2]); j += 1
            if not cmd_lines:
                raise err(f"Step [{name}] has an empty 'always run:' block", ln)
            run_cmd = _join_cgr_command_block(cmd_lines); always_run = True
            i = j; continue

        # always run $ command
        if btext.startswith("always run "):
            cmd = btext[11:].strip()
            if cmd.startswith("$ "): cmd = cmd[2:]
            run_cmd = cmd; always_run = True
            j = i + 1
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                run_cmd += "\n" + body[j][2]; j += 1
            i = j; continue

        # HTTP verbs: get/post/put/patch/delete "URL"
        http_m = re.match(r'(get|post|put|patch|delete)\s+"([^"]*)"', btext, re.IGNORECASE)
        if http_m:
            http_method = http_m.group(1).upper()
            http_url = http_m.group(2)
            i += 1; continue

        # HTTP auth: auth bearer "TOKEN" / auth basic "USER" "PASS"
        auth_bearer_m = re.match(r'auth\s+bearer\s+"([^"]*)"', btext)
        if auth_bearer_m:
            http_auth = ("bearer", auth_bearer_m.group(1))
            i += 1; continue
        auth_basic_m = re.match(r'auth\s+basic\s+"([^"]*)"\s+"([^"]*)"', btext)
        if auth_basic_m:
            http_auth = ("basic", f"{auth_basic_m.group(1)}:{auth_basic_m.group(2)}")
            i += 1; continue
        auth_header_m = re.match(r'auth\s+header\s+"([^"]*)"\s*=\s*"([^"]*)"', btext)
        if auth_header_m:
            http_auth = ("header", f"{auth_header_m.group(1)}:{auth_header_m.group(2)}")
            i += 1; continue

        # HTTP header: header "Name" = "Value"
        hdr_m = re.match(r'header\s+"([^"]*)"\s*=\s*"([^"]*)"', btext)
        if hdr_m:
            http_headers.append((hdr_m.group(1), hdr_m.group(2)))
            i += 1; continue

        # HTTP body: body json '...' / body json "..." / body form "..."
        body_m = re.match(r"body\s+(json|form)\s+['\"](.+)['\"]$", btext, re.DOTALL)
        if body_m:
            http_body_type = body_m.group(1)
            http_body = body_m.group(2)
            # Multi-line body: collect indented continuation lines
            j = i + 1
            while j < len(body) and body[j][1] > bindent:
                http_body += "\n" + body[j][2]
                j += 1
            # Strip trailing quote if body was multi-line
            http_body = http_body.rstrip("'\"")
            i = j; continue

        # HTTP expect: expect 200 / expect 200..299 / expect 200, 201, 204
        expect_m = re.match(r'expect\s+(.+)', btext)
        if expect_m:
            http_expect = expect_m.group(1).strip()
            i += 1; continue

        wait_webhook_m = re.match(r"wait\s+for\s+webhook\s+(['\"])(.+)\1$", btext)
        if wait_webhook_m:
            wait_kind = "webhook"
            wait_target = wait_webhook_m.group(2)
            i += 1; continue

        wait_file_m = re.match(r"wait\s+for\s+file\s+(['\"])(.+)\1$", btext)
        if wait_file_m:
            wait_kind = "file"
            wait_target = wait_file_m.group(2)
            i += 1; continue

        # run:  (multiline block form — lines joined with ' && ', fail-fast)
        if re.match(r'run\s*:\s*$', btext):
            j = i + 1
            cmd_lines = []
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                cmd_lines.append(body[j][2]); j += 1
            if not cmd_lines:
                raise err(f"Step [{name}] has an empty 'run:' block", ln)
            run_cmd = _join_cgr_command_block(cmd_lines)
            i = j; continue

        # run $ command
        if btext.startswith("run "):
            cmd = btext[4:].strip()
            if cmd.startswith("$ "): cmd = cmd[2:]
            run_cmd = cmd
            j = i + 1
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                run_cmd += "\n" + body[j][2]; j += 1
            i = j; continue

        # script "./path/to/script.sh" or script $./path/to/script.sh
        if btext.startswith("script "):
            script_spec = btext[7:].strip()
            if script_spec.startswith("$"):
                script_spec = script_spec[1:].strip()
            if len(script_spec) >= 2 and script_spec[0] == '"' and script_spec[-1] == '"':
                script_path = script_spec[1:-1]
            else:
                script_path = script_spec
            i += 1; continue

        # ── Parallel constructs ────────────────────────────────────────

        # parallel: / parallel N at a time: / parallel, if one fails stop all:
        par_m = re.match(r'parallel(?:\s+(\d+)\s+at a time)?(?:,\s*if one fails?\s+(wait for rest|stop all|ignore))?\s*:\s*$', btext)
        if par_m:
            if par_m.group(1): parallel_limit = int(par_m.group(1))
            if par_m.group(2):
                fp = par_m.group(2)
                if "stop" in fp: parallel_fail_policy = "stop"
                elif "ignore" in fp: parallel_fail_policy = "ignore"
            # Collect children of the parallel block
            j = i + 1
            par_body = []
            while j < len(body) and body[j][1] > bindent:
                par_body.append(body[j]); j += 1
            # Parse each child step inside the parallel block
            ki = 0
            while ki < len(par_body):
                pln, pind, ptxt = par_body[ki]
                child_spec = _parse_cgr_step_line(ptxt)
                if child_spec:
                    cname, cheader, inline_child = child_spec
                    cbody = []
                    kj = ki + 1
                    while kj < len(par_body) and par_body[kj][1] > pind:
                        cbody.append(par_body[kj]); kj += 1
                    if inline_child is not None:
                        cbody = [(pln, pind + 2, inline_child)] + cbody
                    parallel_block.append(_parse_cgr_step(cname, cheader, cbody, pln, err))
                    ki = kj
                else:
                    ki += 1
            i = j; continue

        # race:  or  race into EXPR:
        race_m = re.match(r'race(?:\s+into\s+(\S+))?\s*:\s*$', btext.strip())
        if race_m:
            race_into = race_m.group(1) or None  # e.g. "${dest}" or "/tmp/out.tar.gz"
            j = i + 1
            race_body = []
            while j < len(body) and body[j][1] > bindent:
                race_body.append(body[j]); j += 1
            ki = 0
            while ki < len(race_body):
                pln, pind, ptxt = race_body[ki]
                child_spec = _parse_cgr_step_line(ptxt)
                if child_spec:
                    cname, cheader, inline_child = child_spec
                    cbody = []
                    kj = ki + 1
                    while kj < len(race_body) and race_body[kj][1] > pind:
                        cbody.append(race_body[kj]); kj += 1
                    if inline_child is not None:
                        cbody = [(pln, pind + 2, inline_child)] + cbody
                    race_block.append(_parse_cgr_step(cname, cheader, cbody, pln, err))
                    ki = kj
                else:
                    ki += 1
            i = j; continue

        # each VAR in ${LIST}, N at a time:   (N may be a literal int or a ${variable})
        each_m = re.match(r'each\s+(\w+)\s+in\s+(\$\{\w+\})(?:,\s*(\$\{\w+\}|\d+)\s+at a time)?\s*:\s*$', btext)
        if each_m:
            each_var = each_m.group(1)
            each_list_expr = each_m.group(2)
            if each_m.group(3):
                g3 = each_m.group(3)
                each_limit = int(g3) if g3.isdigit() else g3
            j = i + 1
            each_body_lines = []
            while j < len(body) and body[j][1] > bindent:
                each_body_lines.append(body[j]); j += 1
            # Parse the body as a single step template (with ${var} placeholders)
            if each_body_lines:
                ki = 0
                while ki < len(each_body_lines):
                    pln, pind, ptxt = each_body_lines[ki]
                    child_spec = _parse_cgr_step_line(ptxt)
                    if child_spec:
                        cname, cheader, inline_child = child_spec
                        cbody = []
                        kj = ki + 1
                        while kj < len(each_body_lines) and each_body_lines[kj][1] > pind:
                            cbody.append(each_body_lines[kj]); kj += 1
                        if inline_child is not None:
                            cbody = [(pln, pind + 2, inline_child)] + cbody
                        each_body = _parse_cgr_step(cname, cheader, cbody, pln, err)
                        ki = kj
                    else:
                        ki += 1
            i = j; continue

        # stage "name":
        stage_m = re.match(r'stage\s+"([^"]+)"\s*:\s*$', btext)
        if stage_m:
            stage_name = stage_m.group(1)
            j = i + 1
            stage_body = []
            while j < len(body) and body[j][1] > bindent:
                stage_body.append(body[j]); j += 1
            # Parse phase blocks within the stage
            ki = 0
            while ki < len(stage_body):
                sln, sind, stxt = stage_body[ki]
                # first [...] at stage level
                if stxt.startswith("first ") or stxt.startswith("needs "):
                    refs = re.findall(r'\[([^\]]+)\]', stxt)
                    needs.extend(_slug(r) for r in refs)
                    ki += 1; continue
                # phase "name" N from ${list}: / phase "name" N% from ${list}: / phase "name" rest from ${list}:
                phase_m = re.match(r'phase\s+"([^"]+)"\s+([\w%]+)\s+from\s+(\$\{\w+\})\s*:\s*$', stxt)
                if phase_m:
                    pname = phase_m.group(1); count_expr = phase_m.group(2)
                    from_list = phase_m.group(3)
                    pbody_lines = []
                    kj = ki + 1
                    while kj < len(stage_body) and stage_body[kj][1] > sind:
                        pbody_lines.append(stage_body[kj]); kj += 1
                    # Parse phase body resources
                    phase_resources = []
                    phase_each_limit = None
                    pi = 0
                    while pi < len(pbody_lines):
                        ppln, ppind, pptxt = pbody_lines[pi]
                        # each inside phase
                        pe_m = re.match(r'each\s+\w+(?:,\s*(\$\{\w+\}|\d+)\s+at a time)?\s*:\s*$', pptxt)
                        if pe_m:
                            if pe_m.group(1):
                                g1 = pe_m.group(1)
                                phase_each_limit = int(g1) if g1.isdigit() else g1
                            pj = pi + 1
                            while pj < len(pbody_lines) and pbody_lines[pj][1] > ppind:
                                ppln2, ppind2, pptxt2 = pbody_lines[pj]
                                child_spec = _parse_cgr_step_line(pptxt2)
                                if child_spec:
                                    cname, cheader, inline_child = child_spec
                                    cbody = []
                                    pk = pj + 1
                                    while pk < len(pbody_lines) and pbody_lines[pk][1] > ppind2:
                                        cbody.append(pbody_lines[pk]); pk += 1
                                    if inline_child is not None:
                                        cbody = [(ppln2, ppind2 + 2, inline_child)] + cbody
                                    phase_resources.append(_parse_cgr_step(cname, cheader, cbody, ppln2, err))
                                    pj = pk
                                else:
                                    pj += 1
                            pi = pj; continue
                        # Direct step inside phase
                        child_spec = _parse_cgr_step_line(pptxt)
                        if child_spec:
                            cname, cheader, inline_child = child_spec
                            cbody = []
                            pj = pi + 1
                            while pj < len(pbody_lines) and pbody_lines[pj][1] > ppind:
                                cbody.append(pbody_lines[pj]); pj += 1
                            if inline_child is not None:
                                cbody = [(ppln, ppind + 2, inline_child)] + cbody
                            phase_resources.append(_parse_cgr_step(cname, cheader, cbody, ppln, err))
                            pi = pj; continue
                        # Verify inside phase
                        vm = re.match(r'verify\s+"([^"]+)"\s*:\s*$', pptxt)
                        if vm:
                            vbody = []
                            pj = pi + 1
                            while pj < len(pbody_lines) and pbody_lines[pj][1] > ppind:
                                vbody.append(pbody_lines[pj]); pj += 1
                            phase_resources.append(_parse_cgr_verify(vm.group(1), vbody, ppln, err))
                            pi = pj; continue
                        pi += 1
                    stage_phases.append(ASTPhase(name=pname, count_expr=count_expr,
                        from_list=from_list, body=phase_resources,
                        each_limit=phase_each_limit, line=sln))
                    ki = kj; continue
                ki += 1
            i = j; continue

        # env KEY = "value"
        if btext.startswith("env "):
            em = re.match(r'env\s+(\w+)\s*=\s*"([^"]*)"(?:\s+when\s+(.+))?$', btext)
            if em:
                env[em.group(1)] = em.group(2)
                if em.group(3):
                    env_when[em.group(1)] = em.group(3).strip().strip('"')
            i += 1; continue

        if subgraph_path:
            assign_m = re.match(r'(\w+)\s*=\s*(.+)$', btext)
            if assign_m:
                subgraph_vars[assign_m.group(1)] = _parse_cgr_stringish(assign_m.group(2))
                i += 1; continue

        if btext.startswith("flag "):
            fm = re.match(r'flag\s+"([^"]*)"(?:\s+when\s+(.+))?$', btext)
            if fm:
                flag_when = fm.group(2).strip() if fm.group(2) else None
                if flag_when:
                    flag_when = flag_when.strip('"')
                flags.append((fm.group(1), flag_when))
            i += 1; continue

        # ── Provisioning operations ────────────────────────────────────────

        # content from FILE > DEST  (single-line; reads file, interpolates vars)
        cont_from_m = re.match(r'content\s+from\s+(\S+)\s*>\s*(\S+)\s*$', btext)
        if cont_from_m:
            prov_content_from = cont_from_m.group(1)
            prov_dest = cont_from_m.group(2)
            i += 1; continue

        # content > DEST:  (multi-line inline body)
        cont_inline_m = re.match(r'content\s*>\s*(\S+)\s*:\s*$', btext)
        if cont_inline_m:
            prov_dest = cont_inline_m.group(1)
            j = i + 1
            content_lines = []
            while j < len(body) and body[j][1] > bindent:
                content_lines.append(body[j][2])
                j += 1
            prov_content_inline = "\n".join(content_lines)
            i = j; continue

        # line variants
        # line "TEXT" in FILE, replacing "REGEX"
        line_replace_m = re.match(r'line\s+"([^"]*)"\s+in\s+(\S+)\s*,\s*replacing\s+"([^"]*)"', btext)
        if line_replace_m:
            dest_file = line_replace_m.group(2)
            if prov_dest and prov_dest != dest_file:
                raise err(f"All 'line' keywords in one step must target the same file (got '{dest_file}' and '{prov_dest}')", bln)
            prov_dest = dest_file
            prov_lines.append(("replace", line_replace_m.group(1), line_replace_m.group(3)))
            i += 1; continue
        # line absent "TEXT" in FILE
        line_absent_m = re.match(r'line\s+absent\s+"([^"]*)"\s+in\s+(\S+)\s*$', btext)
        if line_absent_m:
            dest_file = line_absent_m.group(2)
            if prov_dest and prov_dest != dest_file:
                raise err(f"All 'line' keywords in one step must target the same file (got '{dest_file}' and '{prov_dest}')", bln)
            prov_dest = dest_file
            prov_lines.append(("absent", line_absent_m.group(1), None))
            i += 1; continue
        # line "TEXT" in FILE
        line_present_m = re.match(r'line\s+"([^"]*)"\s+in\s+(\S+)\s*$', btext)
        if line_present_m:
            dest_file = line_present_m.group(2)
            if prov_dest and prov_dest != dest_file:
                raise err(f"All 'line' keywords in one step must target the same file (got '{dest_file}' and '{prov_dest}')", bln)
            prov_dest = dest_file
            prov_lines.append(("present", line_present_m.group(1), None))
            i += 1; continue

        # put SRC > DEST [, mode XXXX] [, transfer rsync|scp] [, transfer_opts "FLAGS"]
        put_m = re.match(r'put\s+(\S+)\s*>\s*(\S+)\s*(,.*)?$', btext)
        if put_m:
            prov_put_src = put_m.group(1)
            prov_dest = put_m.group(2).rstrip(',')
            prov_put_mode = None; prov_put_transfer = None; prov_put_transfer_opts = None
            opts_tail = put_m.group(3)
            if opts_tail:
                for part in re.split(r',\s*', opts_tail.strip()):
                    part = part.strip()
                    if not part: continue
                    kv = re.match(r'(\w+)\s+("(?:[^"]*)"|(\S+))', part)
                    if not kv: raise err(f"put: malformed option '{part}'", bln)
                    key = kv.group(1)
                    val = kv.group(2).strip('"')
                    if key == "mode": prov_put_mode = val
                    elif key == "transfer":
                        if val not in ("rsync", "scp"):
                            raise err(f"put: transfer must be 'rsync' or 'scp', got '{val}'", bln)
                        prov_put_transfer = val
                    elif key == "transfer_opts": prov_put_transfer_opts = val
                    else: raise err(f"put: unknown option '{key}'", bln)
            i += 1; continue

        # validate $ COMMAND  (multi-line continuation supported)
        if btext.startswith("validate "):
            vcmd = btext[9:].strip()
            if vcmd.startswith("$ "): vcmd = vcmd[2:]
            prov_validate = vcmd
            j = i + 1
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                prov_validate += "\n" + body[j][2]; j += 1
            i = j; continue

        # ── Phase 2 structured config operations ──────────────────────────

        # block absent in FILE, marker "MARKER"
        block_absent_m = re.match(r'block\s+absent\s+in\s+(\S+)\s*,\s*marker\s+"([^"]*)"', btext)
        if block_absent_m:
            prov_block_dest = block_absent_m.group(1)
            prov_block_marker = block_absent_m.group(2)
            prov_block_absent = True
            i += 1; continue

        # block from FILE in DEST, marker "MARKER"
        block_from_m = re.match(r'block\s+from\s+(\S+)\s+in\s+(\S+)\s*,\s*marker\s+"([^"]*)"', btext)
        if block_from_m:
            prov_block_from = block_from_m.group(1)
            prov_block_dest = block_from_m.group(2)
            prov_block_marker = block_from_m.group(3)
            i += 1; continue

        # block in FILE, marker "MARKER":  (inline body)
        block_inline_m = re.match(r'block\s+in\s+(\S+)\s*,\s*marker\s+"([^"]*)"\s*:\s*$', btext)
        if block_inline_m:
            prov_block_dest = block_inline_m.group(1)
            prov_block_marker = block_inline_m.group(2)
            j = i + 1
            block_lines = []
            while j < len(body) and body[j][1] > bindent:
                block_lines.append(body[j][2])
                j += 1
            prov_block_inline = "\n".join(block_lines)
            i = j; continue

        # ini FILE:  (multi-line key=value block with optional section "NAME": headers)
        ini_m = re.match(r'ini\s+(\S+)\s*:\s*$', btext)
        if ini_m:
            prov_ini_dest = ini_m.group(1)
            j = i + 1
            cur_section: str|None = None
            while j < len(body) and body[j][1] > bindent:
                iln, iind, itxt = body[j]
                # section "NAME":
                sec_m = re.match(r'section\s+"([^"]*)"\s*:\s*$', itxt)
                if sec_m:
                    cur_section = sec_m.group(1)
                    j += 1; continue
                # key = "value" or key = absent
                kv_m = re.match(r'(\w[\w.-]*)\s*=\s*("([^"]*)"|absent)', itxt)
                if kv_m:
                    key = kv_m.group(1)
                    val = kv_m.group(3) if kv_m.group(2) != "absent" else "absent"
                    prov_ini_ops.append((cur_section, key, val))
                j += 1
            i = j; continue

        # json FILE:  (multi-line dot-path key=value block)
        json_m = re.match(r'json\s+(\S+)\s*:\s*$', btext)
        if json_m:
            prov_json_dest = json_m.group(1)
            j = i + 1
            while j < len(body) and body[j][1] > bindent:
                jln, jind, jtxt = body[j]
                # path = "value" | path = absent | path = null | path = true/false | path = NUMBER | path[] = "value"
                jkv_m = re.match(r'([\w.\[\]-]+)\s*=\s*("([^"]*)"|absent|null|true|false|(-?\d+(?:\.\d+)?))', jtxt)
                if jkv_m:
                    path = jkv_m.group(1)
                    raw_val = jkv_m.group(2)
                    if raw_val == "absent":
                        val = "absent"
                    elif raw_val == "null":
                        val = None
                    elif raw_val == "true":
                        val = True
                    elif raw_val == "false":
                        val = False
                    elif jkv_m.group(4) is not None:
                        val = float(raw_val) if '.' in raw_val else int(raw_val)
                    else:
                        val = jkv_m.group(3)  # string value
                    prov_json_ops.append((path, val))
                j += 1
            i = j; continue

        # assert $ COMMAND [, "message"]
        if btext.startswith("assert "):
            assert_m = re.match(r'assert\s+\$\s+(.+?)(?:\s*,\s*"([^"]*)")?\s*$', btext)
            if assert_m:
                prov_asserts.append((assert_m.group(1), assert_m.group(2)))
            i += 1; continue

        # when "expr" or when expr (unquoted — for variable-based conditions)
        if btext.startswith("when "):
            wm = re.match(r'when\s+"([^"]*)"', btext)
            if wm:
                when = wm.group(1)
            else:
                when = btext[5:].strip()
            i += 1; continue

        if btext.startswith("until "):
            um = re.match(r'until\s+"([^"]*)"', btext)
            until = um.group(1) if um else btext[6:].strip().strip('"')
            i += 1; continue

        # retry Nx backoff Xs[..Ys] [jitter N%]  (new exponential backoff shorthand — MUST be before prop_m)
        retry_backoff_m = re.match(
            r'retry\s+(\d+)x\s+backoff\s+(\d+)(s|m|h)(?:\.\.(\d+)(s|m|h))?(?:\s+jitter\s+(\d+)%)?', btext)
        if retry_backoff_m:
            retries = int(retry_backoff_m.group(1))
            base = int(retry_backoff_m.group(2))
            base = _duration_to_secs(base, retry_backoff_m.group(3))
            backoff_max = 0
            if retry_backoff_m.group(4):
                backoff_max = int(retry_backoff_m.group(4))
                backoff_max = _duration_to_secs(backoff_max, retry_backoff_m.group(5))
            jitter_pct = int(retry_backoff_m.group(6)) if retry_backoff_m.group(6) else 0
            hp["retries"] = retries; hp["retry_delay"] = base; hp["retry_backoff"] = True
            retry_backoff_max = backoff_max; retry_jitter_pct = jitter_pct
            i += 1; continue

        # Body-level properties: as root, timeout 3m, retry 2x wait 5s, if fails warn
        # These can appear as standalone lines or comma-separated on one line
        prop_m = re.match(r'(as\s+\w+|timeout\s+\d+[smh]?(?:\s+reset\s+on\s+output)?|retry\s+\d+x(?:\s+(?:wait|every)\s+\d+[smh]?)?(?:\s+with\s+backoff)?|if\s+fails?\s+(?:stop|warn|ignore))', btext)
        if prop_m:
            body_props = _parse_cgr_header_props(btext)
            hp.update(body_props)
            i += 1; continue

        # collect "key" [as varname]  (varname may be "json" for format or a variable name)
        collect_m = re.match(r'collect\s+"([^"]*)"(?:\s+as\s+(\w+))?', btext)
        if collect_m:
            collect_key = collect_m.group(1)
            g2 = collect_m.group(2)
            if g2 == "json":
                collect_format = g2
            elif g2:
                collect_var = g2
            i += 1; continue

        # reduce "key" [as varname]
        reduce_m = re.match(r'reduce\s+"([^"]+)"(?:\s+as\s+(\w+))?', btext)
        if reduce_m:
            reduce_key = reduce_m.group(1)
            reduce_var = reduce_m.group(2)  # may be None
            i += 1; continue

        # on success: set VAR = "VALUE"
        if btext.startswith("on success:") or btext.startswith("on success :"):
            m = re.match(r'on\s+success\s*:\s*set\s+(\w+)\s*=\s*"([^"]*)"', btext)
            if m:
                on_success_set = list(on_success_set)
                on_success_set.append((m.group(1), m.group(2)))
            i += 1; continue

        # on failure: set VAR = "VALUE"
        if btext.startswith("on failure:") or btext.startswith("on failure :"):
            m = re.match(r'on\s+failure\s*:\s*set\s+(\w+)\s*=\s*"([^"]*)"', btext)
            if m:
                on_failure_set = list(on_failure_set)
                on_failure_set.append((m.group(1), m.group(2)))
            i += 1; continue

        # tags keyword1, keyword2, ...
        tags_m = re.match(r'tags\s+(.+)', btext)
        if tags_m:
            tags.extend(t.strip() for t in tags_m.group(1).split(",") if t.strip())
            i += 1; continue

        # interactive  — force PTY + stdin forwarding
        if btext.strip() == "interactive":
            interactive = True
            i += 1; continue

        # description "text" (for templates)
        desc_m = re.match(r'description\s+"([^"]*)"', btext)
        if desc_m:
            i += 1; continue  # handled by caller for templates

        i += 1

    if always_run and check is None:
        check = "false"

    # Steps with parallel/race/each/stage blocks don't need a run command
    has_construct = bool(parallel_block or race_block or each_body or stage_phases)
    has_http = bool(http_method and http_url)
    has_prov = bool(
        prov_content_inline is not None or prov_content_from or prov_lines or prov_put_src
        or prov_block_dest or prov_ini_dest or prov_json_dest or prov_asserts
        or reduce_key
    )
    if (run_cmd is None and script_path is None and not has_construct and not has_http and not has_prov
            and wait_kind is None and subgraph_path is None
            and not (allow_no_run_with_children and children)):
        raise err(f"Step [{name}] has no 'run' command", ln)
    if flags and run_cmd is None:
        raise err(f"Step [{name}] uses 'flag' without 'run'", ln)
    if until and hp.get("retries", 0) <= 0:
        raise err(f"Step [{name}] uses 'until' without 'retry'", ln)

    return ASTResource(
        name=slug, description=name, needs=needs,
        check=check, run=run_cmd, run_as=hp.get("run_as"), script_path=script_path,
        timeout=hp.get("timeout"), retries=hp.get("retries", 0),
        retry_delay=hp.get("retry_delay", 5), retry_backoff=hp.get("retry_backoff", False), on_fail=hp.get("on_fail", "stop"),
        when=when, env=env, is_verify=False, line=ln,
        timeout_reset_on_output=hp.get("timeout_reset_on_output", False),
        collect_key=collect_key, collect_format=collect_format,
        tags=tags, children=children,
        http_method=http_method, http_url=http_url, http_headers=http_headers,
        http_body=http_body, http_body_type=http_body_type,
        http_auth=http_auth, http_expect=http_expect,
        wait_kind=wait_kind, wait_target=wait_target,
        subgraph_path=subgraph_path, subgraph_vars=dict(subgraph_vars),
        parallel_block=parallel_block, parallel_limit=parallel_limit,
        parallel_fail_policy=parallel_fail_policy, race_block=race_block, race_into=race_into,
        each_var=each_var, each_list_expr=each_list_expr,
        each_limit=each_limit, each_body=each_body,
        stage_name=stage_name, stage_phases=stage_phases,
        prov_content_inline=prov_content_inline, prov_content_from=prov_content_from,
        prov_dest=prov_dest, prov_lines=list(prov_lines),
        prov_put_src=prov_put_src, prov_put_mode=prov_put_mode,
        prov_put_transfer=prov_put_transfer, prov_put_transfer_opts=prov_put_transfer_opts,
        prov_validate=prov_validate,
        prov_block_dest=prov_block_dest, prov_block_marker=prov_block_marker,
        prov_block_inline=prov_block_inline, prov_block_from=prov_block_from,
        prov_block_absent=prov_block_absent,
        prov_ini_dest=prov_ini_dest, prov_ini_ops=list(prov_ini_ops),
        prov_json_dest=prov_json_dest, prov_json_ops=list(prov_json_ops),
        prov_asserts=list(prov_asserts),
        retry_backoff_max=retry_backoff_max, retry_jitter_pct=retry_jitter_pct,
        on_success_set=list(on_success_set), on_failure_set=list(on_failure_set),
        collect_var=collect_var, reduce_key=reduce_key, reduce_var=reduce_var,
        flags=list(flags), env_when=dict(env_when), until=until,
        interactive=interactive)


def _parse_cgr_verify(desc: str, body: list, ln: int, err) -> ASTResource:
    """Parse a verify block."""
    needs: list[str] = []
    run_cmd: str | None = None
    retries = 0; retry_delay = 5
    timeout = None
    timeout_reset_on_output = False

    i = 0
    while i < len(body):
        bln, bindent, btext = body[i]
        if btext.startswith("needs ") or btext.startswith("first "):
            refs = re.findall(r'\[([^\]]+)\]', btext)
            for ref in refs:
                needs.append(_parse_cgr_dep_ref(ref))
            i += 1; continue
        elif btext.startswith("run "):
            cmd = btext[4:].strip()
            if cmd.startswith("$ "): cmd = cmd[2:]
            run_cmd = cmd
            j = i + 1
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                run_cmd += "\n" + body[j][2]; j += 1
            i = j; continue
        elif btext.startswith("retry "):
            m = re.match(r'retry\s+(\d+)x(?:\s+(?:wait|every)\s+(\d+)(s|m|h)?)?', btext)
            if m:
                retries = int(m.group(1))
                if m.group(2):
                    retry_delay = int(m.group(2))
                    if m.group(3) == "m":
                        retry_delay *= 60
                    elif m.group(3) == "h":
                        retry_delay *= 3600
            i += 1; continue
        else:
            timeout_prop = _parse_timeout_text(btext)
            if timeout_prop is not None:
                timeout, timeout_reset_on_output = timeout_prop
                i += 1; continue
        i += 1

    if not run_cmd:
        raise err(f"verify block has no 'run' command", ln)

    return ASTResource(
        name=f"__verify_{ln}", description=desc, needs=needs,
        check=None, run=run_cmd, run_as=None, timeout=timeout,
        retries=retries, retry_delay=retry_delay, retry_backoff=False, on_fail="warn",
        when=None, env={}, is_verify=True, line=ln,
        timeout_reset_on_output=timeout_reset_on_output, children=[])

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
