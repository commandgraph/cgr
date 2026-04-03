#!/usr/bin/env python3
"""
cgr (CommandGraph) v3 — DSL with nested dependencies, reusable templates, and
a copy-on-read repository.

Usage:
    cgr plan      FILE [--repo DIR] [-v]
    cgr apply     FILE [--repo DIR] [--dry-run] [--parallel N]
    cgr validate  FILE [--repo DIR] [-q] [--json]
    cgr dot       FILE [--repo DIR]
    cgr visualize FILE [--repo DIR] [-o FILE.html] [--state .state/FILE.state]
    cgr state     show|test|reset|set|drop|diff FILE [STEP|FILE2] [VALUE]
    cgr report    FILE [--format table|json|csv] [-o FILE] [--keys K1,K2]
    cgr repo      index [--repo DIR]
    cgr serve     [FILE] [--port 8420] [--no-open]
    cgr version

Requires: Python 3.9+.  No external dependencies.
"""
from __future__ import annotations
import argparse, codecs, datetime, fcntl, hashlib, hmac, json, os, pty, re, secrets, select, selectors, shlex, signal, subprocess, sys, tempfile, termios, textwrap, threading, time, tty, warnings
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from enum import Enum, auto
from pathlib import Path
from typing import Any

__version__ = "0.6.0"

def _duration_to_secs(value: int, suffix) -> int:
    if suffix == "m": return value * 60
    if suffix == "h": return value * 3600
    return value

def _parse_duration_str(s: str) -> int:
    import re as _re
    m = _re.fullmatch(r"(\d+)(s|m|h)?", s.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"Invalid duration: {s!r} (expected e.g. 300, 300s, 5m, 2h)")
    return _duration_to_secs(int(m.group(1)), m.group(2))

# ═══════════════════════════════════════════════════════════════════════════════
# Terminal colours
# ═══════════════════════════════════════════════════════════════════════════════
_COLOR = sys.stdout.isatty()
def _c(code: str, t: str) -> str: return f"\033[{code}m{t}\033[0m" if _COLOR else t
def green(t):   return _c("32", t)
def blue(t):    return _c("34", t)
def red(t):     return _c("31", t)
def yellow(t):  return _c("33", t)
def dim(t):     return _c("2", t)
def bold(t):    return _c("1", t)
def cyan(t):    return _c("36", t)
def magenta(t): return _c("35", t)
_ANSI_RE = re.compile(r'\033\[[0-9;]*m')
def _strip_ansi(t: str) -> str: return _ANSI_RE.sub('', t)
def _html_esc(s: str) -> str:
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;")
             .replace('"', "&quot;")
             .replace("'", "&#x27;"))

# ═══════════════════════════════════════════════════════════════════════════════
# 1. LEXER
# ═══════════════════════════════════════════════════════════════════════════════
class TT(Enum):
    IDENT=auto(); STRING=auto(); COMMAND=auto(); NUMBER=auto()
    LBRACE=auto(); RBRACE=auto(); EQUALS=auto(); COMMA=auto()
    LPAREN=auto(); RPAREN=auto(); DOT=auto(); GTE=auto()
    COMPAT=auto(); EOF=auto()  # GTE: >=  COMPAT: ~>

@dataclass(frozen=True)
class Token:
    type: TT; value: str; line: int; col: int
    def __repr__(self): return f"Tok({self.type.name},{self.value!r},{self.line}:{self.col})"

class LexError(Exception):
    def __init__(self, msg, line=0, col=0, src=""):
        self.msg=msg; self.line=line; self.col=col; self.src=src; super().__init__(msg)

def lex(source: str, filename: str = "<input>") -> list[Token]:
    tokens: list[Token] = []; lines = source.split("\n")
    i = 0; ln = 1; L = len(source)
    def col_of(p): return p - source.rfind("\n", 0, p)
    def srcl(): return lines[ln-1] if ln <= len(lines) else ""
    while i < L:
        ch = source[i]
        if ch in " \t\r": i+=1; continue
        if ch == "\n": ln+=1; i+=1; continue
        if ch == "#":
            e = source.find("\n", i); i = L if e < 0 else e; continue
        c = col_of(i)
        # Two-character operators: >=, ~>
        if ch == ">" and i+1<L and source[i+1]=="=":
            tokens.append(Token(TT.GTE, ">=", ln, c)); i+=2; continue
        if ch == "~" and i+1<L and source[i+1]==">":
            tokens.append(Token(TT.COMPAT, "~>", ln, c)); i+=2; continue
        simple = {"{":TT.LBRACE, "}":TT.RBRACE, "=":TT.EQUALS,
                  ",":TT.COMMA, "(":TT.LPAREN, ")":TT.RPAREN}
        if ch in simple:
            tokens.append(Token(simple[ch], ch, ln, c)); i+=1; continue
        if ch == '"':
            i+=1; parts=[]
            while i<L and source[i]!='"':
                if source[i]=="\\" and i+1<L:
                    esc=source[i+1]
                    parts.append({"n":"\n","t":"\t",'"':'"',"\\":"\\"}.get(esc, source[i:i+2]))
                    i+=2
                else:
                    if source[i]=="\n": ln+=1
                    parts.append(source[i]); i+=1
            if i>=L: raise LexError("Unterminated string", ln, c, srcl())
            i+=1; tokens.append(Token(TT.STRING, "".join(parts), ln, c)); continue
        if ch == "`":
            sl=ln; i+=1; parts=[]
            while i<L and source[i]!="`":
                if source[i]=="\n": ln+=1
                parts.append(source[i]); i+=1
            if i>=L: raise LexError("Unterminated command", sl, c, lines[sl-1] if sl<=len(lines) else "")
            i+=1; tokens.append(Token(TT.COMMAND, "".join(parts).strip(), sl, c)); continue
        if ch.isdigit():
            s=i
            while i<L and source[i].isdigit(): i+=1
            tokens.append(Token(TT.NUMBER, source[s:i], ln, c)); continue
        if ch.isalpha() or ch=="_":
            s=i
            while i<L and (source[i].isalnum() or source[i] in "_-"): i+=1
            tokens.append(Token(TT.IDENT, source[s:i], ln, c)); continue
        if ch==".":
            i+=1; tokens.append(Token(TT.DOT, ".", ln, c)); continue
        raise LexError(f"Unexpected char {ch!r}", ln, c, srcl())
    tokens.append(Token(TT.EOF, "", ln, col_of(L)))
    return tokens

# ═══════════════════════════════════════════════════════════════════════════════
# 2. AST
# ═══════════════════════════════════════════════════════════════════════════════
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
    on_complete: ASTResource|None = None   # hook: runs after successful apply
    on_failure: ASTResource|None = None    # hook: runs after failed apply


_DEFERRED_SECRET_CMD = "<deferred:cmd>"
_DEFERRED_SECRET_VAULT = "<deferred:vault>"

# ═══════════════════════════════════════════════════════════════════════════════
# 3. PARSER
# ═══════════════════════════════════════════════════════════════════════════════
class ParseError(Exception):
    def __init__(self, msg, token=None, source_lines=None, filename=""):
        self.msg=msg; self.token=token
        self.source_lines=source_lines or []; self.filename=filename
        super().__init__(msg)
    def pretty(self) -> str:
        parts=[]
        if self.token:
            loc = f"{self.filename}:{self.token.line}:{self.token.col}" if self.filename else f"{self.token.line}:{self.token.col}"
            parts.append(red(f"error: {self.msg}") + dim(f"  at {loc}"))
            if self.source_lines and 1<=self.token.line<=len(self.source_lines):
                parts.append(dim(f"  {self.token.line:>4} │ ") + self.source_lines[self.token.line-1])
                parts.append("       " + " "*max(0,self.token.col-1) + red("^"))
        else: parts.append(red(f"error: {self.msg}"))
        return "\n".join(parts)

RESOURCE_STMTS = {"description","needs","check","run","script","as","timeout",
                  "retry","on_fail","when","env","collect","flag","until",
                  "parallel","race","each","stage"}

class Parser:
    def __init__(self, tokens, source, filename=""):
        self.tokens=tokens; self.pos=0
        self.sl=source.split("\n"); self.fn=filename

    def _peek(self): return self.tokens[min(self.pos,len(self.tokens)-1)]
    def _advance(self):
        t=self._peek()
        if t.type!=TT.EOF: self.pos+=1
        return t
    def _at(self, tt, v=None):
        t=self._peek(); return t.type==tt and (v is None or t.value==v)
    def _at_id(self, v): return self._at(TT.IDENT, v)
    def _expect(self, tt, v=None, hint=""):
        t=self._advance()
        if t.type!=tt or (v is not None and t.value!=v):
            exp = f"'{v}'" if v else tt.name
            msg = f"Expected {exp}, got {t.type.name} '{t.value}'"
            if hint: msg += f"\n  Hint: {hint}"
            raise ParseError(msg, t, self.sl, self.fn)
        return t
    def _err(self, msg, hint=""): return ParseError(f"{msg}\n  Hint: {hint}" if hint else msg, self._peek(), self.sl, self.fn)

    def parse(self) -> ASTProgram:
        vs=[]; us=[]; ts=[]; ns=[]; te_blocks=[]; invs=[]; secs=[]; gf=False
        on_comp=None; on_fail=None
        while not self._at(TT.EOF):
            if self._at_id("var"):      vs.append(self._p_var())
            elif self._at_id("use"):    us.append(self._p_use())
            elif self._at_id("inventory"): invs.append(self._p_inventory())
            elif self._at_id("secrets"): secs.append(self._p_secrets())
            elif self._at_id("gather"):
                self._advance()  # consume 'gather'
                self._expect(TT.IDENT, "facts")  # consume 'facts'
                gf = True
            elif self._at_id("include"):
                self._advance()
                inc_path = self._expect(TT.STRING).value
                if self.fn and os.path.exists(self.fn):
                    inc_path = os.path.join(os.path.dirname(os.path.abspath(self.fn)), inc_path)
                if os.path.isfile(inc_path):
                    inc_source = open(inc_path).read()
                    if inc_path.endswith(".cg"):
                        inc_ast = Parser(lex(inc_source, inc_path), inc_path).parse()
                    else:
                        inc_ast = parse_cgr(inc_source, inc_path)
                    existing = {v.name for v in vs}
                    for v in inc_ast.variables:
                        if v.name not in existing: vs.append(v)
                    us.extend(inc_ast.uses); ns.extend(inc_ast.nodes); ts.extend(inc_ast.templates)
                    te_blocks.extend(inc_ast.target_each_blocks)
            elif self._at_id("template"): ts.append(self._p_template())
            elif self._at_id("node"):   ns.append(self._p_node())
            elif self._at_id("each"):   te_blocks.append(self._p_each_targets(te_blocks))
            elif self._at_id("on_complete"):
                self._advance()  # consume 'on_complete'
                self._expect(TT.LBRACE)
                on_comp = self._p_resource_body("__on_complete")
                self._expect(TT.RBRACE)
            elif self._at_id("on_failure"):
                self._advance()  # consume 'on_failure'
                self._expect(TT.LBRACE)
                on_fail = self._p_resource_body("__on_failure")
                self._expect(TT.RBRACE)
            else: raise self._err(f"Expected var/use/inventory/secrets/gather/template/node/each/on_complete/on_failure, got '{self._peek().value}'")
        return ASTProgram(variables=vs, uses=us, templates=ts, nodes=ns,
                          target_each_blocks=te_blocks, inventories=invs, secrets=secs,
                          gather_facts=gf, on_complete=on_comp, on_failure=on_fail)

    def _p_var(self):
        tok=self._expect(TT.IDENT,"var"); n=self._expect(TT.IDENT); self._expect(TT.EQUALS)
        # Support env("NAME") or env("NAME", "default")
        if self._at_id("env") and self.pos+1<len(self.tokens) and self.tokens[self.pos+1].type==TT.LPAREN:
            self._advance()  # consume 'env'
            self._expect(TT.LPAREN)
            env_key = self._expect(TT.STRING).value
            env_default = None
            if self._at(TT.COMMA):
                self._advance()
                env_default = self._expect(TT.STRING).value
            self._expect(TT.RPAREN)
            val = os.environ.get(env_key)
            if val is None:
                if env_default is not None:
                    val = env_default
                else:
                    raise self._err(f"Environment variable '{env_key}' is not set and no default provided")
            return ASTVar(name=n.value, value=val, line=tok.line)
        v=self._advance()
        if v.type not in (TT.STRING,TT.NUMBER,TT.IDENT): raise self._err("Expected value for var")
        return ASTVar(name=n.value, value=v.value, line=tok.line)

    def _p_use(self):
        tok=self._expect(TT.IDENT,"use")
        path=self._expect(TT.STRING, hint='use "path/to/template"')
        alias=None
        if self._at_id("as"): self._advance(); alias=self._expect(TT.IDENT).value
        # Optional version constraint: use "path" >= "1.0" or ~> "1.2" or = "1.0.0"
        ver_op = ver_req = None
        if self._at(TT.GTE): self._advance(); ver_op = ">="; ver_req = self._expect(TT.STRING).value
        elif self._at(TT.COMPAT): self._advance(); ver_op = "~>"; ver_req = self._expect(TT.STRING).value
        elif self._at(TT.EQUALS) and self.pos+1<len(self.tokens) and self.tokens[self.pos+1].type==TT.STRING:
            # Distinguish version = "1.0" from next key = value
            self._advance(); ver_op = "="; ver_req = self._expect(TT.STRING).value
        return ASTUse(path=path.value, alias=alias, line=tok.line,
                      version_op=ver_op, version_req=ver_req)

    def _p_inventory(self):
        tok=self._expect(TT.IDENT,"inventory")
        path=self._expect(TT.STRING, hint='inventory "hosts.ini"')
        return ASTInventory(path=path.value, line=tok.line)

    def _p_secrets(self):
        tok=self._expect(TT.IDENT,"secrets")
        path=self._expect(TT.STRING, hint='secrets "myapp.secrets"')
        return ASTSecrets(path=path.value, line=tok.line)

    def _p_template(self):
        tok=self._expect(TT.IDENT,"template")
        name=self._expect(TT.IDENT)
        self._expect(TT.LPAREN, hint="template name(param1, param2 = default)")
        params=self._p_param_list()
        self._expect(TT.RPAREN)
        self._expect(TT.LBRACE)
        # Optional: version "X.Y.Z"
        tpl_version = None
        if self._at_id("version") and self.pos+1<len(self.tokens) and self.tokens[self.pos+1].type==TT.STRING:
            self._advance()  # consume 'version'
            tpl_version = self._expect(TT.STRING).value
        body=self._p_resource_body(name.value, is_template_root=True)
        self._expect(TT.RBRACE)
        return ASTTemplate(name=name.value, params=params, description=body.description,
                           body=body, source_file=self.fn, line=tok.line,
                           version=tpl_version)

    def _p_param_list(self) -> list[ASTParam]:
        params=[]
        while not self._at(TT.RPAREN) and not self._at(TT.EOF):
            n=self._expect(TT.IDENT)
            default=None
            if self._at(TT.EQUALS):
                self._advance()
                d=self._advance()
                default=d.value
            params.append(ASTParam(name=n.value, default=default, line=n.line))
            if self._at(TT.COMMA): self._advance()
        return params

    def _p_node(self):
        tok=self._expect(TT.IDENT,"node")
        name=self._expect(TT.STRING); self._expect(TT.LBRACE)
        via=self._p_via()
        after_nodes=[]
        # Optional: after "node1", "node2" or after each
        while self._at_id("after"):
            self._advance()
            if self._at_id("each"):
                self._advance()
                after_nodes.append("__each__")
            else:
                after_nodes.append(self._expect(TT.STRING).value)
            while self._at(TT.COMMA):
                self._advance()
                if self._at_id("each"):
                    self._advance()
                    after_nodes.append("__each__")
                elif self._at(TT.STRING):
                    after_nodes.append(self._expect(TT.STRING).value)
                else:
                    break
        resources=[]; groups=[]; insts=[]
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            if self._at_id("resource"):  resources.append(self._p_resource())
            elif self._at_id("group"):   groups.append(self._p_group())
            elif self._at_id("verify"):  resources.append(self._p_verify())
            elif self._at(TT.IDENT) and self._peek().value not in RESOURCE_STMTS and not self._at_id("var") and not self._at_id("node") and not self._at_id("use") and not self._at_id("template") and not self._at_id("after"):
                # Template instantiation: name { args }
                insts.append(self._p_instantiation())
            else:
                raise self._err(f"Expected resource/group/verify/template_call, got '{self._peek().value}'")
        self._expect(TT.RBRACE)
        return ASTNode(name=name.value, via=via, resources=resources,
                       groups=groups, instantiations=insts, line=tok.line,
                       after_nodes=after_nodes)

    def _p_each_targets(self, each_blocks) -> ASTTargetEach:
        """Parse: each var1, var2 in var_name { node "..." { ... } }"""
        tok = self._expect(TT.IDENT, "each")
        each_vars = [self._expect(TT.IDENT).value]
        while self._at(TT.COMMA):
            self._advance()
            if self._at(TT.IDENT) and self._peek().value != "in":
                each_vars.append(self._expect(TT.IDENT).value)
            else:
                break
        self._expect(TT.IDENT, "in")
        list_name = self._expect(TT.IDENT).value
        self._expect(TT.LBRACE)

        # Capture tokens for the body (between braces)
        body_tokens = []
        depth = 1
        while depth > 0 and not self._at(TT.EOF):
            t = self._advance()
            if t.type == TT.LBRACE:
                depth += 1
            elif t.type == TT.RBRACE:
                depth -= 1
                if depth == 0: break
            body_tokens.append(t)

        te = ASTTargetEach(
            var_names=each_vars, list_expr=f"${{{list_name}}}",
            body_source="", body_format="cg",
            line=tok.line, body_tokens=body_tokens)
        return te

    def _p_instantiation(self) -> ASTInstantiation:
        name_tok=self._advance()  # template name
        self._expect(TT.LBRACE)
        args={}
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            k=self._expect(TT.IDENT)
            self._expect(TT.EQUALS)
            v=self._advance()
            args[k.value]=v.value
            # optional comma
            if self._at(TT.COMMA): self._advance()
        self._expect(TT.RBRACE)
        return ASTInstantiation(template_name=name_tok.value, args=args, line=name_tok.line)

    def _p_via(self):
        if not self._at_id("via"): return ASTVia(method="local", props={}, line=self._peek().line)
        tok=self._expect(TT.IDENT,"via")
        if self._at_id("ssh"): self._advance(); return ASTVia(method="ssh", props=self._p_kv(), line=tok.line)
        elif self._at_id("local"): self._advance(); return ASTVia(method="local", props={}, line=tok.line)
        else: raise self._err("Expected 'ssh' or 'local'")

    def _p_kv(self) -> dict[str,str]:
        props={}; stops={"resource","group","verify","via","template","use","node","var"}
        while True:
            t=self._peek()
            if t.type in (TT.LBRACE,TT.RBRACE,TT.EOF): break
            if t.type==TT.IDENT and t.value in stops: break
            if t.type!=TT.IDENT: break
            if self.pos+1<len(self.tokens) and self.tokens[self.pos+1].type==TT.EQUALS:
                k=self._advance(); self._expect(TT.EQUALS); v=self._advance()
                props[k.value]=v.value
            else: break
        return props

    def _p_resource(self, gd=None):
        tok=self._expect(TT.IDENT,"resource")
        name=self._expect(TT.IDENT)
        self._expect(TT.LBRACE)
        body=self._p_resource_body(name.value, group_defaults=gd)
        self._expect(TT.RBRACE)
        return body

    def _p_resource_body(self, name, group_defaults=None, is_template_root=False) -> ASTResource:
        d=group_defaults or {}
        desc=""; needs=[]; check=None; run_cmd=None; script_path=None; run_as=d.get("as")
        timeout=d.get("timeout",300); retries=0; retry_delay=5; retry_backoff=False
        on_fail=d.get("on_fail","stop"); when=None; env={}; env_when={}; children=[]; flags=[]; until=None
        collect_key=None
        collect_format=None
        http_method=None; http_url=None; http_headers=[]; http_body=None
        http_body_type=None; http_auth=None; http_expect=None

        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            s=self._peek()
            if s.type==TT.IDENT and s.value=="resource":
                children.append(self._p_resource(group_defaults))
            elif s.value=="description": self._advance(); desc=self._expect(TT.STRING).value
            elif s.value=="needs":       self._advance(); needs.extend(self._p_ident_list())
            elif s.value=="check":       self._advance(); check=self._expect(TT.COMMAND).value
            elif s.value=="run":         self._advance(); run_cmd=self._expect(TT.COMMAND).value
            elif s.value=="script":      self._advance(); script_path=self._expect(TT.STRING).value
            elif s.value=="as":          self._advance(); run_as=self._expect(TT.IDENT).value
            elif s.value=="collect":
                self._advance(); collect_key=self._expect(TT.STRING).value
                if self._at_id("as"): self._advance(); collect_format=self._expect(TT.IDENT).value
            elif s.value=="tags":
                self._advance()
                tag_list = [self._expect(TT.IDENT).value]
                while self._at(TT.COMMA): self._advance(); tag_list.append(self._expect(TT.IDENT).value)
                tags = tag_list
            elif s.value=="timeout":
                self._advance(); timeout=int(self._expect(TT.NUMBER).value)
                if self._at(TT.IDENT) and self._peek().value in ("s","m","h"):
                    v=self._advance().value
                    if v=="m": timeout*=60
                    elif v=="h": timeout*=3600
            elif s.value=="retry":
                self._advance(); retries=int(self._expect(TT.NUMBER).value)
                if self._at_id("delay"):
                    self._advance(); retry_delay=int(self._expect(TT.NUMBER).value)
                    if self._at(TT.IDENT) and self._peek().value in ("s","m","h"):
                        v=self._advance().value
                        if v=="m": retry_delay*=60
                        elif v=="h": retry_delay*=3600
                if self._at_id("backoff"):
                    self._advance(); retry_backoff=True
                    if retry_delay == 5: retry_delay = 1  # default base for backoff
            elif s.value=="on_fail":
                self._advance(); of=self._expect(TT.IDENT)
                if of.value not in ("stop","warn","ignore"): raise self._err(f"Invalid on_fail '{of.value}'")
                on_fail=of.value
            elif s.value=="when": self._advance(); when=self._expect(TT.STRING).value
            elif s.value=="env":
                self._advance(); k=self._expect(TT.IDENT).value
                self._expect(TT.EQUALS); env[k]=self._advance().value
                if self._at_id("when"):
                    self._advance()
                    env_when[k] = self._expect(TT.STRING).value
            elif s.value=="flag":
                self._advance()
                flag_text = self._expect(TT.STRING).value
                flag_when = None
                if self._at_id("when"):
                    self._advance()
                    flag_when = self._expect(TT.STRING).value
                flags.append((flag_text, flag_when))
            elif s.value=="until":
                self._advance()
                until = self._expect(TT.STRING).value
            # ── HTTP verbs ───────────────────────────────────────────────
            elif s.value in ("get","post","put","patch","delete"):
                http_method=self._advance().value.upper()
                http_url=self._expect(TT.STRING).value
            elif s.value=="auth":
                self._advance()
                atype=self._expect(TT.IDENT).value
                if atype=="bearer":
                    http_auth=("bearer", self._expect(TT.STRING).value)
                elif atype=="basic":
                    u=self._expect(TT.STRING).value; p=self._expect(TT.STRING).value
                    http_auth=("basic", f"{u}:{p}")
                elif atype=="header":
                    hname=self._expect(TT.STRING).value; self._expect(TT.EQUALS)
                    hval=self._expect(TT.STRING).value
                    http_auth=("header", f"{hname}:{hval}")
                else:
                    raise self._err(f"Unknown auth type '{atype}'", "Valid: bearer, basic, header")
            elif s.value=="header":
                self._advance()
                hname=self._expect(TT.STRING).value; self._expect(TT.EQUALS)
                hval=self._expect(TT.STRING).value
                http_headers.append((hname, hval))
            elif s.value=="body":
                self._advance()
                http_body_type=self._expect(TT.IDENT).value
                http_body=self._expect(TT.STRING).value
            elif s.value=="expect":
                self._advance(); http_expect=self._expect(TT.STRING).value
            # ── Parallel constructs ──────────────────────────────────────
            elif s.value=="parallel":
                plimit, ppolicy, pchildren = self._p_parallel(group_defaults)
                parallel_block = pchildren; parallel_limit = plimit; parallel_fail_policy = ppolicy
            elif s.value=="race":
                race_into, race_block = self._p_race(group_defaults)
            elif s.value=="each":
                each_var, each_list_expr, each_limit, each_body = self._p_each(group_defaults)
            elif s.value=="stage":
                stage_name, stage_phases = self._p_stage(group_defaults)
            else:
                raise self._err(f"Unknown '{s.value}' in resource",
                    "Valid: description, needs, check, run, script, as, timeout, retry, on_fail, when, env, collect, flag, until, tags, resource, parallel, race, each, stage, get, post, put, patch, delete, auth, header, body, expect")
        if flags and not run_cmd:
            raise self._err("'flag' requires a 'run' command in the same step")
        if until and retries <= 0:
            raise self._err("'until' requires 'retry'")
        res = ASTResource(name=name, description=desc, needs=needs,
            check=check, run=run_cmd, run_as=run_as, timeout=timeout,
            retries=retries, retry_delay=retry_delay, retry_backoff=retry_backoff, on_fail=on_fail,
            when=when, env=env, is_verify=False, line=s.line if s else 0,
            script_path=script_path, collect_key=collect_key, collect_format=collect_format,
            children=children, flags=flags, env_when=env_when, until=until,
            http_method=http_method, http_url=http_url, http_headers=http_headers,
            http_body=http_body, http_body_type=http_body_type,
            http_auth=http_auth, http_expect=http_expect)
        # Attach optional fields if parsed
        if 'tags' in dir(): res.tags = tags
        if 'parallel_block' in dir(): res.parallel_block = parallel_block; res.parallel_limit = parallel_limit; res.parallel_fail_policy = parallel_fail_policy
        if 'race_block' in dir(): res.race_block = race_block; res.race_into = race_into if 'race_into' in dir() else None
        if 'each_var' in dir(): res.each_var = each_var; res.each_list_expr = each_list_expr; res.each_limit = each_limit; res.each_body = each_body
        if 'stage_name' in dir(): res.stage_name = stage_name; res.stage_phases = stage_phases
        return res

    # ── Parallel construct parsers (.cg format) ──────────────────────────

    def _p_parallel(self, gd=None):
        """Parse: parallel [N] [, if_one_fails wait|stop|ignore] { resource ... }"""
        self._advance()  # consume 'parallel'
        limit = None; policy = "wait"
        # Optional concurrency limit
        if self._at(TT.NUMBER):
            limit = int(self._advance().value)
        # Optional failure policy: , if_one_fails wait|stop|ignore
        if self._at(TT.COMMA):
            self._advance()
            self._expect(TT.IDENT, "if_one_fails")
            p = self._expect(TT.IDENT)
            if p.value not in ("wait", "stop", "ignore"):
                raise self._err(f"Invalid parallel fail policy '{p.value}', expected wait|stop|ignore")
            policy = p.value
        self._expect(TT.LBRACE)
        children = []
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            if self._at_id("resource"):
                children.append(self._p_resource(gd))
            else:
                raise self._err(f"Expected 'resource' inside parallel block")
        self._expect(TT.RBRACE)
        if not children: raise self._err("parallel block must contain at least one resource")
        return limit, policy, children

    def _p_race(self, gd=None):
        """Parse: race [into "expr"] { resource ... }"""
        self._advance()  # consume 'race'
        race_into = None
        if self._at_id("into"):
            self._advance()  # consume 'into'
            race_into = self._expect(TT.STRING).value
        self._expect(TT.LBRACE)
        children = []
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            if self._at_id("resource"):
                children.append(self._p_resource(gd))
            else:
                raise self._err(f"Expected 'resource' inside race block")
        self._expect(TT.RBRACE)
        if len(children) < 2: raise self._err("race block must contain at least 2 resources")
        return race_into, children

    def _p_each(self, gd=None):
        """Parse: each VAR in "list_expr" [, N] { resource ... }"""
        self._advance()  # consume 'each'
        var = self._expect(TT.IDENT).value
        self._expect(TT.IDENT, "in")
        list_expr = self._expect(TT.STRING).value
        limit = None
        if self._at(TT.COMMA):
            self._advance()
            if self._at(TT.NUMBER):
                limit = int(self._advance().value)
            elif self._at(TT.STRING):
                val = self._advance().value
                if not re.match(r'^\$\{\w+\}$', val):
                    raise self._err(f"each concurrency limit must be an integer or a ${{variable}} expression, got: \"{val}\"")
                limit = val  # "${n}" — resolved during flatten
            else:
                raise self._err("Expected concurrency limit: a number or a ${variable} expression")
        self._expect(TT.LBRACE)
        if not self._at_id("resource"):
            raise self._err("each block must contain exactly one resource template")
        body = self._p_resource(gd)
        self._expect(TT.RBRACE)
        return var, list_expr, limit, body

    def _p_stage(self, gd=None):
        """Parse: stage "name" { phase "name" COUNT from "list" { ... } ... }"""
        self._advance()  # consume 'stage'
        stage_name = self._expect(TT.STRING).value
        self._expect(TT.LBRACE)
        phases = []
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            if not self._at_id("phase"):
                raise self._err("Expected 'phase' inside stage block")
            self._advance()  # consume 'phase'
            pname = self._expect(TT.STRING).value
            # Count: NUMBER, or IDENT for "rest"/"remaining", or NUMBER followed by %
            count_tok = self._advance()
            if count_tok.type == TT.NUMBER:
                count_expr = count_tok.value
                # Check for trailing % (lexed as separate IDENT)
                if self._at(TT.IDENT) and self._peek().value == "%":
                    self._advance(); count_expr += "%"
            elif count_tok.type == TT.IDENT and count_tok.value in ("rest", "remaining"):
                count_expr = count_tok.value
            else:
                raise self._err(f"Expected count (number, N%, rest) in phase, got '{count_tok.value}'")
            self._expect(TT.IDENT, "from")
            from_list = self._expect(TT.STRING).value
            self._expect(TT.LBRACE)
            phase_resources = []
            phase_each_limit = None
            while not self._at(TT.RBRACE) and not self._at(TT.EOF):
                if self._at_id("resource"):
                    phase_resources.append(self._p_resource(gd))
                elif self._at_id("verify"):
                    phase_resources.append(self._p_verify())
                elif self._at_id("each"):
                    # each inside phase: each VAR, N { resource ... }
                    self._advance()
                    evar = self._expect(TT.IDENT).value
                    elimit = None
                    if self._at(TT.COMMA):
                        self._advance()
                        if self._at(TT.NUMBER):
                            elimit = int(self._advance().value)
                        elif self._at(TT.STRING):
                            val = self._advance().value
                            if not re.match(r'^\$\{\w+\}$', val):
                                raise self._err(f"each concurrency limit must be an integer or a ${{variable}} expression, got: \"{val}\"")
                            elimit = val  # "${n}" — resolved during flatten
                        else:
                            raise self._err("Expected concurrency limit: a number or a ${variable} expression")
                    phase_each_limit = elimit
                    self._expect(TT.LBRACE)
                    while not self._at(TT.RBRACE) and not self._at(TT.EOF):
                        if self._at_id("resource"):
                            phase_resources.append(self._p_resource(gd))
                        elif self._at_id("verify"):
                            phase_resources.append(self._p_verify())
                        else:
                            raise self._err("Expected 'resource' or 'verify' inside phase each block")
                    self._expect(TT.RBRACE)
                else:
                    raise self._err("Expected 'resource', 'verify', or 'each' inside phase block")
            self._expect(TT.RBRACE)
            phases.append(ASTPhase(name=pname, count_expr=count_expr,
                from_list=from_list, body=phase_resources,
                each_limit=phase_each_limit, line=count_tok.line))
        self._expect(TT.RBRACE)
        if not phases: raise self._err("stage block must contain at least one phase")
        return stage_name, phases

    def _p_verify(self):
        tok=self._expect(TT.IDENT,"verify")
        desc=self._advance().value if self._at(TT.STRING) else "Verification"
        self._expect(TT.LBRACE)
        needs=[]; run_cmd=None; retries=0; retry_delay=5
        on_fail="warn"; timeout=30; run_as=None
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            s=self._peek()
            if   s.value=="needs":   self._advance(); needs=self._p_ident_list()
            elif s.value=="run":     self._advance(); run_cmd=self._expect(TT.COMMAND).value
            elif s.value=="as":      self._advance(); run_as=self._expect(TT.IDENT).value
            elif s.value=="timeout":
                self._advance(); timeout=int(self._expect(TT.NUMBER).value)
                if self._at(TT.IDENT) and self._peek().value in ("s","m","h"):
                    v=self._advance().value
                    if v=="m": timeout*=60
                    elif v=="h": timeout*=3600
            elif s.value=="on_fail": self._advance(); on_fail=self._expect(TT.IDENT).value
            elif s.value=="retry":
                self._advance(); retries=int(self._expect(TT.NUMBER).value)
                if self._at_id("delay"):
                    self._advance(); retry_delay=int(self._expect(TT.NUMBER).value)
                    if self._at(TT.IDENT) and self._peek().value in ("s","m","h"):
                        v=self._advance().value
                        if v=="m": retry_delay*=60
                        elif v=="h": retry_delay*=3600
            else: raise self._err(f"Unknown '{s.value}' in verify")
        self._expect(TT.RBRACE)
        if not run_cmd: raise ParseError("verify missing 'run'", tok, self.sl, self.fn)
        return ASTResource(name=f"__verify_{tok.line}", description=desc, needs=needs,
            check=None, run=run_cmd, run_as=run_as, timeout=timeout,
            retries=retries, retry_delay=retry_delay, retry_backoff=False, on_fail=on_fail,
            when=None, env={}, is_verify=True, line=tok.line, children=[])

    def _p_group(self):
        tok=self._expect(TT.IDENT,"group"); nm=self._expect(TT.IDENT)
        run_as=None; on_fail=None; timeout=None
        while not self._at(TT.LBRACE) and not self._at(TT.EOF):
            if self._at_id("as"): self._advance(); run_as=self._expect(TT.IDENT).value
            elif self._at_id("on_fail"): self._advance(); on_fail=self._expect(TT.IDENT).value
            elif self._at_id("timeout"):
                self._advance(); timeout=int(self._expect(TT.NUMBER).value)
                if self._at(TT.IDENT) and self._peek().value in ("s","m","h"):
                    v=self._advance().value
                    if v=="m": timeout*=60
                    elif v=="h": timeout*=3600
            else: break
        self._expect(TT.LBRACE)
        gd={}
        if run_as: gd["as"]=run_as
        if on_fail: gd["on_fail"]=on_fail
        if timeout: gd["timeout"]=timeout
        resources=[]
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            if self._at_id("resource"): resources.append(self._p_resource(gd))
            else: raise self._err(f"Expected 'resource' in group")
        self._expect(TT.RBRACE)
        return ASTGroup(name=nm.value, run_as=run_as, on_fail=on_fail,
                        timeout=timeout, resources=resources, line=tok.line)

    def _p_ident_list(self):
        # Support dot-qualified identifiers for cross-node refs: needs db.step_name
        def _read_dotted():
            name = self._expect(TT.IDENT).value
            while self._at(TT.DOT):
                self._advance()
                name += "." + self._expect(TT.IDENT).value
            return name
        ids=[_read_dotted()]
        while self._at(TT.COMMA): self._advance(); ids.append(_read_dotted())
        return ids

# ═══════════════════════════════════════════════════════════════════════════════
# 3b. CGR PARSER — indentation-based human-readable format (.cgr)
# ═══════════════════════════════════════════════════════════════════════════════

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

def parse_cgr(source: str, filename: str = "") -> ASTProgram:
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
            # Resolve relative to the current file's directory
            if filename:
                inc_path = os.path.join(os.path.dirname(os.path.abspath(filename)), inc_path)
            if not os.path.isfile(inc_path):
                raise err(f"Included file not found: {inc_path}", ln, text)
            inc_source = open(inc_path).read()
            inc_ast = parse_cgr(inc_source, inc_path)
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
                      secrets=secrets_list, gather_facts=gather_facts,
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
        check=None, run=None, run_as=None, timeout=300,
        retries=0, retry_delay=5, retry_backoff=False, on_fail="stop",
        when=None, env={}, is_verify=False, line=ln,
        inner_template_name=tpl_path.split("/")[-1],
        inner_template_args=args,
    )


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

    # Parse steps and verify blocks from body
    resources: list[ASTResource] = []
    instantiations: list[ASTInstantiation] = []
    i = 0

    while i < len(body_lines):
        bln, bindent, btext = body_lines[i]

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
                # Find the template alias (last segment of path)
                tpl_alias = tpl_path.split("/")[-1]
                args = {}
                for sln, sind, stxt in step_body:
                    am = re.match(r'(\w+)\s*=\s*"([^"]*)"', stxt)
                    if am: args[am.group(1)] = am.group(2)
                    else:
                        am = re.match(r'(\w+)\s*=\s*(\S+)', stxt)
                        if am: args[am.group(1)] = am.group(2)
                # Create an instantiation with the human name as mapping
                inst = ASTInstantiation(template_name=tpl_alias, args=args, line=bln)
                inst._cgr_human_name = step_name  # stash for resolver
                instantiations.append(inst)
            else:
                # Inline resource
                res = _parse_cgr_step(step_name, step_header, step_body, bln, err)
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
            resources.append(res)
            i = j; continue

        # stage "name": at target level — wraps in a synthetic step
        stage_tm = re.match(r'stage\s+"([^"]+)"\s*:\s*$', btext)
        if stage_tm:
            stage_body = []
            j = i + 1
            while j < len(body_lines) and body_lines[j][1] > bindent:
                stage_body.append(body_lines[j]); j += 1
            # Parse as a step named after the stage
            sname = stage_tm.group(1)
            res = _parse_cgr_step(sname, "", [(bln, bindent, btext)] + stage_body, bln, err)
            resources.append(res)
            i = j; continue

        i += 1  # skip unrecognised lines

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
    m = re.search(r'\btimeout\s+(\d+)(s|m|h)?', header)
    if m:
        t = _duration_to_secs(int(m.group(1)), m.group(2))
        props["timeout"] = t
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
    "as ", "timeout ", "retry ", "if fails",
    "content ", "line ", "put ", "validate ",
    "block ", "ini ", "json ", "assert ",
    "on success:", "on success :", "on failure:", "on failure :",
    "reduce ",
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


def _parse_cgr_step_line(text: str):
    """Parse either a block step header or a one-line step body."""
    block_m = re.match(r'\[([^\]]+)\](.*):\s*$', text)
    if block_m:
        return block_m.group(1), block_m.group(2).strip(), None
    inline_m = re.match(r'\[([^\]]+)\]\s*:\s*(.+)\s*$', text)
    if inline_m:
        return inline_m.group(1), "", inline_m.group(2).strip()
    return None


def _parse_cgr_step(name: str, header: str, body: list, ln: int, err,
                    allow_no_run_with_children: bool = False) -> ASTResource:
    """Parse a [step name] block into an ASTResource."""
    hp = _parse_cgr_header_props(header)
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
    # Provisioning fields (Phase 2)
    prov_block_dest: str|None = None; prov_block_marker: str|None = None
    prov_block_inline: str|None = None; prov_block_from: str|None = None
    prov_block_absent: bool = False
    prov_ini_dest: str|None = None; prov_ini_ops: list[tuple] = []
    prov_json_dest: str|None = None; prov_json_ops: list[tuple] = []
    prov_asserts: list[tuple] = []

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
            from_m = re.search(r'from\s+([\w/]+)', child_header)
            if from_m:
                child = _parse_cgr_template_child_instantiation(
                    child_name, from_m.group(1), child_body, bln)
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

        # skip if $ command  (supports multi-line continuation via indentation)
        if btext.startswith("skip if "):
            cmd = btext[8:].strip()
            if cmd.startswith("$ "): cmd = cmd[2:]
            check = cmd
            j = i + 1
            while j < len(body) and body[j][1] > bindent and not _is_cgr_keyword(body[j][2]):
                check += "\n" + body[j][2]; j += 1
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
        prop_m = re.match(r'(as\s+\w+|timeout\s+\d+[smh]?|retry\s+\d+x(?:\s+(?:wait|every)\s+\d+[smh]?)?(?:\s+with\s+backoff)?|if\s+fails?\s+(?:stop|warn|ignore))', btext)
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
            and not (allow_no_run_with_children and children)):
        raise err(f"Step [{name}] has no 'run' command", ln)
    if flags and run_cmd is None:
        raise err(f"Step [{name}] uses 'flag' without 'run'", ln)
    if until and hp.get("retries", 0) <= 0:
        raise err(f"Step [{name}] uses 'until' without 'retry'", ln)

    return ASTResource(
        name=slug, description=name, needs=needs,
        check=check, run=run_cmd, run_as=hp.get("run_as"), script_path=script_path,
        timeout=hp.get("timeout", 300), retries=hp.get("retries", 0),
        retry_delay=hp.get("retry_delay", 5), retry_backoff=hp.get("retry_backoff", False), on_fail=hp.get("on_fail", "stop"),
        when=when, env=env, is_verify=False, line=ln, collect_key=collect_key, collect_format=collect_format,
        tags=tags, children=children,
        http_method=http_method, http_url=http_url, http_headers=http_headers,
        http_body=http_body, http_body_type=http_body_type,
        http_auth=http_auth, http_expect=http_expect,
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
        flags=list(flags), env_when=dict(env_when), until=until)


def _parse_cgr_verify(desc: str, body: list, ln: int, err) -> ASTResource:
    """Parse a verify block."""
    needs: list[str] = []
    run_cmd: str | None = None
    retries = 0; retry_delay = 5

    for bln, bindent, btext in body:
        if btext.startswith("needs ") or btext.startswith("first "):
            refs = re.findall(r'\[([^\]]+)\]', btext)
            for ref in refs:
                needs.append(_parse_cgr_dep_ref(ref))
        elif btext.startswith("run "):
            cmd = btext[4:].strip()
            if cmd.startswith("$ "): cmd = cmd[2:]
            run_cmd = cmd
        elif btext.startswith("retry "):
            m = re.match(r'retry\s+(\d+)x(?:\s+wait\s+(\d+)(s|m)?)?', btext)
            if m:
                retries = int(m.group(1))
                if m.group(2):
                    retry_delay = int(m.group(2))
                    if m.group(3) == "m": retry_delay *= 60

    if not run_cmd:
        raise err(f"verify block has no 'run' command", ln)

    return ASTResource(
        name=f"__verify_{ln}", description=desc, needs=needs,
        check=None, run=run_cmd, run_as=None, timeout=30,
        retries=retries, retry_delay=retry_delay, retry_backoff=False, on_fail="warn",
        when=None, env={}, is_verify=True, line=ln, children=[])


# ═══════════════════════════════════════════════════════════════════════════════
# 4. REPOSITORY — load templates from the filesystem
# ═══════════════════════════════════════════════════════════════════════════════

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

# ═══════════════════════════════════════════════════════════════════════════════
# 5. RESOLVER — template expansion, dedup, DAG construction
# ═══════════════════════════════════════════════════════════════════════════════

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
                line=body.line, script_path=body.script_path,
                collect_key=body.collect_key, tags=list(body.tags), children=body.children,
                collect_format=body.collect_format, retry_backoff_max=body.retry_backoff_max,
                retry_jitter_pct=body.retry_jitter_pct, on_success_set=list(body.on_success_set),
                on_failure_set=list(body.on_failure_set), collect_var=body.collect_var,
                reduce_key=body.reduce_key, reduce_var=body.reduce_var,
                inner_template_name=body.inner_template_name,
                inner_template_args=dict(body.inner_template_args), flags=list(body.flags),
                env_when=dict(body.env_when), until=body.until)
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
                            line=pbody.line, script_path=pbody.script_path,
                            collect_key=pbody.collect_key, tags=list(pbody.tags), children=[],
                            collect_format=pbody.collect_format, retry_backoff_max=pbody.retry_backoff_max,
                            retry_jitter_pct=pbody.retry_jitter_pct, on_success_set=list(pbody.on_success_set),
                            on_failure_set=list(pbody.on_failure_set), collect_var=pbody.collect_var,
                            reduce_key=pbody.reduce_key, reduce_var=pbody.reduce_var,
                            inner_template_name=pbody.inner_template_name,
                            inner_template_args=dict(pbody.inner_template_args), flags=list(pbody.flags),
                            env_when=dict(pbody.env_when), until=pbody.until)
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
                            line=pbody.line, script_path=pbody.script_path, collect_key=pbody.collect_key,
                            tags=list(pbody.tags), children=pbody.children,
                            collect_format=pbody.collect_format, retry_backoff_max=pbody.retry_backoff_max,
                            retry_jitter_pct=pbody.retry_jitter_pct, on_success_set=list(pbody.on_success_set),
                            on_failure_set=list(pbody.on_failure_set), collect_var=pbody.collect_var,
                            reduce_key=pbody.reduce_key, reduce_var=pbody.reduce_var,
                            inner_template_name=pbody.inner_template_name,
                            inner_template_args=dict(pbody.inner_template_args), flags=list(pbody.flags),
                            env_when=dict(pbody.env_when), until=pbody.until)
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

# ═══════════════════════════════════════════════════════════════════════════════
# 6. PLAN / APPLY (same core as v2 but with provenance display)
# ═══════════════════════════════════════════════════════════════════════════════

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
    print(bold("─" * 64)); print()

    total = sum(len(w) for w in graph.waves)
    n_check = n_run = n_verify = n_tag_skip = 0

    for wi, wave in enumerate(graph.waves):
        print(dim(f"  ── Wave {wi+1} ({len(wave)} resources) ──"))
        for rid in wave:
            res = graph.all_resources[rid]
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
    try:
        stdin_data = None
        if _become_pass and res.run_as:
            # Pipe sudo password via stdin using sudo -S
            if isinstance(full, str):
                full = full.replace("sudo -u", "sudo -S -u")
            elif isinstance(full, list):
                full = [x.replace("sudo -u", "sudo -S -u") if "sudo" in x else x for x in full]
            stdin_data = (_become_pass + "\n").encode()
        use_pty = bool(on_output)
        if (not use_pty and node.via_method != "ssh" and stdin_data is None
                and isinstance(full, str) and "ssh-copy-id" in full and sys.stdin and sys.stdin.isatty()):
            use_pty = True
        if use_pty:
            master_fd, slave_fd = pty.openpty()
            echo_pty = on_output is None
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
            elif sys.stdin and sys.stdin.isatty():
                def _pump_stdin():
                    stdin_fd = None
                    stdin_attrs = None
                    try:
                        stdin_fd = sys.stdin.fileno()
                        stdin_attrs = termios.tcgetattr(stdin_fd)
                        tty.setcbreak(stdin_fd)
                        cur_attrs = termios.tcgetattr(stdin_fd)
                        cur_attrs[3] &= ~termios.ECHO
                        termios.tcsetattr(stdin_fd, termios.TCSANOW, cur_attrs)
                        while proc.poll() is None:
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
                        if stdin_fd is not None and stdin_attrs is not None:
                            try:
                                termios.tcsetattr(stdin_fd, termios.TCSANOW, stdin_attrs)
                            except termios.error:
                                pass
                threading.Thread(target=_pump_stdin, daemon=True).start()

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
                    if on_output:
                        on_output("stdout", tail)
                proc.wait()
                output = "".join(out_chunks).replace("\r\n", "\n").replace("\r", "\n")
                return proc.returncode, output, ""
            finally:
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
    )

def _effective_run_cmd(res: Resource, graph_file: str|None = None) -> str:
    if res.run:
        return res.run
    if res.script_path:
        return shlex.quote(_resolve_script_fs_path(res.script_path, graph_file))
    return ""

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
                  accumulated_collected=None, on_output=None, register_proc=None, unregister_proc=None, cancel_check=None):
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
    if dry_run:
        is_http = bool(res.http_method and res.http_url)
        if is_http:
            parts = [f"dry-run: would {res.http_method} {res.http_url}"]
            if res.http_auth: parts.append(f"auth={res.http_auth[0]}")
            if res.http_headers: parts.append(f"headers={len(res.http_headers)}")
            if res.http_body_type: parts.append(f"body={res.http_body_type}")
            if res.http_expect: parts.append(f"expect={res.http_expect}")
            reason = " | ".join(parts)
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

def cmd_apply(graph, *, dry_run=False, max_parallel=4, progress_mode=False):
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
    inline_updates = progress_mode and _COLOR and max_parallel == 1
    runtime_collected: dict = {}  # key → list[str] for collect/reduce
    for wi,wave in enumerate(graph.waves):
        if hard_fail: break
        print(dim(f"── Wave {wi+1}/{len(graph.waves)} ({done}/{total} done, {len(wave)} to run) ──"))
        def _run_with_start(rid):
            nonlocal started
            res = graph.all_resources[rid]
            if progress_mode:
                with start_lock:
                    started += 1
                    _print_exec_start(started, total, res, graph, inline=inline_updates)
            return exec_resource(
                res,
                graph.nodes[res.node_name],
                dry_run=dry_run,
                variables=graph.variables,
                graph_file=graph.graph_file,
                accumulated_collected=runtime_collected,
            )
        with ThreadPoolExecutor(max_workers=min(max_parallel,len(wave))) as pool:
            futs={pool.submit(_run_with_start, rid):rid for rid in wave}
            for f in as_completed(futs):
                rid=futs[f]; r=f.result(); results.append(r); done+=1
                res=graph.all_resources[rid]
                _print_exec(done,total,res,r,graph,inline=inline_updates)
                if r.var_bindings:
                    graph.variables.update(r.var_bindings)
                if res.collect_key and r.status == Status.SUCCESS:
                    runtime_collected.setdefault(res.collect_key, []).append(r.stdout)
                if r.status in (Status.FAILED, Status.TIMEOUT): hard_fail=True
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
        Status.TIMEOUT:   (red("⏱ time "),red(f"timed out after {res.timeout}s")),
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

# ═══════════════════════════════════════════════════════════════════════════════
# 6b. STATE TRACKING — persistent execution journal for crash recovery
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class StateEntry:
    id: str; status: str; rc: int|None; ms: int; ts: str
    wave: int; check_rc: int|None; manual: bool = False
    observed_value: str|None = None
    var_bindings: dict[str, str] = field(default_factory=dict)

class StateFile:
    """Append-only JSON Lines state journal with crash-safe writes."""

    def __init__(self, path: str|Path):
        self.path = Path(path)
        self.lock_path = Path(f"{self.path}.lock")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._entries: dict[str, StateEntry] = {}  # latest entry per resource id
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
            f.flush()
            os.fsync(f.fileno())
        new_lines = len(self._entries)
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

def _output_path(graph_file: str) -> str:
    return graph_file + ".output"


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


def cmd_apply_stateful(graph, graph_file, *, dry_run=False, max_parallel=4, no_resume=False, verbose=False, global_timeout=None, start_from=None, log_file=None, on_complete=None, become_pass=None, blast_radius=False, include_tags=None, exclude_tags=None, skip_steps=None, confirm=False, run_id=None, state_path=None, progress_mode=False):
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
    state = StateFile(sp) if not no_resume else None

    # Output collector for steps with collect clauses
    has_collect = any(r.collect_key for r in graph.all_resources.values())
    output = OutputFile(_output_path(graph_file)) if has_collect and not dry_run else None

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
    runtime_collected: dict = {}  # key → list[str] for collect/reduce
    try:
        with lock_cm:
            for wi, wave in enumerate(graph.waves):
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

                skip_info = []
                if to_skip_start: skip_info.append(f"{len(to_skip_start)} before start")
                if to_skip_state: skip_info.append(f"{len(to_skip_state)} from state")
                if to_skip_manual: skip_info.append(f"{len(to_skip_manual)} skipped")
                if to_skip_tag: skip_info.append(f"{len(to_skip_tag)} by tag")
                if skip_info:
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
                    print(dim(f"── Wave {wi+1}/{len(graph.waves)} ({len(to_run)} resources) ──"))

                if not to_run: continue

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
                        gtype, glabel, ginfo = _describe_group(group_key, len(group_rids), limit, is_race)
                        print(f"  {cyan(gtype)} {bold(glabel)} {dim(ginfo)}")

                    gi = "    " if group_key is not None else ""  # group indent

                    inline_updates = progress_mode and _COLOR and limit == 1

                    def _run_with_start(rid, *, cancel_check=None, register_proc=None, unregister_proc=None):
                        nonlocal started
                        res = graph.all_resources[rid]
                        if progress_mode:
                            with start_lock:
                                started += 1
                                _print_exec_start(started, total, res, graph, indent=gi, inline=inline_updates)
                        return exec_resource(
                            res,
                            graph.nodes[res.node_name],
                            dry_run=dry_run,
                            blast_radius=blast_radius,
                            variables=graph.variables,
                            accumulated_collected=runtime_collected,
                            cancel_check=cancel_check,
                            register_proc=register_proc,
                            unregister_proc=unregister_proc,
                        )

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
    except RuntimeError as e:
        print(red(f"error: {e}"))
        print(f"  If the process crashed, run: cgr state reset {graph_file}")
        sys.exit(1)
    finally:
        # Always release lock and close SSH pool, even on crash/interrupt
        _become_pass = None
        if has_ssh and not dry_run:
            _ssh_pool_stop()

    _print_summary(results)
    if output and output.all_outputs():
        op = _output_path(graph_file)
        n = len(output.all_outputs())
        print(f"  {cyan('📋 Collected')} {n} output(s) → {dim(op)}")
        print(f"  View with: cgr report {graph_file}")
        print()
    wall_ms = int((time.monotonic() - wall_t0) * 1000)

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

# ── Report command ─────────────────────────────────────────────────────

def cmd_report(graph_file: str, *, fmt: str = "table", output_file: str|None = None,
               filter_keys: list[str]|None = None):
    """Generate a report from collected outputs."""
    op = _output_path(graph_file)
    if not Path(op).exists():
        print(red(f"error: No output file found at {op}"))
        print(dim(f"  Run 'cgr apply {graph_file}' on a graph with 'collect' clauses first."))
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
    print(dim(f"  Export: cgr report {graph_file} --format csv|json [-o FILE]"))
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
        vars_part = ", ".join(te.loop_vars) if hasattr(te, 'loop_vars') else te.loop_var
        limit_str = f", {te.limit} at a time" if hasattr(te, 'limit') and te.limit else ""
        lines.append(f"each {vars_part} in ${{{te.list_expr}}}{limit_str}:")
        # Emit the inner node
        node = te.node
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
                    if an == "__each__":
                        after_parts.append("each")
                    else:
                        after_parts.append(f'"{an}"')
                after_str = f", after {', '.join(after_parts)}"
            lines.append(f'  target "{node.name}" ssh {tgt}{after_str}:')
        else:
            after_str = ""
            if node.after_nodes:
                after_parts = []
                for an in node.after_nodes:
                    if an == "__each__":
                        after_parts.append("each")
                    else:
                        after_parts.append(f'"{an}"')
                after_str = f", after {', '.join(after_parts)}"
            lines.append(f'  target "{node.name}" local{after_str}:')
        for res in node.resources:
            _emit_cgr_resource(res, lines, indent=4)
        for inst in node.instantiations:
            human = getattr(inst, '_cgr_human_name', inst.template_name)
            lines.append(f"    [{human}] from {inst.template_name}:")
            for k, v in inst.args.items():
                lines.append(f'      {k} = "{v}"')
            lines.append("")
        lines.append("")

    # Nodes
    for node in prog.nodes:
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
                    if an == "__each__":
                        after_parts.append("each")
                    else:
                        after_parts.append(f'"{an}"')
                after_str = f", after {', '.join(after_parts)}"
            lines.append(f'target "{node.name}" ssh {tgt}{after_str}:')
        else:
            after_str = ""
            if node.after_nodes:
                after_parts = []
                for an in node.after_nodes:
                    if an == "__each__":
                        after_parts.append("each")
                    else:
                        after_parts.append(f'"{an}"')
                after_str = f", after {', '.join(after_parts)}"
            lines.append(f'target "{node.name}" local{after_str}:')

        for res in node.resources:
            _emit_cgr_resource(res, lines, indent=2)
        for inst in node.instantiations:
            human = getattr(inst, '_cgr_human_name', inst.template_name)
            lines.append(f"  [{human}] from {inst.template_name}:")
            for k, v in inst.args.items():
                lines.append(f'    {k} = "{v}"')
            lines.append("")
        lines.append("")

    return "\n".join(lines)


def _emit_cgr_dep_ref(dep: str) -> str:
    """Render a dependency reference in .cgr syntax."""
    return "[" + "/".join(part.replace("_", " ") for part in dep.split(".")) + "]"


def _emit_cgr_resource(res: ASTResource, lines: list, indent: int = 2):
    """Emit a single ASTResource in .cgr format."""
    pfx = " " * indent
    desc = res.description or res.name
    header_parts = []
    if res.run_as:
        header_parts.append(f"as {res.run_as}")
    if res.timeout != 300:
        header_parts.append(f"timeout {res.timeout}s")
    if res.retries > 0:
        header_parts.append(f"retry {res.retries}x")
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
        if res.script_path:
            lines.append(f'{pfx}  script "{res.script_path}"')
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
            lines.append(f'{pfx}  script "{res.script_path}"')
        elif res.run:
            lines.append(f"{pfx}  run $ {res.run}")
        lines.append("")
        return

    lines.append(f"{pfx}[{desc}]{header}:")

    for dep in res.needs:
        lines.append(f"{pfx}  first {_emit_cgr_dep_ref(dep)}")

    if res.check and res.check.strip().lower() == "false":
        if res.script_path:
            lines.append(f'{pfx}  script "{res.script_path}"')
        elif res.run:
            lines.append(f"{pfx}  always run $ {res.run}")
    else:
        if res.check:
            lines.append(f"{pfx}  skip if $ {res.check}")
        if res.script_path:
            lines.append(f'{pfx}  script "{res.script_path}"')
        elif res.run:
            lines.append(f"{pfx}  run $ {res.run}")

    if res.when:
        lines.append(f'{pfx}  when "{res.when}"')
    for k, v in res.env.items():
        env_when = res.env_when.get(k)
        suffix = f' when "{env_when}"' if env_when else ""
        lines.append(f'{pfx}  env {k} = "{v}"{suffix}')
    for flag_text, flag_when in res.flags:
        suffix = f' when "{flag_when}"' if flag_when else ""
        lines.append(f'{pfx}  flag "{flag_text}"{suffix}')
    if res.until is not None:
        lines.append(f'{pfx}  until "{res.until}"')
    if res.collect_key:
        lines.append(f'{pfx}  collect "{res.collect_key}"')
    if res.tags:
        lines.append(f'{pfx}  tags {", ".join(res.tags)}')

    # Parallel constructs
    if res.parallel_block:
        limit_str = f" {res.parallel_limit} at a time" if res.parallel_limit else ""
        policy_str = ""
        if res.parallel_fail_policy == "stop":
            policy_str = ", if one fails stop all"
        elif res.parallel_fail_policy == "ignore":
            policy_str = ", if one fails ignore"
        lines.append(f"{pfx}  parallel{limit_str}{policy_str}:")
        for child in res.parallel_block:
            _emit_cgr_resource(child, lines, indent + 4)

    if res.race_block:
        into_str = f" into {res.race_into}" if res.race_into else ""
        lines.append(f"{pfx}  race{into_str}:")
        for child in res.race_block:
            _emit_cgr_resource(child, lines, indent + 4)

    if res.each_var and res.each_body:
        limit_str = f", {res.each_limit} at a time" if res.each_limit else ""
        lines.append(f"{pfx}  each {res.each_var} in {res.each_list_expr}{limit_str}:")
        _emit_cgr_resource(res.each_body, lines, indent + 4)

    for child in res.children:
        _emit_cgr_resource(child, lines, indent + 2)

    lines.append("")


def _emit_cg(prog: ASTProgram) -> str:
    """Emit an ASTProgram as .cg format text."""
    lines = []
    for v in prog.variables:
        lines.append(f'var {v.name} = "{v.value}"')
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
        lines.append(f'use "{u.path}"{ver}')
    if prog.uses:
        lines.append("")
    for inv in prog.inventories:
        lines.append(f'inventory "{inv.path}"')
    if prog.inventories:
        lines.append("")
    if prog.gather_facts:
        lines.append("gather facts")
        lines.append("")
    for node in prog.nodes:
        via = node.via
        if via.method == "ssh":
            props = " ".join(f'{k} = "{v}"' for k, v in via.props.items())
            lines.append(f'node "{node.name}" {{')
            lines.append(f"  via ssh {props}")
        else:
            lines.append(f'node "{node.name}" {{')
            lines.append(f"  via local")
        if node.after_nodes:
            for an in node.after_nodes:
                if an == "__each__":
                    lines.append(f"  after each")
                else:
                    lines.append(f'  after "{an}"')
        lines.append("")
        for res in node.resources:
            _emit_cg_resource(res, lines, indent=2)
        for inst in node.instantiations:
            lines.append(f"  {inst.template_name} {{")
            for k, v in inst.args.items():
                lines.append(f'    {k} = "{v}"')
            lines.append("  }")
        lines.append("}")
        lines.append("")
    return "\n".join(lines)


def _emit_cg_resource(res: ASTResource, lines: list, indent: int = 2):
    """Emit a single ASTResource in .cg format."""
    pfx = " " * indent
    rtype = "verify" if res.is_verify else "resource"
    if res.is_verify:
        lines.append(f'{pfx}verify "{res.description or res.name}" {{')
    else:
        lines.append(f"{pfx}resource {res.name} {{")
    if res.description and not res.is_verify:
        lines.append(f'{pfx}  description "{res.description}"')
    if res.needs:
        lines.append(f'{pfx}  needs {", ".join(res.needs)}')
    if res.check:
        lines.append(f"{pfx}  check `{res.check}`")
    if res.script_path:
        lines.append(f'{pfx}  script "{res.script_path}"')
    if res.run:
        lines.append(f"{pfx}  run `{res.run}`")
    if res.run_as:
        lines.append(f"{pfx}  as {res.run_as}")
    if res.timeout != 300:
        lines.append(f"{pfx}  timeout {res.timeout}")
    if res.retries > 0:
        lines.append(f"{pfx}  retry {res.retries}")
    if res.on_fail not in ("stop",):
        lines.append(f"{pfx}  on_fail {res.on_fail}")
    if res.when:
        lines.append(f'{pfx}  when "{res.when}"')
    for k, v in res.env.items():
        env_when = res.env_when.get(k)
        suffix = f' when "{env_when}"' if env_when else ""
        lines.append(f'{pfx}  env {k} = "{v}"{suffix}')
    for flag_text, flag_when in res.flags:
        suffix = f' when "{flag_when}"' if flag_when else ""
        lines.append(f'{pfx}  flag "{flag_text}"{suffix}')
    if res.until is not None:
        lines.append(f'{pfx}  until "{res.until}"')
    if res.collect_key:
        lines.append(f'{pfx}  collect "{res.collect_key}"')
    if res.tags:
        lines.append(f'{pfx}  tags {", ".join(res.tags)}')
    # Parallel constructs
    if res.parallel_block:
        limit_str = f" {res.parallel_limit}" if res.parallel_limit else ""
        fp = res.parallel_fail_policy
        policy_str = f", if_one_fails {fp}" if fp and fp != "wait" else ""
        lines.append(f"{pfx}  parallel{limit_str}{policy_str} {{")
        for child in res.parallel_block:
            _emit_cg_resource(child, lines, indent + 4)
        lines.append(f"{pfx}  }}")
    if res.race_block:
        into_str = f' into "{res.race_into}"' if res.race_into else ""
        lines.append(f"{pfx}  race{into_str} {{")
        for child in res.race_block:
            _emit_cg_resource(child, lines, indent + 4)
        lines.append(f"{pfx}  }}")
    if res.each_var and res.each_body:
        limit_str = f", {res.each_limit}" if res.each_limit else ""
        lines.append(f'{pfx}  each {res.each_var} in "{res.each_list_expr}"{limit_str} {{')
        _emit_cg_resource(res.each_body, lines, indent + 4)
        lines.append(f"{pfx}  }}")
    if res.stage_name and res.stage_phases:
        lines.append(f'{pfx}  stage "{res.stage_name}" {{')
        for phase in res.stage_phases:
            lines.append(f'{pfx}    phase "{phase.name}" {phase.count_expr} from "{phase.from_list}" {{')
            if phase.each_limit:
                lines.append(f'{pfx}      each server, {phase.each_limit} {{')
                for child in phase.body:
                    _emit_cg_resource(child, lines, indent + 8)
                lines.append(f"{pfx}      }}")
            else:
                for child in phase.body:
                    _emit_cg_resource(child, lines, indent + 6)
            lines.append(f"{pfx}    }}")
        lines.append(f"{pfx}  }}")
    for child in res.children:
        _emit_cg_resource(child, lines, indent + 2)
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
        if ra.run != rb.run or ra.check != rb.check:
            diffs = []
            if ra.run != rb.run: diffs.append("command")
            if ra.check != rb.check: diffs.append("check")
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

            # Phase 4: Check all resources have run or http_method
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
                if not res.run and not res.script_path and not res.http_method and not has_provisioning and not has_descendants:
                    errors.append((name, f"Resource '{res.short_name}' has no run command or HTTP method"))
                    failed += 1
                    if verbose:
                        print(f"  {red('✗')} {name}: resource '{res.short_name}' missing run/HTTP")
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
    ok = major >= 3 and minor >= 9
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


def _masked_secret_display(value: str) -> str:
    """Return a non-reversible display string for secret values."""
    if not value:
        return "<hidden>"
    return f"<hidden:{len(value)} chars>"


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
        m = re.match(r'(\w+)\s*=\s*"([^"]*)"', line)
        if not m:
            m = re.match(r'(\w+)\s*=\s*(\S+)', line)
        if m:
            result[m.group(1)] = m.group(2)
    return result


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
        initial = b"# CommandGraph secrets file\n# Format: KEY = \"value\"\n\n"
        encrypted = _encrypt_data(initial, passphrase)
        path.write_bytes(encrypted)
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
            for k, v in secrets.items():
                display_value = _masked_secret_display(v)
                print(f"  {cyan(k)} = {dim(display_value)}")
            if not secrets:
                print(dim("  (empty)"))
        except (ValueError, RuntimeError) as e:
            print(red(f"error: {e}")); sys.exit(1)

    elif action == "add":
        if not path.exists():
            print(red(f"error: {filepath} not found")); sys.exit(1)
        if not key or value is None:
            print(red("Usage: cgr secrets add FILE KEY VALUE")); sys.exit(1)
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
                new_lines.append(f'{key} = "{value}"')
                updated = True
            else:
                new_lines.append(line)
        if not updated:
            new_lines.append(f'{key} = "{value}"  # @updated {time.strftime("%Y-%m-%d")}')
        else:
            # Update the timestamp on the modified line
            new_lines = [re.sub(r'#\s*@updated\s+\S+', f'# @updated {time.strftime("%Y-%m-%d")}', l)
                         if l.strip().startswith(f'{key} ') else l for l in new_lines]
        encrypted = _encrypt_data("\n".join(new_lines).encode(), passphrase)
        path.write_bytes(encrypted)
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
        encrypted = _encrypt_data("\n".join(new_lines).encode(), passphrase)
        path.write_bytes(encrypted)
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
            encrypted = _encrypt_data(new_text.encode(), passphrase)
            path.write_bytes(encrypted)
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


# ═══════════════════════════════════════════════════════════════════════════════
# 7. DOT OUTPUT (with provenance coloring)
# ═══════════════════════════════════════════════════════════════════════════════
def cmd_dot(graph):
    wm={}
    for wi,w in enumerate(graph.waves):
        for rid in w: wm[rid]=wi
    colors=["#c6e5c6","#c6d4e5","#e5d4c6","#e5c6d4","#d4e5c6","#d4c6e5"]
    src_colors={}; ci=0
    for p in graph.provenance_log:
        if p.source_file not in src_colors:
            src_colors[p.source_file]=colors[ci%len(colors)]; ci+=1
    print("digraph commandgraph {"); print('  rankdir=TB;')
    print('  node [shape=box, style="rounded,filled", fontname="monospace", fontsize=10];')
    # Legend
    for sf,sc in src_colors.items():
        print(f'  "legend_{sf}" [label="← {sf}", fillcolor="{sc}", shape=note, fontsize=8];')
    print()
    for rid,res in graph.all_resources.items():
        c=("#f0c6f0" if res.is_verify else
           "#c6efe5" if res.script_path else
           (src_colors.get(res.provenance.source_file,"#e5e5e5") if res.provenance else colors[wm.get(rid,0)%len(colors)]))
        label_name = f"[script] {res.short_name}" if res.script_path else res.short_name
        l=f"{label_name}\\n{res.description[:30]}"
        shared=graph.dedup_map.get(res.identity_hash,[])
        if len(shared)>1: l+=f"\\n(shared x{len(shared)})"
        print(f'  "{rid}" [label="{l}", fillcolor="{c}"];')
    print()
    for rid,res in graph.all_resources.items():
        for dep in res.needs:
            if dep in graph.all_resources:
                dep_res = graph.all_resources[dep]
                if dep_res.node_name != res.node_name:
                    print(f'  "{dep}" -> "{rid}" [style=dashed, color="#4488cc", penwidth=1.5];')
                else:
                    print(f'  "{dep}" -> "{rid}";')
    print("}")

# ═══════════════════════════════════════════════════════════════════════════════
# 8. HTML VISUALIZER
# ═══════════════════════════════════════════════════════════════════════════════

def _graph_to_json(graph: Graph, state_data: dict|None = None) -> str:
    """Serialize a resolved graph into JSON for the HTML visualizer."""
    sensitive = graph.sensitive_values if hasattr(graph, "sensitive_values") else set()
    resources = []
    for rid, res in graph.all_resources.items():
        r = {
            "id": rid, "short_name": res.short_name, "node_name": res.node_name,
            "description": res.description,
            "needs": [d for d in res.needs if d in graph.all_resources],
            "check": res.check or "", "run": res.run, "script_path": res.script_path or "",
            "run_as": res.run_as or "", "timeout": res.timeout,
            "retries": res.retries, "retry_delay": res.retry_delay,
            "on_fail": res.on_fail.value, "is_verify": res.is_verify,
            "provenance": res.provenance.source_file if res.provenance else None,
            "identity_hash": res.identity_hash,
            "parallel_group": res.parallel_group,
            "concurrency_limit": res.concurrency_limit,
            "is_race": res.is_race,
            "race_into": res.race_into,
            "is_barrier": res.is_barrier,
            "source_line": res.source_line,
            "collect_key": res.collect_key,
            "http_method": res.http_method,
        }
        if state_data and rid in state_data:
            r["state"] = state_data[rid]
        resources.append(r)
    nodes_info = {}
    for nn, hn in graph.nodes.items():
        nodes_info[nn] = {
            "via": hn.via_method,
            "host": hn.via_props.get("host", ""),
            "user": hn.via_props.get("user", ""),
            "port": hn.via_props.get("port", "22"),
        }
    provenance = []
    for p in graph.provenance_log:
        provenance.append({
            "source": p.source_file,
            "template": p.template_name,
            "params": p.params_used,
        })
    dedup = {h: ids for h, ids in graph.dedup_map.items() if len(ids) > 1}

    data = {
        "resources": resources,
        "waves": graph.waves,
        "nodes": nodes_info,
        "provenance": provenance,
        "dedup": dedup,
        "variables": graph.variables,
    }
    return json.dumps(_redact_obj(data, sensitive))


def cmd_visualize(graph: Graph, output_path: str, source_file: str = "", state_path: str|None = None):
    """Generate a self-contained interactive HTML visualization."""
    state_data = {}
    if state_path and Path(state_path).exists():
        sf = StateFile(state_path)
        for rid, se in sf.all_entries().items():
            state_data[rid] = {"status": se.status, "ms": se.ms, "ts": se.ts}

    graph_json = _graph_to_json(graph, state_data=state_data)

    # Determine title from first node name or source file
    first_node = next(iter(graph.nodes), "graph")
    title_text = Path(source_file).stem if source_file else first_node

    html = _build_html(graph_json, title_text, source_file)

    Path(output_path).write_text(html)
    print(green(f"✓ Visualization written to {output_path}"))
    print(f"  Open in browser: {dim('file://' + str(Path(output_path).resolve()))}")


# BEGIN GENERATED: VISUALIZER TEMPLATE
# Generated by build_cgr.py from visualize_template.py. Do not edit by hand.
def _build_html(graph_json: str, title: str = "CommandGraph", source_file: str = "") -> str:
    """Generate a complete self-contained HTML visualization."""
    safe_title = (title.replace("&", "&amp;")
                       .replace("<", "&lt;")
                       .replace(">", "&gt;")
                       .replace('"', "&quot;")
                       .replace("'", "&#x27;"))
    safe_source_file = (source_file.replace("&", "&amp;")
                                   .replace("<", "&lt;")
                                   .replace(">", "&gt;")
                                   .replace('"', "&quot;")
                                   .replace("'", "&#x27;"))
    safe_graph_json = graph_json.replace("<", "\\u003c")

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{safe_title} — CommandGraph</title>
<style>
:root {{
  --bg: #0e1117; --bg2: #161b22; --bg3: #1c2129; --bg-hover: #21262d;
  --border: #30363d; --border-hi: #484f58;
  --text: #e6edf3; --text2: #8b949e; --text3: #6e7681;
  --accent: #58a6ff; --green: #3fb950; --amber: #d29922;
  --red: #f85149; --purple: #bc8cff; --coral: #f0883e;
  --teal: #39d353; --pink: #f778ba;
  --font: "SF Mono", "Cascadia Code", "Fira Code", "JetBrains Mono", "Consolas", monospace;
  --font-body: "Segoe UI", system-ui, -apple-system, sans-serif;
  --radius: 6px; --radius-lg: 10px;
}}
@media (prefers-color-scheme: light) {{
  :root {{
    --bg: #f6f8fa; --bg2: #ffffff; --bg3: #f0f2f5; --bg-hover: #eaeef2;
    --border: #d0d7de; --border-hi: #afb8c1;
    --text: #1f2328; --text2: #656d76; --text3: #8c959f;
    --accent: #0969da; --green: #1a7f37; --amber: #9a6700;
    --red: #cf222e; --purple: #8250df; --coral: #bc4c00;
    --teal: #0f6e56; --pink: #bf3989;
  }}
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  background: var(--bg); color: var(--text); font-family: var(--font-body);
  line-height: 1.5; min-height: 100vh;
}}
.header {{
  background: var(--bg2); border-bottom: 1px solid var(--border);
  padding: 16px 24px; display: flex; align-items: center; gap: 16px;
  position: sticky; top: 0; z-index: 100;
}}
.header h1 {{
  font-family: var(--font); font-size: 15px; font-weight: 600;
  letter-spacing: -0.3px; color: var(--text);
}}
.header .meta {{
  font-size: 12px; color: var(--text2); font-family: var(--font);
  display: flex; gap: 16px;
}}
.header .meta span {{ display: flex; align-items: center; gap: 4px; }}
.header .dot {{ width: 7px; height: 7px; border-radius: 50%; display: inline-block; }}
.controls {{
  display: flex; gap: 8px; margin-left: auto; align-items: center;
}}
.controls button {{
  background: var(--bg3); border: 1px solid var(--border); border-radius: var(--radius);
  color: var(--text2); font-size: 12px; padding: 4px 10px; cursor: pointer;
  font-family: var(--font); transition: all 0.15s;
}}
.controls button:hover {{ background: var(--bg-hover); color: var(--text); border-color: var(--border-hi); }}
.controls button.active {{ background: var(--accent); color: #fff; border-color: var(--accent); }}
.layout {{ display: flex; height: calc(100vh - 53px); }}
.graph-panel {{
  flex: 1; overflow: auto; position: relative; padding: 24px;
}}
.detail-panel {{
  width: 340px; min-width: 340px; background: var(--bg2);
  border-left: 1px solid var(--border); overflow-y: auto;
  transition: width 0.2s, min-width 0.2s, padding 0.2s;
}}
.detail-panel.collapsed {{ width: 0; min-width: 0; padding: 0; overflow: hidden; }}
.detail-inner {{ padding: 16px; }}
.detail-inner h2 {{
  font-family: var(--font); font-size: 14px; font-weight: 600;
  margin: 0 0 12px; color: var(--accent); word-break: break-all;
}}
.detail-inner .empty {{
  color: var(--text3); font-size: 13px; font-style: italic; padding: 32px 0;
  text-align: center;
}}
.d-section {{ margin: 0 0 16px; }}
.d-section h3 {{
  font-size: 11px; font-weight: 600; text-transform: uppercase;
  letter-spacing: 0.5px; color: var(--text3); margin: 0 0 6px;
}}
.d-row {{ display: flex; gap: 8px; margin: 0 0 4px; font-size: 13px; }}
.d-key {{ color: var(--text2); min-width: 72px; flex-shrink: 0; }}
.d-val {{ color: var(--text); font-family: var(--font); font-size: 12px; word-break: break-all; }}
.d-cmd {{
  background: var(--bg3); border: 1px solid var(--border); border-radius: var(--radius);
  padding: 8px 10px; font-family: var(--font); font-size: 11px;
  color: var(--text); white-space: pre-wrap; word-break: break-all;
  margin: 4px 0; line-height: 1.6;
}}
.d-badge {{
  display: inline-block; padding: 2px 8px; border-radius: 10px;
  font-size: 11px; font-weight: 500; font-family: var(--font);
}}
.d-provenance {{
  background: var(--bg3); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 8px 10px; margin: 4px 0;
}}
.d-provenance .src {{ font-family: var(--font); font-size: 12px; color: var(--accent); }}
.d-provenance .params {{ font-size: 11px; color: var(--text2); margin-top: 4px; }}
.d-deps {{
  display: flex; flex-wrap: wrap; gap: 4px; margin: 4px 0;
}}
.d-dep-chip {{
  background: var(--bg3); border: 1px solid var(--border); border-radius: var(--radius);
  padding: 2px 8px; font-size: 11px; font-family: var(--font); color: var(--text2);
  cursor: pointer; transition: all 0.15s;
}}
.d-dep-chip:hover {{ border-color: var(--accent); color: var(--accent); }}
.wave-container {{ margin: 0 0 4px; }}
.wave-label {{
  font-family: var(--font); font-size: 11px; color: var(--text3);
  font-weight: 600; letter-spacing: 0.3px; padding: 0 0 6px;
  display: flex; align-items: center; gap: 8px;
}}
.wave-label::after {{
  content: ""; flex: 1; height: 1px; background: var(--border);
}}
.wave-row {{
  display: flex; flex-wrap: wrap; gap: 8px; padding: 8px 0 8px 16px;
  border-left: 2px solid var(--border); margin-left: 4px;
}}
.node-card {{
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: var(--radius); padding: 8px 12px; cursor: pointer;
  transition: all 0.15s; position: relative; max-width: 220px;
}}
.node-card:hover {{ border-color: var(--border-hi); background: var(--bg-hover); transform: translateY(-1px); }}
.node-card.selected {{ border-color: var(--accent); box-shadow: 0 0 0 1px var(--accent); }}
.node-card.dep-highlight {{ border-color: var(--green); background: rgba(63,185,80,0.06); }}
.node-card.dependent-highlight {{ border-color: var(--purple); background: rgba(188,140,255,0.06); }}
.node-card.dimmed {{ opacity: 0.25; }}
.node-name {{
  font-family: var(--font); font-size: 12px; font-weight: 600;
  color: var(--text); margin: 0 0 2px; display: flex; align-items: center; gap: 6px;
}}
.node-desc {{
  font-size: 11px; color: var(--text2); line-height: 1.4;
  overflow: hidden; text-overflow: ellipsis; display: -webkit-box;
  -webkit-line-clamp: 2; -webkit-box-orient: vertical;
}}
.node-badges {{ display: flex; gap: 4px; margin-top: 4px; flex-wrap: wrap; }}
.badge {{
  font-size: 10px; padding: 1px 6px; border-radius: 8px;
  font-family: var(--font); font-weight: 500;
}}
.badge-prov {{ background: rgba(88,166,255,0.12); color: var(--accent); }}
.badge-shared {{ background: rgba(63,185,80,0.12); color: var(--green); }}
.badge-verify {{ background: rgba(247,120,186,0.12); color: var(--pink); }}
.badge-root {{ background: rgba(210,153,34,0.12); color: var(--amber); }}
.prov-indicator {{
  width: 3px; height: 100%; position: absolute; left: 0; top: 0;
  border-radius: var(--radius) 0 0 var(--radius);
}}
.legend {{
  display: flex; flex-wrap: wrap; gap: 12px; padding: 12px 24px;
  background: var(--bg2); border-bottom: 1px solid var(--border);
  font-size: 12px;
}}
.legend-item {{
  display: flex; align-items: center; gap: 5px; cursor: pointer;
  padding: 2px 8px; border-radius: var(--radius); transition: all 0.15s;
  border: 1px solid transparent;
}}
.legend-item:hover {{ background: var(--bg-hover); }}
.legend-item.active {{ border-color: var(--border-hi); background: var(--bg3); }}
.legend-dot {{ width: 8px; height: 8px; border-radius: 3px; flex-shrink: 0; }}
.legend-label {{ color: var(--text2); white-space: nowrap; }}
.svg-arrows {{
  position: absolute; top: 0; left: 0; pointer-events: none; z-index: 1;
}}
.arrow-path {{
  fill: none; stroke: var(--border-hi); stroke-width: 1; opacity: 0.4;
  transition: all 0.2s;
}}
.arrow-path.highlight-dep {{ stroke: var(--green); opacity: 0.8; stroke-width: 1.5; }}
.arrow-path.highlight-dependent {{ stroke: var(--purple); opacity: 0.8; stroke-width: 1.5; }}
.stats-bar {{
  display: flex; gap: 16px; padding: 10px 24px; background: var(--bg3);
  border-bottom: 1px solid var(--border); font-family: var(--font);
  font-size: 12px; color: var(--text2);
}}
.stat {{ display: flex; align-items: center; gap: 4px; }}
.stat-num {{ color: var(--text); font-weight: 600; }}
.search-box {{
  background: var(--bg3); border: 1px solid var(--border); border-radius: var(--radius);
  color: var(--text); font-size: 12px; padding: 4px 8px; width: 160px;
  font-family: var(--font); outline: none; transition: border-color 0.15s;
}}
.search-box:focus {{ border-color: var(--accent); }}
</style>

<div class="header">
  <h1>&#9671; {safe_title}</h1>
  <div class="meta">
    <span>from <code style="color:var(--accent)">{safe_source_file}</code></span>
  </div>
  <div class="controls">
    <input type="text" class="search-box" id="search" placeholder="Filter resources..." oninput="filterNodes(this.value)">
    <button id="btn-arrows" onclick="toggleArrows()" class="active">Arrows</button>
    <button id="btn-detail" onclick="toggleDetail()">Detail</button>
  </div>
</div>

<div id="legend" class="legend"></div>

<div id="stats" class="stats-bar"></div>

<div class="layout">
  <div class="graph-panel" id="graph-panel">
    <svg class="svg-arrows" id="svg-arrows"></svg>
    <div id="waves"></div>
  </div>
  <div class="detail-panel" id="detail-panel">
    <div class="detail-inner" id="detail-inner">
      <div class="empty">Click a resource to inspect</div>
    </div>
  </div>
</div>

<script>
const G = {safe_graph_json};

// ── Derived data ──
const provColors = {{}};
const palette = [
  "var(--accent)","var(--green)","var(--amber)","var(--coral)",
  "var(--purple)","var(--teal)","var(--pink)","var(--red)"
];
const paletteHex = ["#58a6ff","#3fb950","#d29922","#f0883e","#bc8cff","#39d353","#f778ba","#f85149"];
const paletteHexLight = ["#0969da","#1a7f37","#9a6700","#bc4c00","#8250df","#0f6e56","#bf3989","#cf222e"];
let ci = 0;
G.provenance.forEach(p => {{
  if (!provColors[p.source]) {{
    provColors[p.source] = {{ idx: ci, css: palette[ci % palette.length],
      hex: paletteHex[ci % paletteHex.length],
      hexLight: paletteHexLight[ci % paletteHexLight.length] }};
    ci++;
  }}
}});
provColors["__inline"] = {{ idx: ci, css: "var(--text3)", hex: "#6e7681", hexLight: "#8c959f" }};

const resById = {{}};
G.resources.forEach(r => {{ resById[r.id] = r; }});

// Reverse dep map: who depends on me?
const dependents = {{}};
G.resources.forEach(r => {{
  r.needs.forEach(dep => {{
    if (!dependents[dep]) dependents[dep] = [];
    dependents[dep].push(r.id);
  }});
}});

// Dedup lookup
const dedupOf = {{}};
Object.entries(G.dedup).forEach(([hash, ids]) => {{
  ids.forEach(id => {{ dedupOf[id] = ids; }});
}});

// ── Render legend ──
const legendEl = document.getElementById("legend");
Object.entries(provColors).forEach(([src, c]) => {{
  if (src === "__inline") return;
  const el = document.createElement("div");
  el.className = "legend-item";
  el.dataset.source = src;
  el.innerHTML = `<div class="legend-dot" style="background:${{c.hex}}"></div><span class="legend-label">${{src}}</span>`;
  el.onclick = () => filterBySource(src);
  legendEl.appendChild(el);
}});
// Inline
const inl = document.createElement("div");
inl.className = "legend-item"; inl.dataset.source = "__inline";
inl.innerHTML = `<div class="legend-dot" style="background:var(--text3)"></div><span class="legend-label">inline</span>`;
inl.onclick = () => filterBySource("__inline");
legendEl.appendChild(inl);

// ── Render stats ──
const statsEl = document.getElementById("stats");
const total = G.resources.length;
const nCheck = G.resources.filter(r => r.check && r.check !== "false").length;
const nRun = G.resources.filter(r => !r.is_verify && (!r.check || r.check === "false")).length;
const nVerify = G.resources.filter(r => r.is_verify).length;
const nShared = Object.values(G.dedup).filter(ids => ids.length > 1).length;
const nWaves = G.waves.length;
statsEl.innerHTML = `
  <span class="stat"><span class="stat-num">${{total}}</span> resources</span>
  <span class="stat"><span class="stat-num">${{nWaves}}</span> waves</span>
  <span class="stat"><span class="stat-num">${{nCheck}}</span> check&#x2192;run</span>
  <span class="stat"><span class="stat-num">${{nRun}}</span> run</span>
  <span class="stat"><span class="stat-num">${{nVerify}}</span> verify</span>
  <span class="stat"><span class="stat-num">${{nShared}}</span> deduped</span>
  ${{Object.keys(G.nodes).map(n => {{
    const nd = G.nodes[n];
    return `<span class="stat" style="margin-left:auto">&#9654; <span class="stat-num">${{n}}</span> via ${{nd.via}} ${{nd.host || "local"}}</span>`;
  }}).join("")}}
`;

// ── Render wave rows ──
const wavesEl = document.getElementById("waves");
G.waves.forEach((wave, wi) => {{
  const container = document.createElement("div");
  container.className = "wave-container";

  const label = document.createElement("div");
  label.className = "wave-label";
  label.textContent = `Wave ${{wi + 1}} (${{wave.length}})`;
  container.appendChild(label);

  const row = document.createElement("div");
  row.className = "wave-row";

  wave.forEach(rid => {{
    const res = resById[rid];
    if (!res) return;
    const card = document.createElement("div");
    card.className = "node-card";
    card.id = "card-" + rid.replace(/[^a-zA-Z0-9]/g, "_");
    card.dataset.id = rid;
    card.onclick = () => selectNode(rid);

    // Provenance indicator
    const src = res.provenance || "__inline";
    const pc = provColors[src] || provColors["__inline"];
    card.innerHTML = `<div class="prov-indicator" style="background:${{pc.hex}}"></div>`;

    // Name
    const nameEl = document.createElement("div");
    nameEl.className = "node-name";
    nameEl.textContent = res.short_name;
    card.appendChild(nameEl);

    // Description
    if (res.description) {{
      const descEl = document.createElement("div");
      descEl.className = "node-desc";
      descEl.textContent = res.description;
      card.appendChild(descEl);
    }}

    // Badges
    const badges = document.createElement("div");
    badges.className = "node-badges";
    if (res.provenance) {{
      const b = document.createElement("span");
      b.className = "badge badge-prov";
      b.textContent = res.provenance.split("/").pop();
      badges.appendChild(b);
    }}
    if (dedupOf[rid] && dedupOf[rid].length > 1) {{
      const b = document.createElement("span");
      b.className = "badge badge-shared";
      b.textContent = "shared x" + dedupOf[rid].length;
      badges.appendChild(b);
    }}
    if (res.is_verify) {{
      const b = document.createElement("span");
      b.className = "badge badge-verify";
      b.textContent = "verify";
      badges.appendChild(b);
    }}
    if (res.needs.length === 0) {{
      const b = document.createElement("span");
      b.className = "badge badge-root";
      b.textContent = "root";
      badges.appendChild(b);
    }}
    if (badges.children.length) card.appendChild(badges);

    row.appendChild(card);
  }});

  container.appendChild(row);
  wavesEl.appendChild(container);
}});

// ── SVG Arrows ──
let showArrows = true;
function drawArrows(highlightDeps, highlightDependents) {{
  const svg = document.getElementById("svg-arrows");
  const panel = document.getElementById("graph-panel");
  const rect = panel.getBoundingClientRect();
  svg.setAttribute("width", panel.scrollWidth);
  svg.setAttribute("height", panel.scrollHeight);
  svg.innerHTML = "";
  if (!showArrows) return;

  const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
  ["dep","dependent","normal"].forEach(type => {{
    const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
    marker.setAttribute("id", "ah-"+type);
    marker.setAttribute("viewBox", "0 0 10 10");
    marker.setAttribute("refX", "8"); marker.setAttribute("refY", "5");
    marker.setAttribute("markerWidth", "5"); marker.setAttribute("markerHeight", "5");
    marker.setAttribute("orient", "auto-start-reverse");
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("d", "M2 2L8 5L2 8");
    path.setAttribute("fill", "none");
    path.setAttribute("stroke", type==="dep" ? "#3fb950" : type==="dependent" ? "#bc8cff" : "#484f58");
    path.setAttribute("stroke-width", "1.5");
    marker.appendChild(path);
    defs.appendChild(marker);
  }});
  svg.appendChild(defs);

  const depSet = new Set(highlightDeps || []);
  const depntSet = new Set(highlightDependents || []);

  G.resources.forEach(res => {{
    res.needs.forEach(dep => {{
      const fromEl = document.getElementById("card-" + dep.replace(/[^a-zA-Z0-9]/g, "_"));
      const toEl = document.getElementById("card-" + res.id.replace(/[^a-zA-Z0-9]/g, "_"));
      if (!fromEl || !toEl) return;

      const fr = fromEl.getBoundingClientRect();
      const tr = toEl.getBoundingClientRect();
      const pr = panel.getBoundingClientRect();
      const sx = panel.scrollLeft; const sy = panel.scrollTop;

      const x1 = fr.left - pr.left + sx + fr.width / 2;
      const y1 = fr.top - pr.top + sy + fr.height;
      const x2 = tr.left - pr.left + sx + tr.width / 2;
      const y2 = tr.top - pr.top + sy;

      const isDep = depSet.has(dep) && depntSet.has(res.id);
      const isDepOf = depntSet.has(dep) && depSet.has(res.id);
      const isHighDep = depSet.has(dep);
      const isHighDepnt = depntSet.has(res.id);

      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      const my = y1 + (y2 - y1) * 0.5;
      path.setAttribute("d", `M${{x1}} ${{y1}} C${{x1}} ${{my}} ${{x2}} ${{my}} ${{x2}} ${{y2}}`);

      let cls = "arrow-path";
      let markerType = "normal";
      if (isHighDep) {{ cls += " highlight-dep"; markerType = "dep"; }}
      else if (isHighDepnt) {{ cls += " highlight-dependent"; markerType = "dependent"; }}
      path.setAttribute("class", cls);
      path.setAttribute("marker-end", `url(#ah-${{markerType}})`);
      svg.appendChild(path);
    }});
  }});
}}

setTimeout(() => drawArrows(), 50);
window.addEventListener("resize", () => drawArrows());

// ── Interaction ──
let selectedId = null;
let activeSource = null;

function selectNode(rid) {{
  const prev = document.querySelector(".node-card.selected");
  if (prev) prev.classList.remove("selected");

  document.querySelectorAll(".node-card").forEach(c => {{
    c.classList.remove("dep-highlight","dependent-highlight","dimmed");
  }});

  if (selectedId === rid) {{
    selectedId = null;
    drawArrows();
    renderDetail(null);
    return;
  }}
  selectedId = rid;
  const card = document.getElementById("card-" + rid.replace(/[^a-zA-Z0-9]/g, "_"));
  if (card) card.classList.add("selected");

  const res = resById[rid];
  const deps = new Set(res.needs);
  const depnts = new Set(dependents[rid] || []);

  // Highlight upstream
  const allUp = new Set();
  function walkUp(id) {{
    const r = resById[id]; if (!r) return;
    r.needs.forEach(d => {{ allUp.add(d); walkUp(d); }});
  }}
  walkUp(rid);

  // Highlight downstream
  const allDown = new Set();
  function walkDown(id) {{
    (dependents[id] || []).forEach(d => {{ allDown.add(d); walkDown(d); }});
  }}
  walkDown(rid);

  document.querySelectorAll(".node-card").forEach(c => {{
    const cid = c.dataset.id;
    if (cid === rid) return;
    if (allUp.has(cid)) c.classList.add("dep-highlight");
    else if (allDown.has(cid)) c.classList.add("dependent-highlight");
    else c.classList.add("dimmed");
  }});

  drawArrows([...allUp, rid], [...allDown, rid]);
  renderDetail(rid);
}}

function renderDetail(rid) {{
  const inner = document.getElementById("detail-inner");
  if (!rid) {{
    inner.innerHTML = '<div class="empty">Click a resource to inspect</div>';
    return;
  }}
  const res = resById[rid];
  const deps = res.needs.map(d => resById[d]).filter(Boolean);
  const depnts_list = (dependents[rid] || []).map(d => resById[d]).filter(Boolean);
  const shared = dedupOf[rid];

  let h = `<h2>${{esc(res.short_name)}}</h2>`;

  // Description
  h += `<div class="d-section"><h3>Description</h3><div style="font-size:13px;color:var(--text)">${{esc(res.description) || "<em>none</em>"}}</div></div>`;

  // Type badge
  let typeBadge = "";
  if (res.is_verify) typeBadge = `<span class="d-badge" style="background:rgba(247,120,186,0.15);color:var(--pink)">verify</span>`;
  else if (res.check && res.check !== "false") typeBadge = `<span class="d-badge" style="background:rgba(88,166,255,0.15);color:var(--accent)">check &#x2192; run</span>`;
  else typeBadge = `<span class="d-badge" style="background:rgba(63,185,80,0.15);color:var(--green)">run</span>`;
  h += `<div class="d-section"><h3>Type</h3>${{typeBadge}}`;
  if (res.run_as) h += ` <span class="d-badge" style="background:rgba(210,153,34,0.15);color:var(--amber)">as ${{esc(res.run_as)}}</span>`;
  if (res.timeout !== 300) h += ` <span class="d-badge" style="background:var(--bg3);color:var(--text2)">timeout ${{res.timeout}}s</span>`;
  if (res.retries > 0) h += ` <span class="d-badge" style="background:var(--bg3);color:var(--text2)">retry x${{res.retries}}</span>`;
  h += `</div>`;

  // Commands
  h += `<div class="d-section"><h3>Commands</h3>`;
  if (res.check && res.check !== "false") {{
    h += `<div style="font-size:11px;color:var(--text3);margin:4px 0">check</div><div class="d-cmd">${{esc(res.check)}}</div>`;
  }}
  h += `<div style="font-size:11px;color:var(--text3);margin:4px 0">run</div><div class="d-cmd">${{esc(res.run)}}</div>`;
  h += `</div>`;

  // Dependencies (upstream)
  if (deps.length > 0) {{
    h += `<div class="d-section"><h3>Depends on (${{deps.length}})</h3><div class="d-deps">`;
    deps.forEach(d => {{
      h += `<span class="d-dep-chip" onclick="selectNode('${{d.id}}')" style="border-left:2px solid var(--green)">${{esc(d.short_name)}}</span>`;
    }});
    h += `</div></div>`;
  }}

  // Dependents (downstream)
  if (depnts_list.length > 0) {{
    h += `<div class="d-section"><h3>Required by (${{depnts_list.length}})</h3><div class="d-deps">`;
    depnts_list.forEach(d => {{
      h += `<span class="d-dep-chip" onclick="selectNode('${{d.id}}')" style="border-left:2px solid var(--purple)">${{esc(d.short_name)}}</span>`;
    }});
    h += `</div></div>`;
  }}

  // Provenance
  if (res.provenance) {{
    const prov = G.provenance.find(p => p.source === res.provenance);
    h += `<div class="d-section"><h3>Provenance</h3><div class="d-provenance">`;
    h += `<div class="src">${{esc(res.provenance)}}</div>`;
    if (prov && prov.params) {{
      h += `<div class="params">${{Object.entries(prov.params).map(([k,v]) => `${{k}}=<strong>${{esc(v)}}</strong>`).join(", ")}}</div>`;
    }}
    h += `</div></div>`;
  }}

  // Dedup
  if (shared && shared.length > 1) {{
    h += `<div class="d-section"><h3>Deduplication</h3>`;
    h += `<div style="font-size:12px;color:var(--green);margin:4px 0">Shared across ${{shared.length}} call sites</div>`;
    h += `<div class="d-deps">`;
    shared.forEach(s => {{
      if (s !== rid) h += `<span class="d-dep-chip">${{esc(s.split(".").pop())}}</span>`;
    }});
    h += `</div></div>`;
  }}

  // Full ID
  h += `<div class="d-section"><h3>Resource ID</h3><div style="font-family:var(--font);font-size:11px;color:var(--text3);word-break:break-all">${{esc(rid)}}</div></div>`;

  inner.innerHTML = h;
}}

function esc(s) {{ return s ? String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;") : ""; }}

function toggleArrows() {{
  showArrows = !showArrows;
  document.getElementById("btn-arrows").classList.toggle("active", showArrows);
  if (selectedId) selectNode(selectedId); // re-trigger
  else drawArrows();
}}

function toggleDetail() {{
  const panel = document.getElementById("detail-panel");
  panel.classList.toggle("collapsed");
  document.getElementById("btn-detail").classList.toggle("active", !panel.classList.contains("collapsed"));
  setTimeout(() => drawArrows(), 250);
}}

function filterNodes(query) {{
  const q = query.toLowerCase().trim();
  document.querySelectorAll(".node-card").forEach(c => {{
    const rid = c.dataset.id;
    const res = resById[rid];
    const haystack = (res.short_name + " " + res.description + " " + (res.provenance||"")).toLowerCase();
    c.style.display = (!q || haystack.includes(q)) ? "" : "none";
  }});
}}

function filterBySource(src) {{
  const items = document.querySelectorAll(".legend-item");
  const already = activeSource === src;
  activeSource = already ? null : src;
  items.forEach(i => i.classList.toggle("active", i.dataset.source === activeSource));
  document.querySelectorAll(".node-card").forEach(c => {{
    const res = resById[c.dataset.id];
    const rSrc = res.provenance || "__inline";
    if (!activeSource) {{ c.style.display = ""; c.classList.remove("dimmed"); }}
    else if (rSrc === activeSource) {{ c.style.display = ""; c.classList.remove("dimmed"); }}
    else {{ c.classList.add("dimmed"); c.style.display = ""; }}
  }});
  drawArrows();
}}

document.getElementById("btn-detail").classList.add("active");
</script>
</html>'''
# END GENERATED: VISUALIZER TEMPLATE



# ═══════════════════════════════════════════════════════════════════════════════
# 9a. WEB IDE SERVER
# ═══════════════════════════════════════════════════════════════════════════════

# BEGIN GENERATED: EMBEDDED IDE HTML
# Generated by build_cgr.py from ide.html. Do not edit by hand.
_EMBEDDED_IDE_HTML = '<!DOCTYPE html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n<title>CommandGraph IDE</title>\n<style>\n:root {\n  --bg: #0e1117; --bg2: #161b22; --bg3: #1c2129; --bg-hover: #21262d;\n  --border: #30363d; --border-hi: #484f58;\n  --text: #e6edf3; --text2: #8b949e; --text3: #6e7681;\n  --accent: #58a6ff; --green: #3fb950; --amber: #d29922;\n  --red: #f85149; --purple: #bc8cff; --coral: #f0883e;\n  --teal: #39d353; --pink: #f778ba;\n  --mono: "SF Mono","Cascadia Code","Fira Code","JetBrains Mono","Consolas",monospace;\n  --sans: "Segoe UI",system-ui,-apple-system,sans-serif;\n  --radius: 6px;\n}\n@media (prefers-color-scheme:light) {\n  :root:not(.dark) {\n    --bg: #f6f8fa; --bg2: #ffffff; --bg3: #f0f2f5; --bg-hover: #eaeef2;\n    --border: #d0d7de; --border-hi: #afb8c1;\n    --text: #1f2328; --text2: #656d76; --text3: #8c959f;\n    --accent: #0969da; --green: #1a7f37; --amber: #9a6700;\n    --red: #cf222e; --purple: #8250df; --coral: #bc4c00;\n    --teal: #0f6e56; --pink: #bf3989;\n  }\n}\n:root.light {\n  --bg: #f6f8fa; --bg2: #ffffff; --bg3: #f0f2f5; --bg-hover: #eaeef2;\n  --border: #d0d7de; --border-hi: #afb8c1;\n  --text: #1f2328; --text2: #656d76; --text3: #8c959f;\n  --accent: #0969da; --green: #1a7f37; --amber: #9a6700;\n  --red: #cf222e; --purple: #8250df; --coral: #bc4c00;\n  --teal: #0f6e56; --pink: #bf3989;\n}\n* { margin:0; padding:0; box-sizing:border-box; }\nbody { background:var(--bg); color:var(--text); font-family:var(--sans); height:100vh; overflow:hidden; }\n\n/* Header */\n.hdr { background:var(--bg2); border-bottom:1px solid var(--border); padding:8px 14px; display:flex; align-items:center; gap:12px; height:42px; z-index:100; }\n.hdr h1 { font-family:var(--mono); font-size:13px; font-weight:600; color:var(--accent); }\n.hdr .sep { color:var(--text3); }\n.hdr .fname { font-family:var(--mono); font-size:12px; color:var(--text); cursor:pointer; }\n.hdr .fname:hover { color:var(--accent); }\n.hdr .dirty { color:var(--amber); font-size:10px; }\n.hdr .actions { margin-left:auto; display:flex; gap:6px; align-items:center; }\n.btn { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); color:var(--text2); font-size:11px; padding:4px 10px; cursor:pointer; font-family:var(--mono); transition:all .15s; }\n.btn:hover { background:var(--bg-hover); color:var(--text); border-color:var(--border-hi); }\n.btn-primary { background:var(--accent); color:#fff; border-color:var(--accent); }\n.btn-primary:hover { opacity:.85; }\n.btn-green { background:var(--green); color:#fff; border-color:var(--green); }\n.btn-green:hover { opacity:.85; }\n.status { font-size:11px; font-family:var(--mono); padding:2px 8px; border-radius:10px; }\n.status-ok { background:rgba(63,185,80,.12); color:var(--green); }\n.status-err { background:rgba(248,81,73,.12); color:var(--red); }\n.status-info { background:rgba(88,166,255,.1); color:var(--accent); }\n.status-warn { background:rgba(210,153,34,.12); color:var(--amber); }\n.problems-panel { padding:0; font-size:12px; max-height:180px; overflow-y:auto; border-top:1px solid var(--border); background:var(--bg); }\n.problems-panel.show { display:block; }\n.prob-item { padding:4px 12px; display:flex; gap:6px; align-items:baseline; cursor:pointer; font-family:var(--mono); font-size:11px; }\n.prob-item:hover { background:var(--bg-hover); }\n.prob-icon { flex-shrink:0; width:14px; text-align:center; }\n.prob-icon.prob-err { color:var(--red); }\n.prob-icon.prob-warn { color:var(--amber); }\n.prob-icon.prob-info { color:var(--text3); }\n.prob-msg { color:var(--text2); flex:1; }\n.prob-loc { color:var(--text3); font-size:10px; white-space:nowrap; }\n.prob-file-link { color:var(--accent); text-decoration:underline; cursor:pointer; }\n.prob-file-link:hover { color:var(--text); }\n.prob-summary { padding:4px 12px; font-size:10px; font-family:var(--mono); color:var(--text3); border-bottom:1px solid var(--border); display:flex; justify-content:space-between; align-items:center; }\n.prob-summary .prob-close { cursor:pointer; color:var(--text3); padding:0 4px; }\n.prob-summary .prob-close:hover { color:var(--text); }\n\n/* Main layout */\n.main { display:flex; flex-direction:column; height:calc(100vh - 42px); }\n.main-panels { display:flex; flex:1; overflow:hidden; }\n\n/* Editor panel */\n.editor-panel { width:40%; min-width:300px; display:flex; flex-direction:column; border-right:1px solid var(--border); }\n.editor-tabs { display:flex; gap:0; background:var(--bg2); border-bottom:1px solid var(--border); padding:0 8px; }\n.tab { padding:6px 12px; font-size:11px; font-family:var(--mono); color:var(--text3); cursor:pointer; border-bottom:2px solid transparent; transition:all .15s; }\n.tab:hover { color:var(--text2); }\n.tab.active { color:var(--accent); border-bottom-color:var(--accent); }\n.editor-wrap { flex:1; position:relative; overflow:hidden; }\n.editor-container { display:flex; height:100%; overflow:auto; background:var(--bg); }\n.line-numbers { padding:10px 0; text-align:right; min-width:40px; padding-right:10px; font-family:var(--mono); font-size:13px; line-height:1.6; color:var(--text3); user-select:none; background:var(--bg2); border-right:1px solid var(--border); flex-shrink:0; }\n.editor-area { position:relative; flex:1; min-height:100%; }\n.highlight-layer { padding:10px 12px; font-family:var(--mono); font-size:13px; line-height:1.6; white-space:pre-wrap; word-wrap:break-word; pointer-events:none; user-select:none; -webkit-user-select:none; color:var(--text); width:100%; min-height:100%; }\ntextarea#code { position:absolute; top:0; left:0; width:100%; height:100%; padding:10px 12px; font-family:var(--mono); font-size:13px; line-height:1.6; background:transparent; color:var(--text); border:none; outline:none; resize:none; white-space:pre-wrap; word-wrap:break-word; caret-color:var(--text); -webkit-text-fill-color:transparent; overflow:hidden; }\n/* Syntax highlighting */\n.hl-kw { color:var(--accent); font-weight:600; }\n.hl-str { color:var(--green); }\n.hl-step { color:var(--coral); font-weight:600; }\n.hl-cmd { color:var(--text); }\n.hl-comment { color:var(--text3); font-style:italic; }\n.hl-var { color:var(--purple); }\n.hl-prop { color:var(--amber); }\n.hl-title { color:var(--pink); font-weight:700; }\n.hl-upath { color:var(--accent); text-decoration:underline; text-decoration-style:dotted; text-underline-offset:2px; }\n/* Error gutter */\n.line-numbers .ln-err { color:var(--red); font-weight:700; }\n\n/* Graph + Log area */\n.graph-log-area { flex:1; display:flex; flex-direction:column; overflow:hidden; }\n/* Graph tab bar */\n.graph-tabs { display:flex; gap:0; background:var(--bg2); border-bottom:1px solid var(--border); padding:0 8px; flex-shrink:0; }\n.gtab { padding:5px 12px; font-size:11px; font-family:var(--mono); color:var(--text3); cursor:pointer; border-bottom:2px solid transparent; transition:all .15s; display:flex; align-items:center; gap:5px; }\n.gtab:hover { color:var(--text2); }\n.gtab.active { color:var(--accent); border-bottom-color:var(--accent); }\n.gtab-close { font-size:14px; color:var(--text3); margin-left:2px; line-height:1; }\n.gtab-close:hover { color:var(--red); }\n/* File viewer panel (replaces graph) */\n.fileview-panel { flex:1; display:flex; flex-direction:column; overflow:hidden; }\n.fileview-hdr { display:flex; align-items:center; gap:8px; padding:6px 12px; background:var(--bg2); border-bottom:1px solid var(--border); flex-shrink:0; }\n.fileview-lock { font-size:12px; }\n.fileview-path { font-family:var(--mono); font-size:11px; color:var(--text2); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }\n.fileview-msg { font-family:var(--mono); font-size:10px; color:var(--text3); }\n.fileview-body { flex:1; overflow:auto; background:var(--bg); }\n.fileview-editor-wrap { display:flex; min-height:100%; }\n.fv-line-numbers { padding:10px 0; text-align:right; min-width:40px; padding-right:10px; font-family:var(--mono); font-size:13px; line-height:1.6; color:var(--text3); user-select:none; background:var(--bg2); border-right:1px solid var(--border); flex-shrink:0; }\n.fv-editor-area { position:relative; flex:1; min-height:100%; }\n.fv-highlight { padding:10px 12px; font-family:var(--mono); font-size:13px; line-height:1.6; white-space:pre-wrap; word-wrap:break-word; pointer-events:none; user-select:none; color:var(--text); width:100%; min-height:100%; }\n.fv-textarea { position:absolute; top:0; left:0; width:100%; height:100%; padding:10px 12px; font-family:var(--mono); font-size:13px; line-height:1.6; background:transparent; color:var(--text); border:none; outline:none; resize:none; white-space:pre-wrap; word-wrap:break-word; caret-color:var(--text); -webkit-text-fill-color:transparent; overflow:hidden; }\n.fv-textarea[readonly] { cursor:default; }\n.graph-panel { flex:1; overflow:auto; position:relative; padding:16px; }\n.graph-panel .empty-msg { color:var(--text3); font-size:13px; text-align:center; padding:60px 20px; }\n/* Log resize handle */\n.log-resize { height:4px; background:var(--border); cursor:ns-resize; flex-shrink:0; }\n.log-resize:hover { background:var(--accent); }\n/* Log pane */\n.log-pane { height:220px; min-height:80px; max-height:60vh; display:flex; flex-direction:column; border-top:1px solid var(--border); background:var(--bg); flex-shrink:0; }\n.log-pane.collapsed { height:auto; min-height:0; max-height:none; }\n.log-pane.collapsed .log-body { display:none; }\n.log-pane.collapsed .log-header { border-bottom:none; }\n.log-header { display:flex; align-items:center; gap:8px; padding:4px 10px; background:var(--bg2); border-bottom:1px solid var(--border); flex-shrink:0; cursor:pointer; }\n.log-title { font-family:var(--mono); font-size:13px; font-weight:600; color:var(--text2); }\n.log-actions { margin-left:auto; display:flex; gap:4px; }\n.log-btn { padding:2px 7px; font-size:11px; }\n.log-follow { display:flex; align-items:center; gap:3px; font-family:var(--mono); font-size:10px; color:var(--text3); cursor:pointer; padding:0 6px; user-select:none; }\n.log-follow input { cursor:pointer; accent-color:var(--accent); }\n.log-body { flex:1; overflow:auto; padding:6px 10px; font-family:var(--mono); font-size:13px; line-height:1.6; }\n/* Log entry highlight flash when scrolled to */\n@keyframes log-flash { 0%{background:rgba(88,166,255,.2)} 100%{background:transparent} }\n.log-entry.flash { animation:log-flash 1.2s ease-out; }\n/* Log entries */\n.log-entry { border-bottom:1px solid var(--border); padding:4px 0; }\n.log-entry:last-child { border-bottom:none; }\n.log-entry-hdr { display:flex; align-items:center; gap:6px; cursor:pointer; padding:2px 0; border-radius:var(--radius); }\n.log-entry-hdr:hover { background:var(--bg-hover); }\n.log-icon { width:16px; text-align:center; flex-shrink:0; }\n.log-icon.ok { color:var(--green); }\n.log-icon.skip { color:var(--text3); }\n.log-icon.fail { color:var(--red); }\n.log-icon.warn { color:var(--amber); }\n.log-name { flex:1; color:var(--text); }\n.log-desc { color:var(--text3); font-size:11px; margin-left:4px; }\n.log-time { color:var(--text3); font-size:11px; flex-shrink:0; }\n.log-rc { font-size:11px; padding:0 5px; border-radius:6px; flex-shrink:0; }\n.log-rc.rc-ok { background:rgba(63,185,80,.12); color:var(--green); }\n.log-rc.rc-fail { background:rgba(248,81,73,.12); color:var(--red); }\n.log-detail { display:none; padding:4px 0 4px 22px; }\n.log-detail.open { display:block; }\n.log-detail-section { margin:3px 0; }\n.log-detail-label { color:var(--text3); font-size:10px; text-transform:uppercase; letter-spacing:.5px; }\n.log-detail-cmd { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); padding:5px 9px; font-size:12px; margin:2px 0; white-space:pre-wrap; word-break:break-all; color:var(--text); max-height:140px; overflow:auto; }\n.log-detail-cmd.cmd-stdout { color:var(--text2); }\n.log-detail-cmd.cmd-stderr { color:var(--red); border-color:rgba(248,81,73,.3); }\n.log-wave-hdr { color:var(--text3); font-size:12px; font-weight:700; letter-spacing:.3px; padding:6px 0 2px; border-bottom:1px solid var(--border); margin:4px 0 2px; }\n.log-summary { padding:8px 0; font-weight:700; font-size:14px; }\n.log-summary.ok { color:var(--green); }\n.log-summary.fail { color:var(--red); }\n/* Reuse existing viz styles */\n.wc { margin:0 0 3px; }\n.wl { font-family:var(--mono); font-size:11px; color:var(--text3); font-weight:700; letter-spacing:.3px; padding:0 0 5px; display:flex; align-items:center; gap:7px; flex-wrap:wrap; }\n.wl::after { content:""; flex:1; height:1px; background:var(--border); min-width:20px; }\n.wl-count { font-weight:400; color:var(--text3); }\n.wl-summary { font-weight:400; font-size:11px; color:var(--text2); letter-spacing:0; max-width:500px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }\n.wl-phase { background:rgba(88,166,255,.12); color:var(--accent); font-size:9px; padding:0 6px; border-radius:8px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; }\n.wr { display:flex; flex-wrap:wrap; gap:6px; padding:6px 0 6px 14px; border-left:2px solid var(--border); margin-left:3px; }\n.nc { background:var(--bg2); border:1px solid var(--border); border-radius:var(--radius); padding:8px 10px 8px 10px; cursor:pointer; transition:all .15s; position:relative; max-width:240px; min-width:110px; }\n.nc:hover { border-color:var(--border-hi); background:var(--bg-hover); transform:translateY(-1px); }\n.nc.sel { border-color:var(--accent); box-shadow:0 0 0 1px var(--accent); }\n.nc.dep { border-color:var(--green); background:rgba(63,185,80,.06); }\n.nc.depnt { border-color:var(--purple); background:rgba(188,140,255,.06); }\n.nc.cdim { opacity:.2; }\n.nc.editor-hl { border-color:var(--accent); background:rgba(88,166,255,.1); box-shadow:0 0 0 1px rgba(88,166,255,.3); }\n.nc.follow-hl { border-color:var(--amber); box-shadow:0 0 0 2px rgba(210,153,34,.4); animation:demo-pulse .8s ease-in-out infinite; }\n.nc.diff-added { border-color:var(--green); box-shadow:0 0 0 2px rgba(63,185,80,.3); }\n.nc.diff-added::after { content:\'NEW\'; position:absolute; top:2px; right:4px; font-size:8px; color:var(--green); font-weight:700; }\n.nc.diff-changed { border-color:var(--amber); box-shadow:0 0 0 2px rgba(210,153,34,.25); }\n.nc.diff-changed::after { content:\'CHANGED\'; position:absolute; top:2px; right:4px; font-size:8px; color:var(--amber); font-weight:700; }\n/* SVG dependency arrows */\n.sa { position:absolute; top:0; left:0; pointer-events:none; z-index:1; }\n.ap { fill:none; stroke:var(--border-hi); stroke-width:1; opacity:.3; transition:opacity .2s, stroke .2s; }\n.ap.hd { stroke:var(--green); opacity:.7; stroke-width:1.5; }\n.ap.ht { stroke:var(--purple); opacity:.7; stroke-width:1.5; }\n/* Editor line highlight strip */\n.line-numbers .ln-hl { background:rgba(88,166,255,.15); }\n.editor-line-hl { position:absolute; left:0; width:100%; height:1.6em; background:rgba(88,166,255,.08); pointer-events:none; z-index:0; transition:top .1s ease-out; }\n.nn { font-family:var(--mono); font-size:13px; font-weight:600; color:var(--text); margin:0 0 2px; padding-right:40px; }\n.nd { font-size:11px; color:var(--text2); line-height:1.4; overflow:hidden; text-overflow:ellipsis; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; }\n.nb { display:flex; gap:3px; margin-top:4px; flex-wrap:wrap; }\n.bg { font-size:10px; padding:1px 6px; border-radius:7px; font-family:var(--mono); font-weight:600; line-height:1.7; }\n.bg-p { background:rgba(88,166,255,.12); color:var(--accent); }\n.bg-v { background:rgba(247,120,186,.12); color:var(--pink); }\n.bg-r { background:rgba(210,153,34,.12); color:var(--amber); }\n.nc.barrier { opacity:.4; border-style:dashed; min-width:60px; max-width:110px; }\n.nc.barrier .nn { font-size:9px; color:var(--text3); }\n.grp { border:1px dashed var(--border-hi); border-radius:8px; padding:8px 8px 6px; margin:2px 0; }\n.grp-par { border-color:var(--accent); }\n.grp-race { border-color:var(--coral); }\n.grp-each { border-color:var(--teal); }\n.grp-phase { border-color:var(--purple); }\n.grp-lbl { font-family:var(--mono); font-size:9px; font-weight:700; text-transform:uppercase; padding:0 5px; margin:0 0 5px; }\n.grp-par .grp-lbl { color:var(--accent); }\n.grp-race .grp-lbl { color:var(--coral); }\n.grp-each .grp-lbl { color:var(--teal); }\n.grp-phase .grp-lbl { color:var(--purple); }\n.grp-cards { display:flex; flex-wrap:wrap; gap:6px; }\n.nc.st-success { border-left:3px solid var(--green); }\n.nc.st-failed { border-left:3px solid var(--red); background:rgba(248,81,73,.05); }\n.nc.st-warned { border-left:3px solid var(--amber); }\n/* Node groups within waves */\n.ng { border:1px solid var(--border); border-radius:6px; margin:2px 0; overflow:hidden; width:100%; }\n.ng-hdr { display:flex; align-items:center; gap:8px; padding:5px 10px; cursor:pointer; font-family:var(--mono); font-size:11px; background:var(--bg2); user-select:none; }\n.ng-hdr:hover { background:var(--bg-hover); }\n.ng-toggle { color:var(--text3); font-size:9px; flex-shrink:0; width:12px; text-align:center; }\n.ng-name { font-weight:700; color:var(--text); }\n.ng-via { color:var(--text3); font-size:10px; }\n.ng-count { color:var(--text2); margin-left:auto; font-size:10px; }\n.ng-state { display:flex; gap:6px; font-size:10px; }\n.ng-state .ns-ok { color:var(--green); }\n.ng-state .ns-fail { color:var(--red); }\n.ng-state .ns-pend { color:var(--text3); }\n.ng-body { display:flex; flex-wrap:wrap; gap:6px; padding:6px 10px; }\n.ng.collapsed .ng-body { display:none; }\n.ng.collapsed { border-color:var(--border); }\n\n/* Info bar */\n.info-bar { background:var(--bg2); border-top:1px solid var(--border); padding:6px 14px; font-family:var(--mono); font-size:11px; color:var(--text2); display:flex; gap:14px; flex-wrap:wrap; }\n.info-bar .st { display:flex; align-items:center; gap:3px; }\n.info-bar .stn { color:var(--text); font-weight:600; }\n\n/* Inspector panel */\n.inspector { width:280px; min-width:250px; background:var(--bg2); border-left:1px solid var(--border); overflow-y:auto; padding:14px; }\n.inspector h2 { font-family:var(--mono); font-size:13px; font-weight:600; color:var(--accent); margin:0 0 10px; word-break:break-all; }\n.inspector .empty { color:var(--text3); font-size:12px; font-style:italic; padding:28px 0; text-align:center; }\n.ds { margin:0 0 12px; }\n.ds h3 { font-size:10px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:var(--text3); margin:0 0 4px; }\n.dcmd { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); padding:6px 8px; font-family:var(--mono); font-size:11px; color:var(--text); white-space:pre-wrap; word-break:break-all; margin:3px 0; }\n.dbadge { display:inline-block; padding:1px 6px; border-radius:9px; font-size:10px; font-weight:600; font-family:var(--mono); }\n.ddeps { display:flex; flex-wrap:wrap; gap:3px; margin:3px 0; }\n.dchip { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); padding:1px 6px; font-size:10px; font-family:var(--mono); color:var(--text2); cursor:pointer; transition:all .15s; }\n.dchip:hover { border-color:var(--accent); color:var(--accent); }\n\n/* Resize handle */\n.resize-handle { width:4px; cursor:col-resize; background:transparent; transition:background .2s; flex-shrink:0; }\n.resize-handle:hover, .resize-handle.active { background:var(--accent); }\n\n/* Autocomplete popup */\n.ac-popup { display:none; position:absolute; z-index:200; background:var(--bg2); border:1px solid var(--border); border-radius:var(--radius); max-height:160px; overflow-y:auto; min-width:180px; box-shadow:0 4px 12px rgba(0,0,0,.3); }\n.ac-popup.show { display:block; }\n.ac-item { padding:4px 10px; font-family:var(--mono); font-size:11px; color:var(--text2); cursor:pointer; display:flex; align-items:center; gap:6px; }\n.ac-item:hover, .ac-item.sel { background:var(--accent); color:#fff; }\n.ac-icon { font-size:9px; opacity:.6; }\n\n/* Inspector tabs */\n.insp-tabs { display:flex; gap:0; border-bottom:1px solid var(--border); margin:0 -14px 10px; padding:0 14px; }\n.insp-tabs .tab { padding:5px 10px; font-size:11px; font-family:var(--mono); color:var(--text3); cursor:pointer; border-bottom:2px solid transparent; }\n.insp-tabs .tab:hover { color:var(--text2); }\n.insp-tabs .tab.active { color:var(--accent); border-bottom-color:var(--accent); }\n\n/* Execution log */\n.exec-log { font-family:var(--mono); font-size:11px; line-height:1.7; }\n.exec-log .ev { padding:2px 0; display:flex; align-items:flex-start; gap:6px; }\n.exec-log .ev-wave { color:var(--text3); font-weight:700; font-size:10px; letter-spacing:.3px; margin:6px 0 2px; border-bottom:1px solid var(--border); padding-bottom:2px; }\n.exec-log .ev-ok { color:var(--green); }\n.exec-log .ev-skip { color:var(--text3); }\n.exec-log .ev-fail { color:var(--red); }\n.exec-log .ev-warn { color:var(--amber); }\n.exec-log .ev-icon { flex-shrink:0; width:14px; text-align:center; }\n.exec-log .ev-name { flex:1; }\n.exec-log .ev-time { color:var(--text3); font-size:10px; }\n.exec-log .ev-err { color:var(--red); font-size:10px; padding:2px 0 2px 20px; word-break:break-all; }\n.exec-log .ev-done { padding:8px 0; font-weight:700; }\n.exec-log .ev-done.ok { color:var(--green); }\n.exec-log .ev-done.fail { color:var(--red); }\n.exec-log .ev-rerun { font-size:10px; color:var(--accent); cursor:pointer; text-decoration:underline; margin-left:4px; }\n.exec-log .ev-rerun:hover { color:var(--text); }\n.exec-log .ev-running { color:var(--amber); }\n.exec-log .ev-block { padding:2px 0 4px; border-bottom:1px solid var(--border); }\n.exec-log .ev-block:last-child { border-bottom:none; }\n.exec-log .ev-stream { margin:2px 0 0 20px; padding:4px 7px; background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); white-space:pre-wrap; word-break:break-all; max-height:140px; overflow:auto; font-size:10px; }\n.exec-log .ev-stream:empty { display:none; }\n.exec-log .ev-stream.stdout { color:var(--text2); }\n.exec-log .ev-stream.stderr { color:var(--red); border-color:rgba(248,81,73,.3); }\n.exec-log .ev-meta { margin:2px 0 0 20px; color:var(--text3); font-size:10px; white-space:pre-wrap; }\n.exec-log .ev-meta.waiting { color:var(--amber); }\n.exec-log .ev-cmd { margin:2px 0 0 20px; padding:4px 7px; background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); white-space:pre-wrap; word-break:break-all; max-height:120px; overflow:auto; font-size:10px; color:var(--text); }\n.exec-input { display:none; gap:8px; align-items:center; padding:8px 0 0; }\n.exec-input.show { display:flex; }\n.exec-input-label { color:var(--text3); font-size:10px; font-family:var(--mono); text-transform:uppercase; letter-spacing:.5px; }\n.exec-input-box { flex:1; min-width:0; background:var(--bg3); color:var(--text); border:1px solid var(--border); border-radius:var(--radius); padding:7px 9px; font-family:var(--mono); font-size:12px; }\n.exec-input-box:focus { outline:none; border-color:var(--accent); box-shadow:0 0 0 1px rgba(88,166,255,.2); }\n.exec-input-send { white-space:nowrap; }\n.exec-input-toggle { white-space:nowrap; }\n.log-entry.running .log-icon { color:var(--amber); }\n.term-fg-black { color:#484f58; }\n.term-fg-red { color:#ff7b72; }\n.term-fg-green { color:#7ee787; }\n.term-fg-yellow { color:#e3b341; }\n.term-fg-blue { color:#79c0ff; }\n.term-fg-magenta { color:#d2a8ff; }\n.term-fg-cyan { color:#56d4dd; }\n.term-fg-white { color:#f0f6fc; }\n.term-bold { font-weight:700; }\n/* Graph toolbar */\n.graph-toolbar { display:flex; gap:6px; align-items:center; margin:0 0 10px; }\n/* State badges on cards */\n.stb { position:absolute; top:4px; right:4px; font-size:10px; font-weight:700; font-family:var(--mono); padding:0 5px; border-radius:4px; line-height:1.6; }\n.stb-success { background:rgba(63,185,80,.15); color:var(--green); }\n.stb-skip { background:rgba(63,185,80,.1); color:var(--green); }\n.stb-failed { background:rgba(248,81,73,.15); color:var(--red); }\n.stb-checking { background:rgba(88,166,255,.15); color:var(--accent); }\n.stb-running { background:rgba(210,153,34,.15); color:var(--amber); }\n.stb-resuming { background:rgba(188,140,255,.15); color:var(--purple); }\n/* Demo animations */\n@keyframes demo-pulse { 0%,100%{opacity:1} 50%{opacity:.6} }\n@keyframes demo-shake { 0%,100%{transform:translateX(0)} 15%{transform:translateX(-3px)} 30%{transform:translateX(3px)} 45%{transform:translateX(-2px)} 60%{transform:translateX(2px)} 75%{transform:translateX(-1px)} }\n.nc.demo-checking { border-left:3px solid var(--accent); animation:demo-pulse .8s ease-in-out infinite; }\n.nc.demo-running { border-left:3px solid var(--amber); animation:demo-pulse .8s ease-in-out infinite; }\n.nc.demo-failed-shake { animation:demo-shake .4s ease-in-out; }\n.nc.demo-resuming { border-left:3px solid var(--purple); animation:demo-pulse .6s ease-in-out infinite; }\n.nc.st-success { border-left:3px solid var(--green); }\n.nc.st-skip_check { border-left:3px solid var(--green); opacity:.7; }\n.nc.st-failed { border-left:3px solid var(--red); background:rgba(248,81,73,.05); }\n/* More menu */\n.more-menu.show { display:block !important; }\n.more-item { padding:5px 10px; font-family:var(--mono); font-size:11px; color:var(--text2); cursor:pointer; }\n.more-item:hover { background:var(--bg-hover); color:var(--text); }\n/* Demo button states */\n#bdemo.demo-active { color:var(--amber); font-weight:700; }\n#bdemo.demo-done { color:var(--text3); }\n/* Wave progress */\n.wl .wprog { display:inline-block; margin-left:8px; font-size:10px; color:var(--text3); font-family:var(--mono); }\n.wprog-bar { display:inline-block; width:60px; height:5px; background:var(--bg3); border-radius:3px; vertical-align:middle; margin-right:4px; overflow:hidden; }\n.wprog-fill { height:100%; background:var(--green); border-radius:3px; transition:width .3s ease-out; }\n/* Toast */\n.toast { background:var(--bg2); border:1px solid var(--border); border-radius:var(--radius); padding:6px 12px; font-size:11px; font-family:var(--mono); color:var(--text); box-shadow:0 4px 12px rgba(0,0,0,.3); animation:toast-in .3s ease-out; max-width:320px; }\n.toast.toast-fail { border-color:var(--red); background:rgba(248,81,73,.08); }\n.toast.toast-resume { border-color:var(--purple); background:rgba(188,140,255,.08); }\n.toast.toast-done { border-color:var(--green); background:rgba(63,185,80,.08); }\n@keyframes toast-in { from{opacity:0;transform:translateY(-10px)} to{opacity:1;transform:translateY(0)} }\n.file-picker-backdrop { position:fixed; inset:0; background:rgba(0,0,0,.45); display:none; align-items:flex-start; justify-content:center; padding-top:72px; z-index:250; }\n.file-picker { width:min(680px, calc(100vw - 40px)); max-height:min(70vh, 720px); background:var(--bg2); border:1px solid var(--border-hi); border-radius:10px; box-shadow:0 18px 60px rgba(0,0,0,.45); display:flex; flex-direction:column; overflow:hidden; }\n.file-picker-hdr { display:flex; align-items:center; gap:8px; padding:10px 12px; border-bottom:1px solid var(--border); font-family:var(--mono); font-size:12px; color:var(--text2); }\n.file-picker-search { margin:12px; margin-bottom:8px; width:calc(100% - 24px); background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-family:var(--mono); font-size:12px; padding:8px 10px; outline:none; }\n.file-picker-search:focus { border-color:var(--accent); box-shadow:0 0 0 1px var(--accent); }\n.file-picker-controls { display:flex; align-items:center; gap:10px; padding:0 12px 10px; }\n.file-picker-filter { display:flex; align-items:center; gap:8px; font-family:var(--mono); font-size:11px; color:var(--text3); }\n.file-picker-filter select { background:var(--bg3); border:1px solid var(--border); border-radius:var(--radius); color:var(--text); font-family:var(--mono); font-size:11px; padding:6px 8px; outline:none; }\n.file-picker-filter select:focus { border-color:var(--accent); box-shadow:0 0 0 1px var(--accent); }\n.file-picker-list { padding:0 12px 12px; overflow:auto; }\n.file-picker-item { padding:8px 10px; margin:0 0 6px; border:1px solid var(--border); border-radius:var(--radius); background:var(--bg3); cursor:pointer; transition:all .15s; }\n.file-picker-item:hover { border-color:var(--accent); background:var(--bg-hover); }\n.file-picker-item.active { border-color:var(--accent); box-shadow:0 0 0 1px rgba(88,166,255,.35); }\n.file-picker-path { font-family:var(--mono); font-size:12px; color:var(--text); display:flex; align-items:center; gap:8px; flex-wrap:wrap; }\n.file-picker-meta { margin-top:2px; font-size:10px; color:var(--text3); }\n.file-picker-tag { display:inline-block; padding:1px 6px; border-radius:999px; font-size:9px; font-weight:700; letter-spacing:.03em; text-transform:uppercase; }\n.file-picker-tag.cgr { background:rgba(88,166,255,.14); color:var(--accent); }\n.file-picker-tag.cg { background:rgba(210,153,34,.14); color:var(--amber); }\n.file-picker-tag.root { background:rgba(139,148,158,.18); color:var(--text2); }\n.file-picker-tag.example { background:rgba(63,185,80,.14); color:var(--green); }\n.file-picker-tag.testing { background:rgba(188,140,255,.14); color:var(--purple); }\n.file-picker-tag.template { background:rgba(240,136,62,.15); color:var(--coral); }\n.file-picker-empty { padding:14px 6px; color:var(--text3); font-size:12px; text-align:center; }\n.file-picker-trigger { display:inline-flex; align-items:center; justify-content:center; width:22px; height:22px; border:1px solid var(--border); border-radius:var(--radius); background:var(--bg3); color:var(--text2); cursor:pointer; font-size:12px; transition:all .15s; }\n.file-picker-trigger:hover { border-color:var(--accent); color:var(--accent); background:var(--bg-hover); }\n.file-picker-trigger.disabled { opacity:.45; cursor:default; }\n</style>\n</head>\n<body>\n\n<div class="hdr">\n  <h1>&#9671; CommandGraph</h1>\n  <span class="sep">|</span>\n  <span class="fname" id="filename" title="Click to change file">—</span>\n  <button class="file-picker-trigger" id="filePickerBtn" title="Open graph file (Ctrl+O)">&#128194;</button>\n  <span class="dirty" id="dirty" style="display:none">● unsaved</span>\n  <div class="actions">\n    <span class="status status-info" id="statusBadge">loading...</span>\n    <button class="btn" onclick="doSave()" title="Ctrl+S">Save</button>\n    <button class="btn btn-primary" onclick="doCheck()" title="Ctrl+Shift+V" id="checkBtn">Check</button>\n    <button class="btn btn-green" id="applyBtn" onclick="doApply()" title="Ctrl+Enter">▶ Apply</button>\n    <button class="btn" id="rerunBtn" onclick="doApplyFresh()" title="Reset state and re-run all steps (Ctrl+Shift+Enter)">⟳ Re-run</button>\n    <button class="btn" id="themeBtn" onclick="toggleTheme()" title="Toggle dark/light mode" style="font-size:15px;padding:2px 8px"></button>\n  </div>\n</div>\n\n<div class="main">\n<div class="main-panels">\n  <!-- Editor -->\n  <div class="editor-panel" id="editorPanel">\n    <div class="editor-wrap">\n      <div class="editor-container" id="editorContainer">\n        <div class="line-numbers" id="lineNumbers"></div>\n        <div class="editor-area" id="editorArea">\n          <div class="highlight-layer" id="highlight"></div>\n          <textarea id="code" spellcheck="false" autocomplete="off" autocapitalize="off"></textarea>\n        </div>\n      </div>\n    </div>\n    <div class="problems-panel" id="problemsPanel" style="display:none"></div>\n  </div>\n\n  <div class="resize-handle" id="resizeH"></div>\n\n  <!-- Graph + Log column -->\n  <div class="graph-log-area">\n    <!-- Tab bar for Graph / File view -->\n    <div class="graph-tabs" id="graphTabs">\n      <div class="gtab active" data-gtab="graph" onclick="switchGraphTab(\'graph\')">Graph</div>\n      <div class="gtab" data-gtab="fileview" id="gtabFile" onclick="switchGraphTab(\'fileview\')" style="display:none">\n        <span id="gtabFileIcon">📦</span>\n        <span id="gtabFileLabel"></span>\n        <span class="gtab-close" onclick="event.stopPropagation();closeSideEditor()" title="Close file">&times;</span>\n      </div>\n    </div>\n    <div class="graph-panel" id="graphPanel">\n      <div class="graph-toolbar" id="graphToolbar" style="display:none">\n        <button class="btn" id="bcollapse" onclick="toggleCollapseAll()" title="Collapse/expand node groups" style="display:none">&#9660; Collapse</button>\n        <button class="btn" id="bedges" onclick="cycleEdgeMode()" title="Cycle edge visibility: all / selection / off">&#x27CB; Edges: all</button>\n        <div style="position:relative;display:inline-block">\n          <button class="btn" onclick="document.getElementById(\'moreMenu\').classList.toggle(\'show\')" title="More options">&#8943;</button>\n          <div id="moreMenu" class="more-menu" style="display:none;position:absolute;right:0;top:100%;z-index:50;background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:4px 0;min-width:160px;box-shadow:0 4px 12px rgba(0,0,0,.3)">\n            <div class="more-item" onclick="demoToggle();document.getElementById(\'moreMenu\').classList.remove(\'show\')">\n              <span id="bdemo">&#9654; Demo</span>\n            </div>\n            <div class="more-item" style="padding:4px 10px;display:flex;align-items:center;gap:6px">\n              <span style="color:var(--text3);font-size:11px">Speed</span>\n              <select id="dspeed" onchange="demoSpeed(this.value)" style="background:var(--bg3);border:1px solid var(--border);border-radius:var(--radius);color:var(--text2);font-size:11px;padding:1px 4px;font-family:var(--mono);cursor:pointer;flex:1">\n                <option value="0.5">0.5x</option><option value="1" selected>1x</option><option value="2">2x</option><option value="4">4x</option>\n              </select>\n            </div>\n          </div>\n        </div>\n      </div>\n      <div class="empty-msg" id="emptyMsg">Edit a .cgr file to see the dependency graph</div>\n      <svg class="sa" id="svg"></svg>\n      <div id="waves"></div>\n    </div>\n    <!-- File viewer panel (shown in place of graph) -->\n    <div class="fileview-panel" id="fileviewPanel" style="display:none">\n      <div class="fileview-hdr">\n        <span class="fileview-lock" id="fvLock" title="Read-only">🔒</span>\n        <span class="fileview-path" id="fvPath"></span>\n        <span class="dbadge" id="fvBadge" style="font-size:9px"></span>\n        <div style="margin-left:auto;display:flex;gap:6px;align-items:center">\n          <span class="fileview-msg" id="fvMsg">Read-only</span>\n          <button class="btn" id="fvEditBtn" onclick="toggleSideEdit()" style="font-size:10px;padding:2px 8px">Enable editing</button>\n        </div>\n      </div>\n      <div class="fileview-body" id="fvBody">\n        <div class="fileview-editor-wrap">\n          <div class="fv-line-numbers" id="fvLineNumbers"></div>\n          <div class="fv-editor-area">\n            <div class="fv-highlight" id="fvHighlight"></div>\n            <textarea id="fvCode" class="fv-textarea" spellcheck="false" autocomplete="off" autocapitalize="off" readonly></textarea>\n          </div>\n        </div>\n      </div>\n    </div>\n    <div class="log-resize" id="logResizeH" style="display:none"></div>\n    <div class="log-pane collapsed" id="logPane" style="display:none">\n      <div class="log-header" onclick="toggleLogCollapse()">\n        <span class="log-title" id="logTitle">Execution log</span>\n        <div class="log-actions">\n          <label class="log-follow" onclick="event.stopPropagation()" title="Auto-scroll to current step during execution">\n            <input type="checkbox" id="logFollowCb" checked> Follow\n          </label>\n          <button class="btn log-btn" onclick="event.stopPropagation();toggleLogExpand()" id="logExpandBtn" title="Expand/collapse all entries">&#8863; Collapse All</button>\n          <button class="btn log-btn" onclick="event.stopPropagation();clearLog()" title="Clear log">Clear</button>\n          <button class="btn log-btn" id="logCollapseBtn" onclick="event.stopPropagation();toggleLogCollapse()" title="Expand/collapse log">&#9650;</button>\n        </div>\n      </div>\n      <div id="progressWrap" style="height:3px;background:var(--border);display:none"><div id="progressBar" style="height:100%;width:0%;background:var(--green);transition:width .3s"></div></div>\n      <div class="log-body" id="logBody"></div>\n    </div>\n  </div>\n  <div id="toast-area" style="position:fixed;top:54px;right:340px;z-index:200;display:flex;flex-direction:column;gap:6px"></div>\n\n  <!-- Inspector -->\n  <div class="inspector" id="inspector">\n    <div class="insp-tabs" id="inspTabs">\n      <div class="tab active" data-tab="inspect" onclick="switchInspTab(\'inspect\')">Inspect</div>\n      <div class="tab" data-tab="files" onclick="switchInspTab(\'files\')" id="tabFiles" style="display:none">Files</div>\n      <div class="tab" data-tab="exec" onclick="switchInspTab(\'exec\')" id="tabExec" style="display:none">Execution <span id="historyBadge" style="display:none;font-size:9px;background:var(--bg-hover);padding:0 4px;border-radius:3px;margin-left:2px"></span></div>\n      <div class="tab" data-tab="report" onclick="switchInspTab(\'report\')" id="tabReport" style="display:none">Report</div>\n    </div>\n    <div id="inspectContent">\n      <div class="empty">Click a resource to inspect</div>\n    </div>\n    <div id="filesContent" style="display:none"></div>\n    <div id="execContent" style="display:none"></div>\n    <div id="reportContent" style="display:none"></div>\n  </div>\n</div>\n<div class="info-bar" id="infoBar"></div>\n</div>\n\n<!-- Autocomplete popup -->\n<div class="ac-popup" id="acPopup"></div>\n<div class="file-picker-backdrop" id="filePickerBackdrop">\n  <div class="file-picker" onclick="event.stopPropagation()">\n    <div class="file-picker-hdr">\n      <span style="color:var(--accent)">Open Graph</span>\n      <span style="margin-left:auto;color:var(--text3);font-size:10px">Served from workspace only</span>\n      <button class="btn" onclick="closeFilePicker()" style="font-size:10px;padding:2px 8px">Close</button>\n    </div>\n    <input class="file-picker-search" id="filePickerSearch" type="text" placeholder="Filter .cgr / .cg files" oninput="renderFilePicker(this.value)">\n    <div class="file-picker-controls">\n      <label class="file-picker-filter" for="filePickerTagFilter">\n        <span>Show</span>\n        <select id="filePickerTagFilter" onchange="renderFilePicker(filePickerSearch.value)">\n          <option value="all">all files</option>\n        </select>\n      </label>\n    </div>\n    <div class="file-picker-list" id="filePickerList"></div>\n  </div>\n</div>\n\n<script>\n// ── State ──────────────────────────────────────────────\nlet currentFile = null;\nlet isDirty = false;\nlet graphData = null;\nlet byId = {};\nlet depnts = {};\nlet selId = null;\nlet lineToRids = {};  // editor line → [resource IDs]\nlet ridToLine = {};   // resource ID → editor line\nlet editorHlLine = 0; // currently highlighted editor line (from graph hover)\nlet resolveTimer = null;\nlet templates = [];\nlet stepNames = [];\nlet collapsedNodes = {};  // node_name → true if collapsed\nlet allCollapsed = false; // global collapse state\nlet collapseInitDone = false; // true after first auto-collapse decision\nlet edgeMode = \'all\';    // \'all\' | \'selection\' | \'off\'\nlet edgeModeInitDone = false;\nlet acIdx = -1;\nlet applyPollTimer = null;\nlet applyCursor = 0;\nlet applyStopClicks = 0;\nlet availableGraphFiles = [];\nlet availableGraphFileEntries = [];\nlet filePickerTagOptions = [];\n\nconst codeEl = document.getElementById(\'code\');\nconst hlEl = document.getElementById(\'highlight\');\nconst lineNumEl = document.getElementById(\'lineNumbers\');\nconst problemsPanel = document.getElementById(\'problemsPanel\');\nconst wavesEl = document.getElementById(\'waves\');\nconst emptyMsg = document.getElementById(\'emptyMsg\');\nconst infoBar = document.getElementById(\'infoBar\');\nconst inspEl = document.getElementById(\'inspectContent\');\nconst execEl = document.getElementById(\'execContent\');\nconst reportEl = document.getElementById(\'reportContent\');\nconst statusBadge = document.getElementById(\'statusBadge\');\nconst filenameEl = document.getElementById(\'filename\');\nconst dirtyEl = document.getElementById(\'dirty\');\nconst acPopup = document.getElementById(\'acPopup\');\nconst applyBtn = document.getElementById(\'applyBtn\');\nconst rerunBtn = document.getElementById(\'rerunBtn\');\nconst filePickerBackdrop = document.getElementById(\'filePickerBackdrop\');\nconst filePickerSearch = document.getElementById(\'filePickerSearch\');\nconst filePickerTagFilter = document.getElementById(\'filePickerTagFilter\');\nconst filePickerList = document.getElementById(\'filePickerList\');\nconst filePickerBtn = document.getElementById(\'filePickerBtn\');\nlet execInputWrap = null;\nlet execInputEl = null;\nlet execInputToggleBtn = null;\nlet csrfToken = null;\nlet vaultRetryPass = null;\nlet execInputTargetRid = null;\nlet execInputTargetName = null;\nlet runningStepMeta = new Map();\nlet runningStepTicker = null;\nlet applyHeartbeatTs = null;\nlet applyPollErrorTs = null;\n\n// ── API helpers ────────────────────────────────────────\n// Compute base path so API calls work through path-prefixed proxies (e.g. Coder).\n// If served at /proxy/8420/ the base is "/proxy/8420"; if at / the base is "".\nconst _API_BASE = window.location.pathname.endsWith(\'/\')\n  ? window.location.pathname.slice(0, -1)\n  : window.location.pathname.replace(/\\/[^/]*$/, \'\');\n\nasync function api(method, path, body) {\n  const opts = { method, headers: {\'Content-Type\':\'application/json\'} };\n  const upper = String(method || \'GET\').toUpperCase();\n  if (![\'GET\', \'HEAD\', \'OPTIONS\'].includes(upper)) {\n    if (!csrfToken) {\n      const tr = await fetch(_API_BASE + \'/api/csrf\', { method: \'GET\' });\n      const tj = await tr.json();\n      if (tj.error) return tj;\n      csrfToken = tj.csrf_token || null;\n    }\n    if (csrfToken) opts.headers[\'X-CSRF-Token\'] = csrfToken;\n  }\n  if (body !== undefined) opts.body = JSON.stringify(body);\n  const r = await fetch(_API_BASE + path, opts);\n  const text = await r.text();\n  let data = null;\n  if (text) {\n    try {\n      data = JSON.parse(text);\n    } catch (_) {\n      data = null;\n    }\n  }\n  if (!r.ok) {\n    const msg = (data && data.error) ? data.error : (text ? text.slice(0, 300) : `${r.status} ${r.statusText}`);\n    return { error: `HTTP ${r.status}: ${msg}` };\n  }\n  if (data !== null) return data;\n  if (!text) return {};\n  return { error: `Invalid JSON response from ${path}: ${text.slice(0, 300)}` };\n}\n\n// ── File operations ────────────────────────────────────\nasync function loadFile(name) {\n  if (name) {\n    const openResp = await api(\'POST\', \'/api/file/open\', { name });\n    if (openResp.error) { setStatus(\'err\', openResp.error); return; }\n  }\n  const r = await api(\'GET\', \'/api/file\' + (name ? \'?name=\' + encodeURIComponent(name) : \'\'));\n  if (r.error) { setStatus(\'err\', r.error); return; }\n  currentFile = r.name;\n  codeEl.value = r.content;\n  filenameEl.textContent = r.name;\n  isDirty = false;\n  collapsedNodes = {}; collapseInitDone = false; allCollapsed = false;\n  edgeModeInitDone = false;\n  dirtyEl.style.display = \'none\';\n  updateHighlight();\n  triggerResolve();\n}\n\nfunction renderFilePicker(filter = \'\') {\n  const q = String(filter || \'\').toLowerCase().trim();\n  const activeTag = filePickerTagFilter ? filePickerTagFilter.value : \'all\';\n  const files = availableGraphFileEntries.filter(f => {\n    const matchesSearch = !q || f.name.toLowerCase().includes(q);\n    const tags = Array.isArray(f.tags) ? f.tags : [];\n    const matchesTag = activeTag === \'all\' || tags.includes(activeTag);\n    return matchesSearch && matchesTag;\n  });\n  filePickerList.innerHTML = \'\';\n  if (!files.length) {\n    filePickerList.innerHTML = \'<div class="file-picker-empty">No matching graph files</div>\';\n    return;\n  }\n  files.forEach(file => {\n    const name = file.name;\n    const isActive = currentFile === name;\n    const label = name.split(\'/\').pop();\n    const tags = Array.isArray(file.tags) ? file.tags : [];\n    const tagsHtml = tags.map(tag => `<span class="file-picker-tag ${esc(tag)}">${esc(tag)}</span>`).join(\'\');\n    const item = document.createElement(\'div\');\n    item.className = \'file-picker-item\' + (isActive ? \' active\' : \'\');\n    item.innerHTML = `<div class="file-picker-path"><span>${esc(label)}</span>${tagsHtml}</div><div class="file-picker-meta">${esc(name)}${isActive ? \' · current file\' : \'\'}</div>`;\n    item.addEventListener(\'click\', () => { void chooseFileFromPicker(name); });\n    filePickerList.appendChild(item);\n  });\n}\n\nfunction filePickerTagLabel(tag) {\n  const labels = {\n    all: \'all files\',\n    root: \'root files\',\n    example: \'examples\',\n    testing: \'testing\',\n    template: \'templates\',\n    cgr: \'.cgr\',\n    cg: \'.cg\',\n  };\n  return labels[tag] || tag;\n}\n\nfunction refreshFilePickerTagOptions() {\n  if (!filePickerTagFilter) return;\n  const current = filePickerTagFilter.value || \'all\';\n  const preferredOrder = [\'root\', \'example\', \'testing\', \'template\', \'cgr\', \'cg\'];\n  const available = new Set();\n  availableGraphFileEntries.forEach(file => {\n    (file.tags || []).forEach(tag => available.add(tag));\n  });\n  filePickerTagOptions = preferredOrder.filter(tag => available.has(tag));\n  available.forEach(tag => {\n    if (!filePickerTagOptions.includes(tag)) filePickerTagOptions.push(tag);\n  });\n  const nextValue = current === \'all\' || available.has(current) ? current : \'all\';\n  filePickerTagFilter.innerHTML = `<option value="all">${esc(filePickerTagLabel(\'all\'))}</option>`\n    + filePickerTagOptions.map(tag => `<option value="${esc(tag)}">${esc(filePickerTagLabel(tag))}</option>`).join(\'\');\n  filePickerTagFilter.value = nextValue;\n}\n\nfunction openFilePicker() {\n  if (!availableGraphFiles.length) return;\n  filePickerBackdrop.style.display = \'flex\';\n  renderFilePicker(filePickerSearch.value || \'\');\n  filePickerSearch.focus();\n  filePickerSearch.select();\n}\n\nfunction closeFilePicker() {\n  filePickerBackdrop.style.display = \'none\';\n  filePickerSearch.value = \'\';\n}\n\nasync function chooseFileFromPicker(name) {\n  closeFilePicker();\n  await loadFile(name);\n}\n\nasync function doSave() {\n  if (!currentFile) return;\n  const r = await api(\'PUT\', \'/api/file\', { name: currentFile, content: codeEl.value });\n  if (r.ok) {\n    isDirty = false;\n    dirtyEl.style.display = \'none\';\n    setStatus(\'ok\', \'Saved\');\n  } else {\n    setStatus(\'err\', r.error || \'Save failed\');\n  }\n}\n\nasync function doCheck() {\n  const pp = document.getElementById(\'problemsPanel\');\n  // Step 1: validate (resolve)\n  const r = await apiWithVaultRetry(\'POST\', \'/api/resolve\', { source: codeEl.value });\n  if (r.error) {\n    setStatus(\'err\', \'Invalid\');\n    showProblems([{ severity: \'error\', line: r.line || 0, name: \'parse\', msg: r.error }]);\n    return;\n  }\n  // Step 2: lint (best-practice warnings)\n  try {\n    const lr = await apiWithVaultRetry(\'POST\', \'/api/lint\', { source: codeEl.value });\n    const warnings = (lr.warnings || []);\n    if (warnings.length === 0) {\n      setStatus(\'ok\', r.resources + \' resources, \' + r.waves + \' waves — no warnings\');\n      hideProblems();\n    } else {\n      setStatus(\'warn\', r.resources + \' res, \' + warnings.length + \' warning(s)\');\n      showProblems(warnings);\n    }\n  } catch (e) {\n    // Lint failed but validation passed\n    setStatus(\'ok\', r.resources + \' resources, \' + r.waves + \' waves\');\n    hideProblems();\n  }\n}\n\nfunction linkifyFilePaths(msgHtml) {\n  // Make file paths in warning messages clickable if they match known external files\n  if (!currentExternalFiles.length) return msgHtml;\n  for (const f of currentExternalFiles) {\n    if (!f.exists) continue;\n    const escaped = f.path.replace(/[.*+?^${}()|[\\]\\\\]/g, \'\\\\$&\');\n    const re = new RegExp("\'" + escaped + "\'", \'g\');\n    const replacement = "\'<span class=\\"prob-file-link\\" data-resolved=\\"" + esc(f.resolved)\n      + "\\" data-type=\\"" + esc(f.type)\n      + "\\" data-path=\\"" + esc(f.path)\n      + "\\">" + esc(f.path) + " &#8599;</span>\'";\n    msgHtml = msgHtml.replace(re, replacement);\n  }\n  return msgHtml;\n}\n\nfunction showProblems(items) {\n  const pp = document.getElementById(\'problemsPanel\');\n  const errs = items.filter(i => i.severity === \'error\');\n  const warns = items.filter(i => i.severity !== \'error\');\n  let html = \'<div class="prob-summary"><span>\';\n  if (errs.length) html += \'<span style="color:var(--red)">\' + errs.length + \' error(s)</span> \';\n  if (warns.length) html += \'<span style="color:var(--amber)">\' + warns.length + \' warning(s)</span>\';\n  if (!errs.length && !warns.length) html += \'No problems\';\n  html += \'</span><span class="prob-close" onclick="hideProblems()">&times;</span></div>\';\n  // Build provenance lookup: step name → template path\n  const provMap = {};\n  if (graphData && graphData.graph && graphData.graph.resources) {\n    for (const r of graphData.graph.resources) {\n      if (r.provenance) provMap[r.short_name] = r.provenance;\n    }\n  }\n  for (const item of items) {\n    const cls = item.severity === \'error\' ? \'prob-err\' : (item.severity === \'warn\' ? \'prob-warn\' : \'prob-info\');\n    const icon = item.severity === \'error\' ? \'&#10006;\' : (item.severity === \'warn\' ? \'&#9888;\' : \'&#9432;\');\n    const loc = item.line ? \'line \' + item.line : \'\';\n    let msgHtml = linkifyFilePaths(esc(item.msg));\n    // If this step comes from a template, add a "View template" link\n    const tplPath = provMap[item.name];\n    const ef = tplPath ? currentExternalFiles.find(f => f.path === tplPath && f.exists) : null;\n    if (ef) {\n      msgHtml += \' <span class="prob-file-link" data-resolved="\' + esc(ef.resolved)\n        + \'" data-type="\' + esc(ef.type) + \'" data-path="\' + esc(ef.path)\n        + \'" data-step="\' + esc(item.name || \'\')\n        + \'">View template &#8599;</span>\';\n    }\n    html += \'<div class="prob-item" onclick="scrollToLine(\' + (item.line || 1) + \')">\'\n      + \'<span class="prob-icon \' + cls + \'">\' + icon + \'</span>\'\n      + \'<span class="prob-msg">\' + msgHtml + \'</span>\'\n      + \'<span class="prob-loc">\' + esc(item.name || \'\') + (loc ? \' :\' + item.line : \'\') + \'</span>\'\n      + \'</div>\';\n  }\n  pp.innerHTML = html;\n  pp.style.display = \'block\';\n  // Highlight error lines in gutter\n  if (errs.length && errs[0].line) {\n    lineNumEl.querySelectorAll(\'div\').forEach(d => d.classList.remove(\'ln-err\'));\n    const errDiv = lineNumEl.children[errs[0].line - 1];\n    if (errDiv) errDiv.classList.add(\'ln-err\');\n  }\n}\n\nfunction hideProblems() {\n  document.getElementById(\'problemsPanel\').style.display = \'none\';\n  lineNumEl.querySelectorAll(\'div\').forEach(d => d.classList.remove(\'ln-err\'));\n}\n\nfunction scrollToLine(line) {\n  const lineHeight = 13 * 1.6;\n  codeEl.focus();\n  const container = document.getElementById(\'editorContainer\');\n  const pos = (line - 1) * lineHeight;\n  container.scrollTop = Math.max(0, pos - container.clientHeight / 3);\n}\n\n// ── Resolve (live graph update) ────────────────────────\nasync function triggerResolve() {\n  clearTimeout(resolveTimer);\n  resolveTimer = setTimeout(async () => {\n    try {\n      const r = await apiWithVaultRetry(\'POST\', \'/api/resolve\', { source: codeEl.value });\n      if (r.error) {\n        showError(r.error, r.line);\n        setStatus(\'err\', \'Parse error\');\n      } else {\n        hideError();\n        graphData = r;\n        renderGraph(r);\n        setStatus(\'ok\', r.resources + \' res, \' + r.waves_count + \' waves\');\n        // Update Files tab if there are external files\n        const ef = r.external_files || [];\n        if (ef.length > 0) {\n          document.getElementById(\'tabFiles\').style.display = \'\';\n          renderFilesTab(ef);\n          // Auto-show Files tab on first load when inspector is at default state\n          // but NOT if the user is currently viewing the Execution tab\n          const inspDefault = document.querySelector(\'#inspectContent .empty\');\n          const execTabActive = document.querySelector(\'.insp-tabs .tab[data-tab="exec"].active\');\n          if (inspDefault && !execTabActive) switchInspTab(\'files\');\n        } else {\n          document.getElementById(\'tabFiles\').style.display = \'none\';\n        }\n      }\n    } catch (e) {\n      console.error(\'Resolve failed:\', e);\n      setStatus(\'err\', \'Resolve failed: \' + (e.message || e));\n    }\n  }, 400);\n}\n\nfunction setStatus(type, text) {\n  statusBadge.className = \'status status-\' + type;\n  statusBadge.textContent = text;\n}\n\nfunction promptForVaultPass(status) {\n  const msg = status === \'invalid\'\n    ? \'Vault passphrase was rejected. Enter the vault passphrase:\'\n    : \'This graph needs a vault passphrase. Enter the vault passphrase:\';\n  return window.prompt(msg, \'\');\n}\n\nasync function apiWithVaultRetry(method, path, body) {\n  const payload = body ? { ...body } : {};\n  for (let attempt = 0; attempt < 3; attempt++) {\n    if (vaultRetryPass && payload.vault_pass === undefined) payload.vault_pass = vaultRetryPass;\n    const r = await api(method, path, payload);\n    if (!r || !r.needs_vault_pass) {\n      vaultRetryPass = null;\n      return r;\n    }\n    vaultRetryPass = null;\n    const entered = promptForVaultPass(r.vault_status || \'missing\');\n    if (entered === null) return { error: r.error || \'Vault passphrase required\', line: r.line || 0 };\n    vaultRetryPass = entered;\n    payload.vault_pass = entered;\n  }\n  return { error: \'Vault passphrase was rejected.\' };\n}\n\n// ── Syntax highlighting ────────────────────────────────\nfunction highlightSyntax(src) {\n  // Use marker tokens (\\x01type\\x02 ... \\x03) so regex passes don\'t match\n  // the HTML class attributes generated by earlier passes.\n  let out = src\n    .replace(/&/g, \'&amp;\').replace(/</g, \'&lt;\').replace(/>/g, \'&gt;\');\n  // Comments\n  out = out.replace(/^(\\s*)(#.*)$/gm, \'$1\\x01comment\\x02$2\\x03\');\n  // Title\n  out = out.replace(/^(---.+---)$/gm, \'\\x01title\\x02$1\\x03\');\n  // Variables ${...}\n  out = out.replace(/(\\$\\{[^}]+\\})/g, \'\\x01var\\x02$1\\x03\');\n  // Strings "..."\n  out = out.replace(/"([^"]*)"/g, \'\\x01str\\x02"$1"\\x03\');\n  // Step names [...]\n  out = out.replace(/\\[([^\\]]+)\\]/g, \'\\x01step\\x02[$1]\\x03\');\n  // Using lines: underline the path to hint it\'s clickable\n  out = out.replace(/^(\\s*)(using)\\s+(\\S+)/gm, \'$1\\x01kw\\x02$2\\x03 \\x01upath\\x02$3\\x03\');\n  // from template lines: underline the template path\n  out = out.replace(/\\b(from)\\s+(\\S+)\\s*:/g, \'\\x01kw\\x02$1\\x03 \\x01upath\\x02$2\\x03:\');\n  // Keywords at line start\n  out = out.replace(/^(\\s*)(set|target|template|verify|parallel|race|race into|each|stage|phase|first|needs|skip if|run|always run|env|when|description|as|timeout|retry|if fails|collect|tags|secrets|inventory|gather facts|get|post|put|patch|delete|auth|header|body|expect)\\b/gm,\n    \'$1\\x01kw\\x02$2\\x03\');\n  // $ command prefix\n  out = out.replace(/(\\$\\s)/g, \'\\x01prop\\x02$1\\x03\');\n  // from keyword\n  out = out.replace(/\\b(from)\\s/g, \'\\x01kw\\x02$1\\x03 \');\n  // Convert markers to real spans\n  out = out.replace(/\\x01(\\w+)\\x02/g, \'<span class="hl-$1">\').replace(/\\x03/g, \'</span>\');\n  return out;\n}\n\nfunction updateHighlight() {\n  hlEl.innerHTML = highlightSyntax(codeEl.value) + \'\\n\';\n  updateLineNumbers();\n  // Sync textarea height to highlight layer so clicks work after scrolling\n  requestAnimationFrame(() => {\n    codeEl.style.height = hlEl.offsetHeight + \'px\';\n  });\n}\n\nfunction updateLineNumbers() {\n  const lines = codeEl.value.split(\'\\n\').length;\n  let html = \'\';\n  for (let i = 1; i <= lines; i++) {\n    html += \'<div>\' + i + \'</div>\';\n  }\n  lineNumEl.innerHTML = html;\n}\n\nfunction showError(msg, line) {\n  showProblems([{ severity: \'error\', line: line || 0, name: \'parse\', msg: msg }]);\n}\n\nfunction hideError() {\n  hideProblems();\n}\n\nfunction cycleEdgeMode() {\n  const modes = [\'all\', \'selection\', \'off\'];\n  edgeMode = modes[(modes.indexOf(edgeMode) + 1) % modes.length];\n  const btn = document.getElementById(\'bedges\');\n  btn.innerHTML = \'&#x27CB; Edges: \' + edgeMode;\n  drawA();\n}\n\nfunction toggleCollapseAll() {\n  if (!graphData || !graphData.graph) return;\n  const nodeNames = Object.keys(graphData.graph.nodes || {});\n  allCollapsed = !allCollapsed;\n  collapsedNodes = {};\n  if (allCollapsed) nodeNames.forEach(nn => { collapsedNodes[nn] = true; });\n  renderGraph(graphData);\n}\n\n// ── Graph diff tracking ───────────────────────────────\nlet prevGraphSnapshot = {};  // id → {run, check, needs_str}\nlet diffAdded = new Set();\nlet diffChanged = new Set();\n\n// ── Graph rendering ────────────────────────────────────\nfunction renderGraph(data) {\n  wavesEl.innerHTML = \'\';\n  emptyMsg.style.display = \'none\';\n  document.getElementById(\'graphToolbar\').style.display = \'flex\';\n  if (typeof demo !== \'undefined\' && demo.state !== \'idle\') demo.reset();\n\n  // Build current snapshot for diff (before overwriting byId)\n  const curSnapshot = {};\n  data.graph.resources.forEach(r => {\n    curSnapshot[r.id] = { run: r.run || \'\', check: r.check || \'\', needs: (r.needs||[]).join(\',\') };\n  });\n  // Compute diffs: added, removed, changed\n  diffAdded = new Set();\n  diffChanged = new Set();\n  const hasPrev = Object.keys(prevGraphSnapshot).length > 0;\n  if (hasPrev) {\n    for (const id of Object.keys(curSnapshot)) {\n      if (!(id in prevGraphSnapshot)) diffAdded.add(id);\n      else {\n        const p = prevGraphSnapshot[id], c = curSnapshot[id];\n        if (p.run !== c.run || p.check !== c.check || p.needs !== c.needs) diffChanged.add(id);\n      }\n    }\n  }\n  prevGraphSnapshot = curSnapshot;\n\n  byId = {};\n  depnts = {};\n  selId = null;\n  inspEl.innerHTML = \'<div class="empty">Click a resource to inspect</div>\';\n\n  data.graph.resources.forEach(r => { byId[r.id] = r; });\n  data.graph.resources.forEach(r => {\n    r.needs.forEach(d => { if (!depnts[d]) depnts[d] = []; depnts[d].push(r.id); });\n  });\n\n  // Build bidirectional line↔resource maps for editor↔graph linking\n  lineToRids = {};\n  ridToLine = {};\n  data.graph.resources.forEach(r => {\n    const ln = r.source_line;\n    if (ln && ln > 0) {\n      if (!lineToRids[ln]) lineToRids[ln] = [];\n      lineToRids[ln].push(r.id);\n      ridToLine[r.id] = ln;\n    }\n  });\n\n  // Determine if we should use node grouping (multi-node graphs)\n  const nodeNames = Object.keys(data.graph.nodes || {});\n  const multiNode = nodeNames.length > 1;\n  const nR = data.graph.resources.length;\n\n  // Clean up stale collapsed state: remove nodes that no longer exist, add new ones if allCollapsed\n  const nodeSet = new Set(nodeNames);\n  Object.keys(collapsedNodes).forEach(nn => { if (!nodeSet.has(nn)) delete collapsedNodes[nn]; });\n  if (allCollapsed) nodeNames.forEach(nn => { collapsedNodes[nn] = true; });\n\n  // Auto-collapse: if >50 resources and multi-node, start collapsed (first render only)\n  if (!collapseInitDone && multiNode && nR > 50) {\n    collapseInitDone = true;\n    allCollapsed = true;\n    nodeNames.forEach(nn => { collapsedNodes[nn] = true; });\n  }\n\n  // Auto edge mode: default to \'selection\' for large graphs (first render only)\n  if (!edgeModeInitDone) {\n    edgeModeInitDone = true;\n    edgeMode = nR > 50 ? \'selection\' : \'all\';\n    const eBtn = document.getElementById(\'bedges\');\n    if (eBtn) eBtn.innerHTML = \'&#x27CB; Edges: \' + edgeMode;\n  }\n\n  // Show/hide collapse button\n  const colBtn = document.getElementById(\'bcollapse\');\n  if (multiNode) {\n    colBtn.style.display = \'\';\n    colBtn.innerHTML = allCollapsed ? \'&#9654; Expand\' : \'&#9660; Collapse\';\n  } else {\n    colBtn.style.display = \'none\';\n  }\n\n  // Info bar\n  const nW = data.graph.waves.length;\n  const nG = new Set(data.graph.resources.filter(r => r.parallel_group).map(r => r.parallel_group)).size;\n  const nHosts = nodeNames.filter(n => (data.graph.nodes[n] || {}).via === \'ssh\').length;\n  const nLocal = nodeNames.length - nHosts;\n  let ib = `<span class="st"><span class="stn">${nR}</span> resources</span>`;\n  ib += `<span class="st"><span class="stn">${nW}</span> waves</span>`;\n  if (nG) ib += `<span class="st"><span class="stn">${nG}</span> groups</span>`;\n  if (nHosts) ib += `<span class="st"><span class="stn">${nHosts}</span> hosts</span>`;\n  if (nHosts && nLocal) ib += `<span class="st">+ <span class="stn">${nLocal}</span> local</span>`;\n  infoBar.innerHTML = ib;\n\n  // Render waves\n  data.graph.waves.forEach((w, wi) => {\n    const wc = document.createElement(\'div\'); wc.className = \'wc\';\n    const wl = document.createElement(\'div\'); wl.className = \'wl\';\n\n    // Build a summary from bracket names\n    const descs = w.map(rid => {\n      const r = byId[rid]; if (!r) return null;\n      return bracketName(rid, r);\n    }).filter(Boolean);\n    const unique = [...new Set(descs)];\n    const maxShow = 3;\n    let summary = unique.length <= maxShow\n      ? unique.join(\', \')\n      : unique.slice(0, maxShow).join(\', \') + ` +${unique.length - maxShow} more`;\n\n    const isFirst = wi === 0;\n    const hasVerify = w.some(rid => byId[rid] && byId[rid].is_verify);\n    let phase = \'\';\n    if (hasVerify) phase = \'verify\';\n    else if (isFirst && w.every(rid => byId[rid] && byId[rid].needs.length === 0)) phase = \'roots\';\n\n    const phaseTag = phase ? `<span class="wl-phase">${phase}</span> ` : \'\';\n    wl.innerHTML = `${phaseTag}WAVE ${wi + 1} <span class="wl-count">(${w.length})</span> <span class="wl-summary">${esc(summary)}</span>`;\n    wc.appendChild(wl);\n    const wr = document.createElement(\'div\'); wr.className = \'wr\';\n\n    if (multiNode) {\n      // Group resources by node_name, then by parallel_group within each node\n      const byNode = {};\n      w.forEach(rid => {\n        const res = byId[rid]; if (!res) return;\n        const nn = res.node_name || \'_local\';\n        if (!byNode[nn]) byNode[nn] = [];\n        byNode[nn].push(rid);\n      });\n\n      Object.entries(byNode).forEach(([nn, rids]) => {\n        const nodeInfo = (data.graph.nodes || {})[nn];\n        const isCollapsed = !!collapsedNodes[nn];\n\n        // Build state summary for this node\'s resources in this wave\n        let nOk = 0, nFail = 0, nPend = 0;\n        rids.forEach(rid => {\n          const r = byId[rid];\n          if (r && r.state) {\n            if (r.state.status === \'success\' || r.state.status === \'skip_check\') nOk++;\n            else if (r.state.status === \'failed\') nFail++;\n            else nPend++;\n          } else nPend++;\n        });\n\n        // Only show node group if there are non-barrier resources, or all are barriers\n        const nonBarrier = rids.filter(rid => byId[rid] && !byId[rid].is_barrier);\n        const showGroup = nonBarrier.length > 0 || rids.length <= 2;\n\n        if (!showGroup) {\n          // Just render barrier cards directly (don\'t wrap in node group)\n          rids.forEach(rid => { const c = mkCard(rid); if (c) wr.appendChild(c); });\n          return;\n        }\n\n        const ng = document.createElement(\'div\');\n        ng.className = \'ng\' + (isCollapsed ? \' collapsed\' : \'\');\n        ng.dataset.node = nn;\n\n        // Header\n        const hdr = document.createElement(\'div\'); hdr.className = \'ng-hdr\';\n        const toggle = document.createElement(\'span\'); toggle.className = \'ng-toggle\';\n        toggle.textContent = isCollapsed ? \'\\u25B6\' : \'\\u25BC\';\n        hdr.appendChild(toggle);\n        const name = document.createElement(\'span\'); name.className = \'ng-name\';\n        name.textContent = nn;\n        hdr.appendChild(name);\n        if (nodeInfo) {\n          const via = document.createElement(\'span\'); via.className = \'ng-via\';\n          via.textContent = nodeInfo.via === \'ssh\'\n            ? `ssh ${nodeInfo.user ? nodeInfo.user + \'@\' : \'\'}${nodeInfo.host}${nodeInfo.port && nodeInfo.port !== \'22\' ? \':\' + nodeInfo.port : \'\'}`\n            : \'local\';\n          hdr.appendChild(via);\n        }\n        const count = document.createElement(\'span\'); count.className = \'ng-count\';\n        count.textContent = nonBarrier.length + \' steps\';\n        hdr.appendChild(count);\n        // State summary\n        const stateEl = document.createElement(\'span\'); stateEl.className = \'ng-state\';\n        if (nOk) { const s = document.createElement(\'span\'); s.className = \'ns-ok\'; s.textContent = \'\\u2713 \' + nOk; stateEl.appendChild(s); }\n        if (nFail) { const s = document.createElement(\'span\'); s.className = \'ns-fail\'; s.textContent = \'\\u2717 \' + nFail; stateEl.appendChild(s); }\n        if (nPend && (nOk || nFail)) { const s = document.createElement(\'span\'); s.className = \'ns-pend\'; s.textContent = \'\\u25CB \' + nPend; stateEl.appendChild(s); }\n        hdr.appendChild(stateEl);\n\n        hdr.onclick = () => {\n          collapsedNodes[nn] = !collapsedNodes[nn];\n          // Update global collapse state\n          const nodeNames = Object.keys(data.graph.nodes || {});\n          allCollapsed = nodeNames.every(n => collapsedNodes[n]);\n          renderGraph(graphData);\n        };\n        ng.appendChild(hdr);\n\n        // Body (cards) — only created if expanded\n        if (!isCollapsed) {\n          const body = document.createElement(\'div\'); body.className = \'ng-body\';\n          // Sub-group by parallel_group within this node\n          const pgroups = {}; const ungrouped = [];\n          rids.forEach(rid => {\n            const res = byId[rid]; if (!res) return;\n            if (res.parallel_group) {\n              if (!pgroups[res.parallel_group]) pgroups[res.parallel_group] = [];\n              pgroups[res.parallel_group].push(rid);\n            } else { ungrouped.push(rid); }\n          });\n          ungrouped.forEach(rid => { const c = mkCard(rid); if (c) body.appendChild(c); });\n          Object.entries(pgroups).forEach(([gkey, gids]) => {\n            const info = describeGroup(gkey, gids);\n            const grp = document.createElement(\'div\'); grp.className = \'grp \' + info.cls;\n            const lbl = document.createElement(\'div\'); lbl.className = \'grp-lbl\';\n            lbl.textContent = info.icon + \' \' + info.label;\n            grp.appendChild(lbl);\n            const cards = document.createElement(\'div\'); cards.className = \'grp-cards\';\n            gids.forEach(rid => { const c = mkCard(rid); if (c) cards.appendChild(c); });\n            grp.appendChild(cards);\n            body.appendChild(grp);\n          });\n          ng.appendChild(body);\n        }\n        wr.appendChild(ng);\n      });\n    } else {\n      // Single-node graph: original flat layout\n      const groups = {}; const ungrouped = [];\n      w.forEach(rid => {\n        const res = byId[rid]; if (!res) return;\n        if (res.parallel_group) {\n          if (!groups[res.parallel_group]) groups[res.parallel_group] = [];\n          groups[res.parallel_group].push(rid);\n        } else { ungrouped.push(rid); }\n      });\n      ungrouped.forEach(rid => { const c = mkCard(rid); if (c) wr.appendChild(c); });\n      Object.entries(groups).forEach(([gkey, gids]) => {\n        const info = describeGroup(gkey, gids);\n        const grp = document.createElement(\'div\'); grp.className = \'grp \' + info.cls;\n        const lbl = document.createElement(\'div\'); lbl.className = \'grp-lbl\';\n        lbl.textContent = info.icon + \' \' + info.label;\n        grp.appendChild(lbl);\n        const cards = document.createElement(\'div\'); cards.className = \'grp-cards\';\n        gids.forEach(rid => { const c = mkCard(rid); if (c) cards.appendChild(c); });\n        grp.appendChild(cards);\n        wr.appendChild(grp);\n      });\n    }\n\n    wc.appendChild(wr);\n    wavesEl.appendChild(wc);\n  });\n  setTimeout(() => drawA(), 60);\n}\n\n// ── SVG dependency arrows ──────────────────────────────\nfunction drawA(hDeps, hDepnts) {\n  const svg = document.getElementById(\'svg\');\n  const gp = document.getElementById(\'graphPanel\');\n  if (!svg || !gp || !graphData || !graphData.graph) return;\n  svg.setAttribute(\'width\', gp.scrollWidth);\n  svg.setAttribute(\'height\', gp.scrollHeight);\n  svg.innerHTML = \'\';\n  // Edge mode: \'off\' draws nothing, \'selection\' only draws when highlighting\n  if (edgeMode === \'off\') return;\n  if (edgeMode === \'selection\' && !hDeps && !hDepnts) return;\n  const NS = \'http://www.w3.org/2000/svg\';\n  const defs = document.createElementNS(NS, \'defs\');\n  [\'a-d\',\'a-t\',\'a-n\'].forEach(id => {\n    const m = document.createElementNS(NS, \'marker\');\n    m.setAttribute(\'id\', id); m.setAttribute(\'viewBox\', \'0 0 10 10\');\n    m.setAttribute(\'refX\', \'9\'); m.setAttribute(\'refY\', \'5\');\n    m.setAttribute(\'markerWidth\', \'6\'); m.setAttribute(\'markerHeight\', \'6\');\n    m.setAttribute(\'orient\', \'auto-start-reverse\');\n    const p = document.createElementNS(NS, \'path\');\n    p.setAttribute(\'d\', \'M2 2L8 5L2 8\');\n    p.setAttribute(\'fill\', id === \'a-d\' ? \'var(--green)\' : id === \'a-t\' ? \'var(--purple)\' : \'var(--border-hi)\');\n    m.appendChild(p); defs.appendChild(m);\n  });\n  svg.appendChild(defs);\n  const hd = new Set(hDeps || []), ht = new Set(hDepnts || []);\n  const gr = gp.getBoundingClientRect();\n  const resources = graphData.graph.resources || [];\n\n  // Build set of collapsed node names for fast lookup\n  const collSet = new Set(Object.keys(collapsedNodes).filter(k => collapsedNodes[k]));\n\n  resources.forEach(res => {\n    // Skip edges where both ends are within the same collapsed node\n    const resNode = res.node_name || \'\';\n    (res.needs || []).forEach(dep => {\n      const depRes = byId[dep];\n      if (depRes && collSet.has(resNode) && collSet.has(depRes.node_name || \'\') && resNode === (depRes.node_name || \'\')) return;\n      const from = document.getElementById(\'c-\' + dep.replace(/[^a-zA-Z0-9]/g, \'_\'));\n      const to = document.getElementById(\'c-\' + res.id.replace(/[^a-zA-Z0-9]/g, \'_\'));\n      if (!from || !to || from.offsetParent === null || to.offsetParent === null) return;\n      const fr = from.getBoundingClientRect(), tr = to.getBoundingClientRect();\n      const x1 = fr.left + fr.width / 2 - gr.left + gp.scrollLeft;\n      const y1 = fr.bottom - gr.top + gp.scrollTop;\n      const x2 = tr.left + tr.width / 2 - gr.left + gp.scrollLeft;\n      const y2 = tr.top - gr.top + gp.scrollTop;\n      const my = (y1 + y2) / 2;\n      const p = document.createElementNS(NS, \'path\');\n      p.setAttribute(\'d\', `M${x1} ${y1} C${x1} ${my} ${x2} ${my} ${x2} ${y2}`);\n      const isDep = hd.has(dep) && hd.has(res.id);\n      const isDepnt = ht.has(dep) && ht.has(res.id);\n      p.setAttribute(\'class\', \'ap\' + (isDep ? \' hd\' : isDepnt ? \' ht\' : \'\'));\n      p.setAttribute(\'marker-end\', `url(#${isDep ? \'a-d\' : isDepnt ? \'a-t\' : \'a-n\'})`);\n      svg.appendChild(p);\n    });\n  });\n}\n\nfunction describeGroup(gkey, items) {\n  const parts = gkey.split(\'.\'); const ct = parts[parts.length - 1];\n  let parent = \'\';\n  for (let i = parts.length - 2; i >= 0; i--) { if (!parts[i].startsWith(\'__\')) { parent = parts[i].replace(/_/g, \' \'); break; } }\n  const n = items.length;\n  if (ct.includes(\'race\')) {\n    const sample = items[0] && byId[items[0]];\n    const into = sample && sample.race_into ? ` into ${sample.race_into}` : \'\';\n    return { cls: \'grp-race\', icon: \'\\u26A1\', label: `race${into}: ${parent} (${n} branches)` };\n  }\n  if (ct.includes(\'each\')) return { cls: \'grp-each\', icon: \'\\u2194\', label: `each: ${parent} (${n} items)` };\n  if (ct.includes(\'phase\')) return { cls: \'grp-phase\', icon: \'\\u25C6\', label: `phase: ${parent} (${n} items)` };\n  return { cls: \'grp-par\', icon: \'\\u2551\', label: `parallel: ${parent} (${n} branches)` };\n}\n\nfunction mkCard(rid) {\n  const res = byId[rid]; if (!res) return null;\n  const card = document.createElement(\'div\');\n  let cls = \'nc\'; if (res.is_barrier) cls += \' barrier\';\n  if (res.state) {\n    if (res.state.status === \'success\' || res.state.status === \'skip_check\') cls += \' st-success\';\n    else if (res.state.status === \'failed\') cls += \' st-failed\';\n    else if (res.state.status === \'warned\') cls += \' st-warned\';\n  }\n  card.className = cls;\n  card.id = \'c-\' + rid.replace(/[^a-zA-Z0-9]/g, \'_\');\n  card.dataset.id = rid;\n  card.onclick = () => selectResource(rid);\n  card.onmouseenter = () => highlightEditorLine(rid);\n  card.onmouseleave = () => clearEditorHighlight();\n  // Primary: bracket name derived from ID (matches what user wrote in editor)\n  const bname = bracketName(rid, res);\n  const nm = document.createElement(\'div\'); nm.className = \'nn\'; nm.textContent = bname;\n  card.appendChild(nm);\n  // Secondary: template description (only if it adds info beyond the bracket name)\n  const desc = res.description || \'\';\n  const bnameClean = bname.replace(/[\\[\\]›]/g, \'\').trim().toLowerCase();\n  const descClean = desc.toLowerCase();\n  if (desc && descClean !== bnameClean && !descClean.startsWith(bnameClean)) {\n    const dd = document.createElement(\'div\'); dd.className = \'nd\'; dd.textContent = desc;\n    card.appendChild(dd);\n  }\n  if (res.state) {\n    const sb = document.createElement(\'span\'); sb.className = \'stb\';\n    if (res.state.status === \'success\') { sb.className += \' stb-success\'; sb.textContent = res.state.ms + \'ms\'; }\n    else if (res.state.status === \'failed\') { sb.className += \' stb-failed\'; sb.textContent = \'FAIL\'; }\n    card.appendChild(sb);\n  }\n  const badges = document.createElement(\'div\'); badges.className = \'nb\';\n  if (res.is_verify) { const b = document.createElement(\'span\'); b.className = \'bg bg-v\'; b.textContent = \'verify\'; badges.appendChild(b); }\n  if (res.http_method) { const b = document.createElement(\'span\'); b.className = \'bg\'; b.style.cssText = \'background:rgba(210,153,34,.15);color:var(--amber);font-weight:700\'; b.textContent = res.http_method; badges.appendChild(b); }\n  if (res.collect_key) { const b = document.createElement(\'span\'); b.className = \'bg\'; b.style.cssText = \'background:rgba(88,166,255,.15);color:var(--accent)\'; b.textContent = \'📋 \' + res.collect_key; badges.appendChild(b); }\n  if (res.needs.length === 0 && !res.is_barrier) { const b = document.createElement(\'span\'); b.className = \'bg bg-r\'; b.textContent = \'root\'; badges.appendChild(b); }\n  if (res.provenance) { const b = document.createElement(\'span\'); b.className = \'bg bg-p\'; b.textContent = res.provenance.split(\'/\').pop(); badges.appendChild(b); }\n  if (badges.children.length) card.appendChild(badges);\n  // Graph diff highlighting\n  if (diffAdded.has(rid)) card.classList.add(\'diff-added\');\n  else if (diffChanged.has(rid)) card.classList.add(\'diff-changed\');\n  return card;\n}\n\n// ── Inspector ──────────────────────────────────────────\nfunction selectResource(rid) {\n  document.querySelectorAll(\'.nc\').forEach(c => c.classList.remove(\'sel\', \'dep\', \'depnt\', \'cdim\'));\n  if (selId === rid) { selId = null; inspEl.innerHTML = \'<div class="empty">Click a resource to inspect</div>\'; drawA(); updateExecInputTarget(); return; }\n  selId = rid;\n  const card = document.getElementById(\'c-\' + rid.replace(/[^a-zA-Z0-9]/g, \'_\'));\n  if (card) card.classList.add(\'sel\');\n\n  // Highlight deps\n  const allUp = new Set(), allDn = new Set();\n  function wU(id) { const r = byId[id]; if (!r) return; r.needs.forEach(d => { allUp.add(d); wU(d); }); }\n  function wD(id) { (depnts[id] || []).forEach(d => { allDn.add(d); wD(d); }); }\n  wU(rid); wD(rid);\n  document.querySelectorAll(\'.nc\').forEach(c => {\n    const cid = c.dataset.id; if (cid === rid) return;\n    if (allUp.has(cid)) c.classList.add(\'dep\');\n    else if (allDn.has(cid)) c.classList.add(\'depnt\');\n    else c.classList.add(\'cdim\');\n  });\n  drawA([...allUp, rid], [...allDn, rid]);\n\n  renderInspector(rid);\n  updateExecInputTarget();\n\n  // Scroll log pane to this resource\'s entry (if it exists)\n  scrollLogToResource(rid);\n\n  // Scroll editor to the source line and highlight it\n  scrollEditorToLine(rid);\n}\n\nfunction esc(s) {\n  return s ? String(s).replace(/&/g,\'&amp;\')\n                      .replace(/</g,\'&lt;\')\n                      .replace(/>/g,\'&gt;\')\n                      .replace(/"/g,\'&quot;\')\n                      .replace(/\'/g,\'&#x27;\') : \'\';\n}\n\n\nfunction termClassList(state) {\n  const cls = [];\n  if (state.bold) cls.push(\'term-bold\');\n  if (state.fg) cls.push(\'term-fg-\' + state.fg);\n  return cls.join(\' \');\n}\n\nfunction normalizeTerminalText(raw) {\n  const s = String(raw || \'\');\n  let out = \'\';\n  let line = \'\';\n  for (let i = 0; i < s.length; i++) {\n    const ch = s[i];\n    if (ch === \'\\x1b\') {\n      const m = s.slice(i).match(/^\\x1b\\[[0-9;?]*[A-Za-z]/);\n      if (m) {\n        const seq = m[0];\n        const op = seq[seq.length - 1];\n        if (op === \'m\') line += seq;\n        i += seq.length - 1;\n        continue;\n      }\n    }\n    if (ch === \'\\r\') {\n      line = \'\';\n      continue;\n    }\n    if (ch === \'\\n\') {\n      out += line + \'\\n\';\n      line = \'\';\n      continue;\n    }\n    if (ch === \'\\b\') {\n      line = line.slice(0, -1);\n      continue;\n    }\n    if (ch < \' \' && ch !== \'\\t\') continue;\n    line += ch;\n  }\n  return out + line;\n}\n\nfunction ansiToHtml(raw) {\n  const text = normalizeTerminalText(raw);\n  const state = { bold: false, fg: null };\n  let html = \'\';\n  let buf = \'\';\n  const flush = () => {\n    if (!buf) return;\n    const cls = termClassList(state);\n    const escBuf = esc(buf);\n    html += cls ? `<span class="${cls}">${escBuf}</span>` : escBuf;\n    buf = \'\';\n  };\n  for (let i = 0; i < text.length; ) {\n    if (text[i] === \'\\x1b\') {\n      const m = text.slice(i).match(/^\\x1b\\[([0-9;]*)m/);\n      if (m) {\n        flush();\n        const codes = m[1] ? m[1].split(\';\').map(n => parseInt(n || \'0\', 10)) : [0];\n        for (const code of codes) {\n          if (code === 0) {\n            state.bold = false;\n            state.fg = null;\n          } else if (code === 1) {\n            state.bold = true;\n          } else if (code === 22) {\n            state.bold = false;\n          } else if (code === 39) {\n            state.fg = null;\n          } else if (30 <= code && code <= 37) {\n            state.fg = [\'black\',\'red\',\'green\',\'yellow\',\'blue\',\'magenta\',\'cyan\',\'white\'][code - 30];\n          } else if (90 <= code && code <= 97) {\n            state.fg = [\'black\',\'red\',\'green\',\'yellow\',\'blue\',\'magenta\',\'cyan\',\'white\'][code - 90];\n          }\n        }\n        i += m[0].length;\n        continue;\n      }\n    }\n    buf += text[i];\n    i += 1;\n  }\n  flush();\n  return html;\n}\n\nfunction renderTerminalInto(el, raw) {\n  if (!el) return;\n  el.dataset.raw = String(raw || \'\');\n  el.innerHTML = ansiToHtml(el.dataset.raw);\n}\n\nfunction appendTerminalChunk(el, chunk) {\n  if (!el) return;\n  const next = (el.dataset.raw || \'\') + String(chunk || \'\');\n  renderTerminalInto(el, next);\n}\n\nfunction formatDurationShort(ms) {\n  const totalSeconds = Math.max(0, Math.floor((ms || 0) / 1000));\n  const hours = Math.floor(totalSeconds / 3600);\n  const minutes = Math.floor((totalSeconds % 3600) / 60);\n  const seconds = totalSeconds % 60;\n  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;\n  if (minutes > 0) return `${minutes}m ${seconds}s`;\n  return `${seconds}s`;\n}\n\nfunction isoToMs(ts) {\n  const parsed = Date.parse(ts || \'\');\n  return Number.isFinite(parsed) ? parsed : null;\n}\n\nfunction runningMetaSummary(meta) {\n  const now = Date.now();\n  const startedMs = isoToMs(meta.startedAt);\n  const lastOutputMs = isoToMs(meta.lastOutputAt);\n  const parts = [];\n  if (startedMs !== null) parts.push(`running ${formatDurationShort(now - startedMs)}`);\n  if (meta.timeoutS) parts.push(`timeout ${formatDurationShort(meta.timeoutS * 1000)}`);\n  if (meta.pid) {\n    if (meta.procAlive === false) {\n      const rc = meta.returnCode === null || meta.returnCode === undefined ? \'?\' : meta.returnCode;\n      parts.push(`pid ${meta.pid} exited rc=${rc}`);\n    } else {\n      parts.push(`pid ${meta.pid}`);\n    }\n  }\n  if (lastOutputMs !== null) parts.push(`last output ${formatDurationShort(now - lastOutputMs)} ago`);\n  else parts.push(\'no output yet\');\n  if (meta.interactive) parts.push(\'waiting for input is possible\');\n  if (meta.procAlive === false) parts.push(\'step has not reported completion yet\');\n  return parts.join(\' | \');\n}\n\nfunction updateRunningMetaDisplay(rid) {\n  const meta = runningStepMeta.get(rid);\n  if (!meta) return;\n  const summary = runningMetaSummary(meta);\n  document.querySelectorAll(`[data-running-meta="${rid}"]`).forEach(el => {\n    el.textContent = summary;\n    el.classList.toggle(\'waiting\', !!meta.interactive);\n  });\n  document.querySelectorAll(`[data-running-elapsed="${rid}"]`).forEach(el => {\n    const startedMs = isoToMs(meta.startedAt);\n    el.textContent = startedMs === null ? \'running...\' : formatDurationShort(Date.now() - startedMs);\n  });\n}\n\nfunction startRunningStepTicker() {\n  if (runningStepTicker) return;\n  runningStepTicker = setInterval(() => {\n    for (const rid of runningStepMeta.keys()) updateRunningMetaDisplay(rid);\n  }, 1000);\n}\n\nfunction stopRunningStepTicker() {\n  if (!runningStepTicker) return;\n  clearInterval(runningStepTicker);\n  runningStepTicker = null;\n}\n\nfunction refreshExecInputAvailability() {\n  const hasInteractive = [...runningStepMeta.values()].some(meta => meta.interactive);\n  setExecInputVisible(hasInteractive);\n}\n\nfunction updateApplyConnectionStatus() {\n  if (!applyPollTimer) return;\n  const now = Date.now();\n  if (applyPollErrorTs && now - applyPollErrorTs < 4000) {\n    logTitle.textContent = \'Executing... (polling lost)\';\n    return;\n  }\n  if (!applyHeartbeatTs) {\n    logTitle.textContent = \'Executing... (waiting for heartbeat)\';\n    return;\n  }\n  const age = now - applyHeartbeatTs;\n  if (age > 4000) {\n    logTitle.textContent = `Executing... (heartbeat stale: ${formatDurationShort(age)})`;\n    return;\n  }\n  logTitle.textContent = `Executing... (connected, last heartbeat ${formatDurationShort(age)} ago)`;\n}\n\n// Extract human-readable bracket name from resource ID\n// web-1.install_nginx → [install nginx]\n// web-1.install_nginx.update_cache → [install nginx] › update cache\n// web-1.__verify_68 → (use description instead)\nfunction bracketName(rid, res) {\n  const parts = rid.split(\'.\');\n  parts.shift(); // remove node name\n  if (parts.length === 0) return res.description || res.short_name;\n  // Verify blocks use their description\n  if (parts[0].startsWith(\'__verify\')) return res.description || \'verify\';\n  // Format: replace underscores with spaces\n  const humanize = s => s.replace(/_/g, \' \');\n  if (parts.length === 1) {\n    return \'[\' + humanize(parts[0]) + \']\';\n  }\n  // Child of a parent: [parent] › child\n  return \'[\' + humanize(parts[0]) + \'] › \' + humanize(parts[parts.length - 1]);\n}\n\nfunction renderInspector(rid) {\n  const r = byId[rid];\n  const deps = r.needs.map(d => byId[d]).filter(Boolean);\n  const dn = (depnts[rid] || []).map(d => byId[d]).filter(Boolean);\n  let h = `<h2>${esc(r.short_name)}</h2>`;\n  h += `<div class="ds"><h3>Description</h3><div style="font-size:12px">${esc(r.description) || \'<em style="color:var(--text3)">none</em>\'}</div></div>`;\n\n  let tb = \'\';\n  if (r.is_verify) tb = `<span class="dbadge" style="background:rgba(247,120,186,.15);color:var(--pink)">verify</span>`;\n  else if (r.check && r.check !== \'false\') tb = `<span class="dbadge" style="background:rgba(88,166,255,.15);color:var(--accent)">check → run</span>`;\n  else tb = `<span class="dbadge" style="background:rgba(63,185,80,.15);color:var(--green)">run</span>`;\n  if (r.run_as) tb += ` <span class="dbadge" style="background:rgba(210,153,34,.15);color:var(--amber)">as ${esc(r.run_as)}</span>`;\n  if (r.is_race && r.race_into) tb += ` <span class="dbadge" style="background:rgba(240,136,62,.15);color:var(--coral)">race into ${esc(r.race_into)}</span>`;\n  else if (r.is_race) tb += ` <span class="dbadge" style="background:rgba(240,136,62,.15);color:var(--coral)">race branch</span>`;\n  if (r.is_barrier) tb += ` <span class="dbadge" style="background:var(--bg3);color:var(--text3)">barrier</span>`;\n  h += `<div class="ds"><h3>Execution</h3>${tb}</div>`;\n\n  h += `<div class="ds"><h3>Commands</h3>`;\n  if (r.check && r.check !== \'false\') h += `<div style="font-size:10px;color:var(--text3);margin:3px 0">check</div><div class="dcmd">${esc(r.check)}</div>`;\n  h += `<div style="font-size:10px;color:var(--text3);margin:3px 0">run</div><div class="dcmd">${esc(r.run)}</div></div>`;\n\n  if (deps.length) {\n    h += `<div class="ds"><h3>Depends on (${deps.length})</h3><div class="ddeps">`;\n    deps.forEach(d => { h += `<span class="dchip" data-action="select-resource" data-rid="${esc(d.id)}" style="border-left:2px solid var(--green)">${esc(d.short_name)}</span>`; });\n    h += `</div></div>`;\n  }\n  if (dn.length) {\n    h += `<div class="ds"><h3>Required by (${dn.length})</h3><div class="ddeps">`;\n    dn.forEach(d => { h += `<span class="dchip" data-action="select-resource" data-rid="${esc(d.id)}" style="border-left:2px solid var(--purple)">${esc(d.short_name)}</span>`; });\n    h += `</div></div>`;\n  }\n  if (r.state) {\n    const s = r.state;\n    let sc = `<span class="dbadge" style="background:rgba(63,185,80,.15);color:var(--green)">${s.status} (${s.ms}ms)</span>`;\n    if (s.ts) sc += ` <span style="font-size:10px;color:var(--text3)">${s.ts}</span>`;\n    h += `<div class="ds"><h3>Last Run</h3>${sc}</div>`;\n  }\n  if (r.collect_key) {\n    h += `<div class="ds"><h3>Collect</h3><div><span class="dbadge" style="background:rgba(88,166,255,.15);color:var(--accent)">📋 ${esc(r.collect_key)}</span></div></div>`;\n  }\n  h += `<div class="ds"><h3>Resource ID</h3><div style="font-family:var(--mono);font-size:10px;color:var(--text3);word-break:break-all">${esc(rid)}</div></div>`;\n  // Link to log pane if an entry exists\n  h += `<div class="ds"><span style="font-size:10px;color:var(--accent);cursor:pointer;text-decoration:underline" data-action="scroll-log-resource" data-rid="${esc(rid)}">Show in execution log &#8599;</span></div>`;\n  inspEl.innerHTML = h;\n}\n\n// ── Bidirectional editor↔graph linking ────────────────\n\n// Graph card hover → highlight editor line\nfunction highlightEditorLine(rid) {\n  const ln = ridToLine[rid];\n  if (!ln || ln <= 0) return;\n  editorHlLine = ln;\n  // Highlight line number in gutter\n  const divs = lineNumEl.querySelectorAll(\'div\');\n  divs.forEach(d => d.classList.remove(\'ln-hl\'));\n  if (divs[ln - 1]) divs[ln - 1].classList.add(\'ln-hl\');\n  // Add/move highlight strip in editor\n  let strip = document.getElementById(\'editorLineHl\');\n  if (!strip) {\n    strip = document.createElement(\'div\');\n    strip.id = \'editorLineHl\';\n    strip.className = \'editor-line-hl\';\n    document.getElementById(\'editorArea\').appendChild(strip);\n  }\n  const lineHeight = 13 * 1.6; // font-size * line-height\n  strip.style.top = (10 + (ln - 1) * lineHeight) + \'px\'; // 10px = padding-top\n  strip.style.display = \'\';\n}\n\nfunction clearEditorHighlight() {\n  editorHlLine = 0;\n  lineNumEl.querySelectorAll(\'.ln-hl\').forEach(d => d.classList.remove(\'ln-hl\'));\n  const strip = document.getElementById(\'editorLineHl\');\n  if (strip) strip.style.display = \'none\';\n  // Clear graph card highlights from editor hover\n  document.querySelectorAll(\'.nc.editor-hl\').forEach(c => c.classList.remove(\'editor-hl\'));\n}\n\n// Graph card click → scroll editor to source line\nfunction scrollEditorToLine(rid) {\n  const ln = ridToLine[rid];\n  if (!ln || ln <= 0) return;\n  highlightEditorLine(rid);\n  // Scroll the editor container so the line is visible\n  const container = document.getElementById(\'editorContainer\');\n  const lineHeight = 13 * 1.6;\n  const targetY = (ln - 1) * lineHeight;\n  const viewH = container.clientHeight;\n  container.scrollTop = Math.max(0, targetY - viewH / 3);\n}\n\n// Editor hover → highlight graph cards\nlet editorHoverTimer = null;\nfunction onEditorHover(e) {\n  clearTimeout(editorHoverTimer);\n  editorHoverTimer = setTimeout(() => {\n    // Determine which line the cursor is on\n    const container = document.getElementById(\'editorContainer\');\n    const area = document.getElementById(\'editorArea\');\n    const rect = area.getBoundingClientRect();\n    const lineHeight = 13 * 1.6;\n    const y = e.clientY - rect.top + container.scrollTop;\n    const ln = Math.floor((y - 10) / lineHeight) + 1; // 10px padding\n    if (ln <= 0) return;\n\n    // Clear previous highlights\n    document.querySelectorAll(\'.nc.editor-hl\').forEach(c => c.classList.remove(\'editor-hl\'));\n\n    // Find resources on this line (or nearby — within 5 lines for multi-line steps)\n    let rids = lineToRids[ln] || [];\n    if (rids.length === 0) {\n      // Search nearby lines (the step body may span several lines)\n      for (let offset = 1; offset <= 5; offset++) {\n        rids = lineToRids[ln - offset] || [];\n        if (rids.length > 0) break;\n      }\n    }\n\n    // Highlight matching cards\n    rids.forEach(rid => {\n      const safeId = \'c-\' + rid.replace(/[^a-zA-Z0-9]/g, \'_\');\n      const card = document.getElementById(safeId);\n      if (card) card.classList.add(\'editor-hl\');\n    });\n  }, 80); // small debounce\n}\n\nfunction onEditorLeave() {\n  clearTimeout(editorHoverTimer);\n  document.querySelectorAll(\'.nc.editor-hl\').forEach(c => c.classList.remove(\'editor-hl\'));\n}\n\n// Wire up editor hover tracking\ndocument.getElementById(\'editorArea\').addEventListener(\'mousemove\', onEditorHover);\ndocument.getElementById(\'editorArea\').addEventListener(\'mouseleave\', onEditorLeave);\n\n// ── Editor events ──────────────────────────────────────\ncodeEl.addEventListener(\'input\', () => {\n  isDirty = true;\n  dirtyEl.style.display = \'\';\n  updateHighlight();\n  triggerResolve();\n});\n\ncodeEl.addEventListener(\'keydown\', e => {\n  // Autocomplete navigation\n  if (acPopup.classList.contains(\'show\')) {\n    const items = acPopup._items || [];\n    if (e.key === \'ArrowDown\') { e.preventDefault(); acIdx = Math.min(acIdx + 1, items.length - 1); renderAc(items); return; }\n    if (e.key === \'ArrowUp\') { e.preventDefault(); acIdx = Math.max(acIdx - 1, 0); renderAc(items); return; }\n    if (e.key === \'Enter\' || e.key === \'Tab\') { e.preventDefault(); if (acIdx >= 0) pickAc(acIdx); else hideAc(); return; }\n    if (e.key === \'Escape\') { e.preventDefault(); hideAc(); return; }\n  }\n  if (e.key === \'Escape\') {\n    codeEl.blur();\n    return;\n  }\n  if (e.key === \'Tab\' && !e.shiftKey) {\n    e.preventDefault();\n    const s = codeEl.selectionStart, end = codeEl.selectionEnd;\n    codeEl.value = codeEl.value.substring(0, s) + \'  \' + codeEl.value.substring(end);\n    codeEl.selectionStart = codeEl.selectionEnd = s + 2;\n    codeEl.dispatchEvent(new Event(\'input\'));\n  }\n  if (e.key === \'Tab\' && e.shiftKey) {\n    e.preventDefault();\n    const s = codeEl.selectionStart;\n    const text = codeEl.value;\n    const lineStart = text.lastIndexOf(\'\\n\', s - 1) + 1;\n    const lineText = text.substring(lineStart, s);\n    const spaces = lineText.match(/^ {1,2}/);\n    if (spaces) {\n      codeEl.value = text.substring(0, lineStart) + text.substring(lineStart + spaces[0].length);\n      codeEl.selectionStart = codeEl.selectionEnd = s - spaces[0].length;\n      codeEl.dispatchEvent(new Event(\'input\'));\n    }\n  }\n  if (e.ctrlKey && e.key === \'s\') { e.preventDefault(); doSave(); }\n  if (e.ctrlKey && e.shiftKey && e.key === \'V\') { e.preventDefault(); doCheck(); }\n  if (e.ctrlKey && e.key === \'Enter\') { e.preventDefault(); doApply(); }\n  if (e.ctrlKey && e.key === \'l\') { e.preventDefault(); toggleLogCollapse(); }\n});\n\n// Click on using/from template lines opens side editor\n// Uses cursor position (reliable) rather than mouse coordinate math\ncodeEl.addEventListener(\'click\', e => {\n  // Use a short delay so selectionStart reflects the click position\n  setTimeout(() => {\n    const pos = codeEl.selectionStart;\n    const textBefore = codeEl.value.substring(0, pos);\n    const lineIdx = textBefore.split(\'\\n\').length - 1;\n    const lines = codeEl.value.split(\'\\n\');\n    if (lineIdx < 0 || lineIdx >= lines.length) return;\n    const line = lines[lineIdx].trim();\n    // Match "using path/template" or "from path/template:"\n    let tplPath = null;\n    const usingMatch = line.match(/^using\\s+(\\S+)/);\n    if (usingMatch) tplPath = usingMatch[1].replace(/\\s.*$/, \'\');\n    const fromMatch = line.match(/from\\s+(\\S+)\\s*:/);\n    if (fromMatch) tplPath = fromMatch[1];\n    if (!tplPath) return;\n    // Find matching external file\n    const ef = currentExternalFiles.find(f => f.path === tplPath && f.exists);\n    if (ef) openSideEditor(ef.resolved, ef.type, ef.path);\n  }, 10);\n});\n\n// Trigger autocomplete on [ or after "from "\ncodeEl.addEventListener(\'keyup\', e => {\n  if (e.key === \'[\' || e.key === \'/\' || e.key === \' \') {\n    updateStepNames(codeEl.value);\n    showAutocomplete();\n  } else if (acPopup.classList.contains(\'show\') && e.key.length === 1) {\n    showAutocomplete();\n  }\n});\n\n// Hide autocomplete on blur\ncodeEl.addEventListener(\'blur\', () => { setTimeout(hideAc, 200); });\n\n// Line numbers scroll naturally as part of editor-container flex layout\n\n// ── Report tab ─────────────────────────────────────────\nasync function loadReport() {\n  reportEl.innerHTML = \'<div class="empty">Loading...</div>\';\n  try {\n    const r = await api(\'GET\', \'/api/report\');\n    const outputs = r.outputs || [];\n    if (!outputs.length) {\n      reportEl.innerHTML = \'<div class="empty">No collected outputs yet.<br><span style="font-size:10px;color:var(--text3)">Add <code>collect "key"</code> to steps and run apply.</span></div>\';\n      return;\n    }\n    let html = \'<div style="margin-bottom:8px;display:flex;gap:6px;align-items:center">\';\n    html += \'<span style="font-size:11px;font-weight:600;color:var(--accent);font-family:var(--mono)">Collected Outputs</span>\';\n    html += \'<span style="color:var(--text3);font-size:10px">(\' + outputs.length + \')</span>\';\n    html += \'<span style="flex:1"></span>\';\n    html += \'<button class="btn" style="font-size:10px;padding:2px 6px" onclick="exportReport(\\\'json\\\')">JSON</button>\';\n    html += \'<button class="btn" style="font-size:10px;padding:2px 6px" onclick="exportReport(\\\'csv\\\')">CSV</button>\';\n    html += \'</div>\';\n    for (const o of outputs) {\n      const status = (o.rc === 0 || o.rc === null) ? \'<span style="color:var(--green)">ok</span>\' : \'<span style="color:var(--red)">exit=\' + o.rc + \'</span>\';\n      html += \'<div style="margin-bottom:10px;border:1px solid var(--border);border-radius:var(--radius);overflow:hidden">\';\n      html += \'<div style="padding:4px 8px;background:var(--bg3);display:flex;align-items:center;gap:6px;font-size:11px;font-family:var(--mono)">\';\n      html += \'<span style="color:var(--accent);font-weight:600">\' + esc(o.key) + \'</span>\';\n      html += \'<span style="color:var(--text3)">on</span>\';\n      html += \'<span style="font-weight:600">\' + esc(o.node) + \'</span>\';\n      html += \'<span style="flex:1"></span>\';\n      html += status;\n      html += \'<span style="color:var(--text3);font-size:10px">\' + (o.ms||0) + \'ms</span>\';\n      html += \'</div>\';\n      const stdout = (o.stdout || \'\').trim();\n      if (stdout) {\n        html += \'<pre style="padding:6px 8px;font-size:10px;font-family:var(--mono);color:var(--text2);white-space:pre-wrap;word-break:break-all;max-height:120px;overflow-y:auto;margin:0">\' + esc(stdout) + \'</pre>\';\n      }\n      html += \'</div>\';\n    }\n    reportEl.innerHTML = html;\n  } catch (e) {\n    reportEl.innerHTML = \'<div class="empty" style="color:var(--red)">\' + esc(String(e)) + \'</div>\';\n  }\n}\n\nasync function exportReport(fmt) {\n  try {\n    const r = await api(\'GET\', \'/api/report\');\n    const outputs = r.outputs || [];\n    let text, mimeType, ext;\n    if (fmt === \'json\') {\n      text = JSON.stringify(outputs, null, 2);\n      mimeType = \'application/json\'; ext = \'json\';\n    } else {\n      // CSV\n      const keys = [...new Set(outputs.map(o => o.key))].sort();\n      const nodes = [...new Set(outputs.map(o => o.node))].sort();\n      let lines = [];\n      if (nodes.length > 1) {\n        lines.push([\'node\', ...keys].join(\',\'));\n        const byNode = {};\n        for (const o of outputs) {\n          if (!byNode[o.node]) byNode[o.node] = {};\n          byNode[o.node][o.key] = (o.stdout||\'\').trim().replace(/"/g,\'""\');\n        }\n        for (const n of nodes) {\n          lines.push([n, ...keys.map(k => \'"\' + (byNode[n]?.[k]||\'\') + \'"\')].join(\',\'));\n        }\n      } else {\n        lines.push(\'key,node,value,status,ms\');\n        for (const o of outputs) {\n          const s = (o.rc===0||o.rc===null) ? \'ok\' : \'exit=\'+o.rc;\n          lines.push([o.key, o.node, \'"\'+(o.stdout||\'\').trim().replace(/"/g,\'""\')+\'"\', s, o.ms||0].join(\',\'));\n        }\n      }\n      text = lines.join(\'\\n\') + \'\\n\';\n      mimeType = \'text/csv\'; ext = \'csv\';\n    }\n    const blob = new Blob([text], {type: mimeType});\n    const a = document.createElement(\'a\');\n    a.href = URL.createObjectURL(blob);\n    a.download = \'report.\' + ext;\n    a.click();\n    URL.revokeObjectURL(a.href);\n  } catch (e) {\n    alert(\'Export failed: \' + e);\n  }\n}\n\n// ── File viewer (tabbed in graph area) ─────────────────\nlet currentExternalFiles = [];\nlet sideFilePath = null;    // resolved path of the open file\nlet sideFileType = null;    // \'template\' | \'inventory\' | \'secrets\'\nlet sideEditing = false;    // true when user has enabled editing\nlet sideOrigContent = \'\';   // original content for dirty detection\nconst fvCodeEl = document.getElementById(\'fvCode\');\nconst fvHlEl = document.getElementById(\'fvHighlight\');\nconst fvLineNumsEl = document.getElementById(\'fvLineNumbers\');\n\nfunction switchGraphTab(tab) {\n  document.querySelectorAll(\'.graph-tabs .gtab\').forEach(t => t.classList.toggle(\'active\', t.dataset.gtab === tab));\n  document.getElementById(\'graphPanel\').style.display = tab === \'graph\' ? \'\' : \'none\';\n  document.getElementById(\'fileviewPanel\').style.display = tab === \'fileview\' ? \'\' : \'none\';\n}\n\nfunction renderFilesTab(files) {\n  currentExternalFiles = files;\n  const el = document.getElementById(\'filesContent\');\n  if (!files.length) { el.innerHTML = \'<div class="empty">No external files</div>\'; return; }\n  const icons = { template: \'📦\', inventory: \'📋\', secrets: \'🔒\' };\n  const colors = { template: \'var(--accent)\', inventory: \'var(--green)\', secrets: \'var(--amber)\' };\n  let h = \'<div style="padding:12px">\';\n  const byType = {};\n  files.forEach(f => { (byType[f.type] = byType[f.type] || []).push(f); });\n  for (const [type, group] of Object.entries(byType)) {\n    h += \'<div class="ds"><h3>\' + esc(icons[type] || \'📄\') + \' \' + esc(type) + \'s (\' + group.length + \')</h3>\';\n    for (const f of group) {\n      if (f.exists) {\n        const isOpen = sideFilePath === f.resolved;\n        h += \'<div style="cursor:pointer;padding:4px 8px;margin:2px 0;border-radius:var(--radius);border:1px solid \'\n          + (isOpen ? colors[type] : \'var(--border)\') + \';background:\' + (isOpen ? \'var(--bg-hover)\' : \'var(--bg3)\')\n          + \';display:flex;align-items:center;gap:6px;transition:all .15s"\'\n          + \' data-action="open-side-editor" data-resolved="\' + esc(f.resolved) + \'" data-type="\' + esc(f.type) + \'" data-path="\' + esc(f.path) + \'"\'\n          + \' onmouseover="this.style.borderColor=\\\'\' + colors[type] + \'\\\'" onmouseout="if(\' + !isOpen + \')this.style.borderColor=\\\'var(--border)\\\'">\'\n          + \'<span style="font-family:var(--mono);font-size:11px;color:\' + colors[type] + \'">\' + esc(f.path) + \'</span>\'\n          + (isOpen ? \'<span style="font-size:9px;color:var(--text3);margin-left:auto">open</span>\'\n                    : \'<span style="font-size:9px;color:var(--text3);margin-left:auto">click to open</span>\')\n          + \'</div>\';\n      } else {\n        h += \'<div style="padding:4px 8px;margin:2px 0;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg3);opacity:.5;display:flex;align-items:center;gap:6px">\'\n          + \'<span style="font-family:var(--mono);font-size:11px;color:var(--red)">\' + esc(f.path) + \'</span>\'\n          + \'<span style="font-size:9px;color:var(--red);margin-left:auto">not found</span>\'\n          + \'</div>\';\n      }\n    }\n    if (type === \'template\') {\n      h += \'<div style="font-size:10px;color:var(--text3);margin-top:4px;font-style:italic">Loaded from repo directory</div>\';\n    }\n    h += \'</div>\';\n  }\n  h += \'</div>\';\n  el.innerHTML = h;\n}\n\nfunction updateFvHighlight() {\n  fvHlEl.innerHTML = highlightSyntax(fvCodeEl.value) + \'\\n\';\n  const lines = fvCodeEl.value.split(\'\\n\').length;\n  let lnHtml = \'\';\n  for (let i = 1; i <= lines; i++) lnHtml += \'<div>\' + i + \'</div>\';\n  fvLineNumsEl.innerHTML = lnHtml;\n  // Sync textarea height\n  requestAnimationFrame(() => { fvCodeEl.style.height = fvHlEl.offsetHeight + \'px\'; });\n}\n\n// Keep file viewer highlight in sync when editing\nfvCodeEl.addEventListener(\'input\', updateFvHighlight);\nfvCodeEl.addEventListener(\'scroll\', () => {\n  fvHlEl.scrollTop = fvCodeEl.scrollTop;\n  fvHlEl.scrollLeft = fvCodeEl.scrollLeft;\n});\n\nasync function openSideEditor(resolvedPath, type, displayPath, scrollToStep) {\n  const icons = { template: \'📦\', inventory: \'📋\', secrets: \'🔒\' };\n  const badgeColors = { template: \'background:rgba(88,166,255,.15);color:var(--accent)\',\n    inventory: \'background:rgba(63,185,80,.15);color:var(--green)\',\n    secrets: \'background:rgba(210,153,34,.15);color:var(--amber)\' };\n\n  sideFilePath = resolvedPath;\n  sideFileType = type;\n  sideEditing = false;\n\n  // Update tab\n  const tabFile = document.getElementById(\'gtabFile\');\n  document.getElementById(\'gtabFileIcon\').textContent = icons[type] || \'📄\';\n  document.getElementById(\'gtabFileLabel\').textContent = displayPath || resolvedPath.split(\'/\').slice(-2).join(\'/\');\n  tabFile.style.display = \'\';\n  switchGraphTab(\'fileview\');\n\n  // Update header\n  document.getElementById(\'fvLock\').textContent = \'🔒\';\n  document.getElementById(\'fvLock\').title = \'Read-only\';\n  document.getElementById(\'fvLock\').style.color = \'\';\n  document.getElementById(\'fvPath\').textContent = resolvedPath;\n  document.getElementById(\'fvPath\').title = resolvedPath;\n  const badge = document.getElementById(\'fvBadge\');\n  badge.textContent = type;\n  badge.style.cssText = badgeColors[type] || \'\';\n  document.getElementById(\'fvMsg\').textContent = \'Read-only\';\n  document.getElementById(\'fvMsg\').style.color = \'\';\n  const editBtn = document.getElementById(\'fvEditBtn\');\n  editBtn.textContent = \'Enable editing\';\n  editBtn.onclick = toggleSideEdit;\n  editBtn.className = \'btn\';\n  editBtn.style.display = \'\';\n  // Remove any leftover cancel/save buttons\n  const oldCancel = document.getElementById(\'fvCancelBtn\');\n  if (oldCancel) oldCancel.remove();\n  const oldSave = document.getElementById(\'fvSaveBtn\');\n  if (oldSave) oldSave.remove();\n\n  // Set textarea to loading, readonly\n  fvCodeEl.value = \'Loading...\';\n  fvCodeEl.readOnly = true;\n  updateFvHighlight();\n\n  // Re-render files tab to highlight open file\n  if (currentExternalFiles.length) renderFilesTab(currentExternalFiles);\n\n  try {\n    const r = await api(\'GET\', \'/api/file/view?path=\' + encodeURIComponent(resolvedPath));\n    if (r.error) {\n      fvCodeEl.value = \'Error: \' + r.error;\n      updateFvHighlight();\n      editBtn.style.display = \'none\';\n    } else {\n      sideOrigContent = r.content;\n      fvCodeEl.value = r.content;\n      updateFvHighlight();\n      // Scroll to a specific step if requested\n      if (scrollToStep) {\n        requestAnimationFrame(() => scrollFvToStep(scrollToStep));\n      }\n    }\n  } catch (e) {\n    fvCodeEl.value = \'Failed to load: \' + e;\n    updateFvHighlight();\n    editBtn.style.display = \'none\';\n  }\n}\n\nfunction scrollFvToStep(stepName) {\n  const lines = fvCodeEl.value.split(\'\\n\');\n  const lineHeight = 20.8; // 13px * 1.6 line-height\n  // Search for [step_name]: pattern or closest match\n  let targetLine = -1;\n  for (let i = 0; i < lines.length; i++) {\n    const trimmed = lines[i].trim();\n    // Match [step name]: exactly\n    if (trimmed.startsWith(\'[\' + stepName + \']\')) { targetLine = i; break; }\n    // Match partial — step name contained in bracket\n    if (trimmed.startsWith(\'[\') && trimmed.includes(stepName)) { targetLine = i; break; }\n    // Match "run $" or "check $" lines containing the step context\n    if (trimmed.includes(stepName)) { if (targetLine < 0) targetLine = i; }\n  }\n  if (targetLine >= 0) {\n    const scrollContainer = document.getElementById(\'fvBody\');\n    const scrollPos = targetLine * lineHeight - scrollContainer.clientHeight / 3;\n    scrollContainer.scrollTop = Math.max(0, scrollPos);\n    // Highlight the line in the line numbers\n    const lnDivs = fvLineNumsEl.children;\n    if (lnDivs[targetLine]) {\n      lnDivs[targetLine].style.background = \'rgba(88,166,255,.2)\';\n      lnDivs[targetLine].style.fontWeight = \'700\';\n      lnDivs[targetLine].style.color = \'var(--accent)\';\n    }\n  }\n}\n\nfunction closeSideEditor() {\n  if (sideEditing && fvCodeEl.value !== sideOrigContent) {\n    if (!confirm(\'Discard unsaved changes to this file?\')) return;\n  }\n  document.getElementById(\'gtabFile\').style.display = \'none\';\n  document.getElementById(\'fileviewPanel\').style.display = \'none\';\n  switchGraphTab(\'graph\');\n  sideFilePath = null;\n  sideFileType = null;\n  sideEditing = false;\n  if (currentExternalFiles.length) renderFilesTab(currentExternalFiles);\n}\n\nfunction toggleSideEdit() {\n  if (!sideEditing) {\n    if (!confirm(\'This is a shared file. Changes will affect all graphs that import it.\\n\\nEnable editing?\')) return;\n    sideEditing = true;\n    fvCodeEl.readOnly = false;\n    fvCodeEl.focus();\n    document.getElementById(\'fvLock\').textContent = \'✎\';\n    document.getElementById(\'fvLock\').title = \'Editing\';\n    document.getElementById(\'fvLock\').style.color = \'var(--amber)\';\n    document.getElementById(\'fvMsg\').textContent = \'Editing — changes affect all graphs\';\n    document.getElementById(\'fvMsg\').style.color = \'var(--amber)\';\n    // Replace edit button with save + cancel\n    const editBtn = document.getElementById(\'fvEditBtn\');\n    editBtn.textContent = \'Save\';\n    editBtn.className = \'btn btn-green\';\n    editBtn.onclick = saveSideEditor;\n    // Add cancel\n    const cancel = document.createElement(\'button\');\n    cancel.className = \'btn\';\n    cancel.textContent = \'Cancel\';\n    cancel.id = \'fvCancelBtn\';\n    cancel.onclick = cancelSideEdit;\n    editBtn.parentElement.appendChild(cancel);\n  }\n}\n\nasync function saveSideEditor() {\n  const msg = document.getElementById(\'fvMsg\');\n  const text = fvCodeEl.value;\n  try {\n    const result = await api(\'POST\', \'/api/file/save\', { path: sideFilePath, content: text });\n    if (result.error) {\n      msg.textContent = \'Save failed: \' + result.error;\n      msg.style.color = \'var(--red)\';\n    } else {\n      sideOrigContent = text;\n      msg.textContent = \'Saved\';\n      msg.style.color = \'var(--green)\';\n      setTimeout(() => resetSideToReadOnly(), 1500);\n      triggerResolve();\n    }\n  } catch (e) {\n    msg.textContent = \'Save failed: \' + e;\n    msg.style.color = \'var(--red)\';\n  }\n}\n\nfunction cancelSideEdit() {\n  if (fvCodeEl.value !== sideOrigContent) {\n    if (!confirm(\'Discard changes?\')) return;\n  }\n  fvCodeEl.value = sideOrigContent;\n  updateFvHighlight();\n  resetSideToReadOnly();\n}\n\nfunction resetSideToReadOnly() {\n  sideEditing = false;\n  fvCodeEl.readOnly = true;\n  document.getElementById(\'fvLock\').textContent = \'🔒\';\n  document.getElementById(\'fvLock\').title = \'Read-only\';\n  document.getElementById(\'fvLock\').style.color = \'\';\n  document.getElementById(\'fvMsg\').textContent = \'Read-only\';\n  document.getElementById(\'fvMsg\').style.color = \'\';\n  const editBtn = document.getElementById(\'fvEditBtn\');\n  editBtn.textContent = \'Enable editing\';\n  editBtn.className = \'btn\';\n  editBtn.onclick = toggleSideEdit;\n  const cancel = document.getElementById(\'fvCancelBtn\');\n  if (cancel) cancel.remove();\n}\n\n// Keyboard shortcuts\ndocument.addEventListener(\'keydown\', e => {\n  // Escape: close side panels\n  if (e.key === \'Escape\') {\n    if (sideFilePath) { closeSideEditor(); return; }\n  }\n  // Ctrl+Enter: Apply (or Stop if running)\n  if (e.key === \'Enter\' && (e.ctrlKey || e.metaKey) && !e.shiftKey) {\n    e.preventDefault(); doApply(); return;\n  }\n  // Ctrl+Shift+Enter: Re-run All\n  if (e.key === \'Enter\' && (e.ctrlKey || e.metaKey) && e.shiftKey) {\n    e.preventDefault(); doApplyFresh(); return;\n  }\n  // Ctrl+Shift+P: Plan (switch to exec tab + show plan)\n  if ((e.key === \'p\' || e.key === \'P\') && (e.ctrlKey || e.metaKey) && e.shiftKey) {\n    e.preventDefault(); switchInspTab(\'exec\'); return;\n  }\n});\n\n// ── Inspector tabs ─────────────────────────────────────\nfunction switchInspTab(tab) {\n  document.querySelectorAll(\'.insp-tabs .tab\').forEach(t => t.classList.toggle(\'active\', t.dataset.tab === tab));\n  document.getElementById(\'inspectContent\').style.display = tab === \'inspect\' ? \'\' : \'none\';\n  document.getElementById(\'filesContent\').style.display = tab === \'files\' ? \'\' : \'none\';\n  document.getElementById(\'execContent\').style.display = tab === \'exec\' ? \'\' : \'none\';\n  document.getElementById(\'reportContent\').style.display = tab === \'report\' ? \'\' : \'none\';\n  if (tab === \'report\') loadReport();\n  if (tab === \'exec\') loadHistory();\n}\n\n// ── Execution history ─────────────────────────────────\nasync function loadHistory() {\n  try {\n    const r = await api(\'GET\', \'/api/history\');\n    if (!r.runs || !r.runs.length) return;\n    const badge = document.getElementById(\'historyBadge\');\n    badge.textContent = r.runs.length;\n    badge.style.display = \'\';\n    // Add history dropdown to exec content if not already present\n    let hdr = document.getElementById(\'historyDropdown\');\n    if (!hdr) {\n      hdr = document.createElement(\'div\');\n      hdr.id = \'historyDropdown\';\n      hdr.style.cssText = \'padding:6px 8px;border-bottom:1px solid var(--border);font-size:11px;display:flex;align-items:center;gap:6px\';\n      hdr.innerHTML = \'<span style="color:var(--text3)">History:</span><select id="historySel" style="flex:1;background:var(--bg3);color:var(--text);border:1px solid var(--border);border-radius:var(--radius);padding:2px 4px;font-size:11px;font-family:var(--mono)" onchange="showHistoryRun(this.value)"><option value="">— current run —</option></select>\';\n      execEl.insertBefore(hdr, execEl.firstChild);\n    }\n    const sel = document.getElementById(\'historySel\');\n    const oldVal = sel.value;\n    sel.innerHTML = \'<option value="">— current run —</option>\';\n    r.runs.forEach((run, i) => {\n      const total = run.ok + run.fail + run.skip + run.timeout;\n      const icon = run.fail || run.timeout ? \'✗\' : \'✓\';\n      const ts = run.ts ? run.ts.replace(\'T\', \' \').substring(0, 19) : \'?\';\n      sel.innerHTML += `<option value="${i}">${icon} ${ts} — ${total} steps (${run.ok}✓ ${run.fail}✗ ${run.skip}↷)</option>`;\n    });\n    sel.value = oldVal;\n    window._historyRuns = r.runs;\n  } catch(e) {}\n}\n\nfunction showHistoryRun(idx) {\n  if (idx === \'\' || !window._historyRuns) return;\n  const run = window._historyRuns[parseInt(idx)];\n  if (!run) return;\n  let log = document.getElementById(\'execLog\');\n  if (!log) {\n    execEl.innerHTML += \'<div class="exec-log" id="execLog"></div>\';\n    log = document.getElementById(\'execLog\');\n  }\n  let h = `<div class="ev-wave" style="margin-bottom:4px">Run: ${esc(run.ts || \'?\')}</div>`;\n  run.entries.forEach(e => {\n    const cls = e.status === \'success\' ? \'ev-ok\' : e.status === \'failed\' ? \'ev-fail\' : e.status === \'timeout\' ? \'ev-fail\' : \'ev-skip\';\n    const icon = e.status === \'success\' ? \'✓\' : e.status === \'failed\' ? \'✗\' : e.status === \'timeout\' ? \'⏱\' : \'↷\';\n    h += `<div class="ev ${cls}"><span class="ev-icon">${icon}</span><span class="ev-name">${esc(e.id)}</span><span class="ev-time">${e.ms}ms</span></div>`;\n  });\n  const total = run.ok + run.fail + run.skip + run.timeout;\n  const hasFail = run.fail > 0 || run.timeout > 0;\n  h += `<div class="ev-done ${hasFail ? \'fail\' : \'ok\'}">${hasFail ? \'✗ FAILED\' : \'✓ COMPLETE\'} (${total} steps)</div>`;\n  log.innerHTML = h;\n}\n\n// ── Log pane controls ──────────────────────────────────\nconst logPane = document.getElementById(\'logPane\');\nconst logResizeH = document.getElementById(\'logResizeH\');\nconst logBody = document.getElementById(\'logBody\');\nconst logTitle = document.getElementById(\'logTitle\');\nconst logCollapseBtn = document.getElementById(\'logCollapseBtn\');\nlet logAllExpanded = true;\n\nfunction showLogPane(expanded) {\n  // Make log visible (may be display:none on first use)\n  logPane.style.display = \'\';\n  logResizeH.style.display = \'\';\n  if (expanded) {\n    logPane.classList.remove(\'collapsed\');\n    // Restore default height if no inline override, or keep current if resized\n    if (!logPane.style.height || logPane.classList.contains(\'collapsed\')) {\n      logPane.style.height = \'\';  // let CSS default take over\n    }\n    logCollapseBtn.innerHTML = \'&#9660;\';\n    logCollapseBtn.title = \'Collapse log\';\n  } else {\n    logPane.classList.add(\'collapsed\');\n    logPane.style.height = \'\';  // CRITICAL: clear inline height from resize drag\n    logResizeH.style.display = \'none\';\n    logCollapseBtn.innerHTML = \'&#9650;\';\n    logCollapseBtn.title = \'Expand log\';\n  }\n}\n\nfunction toggleLogCollapse() {\n  if (logPane.style.display === \'none\') {\n    showLogPane(true); return;\n  }\n  const isCollapsed = logPane.classList.contains(\'collapsed\');\n  showLogPane(isCollapsed);\n}\n\nfunction clearLog() { logBody.innerHTML = \'\'; logTitle.textContent = \'Execution log\'; }\n\nfunction toggleLogExpand() {\n  logAllExpanded = !logAllExpanded;\n  document.getElementById(\'logExpandBtn\').innerHTML = logAllExpanded ? \'&#8863; Collapse All\' : \'&#8862; Expand All\';\n  logBody.querySelectorAll(\'.log-detail\').forEach(d => d.classList.toggle(\'open\', logAllExpanded));\n}\n\n// Scroll log to a specific resource entry and flash it\nfunction scrollLogToResource(rid) {\n  // Always open the log pane first — even if no entry exists yet\n  showLogPane(true);\n  const entry = logBody.querySelector(`.log-entry[data-id="${rid}"]`);\n  if (!entry) {\n    // No log entry yet — log is now visible but nothing to scroll to\n    return;\n  }\n  // Expand this entry\'s detail\n  const detail = entry.querySelector(\'.log-detail\');\n  if (detail) detail.classList.add(\'open\');\n  // Scroll into view\n  entry.scrollIntoView({ behavior: \'smooth\', block: \'center\' });\n  // Flash highlight\n  entry.classList.remove(\'flash\');\n  void entry.offsetWidth; // force reflow\n  entry.classList.add(\'flash\');\n}\n\n// Log resize drag\nlogResizeH.addEventListener(\'mousedown\', e => {\n  e.preventDefault();\n  const area = document.querySelector(\'.graph-log-area\');\n  const startY = e.clientY;\n  const startH = logPane.offsetHeight;\n  function onMove(ev) { const dy = startY - ev.clientY; logPane.style.height = Math.max(80, startH + dy) + \'px\'; }\n  function onUp() { document.removeEventListener(\'mousemove\', onMove); document.removeEventListener(\'mouseup\', onUp); }\n  document.addEventListener(\'mousemove\', onMove);\n  document.addEventListener(\'mouseup\', onUp);\n});\n\nfunction addLogWave(wave, count) {\n  logBody.innerHTML += `<div class="log-wave-hdr">── Wave ${wave} (${count}) ──</div>`;\n  logBody.scrollTop = logBody.scrollHeight;\n}\n\nfunction addRunningLogEntry(ev) {\n  runningStepMeta.set(ev.id, {\n    id: ev.id,\n    name: ev.name,\n    startedAt: ev.ts || new Date().toISOString(),\n    timeoutS: ev.timeout_s || 0,\n    interactive: false,\n    pid: null,\n    procAlive: null,\n    returnCode: null,\n    lastOutputAt: null,\n  });\n  startRunningStepTicker();\n  const entry = document.createElement(\'div\');\n  entry.className = \'log-entry running\';\n  entry.dataset.id = ev.id;\n  entry.innerHTML = `\n    <div class="log-entry-hdr">\n      <span class="log-icon">&#9655;</span>\n      <span class="log-name">[${ev.idx || \'?\'} / ${ev.total || \'?\'}] ${esc(ev.name)}</span>\n      <span class="log-time" style="color:var(--amber)" data-running-elapsed="${esc(ev.id)}">running...</span>\n    </div>\n    <div class="log-detail open">\n      <div class="log-detail-section">\n        <span class="log-detail-label">Status</span>\n        <div class="log-detail-cmd" data-running-meta="${esc(ev.id)}">starting...</div>\n      </div>\n      ${ev.check ? `<div class="log-detail-section"><span class="log-detail-label">Check</span><div class="log-detail-cmd">$ ${esc(ev.check)}</div></div>` : \'\'}\n      ${ev.run ? `<div class="log-detail-section"><span class="log-detail-label">Run${ev.run_as ? \' (as \' + esc(ev.run_as) + \')\' : \'\'}</span><div class="log-detail-cmd">$ ${esc(ev.run)}</div></div>` : \'\'}\n      <div class="log-detail-section" data-stream="stdout" style="display:none">\n        <span class="log-detail-label">stdout</span>\n        <div class="log-detail-cmd cmd-stdout"></div>\n      </div>\n      <div class="log-detail-section" data-stream="stderr" style="display:none">\n        <span class="log-detail-label">stderr</span>\n        <div class="log-detail-cmd cmd-stderr"></div>\n      </div>\n    </div>`;\n  logBody.appendChild(entry);\n  logBody.scrollTop = logBody.scrollHeight;\n  updateRunningMetaDisplay(ev.id);\n}\n\nfunction appendRunningLogOutput(rid, stream, chunk) {\n  const entry = logBody.querySelector(`.log-entry.running[data-id="${rid}"]`);\n  if (!entry) return;\n  const meta = runningStepMeta.get(rid);\n  if (meta) {\n    meta.lastOutputAt = new Date().toISOString();\n    meta.interactive = false;\n    updateRunningMetaDisplay(rid);\n    refreshExecInputAvailability();\n  }\n  const section = entry.querySelector(`.log-detail-section[data-stream="${stream}"]`);\n  const box = section ? section.querySelector(\'.log-detail-cmd\') : null;\n  if (!section || !box) return;\n  section.style.display = \'\';\n  appendTerminalChunk(box, chunk);\n  box.scrollTop = box.scrollHeight;\n  if (document.getElementById(\'logFollowCb\').checked) logBody.scrollTop = logBody.scrollHeight;\n}\n\nfunction addLogEntry(ev) {\n  const isSkip = ev.status === \'skip\' || ev.status === \'skip_check\' || ev.status === \'skip_when\';\n  const isCancelled = ev.status === \'cancelled\';\n  const isFail = ev.status === \'failed\' || ev.status === \'timeout\';\n  const isWarn = ev.status === \'warned\';\n  const iconCls = isFail ? \'fail\' : isCancelled ? \'warn\' : isWarn ? \'warn\' : isSkip ? \'skip\' : \'ok\';\n  const icon = isFail ? \'✗\' : isCancelled ? \'■\' : isWarn ? \'⚠\' : isSkip ? \'↷\' : \'✓\';\n  const rcCls = isFail ? \'rc-fail\' : \'rc-ok\';\n  const rcText = ev.rc !== undefined && ev.rc !== null ? `exit=${ev.rc}` : \'\';\n\n  const entry = document.createElement(\'div\');\n  entry.className = \'log-entry\';\n  entry.dataset.id = ev.id;\n\n  // Header row — click toggles detail AND highlights graph card\n  let hdrHTML = `<div class="log-entry-hdr" data-action="toggle-log-detail" data-rid="${esc(ev.id)}">`;\n  hdrHTML += `<span class="log-icon ${iconCls}">${icon}</span>`;\n  hdrHTML += `<span class="log-name">[${ev.idx}/${ev.total}] ${esc(ev.name)}</span>`;\n  if (ev.desc) hdrHTML += `<span class="log-desc">${esc(ev.desc)}</span>`;\n  if (rcText && (isFail || isWarn)) hdrHTML += `<span class="log-rc ${rcCls}">${rcText}</span>`;\n  hdrHTML += `<span class="log-time">${ev.ms}ms</span>`;\n  if (isFail || isCancelled) hdrHTML += `<span class="ev-rerun" data-action="rerun-step" data-step="${esc(ev.name)}">rerun</span>`;\n  hdrHTML += `</div>`;\n\n  // Detail section (expandable)\n  let detHTML = `<div class="log-detail${logAllExpanded || isFail || isCancelled ? \' open\' : \'\'}">`;\n  if (ev.reason) detHTML += `<div class="log-detail-section"><span class="log-detail-label">Reason</span><div class="log-detail-cmd">${esc(ev.reason)}</div></div>`;\n  if (ev.check) detHTML += `<div class="log-detail-section"><span class="log-detail-label">Check</span><div class="log-detail-cmd">$ ${esc(ev.check)}</div></div>`;\n  if (ev.run) detHTML += `<div class="log-detail-section"><span class="log-detail-label">Run${ev.run_as ? \' (as \' + esc(ev.run_as) + \')\' : \'\'}</span><div class="log-detail-cmd">$ ${esc(ev.run)}</div></div>`;\n  if (ev.stdout) detHTML += `<div class="log-detail-section"><span class="log-detail-label">stdout</span><div class="log-detail-cmd cmd-stdout">${ansiToHtml(ev.stdout)}</div></div>`;\n  if (ev.stderr) detHTML += `<div class="log-detail-section"><span class="log-detail-label">stderr</span><div class="log-detail-cmd cmd-stderr">${ansiToHtml(ev.stderr)}</div></div>`;\n  if (ev.attempts && ev.attempts > 1) detHTML += `<div class="log-detail-section"><span class="log-detail-label">Attempts</span><div class="log-detail-cmd">${ev.attempts} attempts</div></div>`;\n  detHTML += `</div>`;\n\n  entry.innerHTML = hdrHTML + detHTML;\n  logBody.appendChild(entry);\n  logBody.scrollTop = logBody.scrollHeight;\n}\n\nfunction replaceRunningLogEntry(ev) {\n  const existing = logBody.querySelector(`.log-entry.running[data-id="${ev.id}"]`);\n  if (!existing) {\n    addLogEntry(ev);\n    return;\n  }\n  const temp = document.createElement(\'div\');\n  temp.innerHTML = \'\';\n  const oldScroll = logBody.scrollTop;\n  addLogEntry(ev);\n  const inserted = logBody.lastElementChild;\n  existing.replaceWith(inserted);\n  if (document.getElementById(\'logFollowCb\').checked) logBody.scrollTop = logBody.scrollHeight;\n  else logBody.scrollTop = oldScroll;\n}\n\nfunction ensureExecShell() {\n  if (!document.getElementById(\'execLog\')) {\n    execEl.innerHTML = `\n      <div class="exec-log" id="execLog"></div>\n      <div class="exec-input" id="execInputWrap">\n        <span class="exec-input-label" id="execInputLabel">Input</span>\n        <input class="exec-input-box" id="execInputEl" type="password" placeholder="Send input to the running command and press Enter" autocomplete="off" spellcheck="false">\n        <button class="btn exec-input-toggle" id="execInputToggleBtn" onclick="toggleExecInputMode()">Show</button>\n        <button class="btn exec-input-send" id="execInputSendBtn" onclick="sendExecInput()">Send</button>\n      </div>`;\n  }\n  execInputWrap = document.getElementById(\'execInputWrap\');\n  execInputEl = document.getElementById(\'execInputEl\');\n  execInputToggleBtn = document.getElementById(\'execInputToggleBtn\');\n  if (execInputEl && !execInputEl.dataset.bound) {\n    execInputEl.addEventListener(\'keydown\', e => {\n      if (e.key === \'Enter\') {\n        e.preventDefault();\n        sendExecInput();\n      }\n    });\n    execInputEl.dataset.bound = \'1\';\n  }\n}\n\nfunction updateExecInputTarget() {\n  ensureExecShell();\n  const label = document.getElementById(\'execInputLabel\');\n  if (!label || !execInputEl) return;\n  const targetRid = (selId && document.querySelector(`.ev-block.ev-running[data-rid="${CSS.escape(selId)}"]`))\n    ? selId\n    : execInputTargetRid;\n  const targetName = (targetRid === selId && selId && byId[selId])\n    ? byId[selId].short_name\n    : execInputTargetName;\n  if (targetRid && targetName) {\n    label.textContent = `Input → ${targetName}`;\n    execInputEl.placeholder = `Send input to ${targetName} and press Enter`;\n  } else {\n    label.textContent = \'Input\';\n    execInputEl.placeholder = \'Select a running step, then send input\';\n  }\n}\n\nfunction toggleExecInputMode() {\n  ensureExecShell();\n  if (!execInputEl || !execInputToggleBtn) return;\n  const show = execInputEl.type === \'password\';\n  execInputEl.type = show ? \'text\' : \'password\';\n  execInputToggleBtn.textContent = show ? \'Hide\' : \'Show\';\n  execInputEl.focus();\n}\n\nfunction setExecInputVisible(show) {\n  ensureExecShell();\n  if (!execInputWrap) return;\n  execInputWrap.classList.toggle(\'show\', !!show);\n  if (execInputEl) {\n    if (!show) execInputEl.value = \'\';\n    execInputEl.type = \'password\';\n  }\n  if (execInputToggleBtn) execInputToggleBtn.textContent = \'Show\';\n  if (!show) {\n    execInputTargetRid = null;\n    execInputTargetName = null;\n  }\n  updateExecInputTarget();\n}\n\nasync function sendExecInput() {\n  ensureExecShell();\n  if (!execInputEl) return;\n  const value = execInputEl.value;\n  if (!value) return;\n  const targetRid = (selId && document.querySelector(`.ev-block.ev-running[data-rid="${CSS.escape(selId)}"]`))\n    ? selId\n    : execInputTargetRid;\n  const r = await api(\'POST\', \'/api/apply/input\', { text: value + \'\\n\', target_step: targetRid || undefined });\n  if (r && r.error) {\n    setStatus(\'err\', r.error);\n    return;\n  }\n  execInputEl.value = \'\';\n  execInputEl.focus();\n}\n\nfunction ensureExecLog() {\n  let log = document.getElementById(\'execLog\');\n  if (!log) {\n    execEl.innerHTML = \'<div class="exec-log" id="execLog"></div>\';\n    log = document.getElementById(\'execLog\');\n  }\n  return log;\n}\n\nfunction addExecRunningEntry(ev) {\n  const log = ensureExecLog();\n  const block = document.createElement(\'div\');\n  block.className = \'ev-block ev-running\';\n  block.dataset.rid = ev.id;\n  block.innerHTML = `\n    <div class="ev">\n      <span class="ev-icon" style="animation:demo-pulse .8s infinite">&#9655;</span>\n      <span class="ev-name">${esc(ev.name)}</span>\n      <span class="ev-time" style="color:var(--amber)" data-running-elapsed="${esc(ev.id)}">running...</span>\n    </div>\n    <div class="ev-meta" data-running-meta="${esc(ev.id)}">starting...</div>\n    ${ev.check ? `<pre class="ev-cmd">$ ${esc(ev.check)}</pre>` : \'\'}\n    ${ev.run ? `<pre class="ev-cmd">$ ${esc(ev.run)}</pre>` : \'\'}\n    <pre class="ev-stream stdout"></pre>\n    <pre class="ev-stream stderr"></pre>`;\n  log.appendChild(block);\n  execInputTargetRid = ev.id;\n  execInputTargetName = ev.name;\n  updateExecInputTarget();\n  updateRunningMetaDisplay(ev.id);\n}\n\nfunction appendExecOutput(rid, stream, chunk) {\n  const log = document.getElementById(\'execLog\');\n  if (!log) return;\n  const block = log.querySelector(`.ev-block.ev-running[data-rid="${rid}"]`);\n  if (!block) return;\n  const meta = runningStepMeta.get(rid);\n  if (meta) {\n    meta.lastOutputAt = new Date().toISOString();\n    meta.interactive = false;\n    updateRunningMetaDisplay(rid);\n    refreshExecInputAvailability();\n  }\n  const box = block.querySelector(`.ev-stream.${stream}`);\n  if (!box) return;\n  execInputTargetRid = rid;\n  execInputTargetName = (byId[rid] && byId[rid].short_name) || execInputTargetName || rid;\n  updateExecInputTarget();\n  appendTerminalChunk(box, chunk);\n  box.scrollTop = box.scrollHeight;\n}\n\nfunction finishExecEntry(ev) {\n  const log = ensureExecLog();\n  const existing = log.querySelector(`.ev-block[data-rid="${ev.id}"]`);\n  const isSkip = ev.status === \'skip\' || ev.status === \'skip_check\' || ev.status === \'skip_when\';\n  const isCancelled = ev.status === \'cancelled\';\n  const isFail = ev.status === \'failed\' || ev.status === \'timeout\';\n  const cls = ev.status === \'success\' ? \'ev-ok\' : isSkip ? \'ev-skip\' : isFail ? \'ev-fail\' : \'ev-warn\';\n  const icon = ev.status === \'success\' ? \'✓\' : isSkip ? \'↷\' : isFail ? \'✗\' : isCancelled ? \'■\' : \'⚠\';\n  let rerun = (isFail || isCancelled) ? `<span class="ev-rerun" data-action="rerun-step" data-step="${esc(ev.name)}">rerun</span>` : \'\';\n  const block = document.createElement(\'div\');\n  block.className = `ev-block ${cls}`;\n  block.dataset.rid = ev.id;\n  block.innerHTML = `\n    <div class="ev ${cls}">\n      <span class="ev-icon">${icon}</span>\n      <span class="ev-name">[${ev.idx}/${ev.total}] ${esc(ev.name)}${rerun}</span>\n      <span class="ev-time">${ev.ms}ms</span>\n    </div>\n    <pre class="ev-stream stdout">${ansiToHtml(ev.stdout || \'\')}</pre>\n    <pre class="ev-stream stderr">${ansiToHtml(ev.stderr || \'\')}</pre>`;\n  if (existing) existing.replaceWith(block);\n  else log.appendChild(block);\n  runningStepMeta.delete(ev.id);\n  if (runningStepMeta.size === 0) stopRunningStepTicker();\n  if (execInputTargetRid === ev.id) {\n    execInputTargetRid = null;\n    execInputTargetName = null;\n    const remaining = document.querySelector(\'.ev-block.ev-running[data-rid]\');\n    if (remaining) {\n      execInputTargetRid = remaining.dataset.rid;\n      execInputTargetName = (byId[execInputTargetRid] && byId[execInputTargetRid].short_name) || execInputTargetRid;\n    }\n  }\n  updateExecInputTarget();\n  refreshExecInputAvailability();\n}\n\nfunction addLogSummary(hasFail, wasStopped) {\n  const div = document.createElement(\'div\');\n  div.className = \'log-summary \' + (hasFail || wasStopped ? \'fail\' : \'ok\');\n  div.textContent = wasStopped ? \'■ EXECUTION STOPPED\' : (hasFail ? \'✗ EXECUTION FAILED\' : \'✓ EXECUTION COMPLETE\');\n  logBody.appendChild(div);\n  logBody.scrollTop = logBody.scrollHeight;\n}\n\n// ── Apply execution ────────────────────────────────────\nasync function doApply() {\n  if (applyPollTimer) { doApplyStop(); return; }\n  applyBtn.textContent = \'■ Stop\';\n  applyBtn.className = \'btn\';\n  applyBtn.style.background = \'var(--red)\'; applyBtn.style.color = \'#fff\'; applyBtn.style.borderColor = \'var(--red)\';\n  rerunBtn.disabled = true;\n\n  // Open log pane expanded and clear it\n  clearLog();\n  showLogPane(true);\n  logTitle.textContent = \'Executing...\';\n  // Default to expanded view so all detail is visible\n  logAllExpanded = true;\n  document.getElementById(\'logExpandBtn\').innerHTML = \'&#8863; Collapse All\';\n  applyCursor = 0;\n  applyStopClicks = 0;\n  applyHeartbeatTs = null;\n  applyPollErrorTs = null;\n\n  // Show progress bar, reset to 0\n  document.getElementById(\'progressWrap\').style.display = \'\';\n  document.getElementById(\'progressBar\').style.width = \'0%\';\n\n  // Show and switch to exec tab\n  document.getElementById(\'tabExec\').style.display = \'\';\n  switchInspTab(\'exec\');\n  ensureExecShell();\n  document.getElementById(\'execLog\').innerHTML = \'<div style="color:var(--text3)">Starting apply...</div>\';\n  setExecInputVisible(false);\n\n  const r = await apiWithVaultRetry(\'POST\', \'/api/apply\', { source: codeEl.value });\n  if (r.error) {\n    logBody.innerHTML = `<div class="log-entry"><div class="log-entry-hdr"><span class="log-icon fail">✗</span><span class="log-name">${esc(r.error)}</span></div></div>`;\n    ensureExecShell();\n    document.getElementById(\'execLog\').innerHTML = `<div class="ev ev-fail">${esc(r.error)}</div>`;\n    setExecInputVisible(false);\n    logTitle.textContent = \'Failed\';\n    resetApplyBtn(); return;\n  }\n  applyPollTimer = setInterval(pollApplyEvents, 300);\n}\n\nasync function doApplyStop() {\n  if (!applyPollTimer) return;\n  applyStopClicks += 1;\n  const force = applyStopClicks > 1;\n  await api(\'POST\', \'/api/apply/stop\', { force });\n  applyBtn.textContent = force ? \'✕ Killing...\' : \'✕ Kill\';\n  applyBtn.className = \'btn\';\n  applyBtn.style.background = force ? \'var(--red)\' : \'var(--amber)\';\n  applyBtn.style.color = \'#fff\';\n  applyBtn.style.borderColor = force ? \'var(--red)\' : \'var(--amber)\';\n}\n\nfunction resetApplyBtn() {\n  clearInterval(applyPollTimer); applyPollTimer = null;\n  applyStopClicks = 0;\n  applyHeartbeatTs = null;\n  applyPollErrorTs = null;\n  runningStepMeta.clear();\n  stopRunningStepTicker();\n  setExecInputVisible(false);\n  applyBtn.textContent = \'▶ Apply\';\n  applyBtn.className = \'btn btn-green\';\n  applyBtn.style.background = \'\'; applyBtn.style.color = \'\'; applyBtn.style.borderColor = \'\';\n  rerunBtn.disabled = false;\n  document.getElementById(\'progressWrap\').style.display = \'none\';\n}\n\nasync function pollApplyEvents() {\n  try {\n    const r = await api(\'GET\', \'/api/apply/events?since=\' + applyCursor);\n    if (r.error) throw new Error(r.error);\n    if (!r.events) return;\n    let stepCount = 0, failCount = 0;\n    let skipCount = 0, cancelCount = 0;\n    r.events.forEach(ev => {\n      if (ev.type === \'apply_heartbeat\') {\n        applyHeartbeatTs = Date.parse(ev.ts || \'\') || Date.now();\n        applyPollErrorTs = null;\n        updateApplyConnectionStatus();\n      } else if (ev.type === \'wave\') {\n        const log = document.getElementById(\'execLog\');\n        if (log) {\n          const tw = ev.total_waves || \'?\', dn = ev.done || 0, tot = ev.total || \'?\';\n          log.innerHTML += `<div class="ev-wave">── Wave ${ev.wave}/${tw} (${dn}/${tot} done, ${ev.count} to run) ──</div>`;\n        }\n        addLogWave(ev.wave, ev.count);\n      } else if (ev.type === \'step_start\') {\n        // Step is starting execution — highlight card as in-progress\n        if (document.getElementById(\'logFollowCb\').checked) {\n          const cardId = ev.id || ev.name;\n          const card = document.querySelector(`.nc[data-id="${CSS.escape(cardId)}"]`);\n          if (card) {\n            card.classList.add(\'follow-hl\');\n            if (document.getElementById(\'graphPanel\').style.display !== \'none\') {\n              card.scrollIntoView({ behavior: \'smooth\', block: \'nearest\' });\n            }\n          }\n        }\n        addExecRunningEntry(ev);\n        addRunningLogEntry(ev);\n      } else if (ev.type === \'step_proc\') {\n        const meta = runningStepMeta.get(ev.id);\n        if (meta) {\n          meta.pid = ev.pid || null;\n          meta.interactive = !!ev.interactive;\n          meta.procAlive = true;\n          meta.returnCode = null;\n          updateRunningMetaDisplay(ev.id);\n          refreshExecInputAvailability();\n        }\n      } else if (ev.type === \'step_proc_state\') {\n        const meta = runningStepMeta.get(ev.id);\n        if (meta) {\n          meta.procAlive = !!ev.alive;\n          meta.returnCode = ev.returncode;\n          updateRunningMetaDisplay(ev.id);\n        }\n      } else if (ev.type === \'step_output\') {\n        appendExecOutput(ev.id, ev.stream, ev.chunk || \'\');\n        appendRunningLogOutput(ev.id, ev.stream, ev.chunk || \'\');\n        if (document.getElementById(\'logFollowCb\').checked) {\n          const log = document.getElementById(\'execLog\');\n          if (log) log.scrollTop = log.scrollHeight;\n        }\n      } else if (ev.type === \'step\') {\n        stepCount++;\n        if (ev.status === \'skip\' || ev.status === \'skip_check\') skipCount++;\n        if (ev.status === \'failed\' || ev.status === \'timeout\') failCount++;\n        if (ev.status === \'cancelled\') cancelCount++;\n        // Update progress bar\n        if (ev.idx && ev.total) {\n          const pct = Math.round(ev.idx / ev.total * 100);\n          document.getElementById(\'progressBar\').style.width = pct + \'%\';\n        }\n        finishExecEntry(ev);\n        // Log pane (rich detail)\n        replaceRunningLogEntry(ev);\n        logTitle.textContent = `Executing... [${ev.idx}/${ev.total}]`;\n        // Auto-follow: remove highlight from completed card, scroll log\n        if (document.getElementById(\'logFollowCb\').checked) {\n          const cardId = ev.id || ev.name;\n          const card = document.querySelector(`.nc[data-id="${CSS.escape(cardId)}"]`);\n          if (card) card.classList.remove(\'follow-hl\');\n          logBody.scrollTop = logBody.scrollHeight;\n        }\n      } else if (ev.type === \'error\') {\n        const log = document.getElementById(\'execLog\');\n        if (log) log.innerHTML += `<div class="ev ev-fail"><span class="ev-icon">✗</span><span class="ev-name">${esc(ev.msg)}</span></div>`;\n        const entry = document.createElement(\'div\'); entry.className = \'log-entry\';\n        entry.innerHTML = `<div class="log-entry-hdr"><span class="log-icon fail">✗</span><span class="log-name">${esc(ev.msg)}</span></div>`;\n        logBody.appendChild(entry);\n      } else if (ev.type === \'failed\') {\n        // Separate failed event — already handled by the step event\n      } else if (ev.type === \'done\') {\n        const hasFail = !!logBody.querySelector(\'.log-icon.fail\') || failCount > 0;\n        const wasStopped = cancelCount > 0 && failCount === 0;\n        const log = document.getElementById(\'execLog\');\n        if (log) {\n          // Show skip banner at the TOP of the exec log so it\'s immediately visible\n          if (skipCount > 0 && skipCount === stepCount) {\n            log.insertAdjacentHTML(\'afterbegin\', `<div class="ev-wave" style="color:var(--yellow);border-color:var(--yellow);margin-bottom:6px">⚠ All ${skipCount} steps skipped — completed previously. Click ⟳ Re-run All to start fresh.</div>`);\n          } else if (stepCount > 0 && skipCount > stepCount * 0.8) {\n            log.insertAdjacentHTML(\'afterbegin\', `<div class="ev-wave" style="color:var(--yellow);border-color:var(--yellow);margin-bottom:6px">⚠ ${skipCount}/${stepCount} steps skipped from state. Click ⟳ Re-run All to re-execute all.</div>`);\n          }\n          log.innerHTML += `<div class="ev-done ${(hasFail || wasStopped) ? \'fail\' : \'ok\'}">${wasStopped ? \'■ STOPPED\' : (hasFail ? \'✗ FAILED\' : \'✓ COMPLETE\')}</div>`;\n        }\n        addLogSummary(hasFail, wasStopped);\n        logTitle.textContent = wasStopped ? \'Execution stopped\' : (hasFail ? \'Execution failed\' : (skipCount === stepCount && stepCount > 0 ? \'All skipped (state)\' : \'Execution complete\'));\n        // Clear follow highlight\n        document.querySelectorAll(\'.nc.follow-hl\').forEach(c => c.classList.remove(\'follow-hl\'));\n        resetApplyBtn();\n        triggerResolve();\n        // Show Report tab if there\'s collect data\n        loadReport().then(() => {\n          const rEl = document.getElementById(\'reportContent\');\n          if (rEl && !rEl.querySelector(\'.empty\')) {\n            document.getElementById(\'tabReport\').style.display = \'\';\n          }\n        }).catch(() => {});\n      }\n    });\n    applyCursor = r.cursor;\n    const log = document.getElementById(\'execLog\');\n    if (log && document.getElementById(\'logFollowCb\').checked) log.scrollTop = log.scrollHeight;\n    updateApplyConnectionStatus();\n  } catch(e) {\n    applyPollErrorTs = Date.now();\n    const msg = e && e.message ? e.message : String(e);\n    logTitle.textContent = `Executing... (polling lost: ${msg})`;\n    setStatus(\'err\', \'Apply polling failed: \' + msg);\n    const log = document.getElementById(\'execLog\');\n    if (log) log.innerHTML += `<div class="ev ev-fail"><span class="ev-icon">✗</span><span class="ev-name">${esc(\'Apply polling failed: \' + msg)}</span></div>`;\n    resetApplyBtn();\n  }\n}\n\nasync function doApplyFresh() {\n  if (applyPollTimer) { doApplyStop(); return; }\n  const resp = await api(\'POST\', \'/api/state/reset\', {});\n  if (resp && resp.error) { appendLog(\'ev-fail\', \'✗ Failed to reset state: \' + resp.error); return; }\n  doApply();\n}\n\nasync function doRerun(stepName) {\n  await api(\'POST\', \'/api/state/set\', { step: stepName, action: \'redo\' });\n  doApply();\n}\n\n// ── Autocomplete ───────────────────────────────────────\nasync function loadTemplates() {\n  try {\n    const r = await api(\'GET\', \'/api/templates\');\n    templates = r.templates || [];\n  } catch(e) {}\n}\n\nfunction updateStepNames(src) {\n  stepNames = [];\n  const re = /\\[([^\\]]+)\\]/g; let m;\n  while ((m = re.exec(src)) !== null) stepNames.push(m[1]);\n}\n\nfunction showAutocomplete() {\n  const pos = codeEl.selectionStart;\n  const before = codeEl.value.substring(0, pos);\n  const lineStart = before.lastIndexOf(\'\\n\') + 1;\n  const line = before.substring(lineStart);\n\n  let items = [];\n  // After "first [" or "needs [" — suggest step names\n  const stepMatch = line.match(/(?:first|needs)\\s+(?:\\[([^\\]]*,\\s*)?)\\[?([^\\]]*)?$/);\n  if (stepMatch) {\n    const partial = (stepMatch[2] || \'\').toLowerCase();\n    items = stepNames.filter(s => s.toLowerCase().includes(partial)).slice(0, 8)\n      .map(s => ({ text: s, insert: `[${s}]`, icon: \'□\' }));\n  }\n  // After "from " — suggest templates\n  const fromMatch = line.match(/from\\s+([\\w/]*)$/);\n  if (fromMatch) {\n    const partial = fromMatch[1].toLowerCase();\n    items = templates.filter(t => t.path.toLowerCase().includes(partial)).slice(0, 8)\n      .map(t => ({ text: t.path, insert: t.path, icon: \'◇\', desc: t.params.map(p => p.name).join(\', \') }));\n  }\n\n  if (items.length === 0) { hideAc(); return; }\n  acIdx = 0;\n  renderAc(items);\n}\n\nfunction renderAc(items) {\n  // Position popup near cursor\n  const rect = codeEl.getBoundingClientRect();\n  const pos = codeEl.selectionStart;\n  const lines = codeEl.value.substring(0, pos).split(\'\\n\');\n  const lineH = 21; // approximate\n  const top = rect.top + (lines.length * lineH) - codeEl.scrollTop + 4;\n  const col = lines[lines.length - 1].length;\n  const left = rect.left + 50 + (col * 7.8) - codeEl.scrollLeft;\n\n  acPopup.style.top = Math.min(top, window.innerHeight - 180) + \'px\';\n  acPopup.style.left = Math.min(left, window.innerWidth - 220) + \'px\';\n  acPopup.innerHTML = items.map((it, i) =>\n    `<div class="ac-item${i === acIdx ? \' sel\' : \'\'}" data-idx="${i}" onmousedown="pickAc(${i})"><span class="ac-icon">${it.icon}</span>${esc(it.text)}${it.desc ? \'<span style="margin-left:auto;color:var(--text3);font-size:9px">\' + esc(it.desc) + \'</span>\' : \'\'}</div>`\n  ).join(\'\');\n  acPopup._items = items;\n  acPopup.classList.add(\'show\');\n}\n\nfunction hideAc() { acPopup.classList.remove(\'show\'); acPopup._items = null; acIdx = -1; }\n\nfunction pickAc(idx) {\n  const items = acPopup._items;\n  if (!items || !items[idx]) return;\n  const it = items[idx];\n  const pos = codeEl.selectionStart;\n  const before = codeEl.value.substring(0, pos);\n  const after = codeEl.value.substring(pos);\n  // Find what to replace: last partial match\n  const lineStart = before.lastIndexOf(\'\\n\') + 1;\n  const line = before.substring(lineStart);\n  let replaceFrom = pos;\n  if (it.icon === \'□\') { // step name\n    const m = line.match(/\\[([^\\]]*)$/);\n    if (m) replaceFrom = pos - m[1].length - 1;\n    codeEl.value = before.substring(0, replaceFrom) + it.insert + after;\n    codeEl.selectionStart = codeEl.selectionEnd = replaceFrom + it.insert.length;\n  } else { // template path\n    const m = line.match(/from\\s+([\\w/]*)$/);\n    if (m) replaceFrom = pos - m[1].length;\n    let ins = it.insert + \':\\n\';\n    const item = templates.find(t => t.path === it.text);\n    if (item) {\n      const indent = line.match(/^(\\s*)/)[1] + \'  \';\n      item.params.forEach(p => { ins += `${indent}${p.name} = "${p.default || \'\'}"\\n`; });\n    }\n    codeEl.value = before.substring(0, replaceFrom) + ins + after;\n    codeEl.selectionStart = codeEl.selectionEnd = replaceFrom + ins.length;\n  }\n  hideAc();\n  codeEl.dispatchEvent(new Event(\'input\'));\n}\n\n// ── Resize handle ──────────────────────────────────────\nconst resizeH = document.getElementById(\'resizeH\');\nconst editorPanel = document.getElementById(\'editorPanel\');\nlet resizing = false;\nresizeH.addEventListener(\'mousedown\', e => { resizing = true; resizeH.classList.add(\'active\'); e.preventDefault(); });\ndocument.addEventListener(\'mousemove\', e => { if (!resizing) return; editorPanel.style.width = Math.max(200, e.clientX) + \'px\'; });\ndocument.addEventListener(\'mouseup\', () => { resizing = false; resizeH.classList.remove(\'active\'); setTimeout(() => drawA(), 50); });\nwindow.addEventListener(\'resize\', () => drawA());\ndocument.addEventListener(\'click\', e => {\n  const fileLink = e.target.closest(\'.prob-file-link[data-resolved]\');\n  if (fileLink) {\n    e.stopPropagation();\n    openSideEditor(\n      fileLink.dataset.resolved,\n      fileLink.dataset.type,\n      fileLink.dataset.path,\n      fileLink.dataset.step || undefined\n    );\n    return;\n  }\n  const actionEl = e.target.closest(\'[data-action]\');\n  if (!actionEl) return;\n  const action = actionEl.dataset.action;\n  if (action === \'open-side-editor\') {\n    openSideEditor(\n      actionEl.dataset.resolved,\n      actionEl.dataset.type,\n      actionEl.dataset.path,\n      actionEl.dataset.step || undefined\n    );\n    return;\n  }\n  if (action === \'select-resource\') {\n    e.stopPropagation();\n    selectResource(actionEl.dataset.rid);\n    return;\n  }\n  if (action === \'scroll-log-resource\') {\n    e.stopPropagation();\n    scrollLogToResource(actionEl.dataset.rid);\n    return;\n  }\n  if (action === \'toggle-log-detail\') {\n    const detail = actionEl.nextElementSibling;\n    if (detail) detail.classList.toggle(\'open\');\n    selectResource(actionEl.dataset.rid);\n    return;\n  }\n  if (action === \'rerun-step\') {\n    e.stopPropagation();\n    doRerun(actionEl.dataset.step);\n  }\n});\n// Close more-menu on outside click\ndocument.addEventListener(\'click\', e => {\n  const mm = document.getElementById(\'moreMenu\');\n  if (mm && mm.classList.contains(\'show\') && !e.target.closest(\'#moreMenu\') && !e.target.closest(\'[onclick*="moreMenu"]\')) {\n    mm.classList.remove(\'show\');\n  }\n});\nfilenameEl.addEventListener(\'click\', () => { if (availableGraphFiles.length) openFilePicker(); });\nfilePickerBtn.addEventListener(\'click\', () => { if (availableGraphFiles.length) openFilePicker(); });\nfilePickerBackdrop.addEventListener(\'click\', closeFilePicker);\ndocument.addEventListener(\'keydown\', e => {\n  if (filePickerBackdrop.style.display !== \'none\' && e.key === \'Escape\') {\n    closeFilePicker();\n    return;\n  }\n  if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === \'o\') {\n    e.preventDefault();\n    openFilePicker();\n  }\n});\n\n// ── Init ───────────────────────────────────────────────\n(async function init() {\n  try {\n    const info = await api(\'GET\', \'/api/info\');\n    availableGraphFiles = info.files || [];\n    availableGraphFileEntries = info.file_entries || availableGraphFiles.map(name => ({ name, is_template: false, tags: [] }));\n    refreshFilePickerTagOptions();\n    filenameEl.title = availableGraphFiles.length ? \'Click to choose file\' : \'No graph files available\';\n    filePickerBtn.classList.toggle(\'disabled\', !availableGraphFiles.length);\n    filePickerBtn.title = availableGraphFiles.length ? \'Open graph file (Ctrl+O)\' : \'No graph files available\';\n    if (info.file) {\n      await loadFile(info.file);\n    } else {\n      setStatus(\'info\', \'No file loaded\');\n      filenameEl.textContent = \'(no file)\';\n    }\n    await loadTemplates();\n  } catch (e) {\n    setStatus(\'err\', \'Server not reachable\');\n  }\n})();\n\n// ═════════════════════════════════════════════════════════════════════════\n// Demo Runner — simulates graph execution wave by wave\n// ═════════════════════════════════════════════════════════════════════════\n\nclass DemoRunner {\n  constructor() {\n    this.speed = 1.0;\n    this.state = \'idle\';\n    this.completed = new Set();\n    this.failed = new Set();\n    this.failTarget = null;\n    this.failRecovered = false;\n    this.timeouts = [];\n    this.waveProg = {};\n    this.origBadges = new Map();\n    this.origClasses = new Map();\n  }\n\n  get waves() { return graphData && graphData.graph ? graphData.graph.waves : []; }\n\n  setSpeed(s) { this.speed = s; }\n  _t(ms) { return Math.round(ms / this.speed); }\n  _delay(fn, ms) { const id = setTimeout(fn, this._t(ms)); this.timeouts.push(id); return id; }\n\n  pickFailTarget() {\n    const waves = this.waves;\n    const minWave = Math.max(0, Math.floor(waves.length * 0.6));\n    const candidates = [];\n    for (let wi = minWave; wi < waves.length; wi++) {\n      for (const rid of waves[wi]) {\n        const r = byId[rid];\n        if (r && !r.is_verify && !r.is_barrier && r.needs.length > 0) candidates.push(rid);\n      }\n    }\n    if (candidates.length) this.failTarget = candidates[Math.floor(Math.random() * candidates.length)];\n  }\n\n  getCard(rid) { return document.getElementById(\'c-\' + rid.replace(/[^a-zA-Z0-9]/g, \'_\')); }\n\n  setBadge(card, cls, text) {\n    let badge = card.querySelector(\'.stb\');\n    if (!badge) { badge = document.createElement(\'span\'); badge.className = \'stb\'; card.appendChild(badge); }\n    badge.className = \'stb \' + cls; badge.textContent = text;\n  }\n\n  removeBadge(card) { const b = card.querySelector(\'.stb\'); if (b) b.remove(); }\n\n  clearDemoClasses(card) {\n    card.classList.remove(\'demo-checking\', \'demo-running\', \'demo-failed-shake\',\n      \'demo-resuming\', \'st-success\', \'st-skip_check\', \'st-failed\', \'st-warned\');\n  }\n\n  showToast(text, cls, duration) {\n    const area = document.getElementById(\'toast-area\');\n    if (!area) return;\n    const t = document.createElement(\'div\'); t.className = \'toast \' + (cls || \'\'); t.textContent = text;\n    area.appendChild(t);\n    this._delay(() => { if (t.parentNode) t.remove(); }, duration || 3000);\n  }\n\n  _initLog() {\n    document.getElementById(\'tabExec\').style.display = \'\';\n    switchInspTab(\'exec\');\n    execEl.innerHTML = \'<div class="exec-log" id="execLog"><div style="color:var(--text3)">Demo starting...</div></div>\';\n  }\n\n  _log(html) {\n    const log = document.getElementById(\'execLog\');\n    if (log) { log.innerHTML += html; log.scrollTop = log.scrollHeight; }\n  }\n\n  initWaveProgress() {\n    document.querySelectorAll(\'.wl\').forEach((wl, wi) => {\n      let pg = wl.querySelector(\'.wprog\');\n      if (!pg) { pg = document.createElement(\'span\'); pg.className = \'wprog\'; wl.appendChild(pg); }\n      const total = this.waves[wi] ? this.waves[wi].length : 0;\n      pg.innerHTML = \'<span class="wprog-bar"><span class="wprog-fill" style="width:0%"></span></span>0/\' + total;\n      this.waveProg[wi] = { el: pg, done: 0, total: total };\n    });\n  }\n\n  updateWaveProgress(wi) {\n    const wp = this.waveProg[wi]; if (!wp) return;\n    wp.done++;\n    const pct = Math.round((wp.done / wp.total) * 100);\n    wp.el.innerHTML = \'<span class="wprog-bar"><span class="wprog-fill" style="width:\' + pct + \'%"></span></span>\' + wp.done + \'/\' + wp.total;\n  }\n\n  saveCardState() {\n    document.querySelectorAll(\'.nc\').forEach(card => {\n      this.origClasses.set(card.dataset.id, card.className);\n      const badge = card.querySelector(\'.stb\');\n      this.origBadges.set(card.dataset.id, badge ? badge.outerHTML : null);\n    });\n  }\n\n  restoreCardState() {\n    document.querySelectorAll(\'.nc\').forEach(card => {\n      const rid = card.dataset.id;\n      if (this.origClasses.has(rid)) card.className = this.origClasses.get(rid);\n      const existingBadge = card.querySelector(\'.stb\');\n      if (existingBadge) existingBadge.remove();\n      const origHTML = this.origBadges.get(rid);\n      if (origHTML) card.insertAdjacentHTML(\'afterbegin\', origHTML);\n    });\n    document.querySelectorAll(\'.wprog\').forEach(p => p.remove());\n    const ta = document.getElementById(\'toast-area\'); if (ta) ta.innerHTML = \'\';\n  }\n\n  start() {\n    if (!graphData || !graphData.graph || !this.waves.length) {\n      this.showToast(\'No graph loaded — validate first\', \'toast-fail\', 2000); return;\n    }\n    this.saveCardState();\n    document.querySelectorAll(\'.nc\').forEach(card => { this.clearDemoClasses(card); this.removeBadge(card); });\n    this.completed.clear(); this.failed.clear(); this.failRecovered = false;\n    this.waveProg = {};\n    this.pickFailTarget();\n    this.initWaveProgress();\n    this._initLog();\n    this.state = \'running\'; this._updateButton();\n    this.showToast(\'Starting execution...\', \'\', 2000);\n    this._delay(() => this.runWave(0), 600);\n  }\n\n  pause() { this.state = \'paused\'; this.timeouts.forEach(clearTimeout); this.timeouts = []; this._updateButton(); }\n\n  reset() {\n    this.state = \'idle\'; this.timeouts.forEach(clearTimeout); this.timeouts = [];\n    this.completed.clear(); this.failed.clear();\n    this.restoreCardState(); this._updateButton();\n  }\n\n  _updateButton() {\n    const btn = document.getElementById(\'bdemo\'); if (!btn) return;\n    btn.classList.remove(\'demo-active\', \'demo-done\');\n    if (this.state === \'running\') { btn.innerHTML = \'\\u23F8 Pause\'; btn.classList.add(\'demo-active\'); }\n    else if (this.state === \'paused\') { btn.innerHTML = \'\\u25B6 Resume\'; }\n    else if (this.state === \'done\') { btn.innerHTML = \'\\u21BA Reset\'; btn.classList.add(\'demo-done\'); }\n    else { btn.innerHTML = \'\\u25B6 Demo\'; }\n  }\n\n  runWave(wi) {\n    if (this.state !== \'running\' || wi >= this.waves.length) {\n      if (this.state === \'running\') {\n        this.state = \'done\'; this._updateButton();\n        this._log(\'<div class="ev-done ok">✓ COMPLETE</div>\');\n        this.showToast(\'All steps completed successfully\', \'toast-done\', 4000);\n      }\n      return;\n    }\n    const wave = this.waves[wi];\n    let waveCompleted = 0; const total = wave.length;\n    this._log(`<div class="ev-wave">── Wave ${wi + 1} (${total}) ──</div>`);\n\n    wave.forEach((rid, i) => {\n      const stagger = i * (120 + Math.random() * 180);\n      this._delay(() => {\n        if (this.state !== \'running\') return;\n        this.simulateResource(rid, wi, () => {\n          waveCompleted++; this.updateWaveProgress(wi);\n          if (waveCompleted >= total) this._delay(() => this.runWave(wi + 1), 400);\n        });\n      }, stagger);\n    });\n  }\n\n  simulateResource(rid, wi, onDone) {\n    const card = this.getCard(rid); const res = byId[rid];\n    if (!card || !res) { onDone(); return; }\n    selectResource(rid);\n\n    const hasCheck = res.check && res.check !== \'false\';\n    const isFail = rid === this.failTarget && !this.failRecovered;\n\n    if (hasCheck && !isFail) {\n      this.clearDemoClasses(card); card.classList.add(\'demo-checking\');\n      this.setBadge(card, \'stb-checking\', \'checking...\');\n      this._delay(() => {\n        this.clearDemoClasses(card); card.classList.add(\'st-skip_check\');\n        this.setBadge(card, \'stb-skip\', \'skip\'); this.completed.add(rid);\n        this._log(`<div class="ev ev-skip"><span class="ev-icon">↷</span><span class="ev-name">[${res.short_name}]</span><span class="ev-time">skip</span></div>`);\n        onDone();\n      }, 200 + Math.random() * 300);\n      return;\n    }\n\n    this.clearDemoClasses(card); card.classList.add(\'demo-checking\');\n    this.setBadge(card, \'stb-checking\', \'checking...\');\n\n    this._delay(() => {\n      this.clearDemoClasses(card); card.classList.add(\'demo-running\');\n      this.setBadge(card, \'stb-running\', \'running...\');\n      const runTime = res.is_verify ? (800 + Math.random() * 700) : (500 + Math.random() * 1500);\n\n      this._delay(() => {\n        if (isFail) {\n          this.clearDemoClasses(card); card.classList.add(\'st-failed\', \'demo-failed-shake\');\n          this.setBadge(card, \'stb-failed\', \'FAIL\'); this.failed.add(rid);\n          this._log(`<div class="ev ev-fail"><span class="ev-icon">✗</span><span class="ev-name">[${res.short_name}] failed (exit=1)</span></div>`);\n          this.showToast(\'\\u2717 \' + res.short_name + \' failed (exit=1)\', \'toast-fail\', 5000);\n          this._delay(() => {\n            this.showToast(\'Resuming from [\' + res.short_name + \']...\', \'toast-resume\', 3000);\n            this._delay(() => {\n              this.clearDemoClasses(card); card.classList.add(\'demo-resuming\');\n              this.setBadge(card, \'stb-resuming\', \'retrying...\');\n              this._delay(() => {\n                this.clearDemoClasses(card); card.classList.add(\'st-success\');\n                const ms = Math.round(400 + Math.random() * 800);\n                this.setBadge(card, \'stb-success\', ms + \'ms\');\n                this._log(`<div class="ev ev-ok"><span class="ev-icon">✓</span><span class="ev-name">[${res.short_name}] (retry)</span><span class="ev-time">${ms}ms</span></div>`);\n                this.failed.delete(rid); this.completed.add(rid); this.failRecovered = true;\n                this.showToast(\'\\u2713 \' + res.short_name + \' succeeded on retry\', \'toast-done\', 3000);\n                onDone();\n              }, 800 + Math.random() * 600);\n            }, 800);\n          }, 1800);\n          return;\n        }\n        this.clearDemoClasses(card); card.classList.add(\'st-success\');\n        const successMs = Math.round(runTime);\n        this.setBadge(card, \'stb-success\', successMs + \'ms\');\n        this._log(`<div class="ev ev-ok"><span class="ev-icon">✓</span><span class="ev-name">[${res.short_name}]</span><span class="ev-time">${successMs}ms</span></div>`);\n        this.completed.add(rid); onDone();\n      }, runTime);\n    }, 200 + Math.random() * 200);\n  }\n}\n\nconst demo = new DemoRunner();\n\nfunction demoToggle() {\n  if (demo.state === \'idle\') demo.start();\n  else if (demo.state === \'running\') demo.pause();\n  else if (demo.state === \'paused\') { demo.reset(); demo.start(); }\n  else if (demo.state === \'done\') demo.reset();\n}\n\nfunction demoSpeed(val) { demo.setSpeed(parseFloat(val)); }\n\n// ── Theme toggle ───────────────────────────────────────\nfunction getTheme() {\n  const saved = localStorage.getItem(\'cgr-theme\');\n  if (saved) return saved;\n  return window.matchMedia(\'(prefers-color-scheme:light)\').matches ? \'light\' : \'dark\';\n}\n\nfunction applyTheme(theme) {\n  document.documentElement.classList.toggle(\'light\', theme === \'light\');\n  document.documentElement.classList.toggle(\'dark\', theme === \'dark\');\n  document.getElementById(\'themeBtn\').textContent = theme === \'light\' ? \'\\u263E\' : \'\\u2600\';\n}\n\nfunction toggleTheme() {\n  const next = getTheme() === \'dark\' ? \'light\' : \'dark\';\n  localStorage.setItem(\'cgr-theme\', next);\n  applyTheme(next);\n}\n\napplyTheme(getTheme());\n</script>\n</body>\n</html>\n'
# END GENERATED: EMBEDDED IDE HTML


_IDE_HTML = None  # lazy-loaded

def _get_ide_html():
    """Return the IDE HTML, preferring a sibling file in dev and embedded HTML in releases."""
    global _IDE_HTML
    if _IDE_HTML: return _IDE_HTML
    engine_dir = Path(__file__).resolve().parent
    ide_path = engine_dir / "ide.html"
    if ide_path.exists():
        _IDE_HTML = ide_path.read_text()
        return _IDE_HTML
    _IDE_HTML = _EMBEDDED_IDE_HTML
    return _IDE_HTML


def _is_loopback_host(host: str) -> bool:
    return host in {"127.0.0.1", "localhost", "::1"}


def _normalize_host_header(host_header: str | None) -> str | None:
    if not host_header:
        return None
    host_header = host_header.strip()
    if not host_header:
        return None
    if host_header.startswith("["):
        end = host_header.find("]")
        if end == -1:
            return None
        return host_header[1:end].lower()
    if ":" in host_header:
        return host_header.rsplit(":", 1)[0].lower()
    return host_header.lower()


def _is_allowed_serve_host(host_header: str | None, bind_host: str,
                           unsafe_host: bool = False,
                           extra_allowed_hosts: list[str] | None = None) -> bool:
    """Validate Host headers for the IDE server.

    By default, loopback-bound servers only accept loopback hostnames to reduce
    DNS rebinding exposure. Reverse proxies can be allowlisted explicitly.
    """
    if unsafe_host and not _is_loopback_host(bind_host):
        return True
    normalized = _normalize_host_header(host_header)
    if normalized is None:
        return True
    allowed_hosts = {"localhost", "127.0.0.1", "::1"}
    for extra_host in extra_allowed_hosts or []:
        normalized_extra = _normalize_host_header(extra_host)
        if normalized_extra:
            allowed_hosts.add(normalized_extra)
    return normalized in allowed_hosts


def _auto_allowed_serve_hosts(env: dict[str, str] | None = None) -> list[str]:
    """Best-effort proxy host detection for VS Code proxy environments."""
    from urllib.parse import urlparse

    env_map = env if env is not None else os.environ
    raw = env_map.get("VSCODE_PROXY_URI", "").strip()
    if not raw:
        return []
    try:
        parsed = urlparse(raw)
    except Exception:
        return []
    if not parsed.hostname:
        return []
    return [parsed.hostname]


def cmd_serve(filepath=None, port=8420, host="127.0.0.1", repo_dir=None,
              no_open=False, unsafe_host=False, allow_hosts=None):
    """Start the web IDE server."""
    import http.server
    import urllib.parse
    import webbrowser
    import threading
    import io

    if not _is_loopback_host(host):
        warning_lines = [
            f"WARNING: Binding to {host} exposes the IDE server to the network.",
            "All endpoints are unauthenticated. Any device on the network can",
            "read files, execute commands, and modify project state.",
        ]
        if not unsafe_host:
            for line in warning_lines:
                print(red(line), file=sys.stderr)
            print(red(f"Refusing to start without --unsafe-host. Use: cgr serve --host {host} --unsafe-host"), file=sys.stderr)
            raise SystemExit(1)
        for line in warning_lines:
            print(yellow(line), file=sys.stderr)

    work_dir = Path.cwd()
    current_file = Path(filepath).resolve() if filepath else None
    current_repo = repo_dir

    # Execution state for live apply streaming
    apply_lock = threading.Lock()
    apply_events = []       # list of {"type": ..., "data": ...} events
    apply_running = False
    apply_thread = None
    apply_stop_count = 0
    apply_proc_lock = threading.Lock()
    apply_procs = {}
    apply_proc_states = {}
    ide_csrf_token = secrets.token_urlsafe(32)
    token_dir = Path.home() / ".config" / "cgr"
    token_path = token_dir / ".ide-token"

    token_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(token_dir, 0o700)
    except OSError:
        pass
    fd = os.open(token_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(ide_csrf_token + "\n")
    try:
        os.chmod(token_path, 0o600)
    except OSError:
        pass
    def _append_apply_event(event):
        with apply_lock:
            apply_events.append(event)

    def _register_apply_proc(proc, input_fd=None, step_id=None, step_name=None):
        with apply_proc_lock:
            apply_procs[proc.pid] = {
                "proc": proc,
                "input_fd": input_fd,
                "step_id": step_id,
                "step_name": step_name,
            }
            apply_proc_states[proc.pid] = (True, None)

    def _unregister_apply_proc(proc):
        with apply_proc_lock:
            entry = apply_procs.pop(proc.pid, None)
        if not entry:
            return
        alive = proc.poll() is None
        returncode = None if alive else proc.returncode
        state = (alive, returncode)
        if apply_proc_states.get(proc.pid) != state:
            apply_proc_states[proc.pid] = state
            _append_apply_event({
                "type": "step_proc_state",
                "id": entry.get("step_id"),
                "step_name": entry.get("step_name"),
                "pid": proc.pid,
                "alive": alive,
                "returncode": returncode,
                "ts": _now(),
            })

    def _stop_apply_processes(force=False):
        sig = signal.SIGKILL if force else signal.SIGTERM
        with apply_proc_lock:
            entries = list(apply_procs.values())
        for entry in entries:
            proc = entry["proc"]
            try:
                os.killpg(proc.pid, sig)
            except ProcessLookupError:
                pass
            except Exception:
                try:
                    (proc.kill if force else proc.terminate)()
                except Exception:
                    pass

    def _monitor_apply_processes():
        while True:
            time.sleep(0.5)
            with apply_proc_lock:
                entries = list(apply_procs.values())
            seen = set()
            for entry in entries:
                proc = entry["proc"]
                pid = proc.pid
                seen.add(pid)
                alive = proc.poll() is None
                returncode = None if alive else proc.returncode
                state = (alive, returncode)
                if apply_proc_states.get(pid) == state:
                    continue
                apply_proc_states[pid] = state
                _append_apply_event({
                    "type": "step_proc_state",
                    "id": entry.get("step_id"),
                    "step_name": entry.get("step_name"),
                    "pid": pid,
                    "alive": alive,
                    "returncode": returncode,
                    "ts": _now(),
                })
            stale = [pid for pid in list(apply_proc_states.keys()) if pid not in seen]
            for pid in stale:
                apply_proc_states.pop(pid, None)

    threading.Thread(target=_monitor_apply_processes, daemon=True).start()

    def _emit_apply_heartbeats():
        last_sent = 0.0
        while True:
            time.sleep(0.5)
            if not apply_running:
                continue
            now = time.monotonic()
            if now - last_sent < 1.0:
                continue
            last_sent = now
            with apply_proc_lock:
                active_pids = sorted(apply_procs.keys())
            _append_apply_event({
                "type": "apply_heartbeat",
                "ts": _now(),
                "active_pids": active_pids,
            })

    threading.Thread(target=_emit_apply_heartbeats, daemon=True).start()

    def _write_apply_input(text, target_step=None):
        if not text:
            return False
        with apply_proc_lock:
            entries = [entry for _, entry in sorted(apply_procs.items()) if entry.get("input_fd") is not None]
        if not entries:
            return False
        if target_step:
            target = target_step.strip()
            entries = [
                entry for entry in entries
                if entry.get("step_id") == target or entry.get("step_name") == target
            ]
            if not entries:
                return False
        elif len(entries) > 1:
            raise RuntimeError("Multiple interactive steps are running; specify target_step.")
        try:
            os.write(entries[-1]["input_fd"], text.encode())
            return True
        except OSError:
            return False

    def _allowed_cors_origin(origin: str | None) -> str | None:
        if not origin:
            return None
        if any(ch in origin for ch in "\r\n"):
            return None
        try:
            parsed = urllib.parse.urlparse(origin)
        except Exception:
            return None
        if parsed.scheme != "http":
            return None
        if parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
            return None
        if parsed.port != port:
            return None
        if parsed.params or parsed.query or parsed.fragment or parsed.username or parsed.password:
            return None
        host = parsed.hostname
        if host is None:
            return None
        canonical_origin_by_host = {
            "localhost": f"http://localhost:{port}",
            "127.0.0.1": f"http://127.0.0.1:{port}",
            "::1": f"http://[::1]:{port}",
        }
        return canonical_origin_by_host.get(host)

    auto_allowed_hosts = _auto_allowed_serve_hosts()
    extra_allowed_hosts = list(dict.fromkeys([*(allow_hosts or []), *auto_allowed_hosts]))

    def _host_rejection_payload(host_header: str | None) -> dict[str, Any]:
        hint = (
            "If you are using a reverse proxy, restart with "
            "--allow-host <proxy-hostname> while keeping --host 127.0.0.1."
        )
        payload = {"error": "Host not allowed", "hint": hint}
        normalized = _normalize_host_header(host_header)
        if normalized:
            payload["host"] = normalized
        if extra_allowed_hosts:
            payload["allowed_hosts"] = ["localhost", "127.0.0.1", "::1", *extra_allowed_hosts]
        return payload

    def _allowed_host_header(host_header: str | None) -> bool:
        return _is_allowed_serve_host(
            host_header,
            bind_host=host,
            unsafe_host=unsafe_host,
            extra_allowed_hosts=extra_allowed_hosts,
        )

    def _atomic_write_text(path: Path, content: str, *, allowed_ext: set[str], require_exists: bool = True):
        safe_path = _resolve_safe_ide_path(path, allowed_ext=allowed_ext, require_exists=require_exists)
        if safe_path is None:
            raise ValueError("Invalid file path")
        safe_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{safe_path.name}.", suffix=".tmp", dir=str(safe_path.parent))
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_name, safe_path)
        finally:
            if os.path.exists(tmp_name):
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    def _vault_error_response(msg, line=0):
        payload = {"error": _strip_ansi(msg), "line": line}
        if "requires a vault passphrase" in msg:
            payload["needs_vault_pass"] = True
            payload["vault_status"] = "missing"
        elif "wrong passphrase" in msg or "Decryption failed" in msg:
            payload["needs_vault_pass"] = True
            payload["vault_status"] = "invalid"
        return payload

    def _parse_ide_graph_source(source: str, fname: str | None = None):
        """Parse IDE source text based on the active file extension."""
        parse_name = fname or "<editor>"
        if fname and fname.endswith(".cg"):
            tokens = lex(source, parse_name)
            return Parser(tokens, source, parse_name).parse()
        return parse_cgr(source, parse_name)

    def _run_apply_bg(source, fname, graph=None, vault_passphrase=None):
        """Run apply in a background thread, accumulating events."""
        nonlocal apply_running, apply_events, apply_stop_count
        with apply_lock:
            apply_events.clear()
        apply_stop_count = 0
        _append_apply_event({"type": "start", "ts": _now()})

        try:
            # Parse and resolve
            if graph is None:
                try:
                    ast = _parse_ide_graph_source(source, fname)
                except (CGRParseError, LexError, ParseError) as e:
                    msg = e.pretty() if hasattr(e, "pretty") else str(e)
                    _append_apply_event({"type": "error", "msg": _strip_ansi(msg)}); return
                try:
                    graph = resolve(ast, repo_dir=current_repo, graph_file=fname,
                                    vault_passphrase=vault_passphrase,
                                    resolve_deferred_secrets=True)
                    _validate_script_paths(graph)
                except ResolveError as e:
                    _append_apply_event(_vault_error_response(str(e))); return

            total = sum(len(w) for w in graph.waves)
            _append_apply_event({"type": "info", "resources": total, "waves": len(graph.waves)})

            # Execute wave by wave
            sp = _state_path(fname) if fname else None
            state = StateFile(sp) if sp else None
            lock_cm = state.lock() if state else nullcontext()

            # Output collector for collected steps
            has_collect = any(r.collect_key for r in graph.all_resources.values())
            output = OutputFile(_output_path(fname)) if fname and has_collect else None

            done = 0
            done_lock = threading.Lock()
            wave_failed = False
            with lock_cm:
                for wi, wave in enumerate(graph.waves):
                    if not apply_running or wave_failed: break
                    _append_apply_event({"type": "wave", "wave": wi + 1, "count": len(wave),
                                         "total_waves": len(graph.waves), "done": done, "total": total})

                    # Split wave into skip vs run, emit start events for runners
                    to_skip = []
                    to_run = []
                    for rid in wave:
                        res = graph.all_resources[rid]
                        if state and state.should_skip(rid):
                            to_skip.append(rid)
                        else:
                            to_run.append(rid)
                            # Emit step_start so the UI can highlight in-progress steps
                            runtime_res = _runtime_resource_view(res, graph.variables)
                            _sv = graph.sensitive_values
                            _append_apply_event({"type": "step_start", "id": rid,
                                "name": res.short_name, "idx": done + len(to_skip) + len(to_run),
                                "total": total, "ts": _now(), "line": res.line,
                                "timeout_s": res.timeout, "is_verify": res.is_verify,
                                "check": _redact(runtime_res.check, _sv) or "",
                                "run": _redact(runtime_res.run, _sv) or "", "run_as": runtime_res.run_as or ""})

                    # Emit skips immediately
                    for rid in to_skip:
                        res = graph.all_resources[rid]
                        se = state.get(rid) if state else None
                        if se and se.var_bindings:
                            graph.variables.update(se.var_bindings)
                        with done_lock:
                            done += 1
                            d = done
                        _append_apply_event({"type": "step", "id": rid, "name": res.short_name,
                            "status": "skip", "idx": d, "total": total, "ms": 0})

                    if not to_run:
                        continue

                    # Execute runners in parallel using ThreadPoolExecutor
                    # Group by parallel_group to respect concurrency limits
                    groups: dict[str|None, list[str]] = {}
                    for rid in to_run:
                        res = graph.all_resources[rid]
                        pg = res.parallel_group
                        groups.setdefault(pg, []).append(rid)

                    def _exec_one(rid_inner):
                        """Execute a single resource and return (rid, result)."""
                        res_inner = graph.all_resources[rid_inner]
                        node_inner = graph.nodes[res_inner.node_name]
                        def _register_step_proc(proc, input_fd=None):
                            _register_apply_proc(proc, input_fd=input_fd,
                                                 step_id=rid_inner, step_name=res_inner.short_name)
                            _append_apply_event({
                                "type": "step_proc",
                                "id": rid_inner,
                                "pid": proc.pid,
                                "interactive": input_fd is not None,
                                "ts": _now(),
                            })
                        def _stream_output(stream_name, chunk):
                            _append_apply_event({
                                "type": "step_output",
                                "id": rid_inner,
                                "stream": stream_name,
                                "ts": _now(),
                                "chunk": _redact(chunk, graph.sensitive_values) or "",
                            })
                        r_inner = exec_resource(
                            res_inner,
                            node_inner,
                            variables=graph.variables,
                            on_output=_stream_output,
                            register_proc=_register_step_proc,
                            unregister_proc=_unregister_apply_proc,
                            cancel_check=lambda: not apply_running,
                        )
                        return rid_inner, r_inner

                    # Determine max parallelism for this wave
                    max_workers = 1
                    for pg, rids in groups.items():
                        if pg is not None:
                            limit = graph.all_resources[rids[0]].concurrency_limit or len(rids)
                            max_workers = max(max_workers, min(limit, len(rids)))
                        else:
                            max_workers = max(max_workers, len(rids))

                    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as pool:
                        futs = {pool.submit(_exec_one, rid): rid for rid in to_run}
                        for f in as_completed(futs):
                            rid, r = f.result()
                            res = graph.all_resources[rid]
                            runtime_res = _runtime_resource_view(res, graph.variables)
                            with done_lock:
                                done += 1
                                d = done
                            if r.var_bindings:
                                graph.variables.update(r.var_bindings)
                            if state: state.record(r, wi + 1, sensitive=graph.sensitive_values)
                            if output and res.collect_key and r.status in (Status.SUCCESS, Status.FAILED, Status.WARNED):
                                output.record(rid, res.collect_key, res.node_name,
                                              r.stdout, r.stderr, r.run_rc, r.duration_ms,
                                              sensitive=graph.sensitive_values)
                            _sv = graph.sensitive_values
                            evt = {"type": "step", "id": rid, "name": res.short_name,
                                "desc": res.description or "",
                                "status": r.status.value, "idx": d, "total": total,
                                "ms": r.duration_ms, "rc": r.run_rc, "check_rc": r.check_rc,
                                "check": _redact(runtime_res.check, _sv) or "", "run": _redact(runtime_res.run, _sv) or "",
                                "run_as": runtime_res.run_as or "",
                                "stdout": (_redact(r.stdout, _sv)[:2000] if r.stdout else ""),
                                "stderr": (_redact(r.stderr, _sv)[:2000] if r.stderr else ""),
                                "reason": r.reason or "", "attempts": r.attempts}
                            if res.collect_key:
                                evt["collect_key"] = res.collect_key
                            _append_apply_event(evt)
                            if r.status in (Status.FAILED, Status.TIMEOUT):
                                _append_apply_event({"type": "failed", "id": rid, "name": res.short_name,
                                    "msg": _redact(r.stderr, _sv)[:500] if r.stderr else f"exit={r.run_rc}"})
                                wave_failed = True
                                break
        except Exception as e:
            _append_apply_event({"type": "error", "msg": str(e)})
        finally:
            _append_apply_event({"type": "done", "ts": _now()})
            apply_running = False

    def _resolve_source(source, fname=None, vault_passphrase=None):
        """Parse and resolve source text, return graph JSON or error."""
        try:
            ast = _parse_ide_graph_source(source, fname)
        except (CGRParseError, LexError, ParseError) as e:
            msg = e.pretty() if hasattr(e, 'pretty') else str(e)
            line = getattr(e, 'line', 0)
            return {"error": _strip_ansi(msg), "line": line}
        try:
            graph = resolve(ast, repo_dir=current_repo, graph_file=str(current_file) if current_file else None,
                            vault_passphrase=vault_passphrase,
                            resolve_deferred_secrets=False)
            _validate_script_paths(graph)
        except ResolveError as e:
            return _vault_error_response(str(e), line=0)

        # Build the same JSON the visualizer uses
        state_data = {}
        if current_file:
            sp = _state_path(str(current_file))
            if Path(sp).exists():
                sf = StateFile(sp)
                for rid, se in sf.all_entries().items():
                    state_data[rid] = {"status": se.status, "ms": se.ms, "ts": se.ts}

        gj = json.loads(_graph_to_json(graph, state_data=state_data))

        # Collect external file references for IDE navigation
        ext_files = []
        repo_dirs = _build_repo_search_path(explicit_repo=current_repo,
                                             graph_file=str(current_file) if current_file else None)
        for u in ast.uses:
            fpath = _find_template_file(repo_dirs, u.path)
            ext_files.append({
                "type": "template", "path": u.path, "line": u.line,
                "resolved": str(fpath) if fpath else None,
                "exists": fpath is not None and fpath.exists(),
            })
        graph_dir = str(Path(current_file).parent) if current_file else None
        for inv in ast.inventories:
            inv_info = _safe_external_file_ref(inv.path, graph_dir)
            ext_files.append({
                "type": "inventory", "path": inv.path, "line": inv.line,
                **inv_info,
            })
        for sec in ast.secrets:
            sec_info = _safe_external_file_ref(sec.path, graph_dir)
            ext_files.append({
                "type": "secrets", "path": sec.path, "line": sec.line,
                **sec_info,
            })

        return {
            "graph": gj,
            "resources": len(graph.all_resources),
            "waves_count": len(graph.waves),
            "waves": len(graph.waves),
            "variables": _redact_variable_map(graph.variables, graph.sensitive_values),
            "external_files": ext_files,
        }

    def _is_safe_file_path(fpath_str):
        """Check if a file path is within allowed directories (repo dirs + graph dir)."""
        fpath = Path(fpath_str).resolve()
        # Allow files in repo directories
        repo_dirs = _build_repo_search_path(explicit_repo=current_repo,
                                             graph_file=str(current_file) if current_file else None)
        for rd in repo_dirs:
            rd_resolved = Path(rd).resolve()
            if rd_resolved.exists() and fpath.is_relative_to(rd_resolved):
                return True
        # Allow files in the graph file's directory
        if current_file:
            graph_dir = Path(current_file).parent.resolve()
            if fpath.is_relative_to(graph_dir):
                return True
        # Allow files in work_dir
        if fpath.is_relative_to(work_dir.resolve()):
            return True
        return False

    def _resolve_safe_ide_path(path_like, *, allowed_ext: set[str] | None = None,
                               require_exists: bool = True, allow_dirs: bool = False):
        """Resolve an IDE file path only if it remains inside allowed roots."""
        try:
            raw_path = Path(path_like)
        except TypeError:
            return None
        if any(ch in str(raw_path) for ch in "\x00\r\n"):
            return None
        if raw_path.exists() and raw_path.is_symlink():
            return None
        try:
            resolved = raw_path.resolve(strict=require_exists)
        except Exception:
            return None
        if require_exists and not resolved.exists():
            return None
        if not allow_dirs and resolved.exists() and resolved.is_dir():
            return None
        if allowed_ext is not None and resolved.suffix.lower() not in allowed_ext:
            return None
        if not _is_safe_file_path(str(resolved)):
            return None
        return resolved

    def _safe_current_graph_file():
        if not current_file:
            return None
        return _resolve_safe_ide_path(current_file, allowed_ext={".cgr", ".cg"}, require_exists=True)

    def _safe_output_path_for_graph(graph_path: Path):
        candidate = graph_path.with_name(graph_path.name + ".output")
        return _resolve_safe_ide_path(candidate, allowed_ext={".output"}, require_exists=False)

    def _safe_external_file_ref(path_str: str, graph_dir: str | None):
        candidate = Path(path_str)
        if not candidate.is_absolute() and graph_dir:
            candidate = Path(graph_dir) / candidate
        try:
            resolved = candidate.resolve(strict=False)
        except Exception:
            return {"resolved": None, "exists": False}
        if not _is_safe_file_path(str(resolved)):
            return {"resolved": None, "exists": False}
        return {"resolved": str(resolved), "exists": resolved.exists()}

    def _resolve_allowed_side_editor_path(path_str: str, allowed_ext: set[str], require_exists: bool = True):
        """Resolve a side-editor file path only if it canonicalizes into an allowed location."""
        if not isinstance(path_str, str):
            return None
        raw = path_str.strip()
        if not raw:
            return None
        return _resolve_safe_ide_path(raw, allowed_ext=allowed_ext, require_exists=require_exists)

    def _list_graph_files():
        files = sorted(str(f.relative_to(work_dir)) for f in work_dir.rglob("*.cgr"))
        files += sorted(str(f.relative_to(work_dir)) for f in work_dir.rglob("*.cg") if not str(f).endswith(".cgr"))
        return files

    def _graph_file_tags(rel_path: str, is_template: bool = False) -> list[str]:
        rel = Path(rel_path)
        tags: list[str] = []
        suffix = rel.suffix.lower()
        if suffix == ".cgr":
            tags.append("cgr")
        elif suffix == ".cg":
            tags.append("cg")
        if len(rel.parts) == 1:
            tags.append("root")
        parts_lower = [part.lower() for part in rel.parts]
        if "examples" in parts_lower:
            tags.append("example")
        if parts_lower and parts_lower[0] in {"testing", "testing-ssh"}:
            tags.append("testing")
        if is_template:
            tags.append("template")
        seen: set[str] = set()
        deduped: list[str] = []
        for tag in tags:
            if tag in seen:
                continue
            seen.add(tag)
            deduped.append(tag)
        return deduped

    def _graph_file_sort_key(rel_path: str):
        rel = Path(rel_path)
        is_root_cgr = len(rel.parts) == 1 and rel.suffix.lower() == ".cgr"
        return (0 if is_root_cgr else 1, rel_path.lower())

    def _is_template_file(rel_path: str) -> bool:
        """Best-effort detection for repo template files shown in the picker."""
        fpath = _resolve_workspace_graph_file(rel_path)
        if not fpath or not fpath.exists():
            return False
        try:
            source = fpath.read_text()
            if fpath.suffix.lower() == ".cg":
                tokens = lex(source, str(fpath))
                program = Parser(tokens, source, str(fpath)).parse()
            else:
                program = parse_cgr(source, str(fpath))
            return bool(program.templates) and not program.nodes
        except Exception:
            return False

    def _normalize_workspace_graph_name(name: str) -> Path | None:
        """Normalize a user-supplied graph path into a safe relative path."""
        if not isinstance(name, str):
            return None
        raw = name.strip()
        if not raw:
            return None
        rel = Path(raw)
        if rel.is_absolute() or rel.anchor:
            return None
        parts = rel.parts
        if not parts:
            return None
        if any(part in ("..", "") for part in parts):
            return None
        return Path(*parts)

    def _resolve_workspace_graph_file(name: str):
        """Resolve a user-selected graph file within work_dir, blocking traversal."""
        rel = _normalize_workspace_graph_name(name)
        if rel is None:
            return None
        bases = [work_dir.resolve()]
        if current_file:
            graph_dir = Path(current_file).parent.resolve()
            if graph_dir not in bases:
                bases.append(graph_dir)
        fallback = None
        for base in bases:
            fpath = (base / rel).resolve()
            if not fpath.is_relative_to(base):
                continue
            if fpath.suffix.lower() not in {".cgr", ".cg"}:
                continue
            if fpath.exists():
                return fpath
            if fallback is None:
                fallback = fpath
        return fallback

    def _resolve_current_file_alias(name: str):
        """Return the active file only for exact known aliases."""
        if not current_file or not isinstance(name, str):
            return None
        raw = name.strip()
        if not raw:
            return None
        aliases = {str(current_file.resolve())}
        label = _file_label(current_file.resolve())
        if label:
            aliases.add(label)
        return current_file.resolve() if raw in aliases else None

    def _resolve_edit_target(name: str):
        """Resolve a file target for save/open.
        Allows the explicit current file, or a workspace graph file."""
        fpath = _resolve_current_file_alias(name)
        if fpath:
            return fpath
        return _resolve_workspace_graph_file(name)

    def _file_label(fpath: Path | None) -> str | None:
        if not fpath:
            return None
        return str(fpath.relative_to(work_dir)) if fpath.is_relative_to(work_dir) else str(fpath)

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt, *args): pass  # silence logs

        def _security_headers(self):
            self.send_header("Cross-Origin-Opener-Policy", "same-origin")
            self.send_header("Cross-Origin-Embedder-Policy", "require-corp")
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")

        def _cors(self):
            allowed_origin = _allowed_cors_origin(self.headers.get("Origin"))
            if not allowed_origin:
                return
            self.send_header("Access-Control-Allow-Origin", allowed_origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, X-CSRF-Token")

        def _csrf_token_from_request(self, body):
            header_token = self.headers.get("X-CSRF-Token")
            if header_token:
                return header_token
            if isinstance(body, dict):
                token = body.get("csrf_token")
                if isinstance(token, str):
                    return token
            return None

        def _require_csrf(self, body=None):
            token = self._csrf_token_from_request(body)
            if token != ide_csrf_token:
                self._json({"error": "Invalid or missing CSRF token"}, 403)
                return False
            return True

        def _require_allowed_host(self):
            host_header = self.headers.get("Host")
            if not _allowed_host_header(host_header):
                self._json(_host_rejection_payload(host_header), 403)
                return False
            return True

        def _read_json_body(self):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            if not raw:
                return {}
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                self._json({"error": "Invalid JSON body"}, 400)
                return None

        def _json(self, data, status=200):
            body = json.dumps(data).encode()
            self.send_response(status)
            self._security_headers()
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        def _html(self, content):
            body = content.encode()
            self.send_response(200)
            self._security_headers()
            self._cors()
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self):
            if not self._require_allowed_host():
                return
            self.send_response(204)
            self._security_headers()
            self._cors()
            self.end_headers()

        def do_GET(self):
            if not self._require_allowed_host():
                return
            parsed = urllib.parse.urlparse(self.path)
            qs = dict(urllib.parse.parse_qsl(parsed.query))

            if parsed.path == "/":
                self._html(_get_ide_html())

            elif parsed.path == "/api/csrf":
                if self.headers.get("Origin") and not _allowed_cors_origin(self.headers.get("Origin")):
                    self._json({"error": "Origin not allowed"}, 403)
                    return
                self._json({"csrf_token": ide_csrf_token})

            elif parsed.path == "/api/info":
                files = _list_graph_files()
                file_entries = []
                for name in files:
                    is_template = _is_template_file(name)
                    file_entries.append({
                        "name": name,
                        "is_template": is_template,
                        "tags": _graph_file_tags(name, is_template=is_template),
                    })
                file_entries.sort(key=lambda entry: _graph_file_sort_key(entry["name"]))
                files = [entry["name"] for entry in file_entries]
                self._json({
                    "version": __version__,
                    "file": _file_label(current_file),
                    "files": files,
                    "file_entries": file_entries,
                    "repo": current_repo,
                })

            elif parsed.path == "/api/file":
                name = qs.get("name")
                if name:
                    fpath = _resolve_edit_target(name)
                else:
                    fpath = current_file
                if not fpath or not fpath.exists():
                    self._json({"error": f"File not found: {fpath}"}, 404)
                    return
                self._json({
                    "name": _file_label(fpath),
                    "content": fpath.read_text(),
                })

            elif parsed.path == "/api/file/view":
                # Secure read-only file viewer for external references (templates, inventory, etc.)
                fpath_raw = qs.get("path", "")
                if not fpath_raw:
                    self._json({"error": "Missing 'path' parameter"}, 400); return
                fpath = Path(fpath_raw).resolve()
                if not fpath.exists():
                    self._json({"error": f"File not found: {fpath_raw}"}, 404); return
                # Security: extension whitelist
                allowed_ext = {".cgr", ".cg", ".ini", ".conf", ".cfg", ".env", ".txt", ".yaml", ".yml", ".json"}
                if fpath.suffix.lower() not in allowed_ext:
                    self._json({"error": f"File type not allowed: {fpath.suffix}"}, 403); return
                # Security: size cap (100KB)
                if fpath.stat().st_size > 102400:
                    self._json({"error": "File too large (max 100KB)"}, 413); return
                # Security: path must be in allowed directories
                if not _is_safe_file_path(str(fpath)):
                    self._json({"error": "Access denied: file outside allowed directories"}, 403); return
                try:
                    content = fpath.read_text()
                except Exception as e:
                    self._json({"error": f"Cannot read file: {e}"}, 500); return
                self._json({
                    "path": str(fpath),
                    "name": fpath.name,
                    "content": content,
                })

            elif parsed.path == "/api/state":
                if not current_file:
                    self._json({"entries": {}})
                    return
                sp = _state_path(str(current_file))
                if not Path(sp).exists():
                    self._json({"entries": {}})
                    return
                sf = StateFile(sp)
                entries = {}
                for rid, se in sf.all_entries().items():
                    entries[rid] = {"status": se.status, "ms": se.ms, "ts": se.ts}
                self._json({"entries": entries})

            elif parsed.path == "/api/history":
                if not current_file:
                    self._json({"runs": []}); return
                sp = _state_path(str(current_file))
                if not Path(sp).exists():
                    self._json({"runs": []}); return
                # Parse raw JSONL to extract runs (lock→entries→unlock)
                runs = []
                current_run = None
                for raw in Path(sp).read_text().splitlines():
                    raw = raw.strip()
                    if not raw: continue
                    try:
                        d = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if d.get("_lock"):
                        current_run = {"ts": d.get("ts", ""), "entries": [], "ok": 0, "fail": 0, "skip": 0, "timeout": 0}
                        continue
                    if d.get("_unlock"):
                        if current_run:
                            current_run["end_ts"] = d.get("ts", "")
                            runs.append(current_run)
                            current_run = None
                        continue
                    if current_run is None:
                        current_run = {"ts": d.get("ts", ""), "entries": [], "ok": 0, "fail": 0, "skip": 0, "timeout": 0}
                    st = d.get("status", "")
                    current_run["entries"].append({"id": d.get("id", ""), "status": st, "ms": d.get("ms", 0)})
                    if st == "success": current_run["ok"] += 1
                    elif st == "failed": current_run["fail"] += 1
                    elif st == "timeout": current_run["timeout"] += 1
                    elif st in ("skip_check", "skip_when"): current_run["skip"] += 1
                if current_run and current_run["entries"]:
                    runs.append(current_run)
                # Return most recent first, limit to last 20
                runs.reverse()
                self._json({"runs": runs[:20]})

            elif parsed.path == "/api/templates":
                repo_path = Path(current_repo) if current_repo else Path(work_dir / "repo")
                templates = []
                if repo_path.exists():
                    for pattern in ("*.cgr", "*.cg"):
                        for tpl_file in sorted(repo_path.rglob(pattern)):
                            rel = tpl_file.relative_to(repo_path)
                            tpl_path = str(rel.with_suffix(""))
                            try:
                                source = tpl_file.read_text()
                                if tpl_file.suffix == ".cgr":
                                    prog = parse_cgr(source, str(tpl_file))
                                else:
                                    tokens = lex(source, str(tpl_file))
                                    prog = Parser(tokens, source, str(tpl_file)).parse()
                                for t in prog.templates:
                                    params = [{"name": p.name, "default": p.default} for p in t.params]
                                    templates.append({"path": tpl_path, "name": t.name, "params": params, "description": t.description})
                            except Exception:
                                pass
                self._json({"templates": templates})

            elif parsed.path == "/api/apply/events":
                since = int(qs.get("since", 0))
                with apply_lock:
                    events = apply_events[since:]
                    cursor = len(apply_events)
                self._json({"events": events, "cursor": cursor, "running": apply_running})

            elif parsed.path == "/api/report":
                if current_file:
                    op = _output_path(str(current_file))
                    if Path(op).exists():
                        of = OutputFile(op)
                        self._json({"outputs": of.all_outputs()})
                    else:
                        self._json({"outputs": []})
                else:
                    self._json({"outputs": []})

            else:
                self.send_error(404)

        def do_PUT(self):
            if not self._require_allowed_host():
                return
            body = self._read_json_body()
            if body is None:
                return

            if self.path == "/api/file":
                if not self._require_csrf(body):
                    return
                nonlocal current_file
                name = body.get("name", "")
                content = body.get("content", "")
                fpath = _resolve_edit_target(name)
                if not fpath:
                    self._json({"error": "Invalid file path"}, 403)
                    return
                try:
                    _atomic_write_text(fpath, content, allowed_ext={".cgr", ".cg"}, require_exists=True)
                    current_file = fpath
                    self._json({"ok": True})
                except Exception as e:
                    self._json({"error": str(e)}, 500)
            else:
                self.send_error(404)

        def do_POST(self):
            if not self._require_allowed_host():
                return
            nonlocal apply_running, apply_thread, current_file
            body = self._read_json_body()
            if body is None:
                return

            if self.path == "/api/resolve":
                source = body.get("source", "")
                vault_pass = body.get("vault_pass")
                try:
                    result = _resolve_source(source, str(current_file) if current_file else None,
                                             vault_passphrase=vault_pass)
                except Exception as e:
                    result = {"error": _strip_ansi(str(e)), "line": 0}
                self._json(result)

            elif self.path == "/api/parse":
                source = body.get("source", "")
                fname = body.get("name")
                if not isinstance(fname, str) or not fname.strip():
                    fname = str(current_file) if current_file else None
                try:
                    ast = _parse_ide_graph_source(source, fname)
                    steps = []
                    for node in ast.nodes:
                        for res in node.resources:
                            steps.append(res.name)
                    self._json({"ok": True, "steps": steps, "nodes": len(ast.nodes)})
                except (CGRParseError, LexError, ParseError) as e:
                    msg = e.pretty() if hasattr(e, "pretty") else str(e)
                    self._json({"error": _strip_ansi(msg), "line": getattr(e, "line", 0)})

            elif self.path == "/api/file/open":
                if not self._require_csrf(body):
                    return
                name = body.get("name", "")
                fpath = _resolve_edit_target(name)
                if not fpath or not fpath.exists():
                    self._json({"error": f"File not found: {name}"}, 404)
                    return
                current_file = fpath
                self._json({
                    "ok": True,
                    "file": _file_label(current_file),
                })

            elif self.path == "/api/apply":
                if not self._require_csrf(body):
                    return
                nonlocal apply_stop_count
                if apply_running:
                    self._json({"error": "Apply already running"}, 409); return
                source = body.get("source", "")
                vault_pass = body.get("vault_pass")
                fname = str(current_file) if current_file else None
                try:
                    ast = _parse_ide_graph_source(source, fname)
                except (CGRParseError, LexError, ParseError) as e:
                    msg = e.pretty() if hasattr(e, "pretty") else str(e)
                    self._json({"error": _strip_ansi(msg)}); return
                try:
                    graph = resolve(ast, repo_dir=current_repo, graph_file=fname,
                                    vault_passphrase=vault_pass,
                                    resolve_deferred_secrets=True)
                    _validate_script_paths(graph)
                except ResolveError as e:
                    self._json(_vault_error_response(str(e))); return
                if current_file:
                    safe_current = _safe_current_graph_file()
                    if not safe_current:
                        self._json({"error": "No file loaded"}, 400); return
                    _atomic_write_text(safe_current, source, allowed_ext={".cgr", ".cg"}, require_exists=True)
                apply_running = True
                apply_stop_count = 0
                apply_thread = threading.Thread(target=_run_apply_bg,
                                                args=(source, fname, graph, vault_pass),
                                                daemon=True)
                apply_thread.start()
                self._json({"ok": True, "msg": "Apply started"})

            elif self.path == "/api/apply/stop":
                if not self._require_csrf(body):
                    return
                force = bool(body.get("force", False))
                apply_running = False
                apply_stop_count += 1
                hard_kill = force or apply_stop_count > 1
                _stop_apply_processes(force=hard_kill)
                self._json({"ok": True, "force": hard_kill, "stop_count": apply_stop_count})

            elif self.path == "/api/apply/input":
                if not self._require_csrf(body):
                    return
                text_in = body.get("text", "")
                target_step = body.get("target_step")
                if not apply_running:
                    self._json({"error": "No apply running"}, 409)
                elif not text_in:
                    self._json({"error": "Missing input text"}, 400)
                else:
                    try:
                        ok = _write_apply_input(text_in, target_step=target_step)
                    except RuntimeError as e:
                        self._json({"error": str(e)}, 409)
                        return
                    if ok:
                        self._json({"ok": True})
                    else:
                        self._json({"error": "No interactive command is accepting input"}, 409)

            elif self.path == "/api/state/reset":
                if not self._require_csrf(body):
                    return
                safe_current = _safe_current_graph_file()
                if safe_current:
                    try:
                        cmd_state_reset(str(safe_current))
                        output_path = _safe_output_path_for_graph(safe_current)
                        if output_path and output_path.exists():
                            output_path.unlink()
                        self._json({"ok": True})
                    except Exception as e:
                        self._json({"error": str(e)}, 500)
                else:
                    self._json({"error": "No file loaded"}, 400)

            elif self.path == "/api/state/set":
                if not self._require_csrf(body):
                    return
                step = body.get("step", "")
                action = body.get("action", "redo")
                safe_current = _safe_current_graph_file()
                if safe_current:
                    try:
                        graph = _load(str(safe_current), repo_dir=current_repo)
                        cmd_state_set(str(safe_current), step, action, graph)
                        self._json({"ok": True})
                    except Exception as e:
                        self._json({"error": str(e)}, 500)
                else:
                    self._json({"error": "No file loaded"}, 400)

            elif self.path == "/api/file/save":
                if not self._require_csrf(body):
                    return
                # Secure write for side-editor (templates, inventory, etc.)
                fpath_raw = body.get("path", "")
                content = body.get("content", "")
                if not fpath_raw:
                    self._json({"error": "Missing 'path' parameter"}, 400); return
                if isinstance(fpath_raw, str):
                    raw_candidate = Path(fpath_raw.strip())
                    if raw_candidate.exists() and raw_candidate.is_symlink():
                        self._json({"error": "Refusing to write through symlink"}, 403); return
                allowed_ext = {".cgr", ".cg", ".ini", ".conf", ".cfg", ".txt", ".yaml", ".yml", ".json"}
                fpath = _resolve_allowed_side_editor_path(fpath_raw, allowed_ext, require_exists=True)
                if not fpath:
                    self._json({"error": f"File not found or not allowed: {fpath_raw}"}, 403); return
                try:
                    _atomic_write_text(fpath, content, allowed_ext=allowed_ext, require_exists=True)
                    self._json({"ok": True, "path": str(fpath)})
                except Exception as e:
                    self._json({"error": f"Write failed: {e}"}, 500); return

            elif self.path == "/api/lint":
                if not self._require_csrf(body):
                    return
                source = body.get("source", "")
                vault_pass = body.get("vault_pass")
                if not current_file:
                    self._json({"warnings": [], "error": "No file loaded"}); return
                try:
                    if str(current_file).endswith(".cg"):
                        tokens = lex(source, str(current_file))
                        ast = Parser(tokens, source, str(current_file)).parse()
                    else:
                        ast = parse_cgr(source, str(current_file))
                    graph = resolve(ast, repo_dir=current_repo, graph_file=str(current_file),
                                    vault_passphrase=vault_pass,
                                    resolve_deferred_secrets=False)
                    _validate_script_paths(graph)
                except (CGRParseError, LexError, ParseError) as e:
                    self._json({"warnings": [], "error": _strip_ansi(e.pretty() if hasattr(e, "pretty") else str(e)),
                                "line": getattr(e, "line", 0)}); return
                except Exception as e:
                    err = _vault_error_response(str(e))
                    err["warnings"] = []
                    self._json(err); return
                # Run lint checks (same logic as cmd_lint but return JSON)
                warnings = []
                for rid, res in graph.all_resources.items():
                    if res.is_barrier: continue
                    if (not res.check or res.check.strip().lower() == "false") and not res.is_verify:
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "info",
                            "msg": "No check clause — step is not idempotent."})
                    if res.run and len(res.run) > 200:
                        msg = (f"Run command is {len(res.run)} chars and contains shell control operators. "
                               f"Consider using the script keyword instead.")
                        if not re.search(r'\||&&|\|\|', res.run):
                            msg = f"Run command is {len(res.run)} chars. Consider a script."
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "info",
                            "msg": msg})
                    run_cmd = res.run or ""
                    node = graph.nodes.get(res.node_name)
                    if re.search(r'\bsed\s+(-[a-z]*i(?!\.[a-z])[a-z]*\b|-[a-z]+\s+-i\b)', run_cmd):
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "'sed -i' without a backup extension."})
                    if run_cmd.count("sed -i") >= 2:
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "Multiple 'sed -i' edits in one shell command. Prefer 'line ... replacing ...'."})
                    if re.search(r'\b(?:echo|printf)\b.*>\s*\S+', run_cmd):
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "Shell-built file content detected. Prefer 'content > PATH:'."})
                    if re.search(r'\btee\b\s+/(?:etc|usr|opt|srv)\b', run_cmd):
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "Writing to system paths with tee is not atomic."})
                    if node and node.via_method == "local" and re.search(r'(^|\s)ssh\s', run_cmd):
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "Local step shells out to ssh manually. Prefer SSH targets."})
                    if re.search(r'if\s+\[\s+-n\s+"?\$\{[^}]+\}"?\s*\].*FLAG=', run_cmd):
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "Shell-built optional flags detected. Prefer 'flag \"...\" when \"...\"'."})
                    if re.search(r'\bwhile\b.*\bsleep\b.*\bdone\b', run_cmd, re.DOTALL):
                        warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                            "msg": "Polling loop detected. Prefer 'until \"VALUE\"' with 'retry'."})
                # when typo check
                for rid, res in graph.all_resources.items():
                    if not res.when or res.is_barrier: continue
                    when_expr = res.when.strip()
                    for op in ("!=", "=="):
                        if op in when_expr:
                            parts = when_expr.split(op, 1)
                            for part in parts:
                                token = part.strip().strip("'\"")
                                if part.strip().startswith(("'", '"')) or token in ("true", "false", "yes", "no", "0", "1", ""):
                                    continue
                                if token not in graph.variables:
                                    warnings.append({"line": res.line, "name": res.short_name, "severity": "warn",
                                        "msg": f"'when' references unknown variable '{token}'."})
                            break
                # race clobber check
                race_groups: dict[str, list] = {}
                for rid, res in graph.all_resources.items():
                    if res.is_race and res.parallel_group:
                        race_groups.setdefault(res.parallel_group, []).append(res)
                for gkey, branches in race_groups.items():
                    if any(b.race_into for b in branches): continue
                    output_re_lint = re.compile(r'(?:-o\s+|>\s*>?\s*)(\S+)')
                    all_dests_lint: list[set] = []
                    for b in branches:
                        dests = set(m.group(1) for m in output_re_lint.finditer(b.run or ""))
                        all_dests_lint.append(dests)
                    if len(all_dests_lint) >= 2:
                        shared = set.intersection(*all_dests_lint) if all_dests_lint else set()
                        if shared:
                            warnings.append({"line": branches[0].line, "name": "race", "severity": "warn",
                                "msg": f"Race branches write to same path ({', '.join(sorted(shared))}). Use 'race into'."})
                # each clobber check
                each_groups: dict[str, list] = {}
                for rid, res in graph.all_resources.items():
                    if res.parallel_group and "__each" in (res.parallel_group or ""):
                        each_groups.setdefault(res.parallel_group, []).append(res)
                for gkey, items in each_groups.items():
                    if len(items) < 2: continue
                    output_re_each = re.compile(r'(?:-o\s+|>\s*>?\s*)(\S+)')
                    dest_counts: dict[str, int] = {}
                    for item in items:
                        for m in output_re_each.finditer(item.run or ""):
                            dest_counts[m.group(1)] = dest_counts.get(m.group(1), 0) + 1
                    for dest, count in dest_counts.items():
                        if count > 1:
                            warnings.append({"line": items[0].line, "name": "each", "severity": "warn",
                                "msg": f"Each-loop branches write to same path ({dest})."})
                            break
                # duplicate collect keys
                collect_map: dict[str, list] = {}
                for rid, res in graph.all_resources.items():
                    if res.collect_key and not res.is_barrier:
                        ck = f"{res.node_name}/{res.collect_key}"
                        collect_map.setdefault(ck, []).append(res.short_name)
                for ck, names in collect_map.items():
                    if len(names) > 1:
                        node, key = ck.split("/", 1)
                        warnings.append({"line": 0, "name": key, "severity": "warn",
                            "msg": f"Collect key '{key}' used by {', '.join(names)} — last write wins."})
                self._json({"warnings": warnings})

    server = http.server.HTTPServer((host, port), Handler)
    url = f"http://{host}:{port}"
    print(bold(f"\n  CommandGraph IDE"))
    print(f"  ────────────────────────────────────────")
    print(f"  {green('▶')} {bold(url)}")
    if current_file:
        print(f"  File: {cyan(str(current_file))}")
    if current_repo:
        print(f"  Repo: {dim(current_repo)}")
    print(f"  CSRF token: {dim(ide_csrf_token)}")
    print(f"  Token file: {dim(str(token_path))}")
    print(f"  {dim('Press Ctrl+C to stop')}\n")

    # Open browser
    if not no_open:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(dim("\n  Shutting down..."))
        server.shutdown()


# ═══════════════════════════════════════════════════════════════════════════════
# 9b. CLI
# ═══════════════════════════════════════════════════════════════════════════════
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
                        apply) opts="$opts --dry-run --parallel --progress --report --no-color --no-resume --start-from --timeout --log --on-complete --blast-radius --tags --skip-tags --vault-pass --vault-pass-ask --vault-pass-file --skip -A -K --ask-become-pass" ;;
                        plan) opts="$opts --tags --skip-tags --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        validate) opts="$opts -q --quiet --json --no-color --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        check) opts="$opts --json --parallel --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        dot|visualize|ping|why|explain|lint|diff) opts="$opts --vault-pass --vault-pass-ask --vault-pass-file -A" ;;
                        visualize) opts="$opts -o --output --state" ;;
                        serve) opts="$opts --port --host --no-open" ;;
                        diff) opts="$opts --json" ;;
                        report) opts="$opts --format -o --output --keys" ;;
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
    sa.add_argument("--no-color",action="store_true",help="Disable colored output")
    sa.add_argument("--no-resume",action="store_true",help="Ignore state file, run everything fresh")
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
    _add_vault_pass_args(ss)

    sr=sub.add_parser("repo"); sr.add_argument("action",choices=["index"]); sr.add_argument("--repo")

    rpt=sub.add_parser("report",help="View or export collected outputs from a run")
    rpt.add_argument("file",help="Graph file (.cg or .cgr)")
    rpt.add_argument("--format",choices=["table","json","csv"],default="table",help="Output format (default: table)")
    rpt.add_argument("-o","--output",metavar="FILE",help="Write report to file instead of stdout")
    rpt.add_argument("--keys",help="Comma-separated list of collect keys to include")

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
    ssc.add_argument("value",nargs="?",default=None,help="Value (for add)")
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
    if hasattr(args, "file") and args.file is None and args.command not in ("serve", "init", "repo", "completion", "version", "doctor", "test"):
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

    if args.command=="secrets":
        cmd_secrets(args.action, args.file, key=args.key, value=args.value, vault_args=args); return

    if args.command=="serve":
        cmd_serve(filepath=args.file, port=args.port, host=args.host,
                  repo_dir=getattr(args,"repo",None), no_open=args.no_open,
                  unsafe_host=getattr(args,"unsafe_host",False),
                  allow_hosts=getattr(args,"allow_host",[])); return

    if args.command=="report":
        fk = args.keys.split(",") if args.keys else None
        cmd_report(args.file, fmt=args.format, output_file=args.output, filter_keys=fk)
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
        need_graph_if_present = {"show", "reset", "set", "drop", "compact"}
        if args.action in need_graph_if_present and not Path(args.file).exists():
            graph = None
        else:
            try:
                graph=_load(args.file, repo_dir=repo, vault_passphrase=vault_passphrase,
                            vault_prompt=vault_prompt)
            except SystemExit:
                pass  # graph may not parse for show/reset

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
                     "version": pv.version, "params": pv.params_used}
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
                print(f"    ← {cyan(pv.source_file)}{ver_tag} ({pv.template_name}) params={pv.params_used}")
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
        results, wall_ms=cmd_apply_stateful(graph, args.file,
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
            progress_mode=getattr(args,"progress",False))
        if args.report:
            rpt={"timestamp":time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                 "version":__version__,
                 "wall_clock_ms":wall_ms,
                 "total_resources":len(results),
                 "dedup":{h:ids for h,ids in graph.dedup_map.items() if len(ids)>1},
                 "provenance":[{"source":p.source_file,"template":p.template_name,"params":p.params_used} for p in graph.provenance_log],
                 "results":[{"id":r.resource_id,"status":r.status.value,"run_rc":r.run_rc,
                             "duration_ms":r.duration_ms,"attempts":r.attempts} for r in results]}
            # Include collected outputs if any
            opath = _output_path(args.file)
            if Path(opath).exists():
                of = OutputFile(opath)
                rpt["outputs"] = {f"{e.get('node','')}/{e.get('key','')}": e.get("stdout","").strip()
                                  for e in of.all_outputs()}
            Path(args.report).write_text(json.dumps(rpt,indent=2))
            print(dim(f"Report → {args.report}"))
        sys.exit(1 if any(r.status==Status.FAILED for r in results) else 0)

if __name__=="__main__": main()
