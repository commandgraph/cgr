"""Brace-delimited .cg parser."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.lexer import *
from cgr_src.ast_nodes import *

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
                  "retry","on_fail","on_success","on_failure","when","env",
                  "collect","reduce","flag","until","wait","from",
                  "parallel","race","each","stage",
                  "get","post","put","patch","delete","auth","header","body","expect"}

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

    def _p_timeout_prop(self) -> tuple[int, bool]:
        self._expect(TT.IDENT, "timeout")
        timeout = int(self._expect(TT.NUMBER).value)
        if self._at(TT.IDENT) and self._peek().value in ("s", "m", "h"):
            v = self._advance().value
            if v == "m":
                timeout *= 60
            elif v == "h":
                timeout *= 3600
        reset_on_output = False
        if self._at_id("reset"):
            self._advance()
            self._expect(TT.IDENT, "on")
            self._expect(TT.IDENT, "output")
            reset_on_output = True
        return timeout, reset_on_output

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
        timeout_reset_on_output=d.get("timeout_reset_on_output", False)
        on_fail=d.get("on_fail","stop"); when=None; env={}; env_when={}; children=[]; flags=[]; until=None
        collect_key=None
        collect_format=None
        collect_var=None
        reduce_key=None
        reduce_var=None
        on_success_set=[]; on_failure_set=[]
        http_method=None; http_url=None; http_headers=[]; http_body=None
        http_body_type=None; http_auth=None; http_expect=None
        wait_kind=None; wait_target=None
        subgraph_path=None; subgraph_vars={}

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
                if self._at_id("as"):
                    self._advance()
                    collect_as = self._expect(TT.IDENT).value
                    if collect_as == "json":
                        collect_format = collect_as
                    else:
                        collect_var = collect_as
            elif s.value=="reduce":
                self._advance(); reduce_key=self._expect(TT.STRING).value
                if self._at_id("as"):
                    self._advance()
                    reduce_var=self._expect(TT.IDENT).value
            elif s.value=="tags":
                self._advance()
                tag_list = [self._expect(TT.IDENT).value]
                while self._at(TT.COMMA): self._advance(); tag_list.append(self._expect(TT.IDENT).value)
                tags = tag_list
            elif s.value=="timeout":
                timeout, timeout_reset_on_output = self._p_timeout_prop()
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
            elif s.value=="on_success":
                self._advance()
                self._expect(TT.IDENT, "set")
                var_name = self._expect(TT.IDENT).value
                self._expect(TT.EQUALS)
                value_tok = self._advance()
                if value_tok.type not in (TT.STRING, TT.NUMBER, TT.IDENT):
                    raise self._err("Expected a value after on_success set VAR =")
                on_success_set.append((var_name, value_tok.value))
            elif s.value=="on_failure":
                self._advance()
                self._expect(TT.IDENT, "set")
                var_name = self._expect(TT.IDENT).value
                self._expect(TT.EQUALS)
                value_tok = self._advance()
                if value_tok.type not in (TT.STRING, TT.NUMBER, TT.IDENT):
                    raise self._err("Expected a value after on_failure set VAR =")
                on_failure_set.append((var_name, value_tok.value))
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
            elif s.value=="wait":
                self._advance()
                if self._at_id("for"):
                    self._advance()
                wait_kind=self._expect(TT.IDENT).value
                if wait_kind not in ("webhook", "file"):
                    raise self._err(f"Unknown wait target '{wait_kind}'", "Valid: webhook, file")
                wait_target=self._expect(TT.STRING).value
            elif s.value=="from":
                self._advance()
                subgraph_path=self._expect(TT.STRING).value
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
            elif (subgraph_path is not None and s.type == TT.IDENT and
                  self.pos+1 < len(self.tokens) and self.tokens[self.pos+1].type == TT.EQUALS):
                arg_name = self._advance().value
                self._expect(TT.EQUALS)
                arg_value = self._advance()
                if arg_value.type not in (TT.STRING, TT.NUMBER, TT.IDENT):
                    raise self._err(f"Expected a simple value for subgraph argument '{arg_name}'")
                subgraph_vars[arg_name] = arg_value.value
            else:
                raise self._err(f"Unknown '{s.value}' in resource",
                    "Valid: description, needs, check, run, script, as, timeout, retry, on_fail, on_success, on_failure, when, env, collect, reduce, flag, until, wait, from, tags, resource, parallel, race, each, stage, get, post, put, patch, delete, auth, header, body, expect")
        if flags and not run_cmd:
            raise self._err("'flag' requires a 'run' command in the same step")
        if until and retries <= 0:
            raise self._err("'until' requires 'retry'")
        res = ASTResource(name=name, description=desc, needs=needs,
            check=check, run=run_cmd, run_as=run_as, timeout=timeout,
            retries=retries, retry_delay=retry_delay, retry_backoff=retry_backoff, on_fail=on_fail,
            when=when, env=env, is_verify=False, line=s.line if s else 0,
            timeout_reset_on_output=timeout_reset_on_output,
            script_path=script_path, collect_key=collect_key, collect_format=collect_format,
            children=children, flags=flags, env_when=env_when, until=until,
            http_method=http_method, http_url=http_url, http_headers=http_headers,
            http_body=http_body, http_body_type=http_body_type,
            http_auth=http_auth, http_expect=http_expect,
            wait_kind=wait_kind, wait_target=wait_target,
            subgraph_path=subgraph_path, subgraph_vars=dict(subgraph_vars),
            on_success_set=list(on_success_set), on_failure_set=list(on_failure_set),
            collect_var=collect_var, reduce_key=reduce_key, reduce_var=reduce_var)
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
        timeout_reset_on_output=False
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            s=self._peek()
            if   s.value=="needs":   self._advance(); needs=self._p_ident_list()
            elif s.value=="run":     self._advance(); run_cmd=self._expect(TT.COMMAND).value
            elif s.value=="as":      self._advance(); run_as=self._expect(TT.IDENT).value
            elif s.value=="timeout":
                timeout, timeout_reset_on_output = self._p_timeout_prop()
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
            when=None, env={}, is_verify=True, line=tok.line,
            timeout_reset_on_output=timeout_reset_on_output, children=[])

    def _p_group(self):
        tok=self._expect(TT.IDENT,"group"); nm=self._expect(TT.IDENT)
        run_as=None; on_fail=None; timeout=None; timeout_reset_on_output=None
        while not self._at(TT.LBRACE) and not self._at(TT.EOF):
            if self._at_id("as"): self._advance(); run_as=self._expect(TT.IDENT).value
            elif self._at_id("on_fail"): self._advance(); on_fail=self._expect(TT.IDENT).value
            elif self._at_id("timeout"):
                timeout, timeout_reset_on_output = self._p_timeout_prop()
            else: break
        self._expect(TT.LBRACE)
        gd={}
        if run_as: gd["as"]=run_as
        if on_fail: gd["on_fail"]=on_fail
        if timeout: gd["timeout"]=timeout
        if timeout_reset_on_output is not None: gd["timeout_reset_on_output"]=timeout_reset_on_output
        resources=[]
        while not self._at(TT.RBRACE) and not self._at(TT.EOF):
            if self._at_id("resource"): resources.append(self._p_resource(gd))
            else: raise self._err(f"Expected 'resource' in group")
        self._expect(TT.RBRACE)
        return ASTGroup(name=nm.value, run_as=run_as, on_fail=on_fail,
                        timeout=timeout, resources=resources, line=tok.line,
                        timeout_reset_on_output=timeout_reset_on_output)

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

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
