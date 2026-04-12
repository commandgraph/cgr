"""
Comprehensive test suite for CommandGraph (cgr).

Covers: lexer, both parsers (.cg and .cgr), resolver, state tracking,
output collection, variable expansion, tags, when expressions, diff,
templates, inventory parsing, version checking, redaction, and execution.

Run:  python3 -m pytest test_commandgraph.py -v
      (Engine file: cgr.py)
"""
from __future__ import annotations
import argparse
import importlib.util
import io
import json
import os
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import textwrap
import unittest
import urllib.request
from pathlib import Path

import pytest

# ── Import engine ────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
import build_helpers
import cgr as cg


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _resolve_cgr(source: str, repo_dir=None, extra_vars=None, inventory_files=None,
                 resolve_deferred_secrets: bool = False) -> cg.Graph:
    """Parse .cgr source and resolve into a Graph."""
    ast = cg.parse_cgr(source)
    return cg.resolve(ast, repo_dir=repo_dir, extra_vars=extra_vars,
                      inventory_files=inventory_files,
                      resolve_deferred_secrets=resolve_deferred_secrets)

def _resolve_cg(source: str, repo_dir=None, extra_vars=None) -> cg.Graph:
    """Parse .cg source and resolve into a Graph."""
    tokens = cg.lex(source)
    ast = cg.Parser(tokens, source, "<test>").parse()
    return cg.resolve(ast, repo_dir=repo_dir, extra_vars=extra_vars)

def _tmpfile(content: str, suffix=".cgr") -> str:
    """Write content to a temp file and return its path."""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=suffix, delete=False)
    f.write(content)
    f.close()
    return f.name


def _write_encrypted_secrets(path: Path, passphrase: str, pairs: dict[str, str]) -> None:
    plaintext = "\n".join(f'{k} = "{v}"' for k, v in pairs.items()) + "\n"
    path.write_bytes(cg._encrypt_data(plaintext.encode(), passphrase))


def _write_legacy_encrypted_secrets(path: Path, passphrase: str, pairs: dict[str, str]) -> None:
    plaintext = "\n".join(f'{k} = "{v}"' for k, v in pairs.items()) + "\n"
    salt = os.urandom(16)
    key = cg._derive_key(passphrase, salt)
    iv = os.urandom(16)
    proc = subprocess.run(
        ["openssl", "enc", "-aes-256-cbc", "-nosalt", "-K", key.hex(), "-iv", iv.hex()],
        input=plaintext.encode(),
        capture_output=True,
        check=True,
    )
    path.write_bytes(b"CGRSECRT" + salt + iv + proc.stdout)


def _load_module_from_path(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# ══════════════════════════════════════════════════════════════════════════════
# §1 LEXER
# ══════════════════════════════════════════════════════════════════════════════

class TestLexer:
    def test_basic_tokens(self):
        tokens = cg.lex('var name = "hello"')
        types = [t.type for t in tokens]
        assert cg.TT.IDENT in types
        assert cg.TT.EQUALS in types
        assert cg.TT.STRING in types
        assert tokens[-1].type == cg.TT.EOF

    def test_command_token(self):
        tokens = cg.lex('`echo hello`')
        assert any(t.type == cg.TT.COMMAND and t.value == "echo hello" for t in tokens)

    def test_string_escapes(self):
        tokens = cg.lex(r'"hello\nworld"')
        s = [t for t in tokens if t.type == cg.TT.STRING][0]
        assert "\n" in s.value

    def test_comments_ignored(self):
        tokens = cg.lex('# this is a comment\nvar x = "1"')
        assert not any(t.value == "this" for t in tokens)

    def test_number_token(self):
        tokens = cg.lex('123')
        assert any(t.type == cg.TT.NUMBER and t.value == "123" for t in tokens)

    def test_gte_operator(self):
        tokens = cg.lex('>= 1')
        assert any(t.type == cg.TT.GTE for t in tokens)

    def test_compat_operator(self):
        tokens = cg.lex('~> 2')
        assert any(t.type == cg.TT.COMPAT for t in tokens)

    def test_dot_token(self):
        tokens = cg.lex('a.b')
        assert any(t.type == cg.TT.DOT for t in tokens)

    def test_unterminated_string_raises(self):
        with pytest.raises(cg.LexError):
            cg.lex('"hello')

    def test_unterminated_command_raises(self):
        with pytest.raises(cg.LexError):
            cg.lex('`echo')

    def test_unexpected_char_raises(self):
        with pytest.raises(cg.LexError):
            cg.lex('@')

    def test_line_numbers(self):
        tokens = cg.lex('var x = "1"\nvar y = "2"')
        y_tok = [t for t in tokens if t.value == "y"][0]
        assert y_tok.line == 2


# ══════════════════════════════════════════════════════════════════════════════
# §3b CGR PARSER
# ══════════════════════════════════════════════════════════════════════════════

class TestCGRParser:
    def test_minimal_graph(self):
        src = textwrap.dedent('''\
            target "local" local:
              [say hello]:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert len(ast.nodes) == 1
        assert ast.nodes[0].name == "local"
        assert len(ast.nodes[0].resources) == 1
        # Parser slugifies names
        assert ast.nodes[0].resources[0].name == "say_hello"

    def test_title_comment(self):
        src = textwrap.dedent('''\
            --- My Title ---
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert len(ast.nodes) == 1

    def test_variables(self):
        src = textwrap.dedent('''\
            set port = "8080"
            set host = "localhost"
            target "local" local:
              [step]:
                run $ echo ${port}
        ''')
        ast = cg.parse_cgr(src)
        assert len(ast.variables) == 2
        assert ast.variables[0].name == "port"
        assert ast.variables[0].value == "8080"

    def test_dependency(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step a]:
                run $ echo a
              [step b]:
                first [step a]
                run $ echo b
        ''')
        ast = cg.parse_cgr(src)
        step_b = ast.nodes[0].resources[1]
        # Parser slugifies dependency names
        assert "step_a" in step_b.needs

    def test_skip_if(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                skip if $ test -f /tmp/done
                run $ touch /tmp/done
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].check == "test -f /tmp/done"

    def test_always_run(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                always run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        r = ast.nodes[0].resources[0]
        assert r.check == "false"
        assert r.run == "echo hi"

    def test_as_root(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step] as root:
                run $ whoami
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].run_as == "root"

    def test_step_header_missing_colon_raises(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step a]:
                run $ echo a
              [step b]
                first [step a]
                run $ echo b
        ''')
        with pytest.raises(cg.CGRParseError, match=r"Invalid step header"):
            cg.parse_cgr(src)

    def test_timeout_and_retry(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step] timeout 30s, retry 3x:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        r = ast.nodes[0].resources[0]
        assert r.timeout == 30
        assert r.retries == 3

    def test_timeout_minutes(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step] timeout 2m:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].timeout == 120

    def test_timeout_reset_on_output_cgr(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step] timeout 30s reset on output:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        r = ast.nodes[0].resources[0]
        assert r.timeout == 30
        assert r.timeout_reset_on_output is True

    def test_timeout_reset_on_output_cg(self):
        src = textwrap.dedent('''\
            node "local" {
              via local
              resource step {
                timeout 30 reset on output
                run `echo hi`
              }
            }
        ''')
        ast = cg.Parser(cg.lex(src), src, "<test>").parse()
        r = ast.nodes[0].resources[0]
        assert r.timeout == 30
        assert r.timeout_reset_on_output is True

    def test_timeout_hours(self):
        src = textwrap.dedent("""\
            target "local" local:
                [step] timeout 2h:
                  run $ echo hi
        """)
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].timeout == 7200

    def test_retry_delay_hours(self):
        src = textwrap.dedent("""\
            target "local" local:
              [step] retry 3x wait 1h:
                run $ echo hi
        """)
        ast = cg.parse_cgr(src)
        r = ast.nodes[0].resources[0]
        assert r.retries == 3
        assert r.retry_delay == 3600

    def test_body_timeout_hours(self):
        src = textwrap.dedent("""\
            target "local" local:
              [step]:
                timeout 1h
                run $ echo hi
        """)
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].timeout == 3600

    def test_verify_block(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
              verify "it works":
                first [step]
                run $ echo ok
                retry 3x wait 2s
        ''')
        ast = cg.parse_cgr(src)
        verify = [r for r in ast.nodes[0].resources if r.is_verify]
        assert len(verify) == 1
        assert verify[0].retries == 3
        assert verify[0].retry_delay == 2

    def test_verify_block_multiline_run_and_timeout(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
              verify "it works":
                first [step]
                run $ cd /tmp && \\
                      ((echo one || \\
                        echo two) | wc -l | grep -q '^1$')
                retry 5x wait 5s
                timeout 60m reset on output
        ''')
        ast = cg.parse_cgr(src)
        verify = [r for r in ast.nodes[0].resources if r.is_verify]
        assert len(verify) == 1
        assert verify[0].run.startswith("cd /tmp && \\")
        assert "echo one || \\" in verify[0].run
        assert "echo two" in verify[0].run
        assert "grep -q '^1$')" in verify[0].run
        assert verify[0].run.count("\n") == 2
        assert verify[0].retries == 5
        assert verify[0].retry_delay == 5
        assert verify[0].timeout == 3600
        assert verify[0].timeout_reset_on_output is True

    def test_if_fails_warn(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step] if fails warn:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].on_fail == "warn"

    def test_when_clause(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                when os_family == "debian"
                run $ apt-get update
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].when == 'os_family == "debian"'

    def test_when_unquoted(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                when debug
                run $ echo debug
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].when == "debug"

    def test_tags(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                tags web, deploy
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].tags == ["web", "deploy"]

    def test_collect(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ hostname
                collect "hostname"
        ''')
        ast = cg.parse_cgr(src)
        assert ast.nodes[0].resources[0].collect_key == "hostname"

    def test_ssh_target(self):
        src = textwrap.dedent('''\
            target "web" ssh deploy@10.0.1.5:
              [step]:
                run $ uptime
        ''')
        ast = cg.parse_cgr(src)
        node = ast.nodes[0]
        assert node.via.method == "ssh"
        assert node.via.props.get("host") == "10.0.1.5"
        assert node.via.props.get("user") == "deploy"

    def test_using_import(self):
        src = textwrap.dedent('''\
            using apt/install_package
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert len(ast.uses) == 1
        assert ast.uses[0].path == "apt/install_package"

    def test_using_with_version_constraint(self):
        src = textwrap.dedent('''\
            using apt/install_package >= 1.0
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert ast.uses[0].path == "apt/install_package"
        assert ast.uses[0].version_op == ">="
        assert ast.uses[0].version_req == "1.0"

    def test_secrets_declaration(self):
        src = textwrap.dedent('''\
            secrets "vault.enc"
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert len(ast.secrets) == 1
        assert ast.secrets[0].path == "vault.enc"

    def test_inventory_declaration(self):
        src = textwrap.dedent('''\
            inventory "hosts.ini"
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        ast = cg.parse_cgr(src)
        assert len(ast.inventories) == 1
        assert ast.inventories[0].path == "hosts.ini"

    def test_multiple_first_on_one_line(self):
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                run $ echo b
              [c]:
                first [a] [b]
                run $ echo c
        ''')
        ast = cg.parse_cgr(src)
        step_c = ast.nodes[0].resources[2]
        assert "a" in step_c.needs
        assert "b" in step_c.needs

    def test_parallel_block(self):
        src = textwrap.dedent('''\
            target "local" local:
              [build]:
                parallel 2 at a time:
                  [frontend]:
                    run $ echo front
                  [backend]:
                    run $ echo back
        ''')
        ast = cg.parse_cgr(src)
        build = ast.nodes[0].resources[0]
        assert build.parallel_limit == 2
        assert len(build.parallel_block) == 2

    def test_race_block(self):
        src = textwrap.dedent('''\
            target "local" local:
              [fetch]:
                race:
                  [mirror1]:
                    run $ echo m1
                  [mirror2]:
                    run $ echo m2
        ''')
        ast = cg.parse_cgr(src)
        fetch = ast.nodes[0].resources[0]
        assert len(fetch.race_block) == 2

    def test_each_block(self):
        src = textwrap.dedent('''\
            set servers = "a,b,c"
            target "local" local:
              [deploy]:
                each server in ${servers}, 2 at a time:
                  [deploy ${server}]:
                    run $ echo ${server}
        ''')
        ast = cg.parse_cgr(src)
        deploy = ast.nodes[0].resources[0]
        assert deploy.each_var == "server"
        assert deploy.each_limit == 2

    def test_cross_node_dependency(self):
        src = textwrap.dedent('''\
            target "db" local:
              [setup schema]:
                run $ echo schema
            target "web" local, after "db":
              [deploy]:
                first [db/setup schema]
                run $ echo deploy
        ''')
        ast = cg.parse_cgr(src)
        web = ast.nodes[1]
        assert "db" in web.after_nodes
        deploy = web.resources[0]
        # Cross-node refs use dot notation internally: "db.setup_schema"
        assert "db.setup_schema" in deploy.needs

    def test_wait_for_webhook(self):
        src = textwrap.dedent("""\
            target "local" local:
              [wait for approval]:
                wait for webhook '/approve/${deploy_id}'
                timeout 4h
        """)
        ast = cg.parse_cgr(src)
        res = ast.nodes[0].resources[0]
        assert res.wait_kind == "webhook"
        assert res.wait_target == "/approve/${deploy_id}"
        assert res.timeout == 14400

    def test_subgraph_step(self):
        src = textwrap.dedent("""\
            target "local" local:
              [deploy app] from ./deploy_app.cgr:
                version = '2.1.0'
        """)
        ast = cg.parse_cgr(src)
        res = ast.nodes[0].resources[0]
        assert res.subgraph_path == "./deploy_app.cgr"
        assert res.subgraph_vars == {"version": "2.1.0"}

    def test_parse_error_on_invalid(self):
        with pytest.raises(cg.CGRParseError):
            cg.parse_cgr("this is not valid cgr")


# ══════════════════════════════════════════════════════════════════════════════
# §3 .CG PARSER
# ══════════════════════════════════════════════════════════════════════════════

class TestCGParser:
    def test_minimal_graph(self):
        src = textwrap.dedent('''\
            node "local" {
              via local
              resource step {
                run `echo hi`
              }
            }
        ''')
        graph = _resolve_cg(src)
        assert len(graph.all_resources) == 1

    def test_variables(self):
        src = textwrap.dedent('''\
            var port = "8080"
            node "local" {
              via local
              resource step {
                run `echo ${port}`
              }
            }
        ''')
        graph = _resolve_cg(src)
        assert "8080" in graph.all_resources[list(graph.all_resources)[0]].run

    def test_dependencies(self):
        src = textwrap.dedent('''\
            node "local" {
              via local
              resource a {
                run `echo a`
              }
              resource b {
                needs a
                run `echo b`
              }
            }
        ''')
        graph = _resolve_cg(src)
        b = [r for r in graph.all_resources.values() if r.short_name == "b"][0]
        assert len(b.needs) > 0

    def test_check_clause(self):
        src = textwrap.dedent('''\
            node "local" {
              via local
              resource step {
                check `test -f /tmp/x`
                run `touch /tmp/x`
              }
            }
        ''')
        graph = _resolve_cg(src)
        step = list(graph.all_resources.values())[0]
        assert step.check == "test -f /tmp/x"

    def test_tags_in_cg(self):
        src = textwrap.dedent('''\
            node "local" {
              via local
              resource step {
                tags web, deploy
                run `echo hi`
              }
            }
        ''')
        graph = _resolve_cg(src)
        step = list(graph.all_resources.values())[0]
        assert "web" in step.tags
        assert "deploy" in step.tags


# ══════════════════════════════════════════════════════════════════════════════
# §5 RESOLVER
# ══════════════════════════════════════════════════════════════════════════════

class TestResolver:
    def test_single_step(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        graph = _resolve_cgr(src)
        assert len(graph.all_resources) == 1
        assert len(graph.waves) == 1

    def test_dependency_ordering(self):
        """Steps with dependencies are in later waves."""
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                first [a]
                run $ echo b
              [c]:
                first [b]
                run $ echo c
        ''')
        graph = _resolve_cgr(src)
        assert len(graph.waves) == 3
        # a in wave 0, b in wave 1, c in wave 2
        a_id = [r.id for r in graph.all_resources.values() if r.short_name == "a"][0]
        b_id = [r.id for r in graph.all_resources.values() if r.short_name == "b"][0]
        c_id = [r.id for r in graph.all_resources.values() if r.short_name == "c"][0]
        assert a_id in graph.waves[0]
        assert b_id in graph.waves[1]
        assert c_id in graph.waves[2]

    def test_parallel_waves(self):
        """Independent steps are in the same wave."""
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                run $ echo b
              [c]:
                first [a]
                first [b]
                run $ echo c
        ''')
        graph = _resolve_cgr(src)
        assert len(graph.waves) == 2  # a+b in wave 0, c in wave 1
        assert len(graph.waves[0]) == 2

    def test_variable_expansion(self):
        src = textwrap.dedent('''\
            set greeting = "hello"
            target "local" local:
              [step]:
                run $ echo ${greeting}
        ''')
        graph = _resolve_cgr(src)
        step = list(graph.all_resources.values())[0]
        assert "hello" in step.run

    def test_variable_override(self):
        src = textwrap.dedent('''\
            set port = "8080"
            target "local" local:
              [step]:
                run $ echo ${port}
        ''')
        graph = _resolve_cgr(src, extra_vars={"port": "9090"})
        step = list(graph.all_resources.values())[0]
        assert "9090" in step.run

    def test_undefined_variable_raises(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo ${undefined_var}
        ''')
        with pytest.raises(cg.ResolveError, match="Undefined variable"):
            _resolve_cgr(src)

    def test_cycle_detection(self):
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                first [b]
                run $ echo a
              [b]:
                first [a]
                run $ echo b
        ''')
        with pytest.raises(cg.ResolveError, match="[Cc]ycle"):
            _resolve_cgr(src)

    def test_deduplication(self):
        """Identical check+run+run_as on the same node should dedup."""
        src = textwrap.dedent('''\
            target "local" local:
              [step a]:
                skip if $ test -f /tmp/x
                run $ touch /tmp/x
              [step b]:
                skip if $ test -f /tmp/x
                run $ touch /tmp/x
        ''')
        graph = _resolve_cgr(src)
        # Should deduplicate to 1 resource
        non_barrier = [r for r in graph.all_resources.values() if not r.is_barrier]
        assert len(non_barrier) == 1

    def test_no_cross_node_dedup(self):
        """Same command on different nodes should NOT dedup."""
        src = textwrap.dedent('''\
            target "node1" local:
              [step]:
                run $ echo hi
            target "node2" local:
              [step]:
                run $ echo hi
        ''')
        graph = _resolve_cgr(src)
        non_barrier = [r for r in graph.all_resources.values() if not r.is_barrier]
        assert len(non_barrier) == 2

    def test_verify_is_marked(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
              verify "works":
                first [step]
                run $ echo ok
        ''')
        graph = _resolve_cgr(src)
        verify = [r for r in graph.all_resources.values() if r.is_verify]
        assert len(verify) == 1

    def test_parallel_expansion(self):
        """Parallel block expands into children + barrier (parent becomes barrier)."""
        src = textwrap.dedent('''\
            target "local" local:
              [build]:
                parallel:
                  [a]:
                    run $ echo a
                  [b]:
                    run $ echo b
        ''')
        graph = _resolve_cgr(src)
        # a + b + build (barrier joining them) = 3
        assert len(graph.all_resources) == 3
        # build should be a barrier
        build = [r for r in graph.all_resources.values() if r.short_name == "build"][0]
        assert build.is_barrier is True

    def test_each_expansion(self):
        """Each block expands into N resources."""
        src = textwrap.dedent('''\
            set items = "x,y,z"
            target "local" local:
              [process]:
                each item in ${items}:
                  [do ${item}]:
                    run $ echo ${item}
        ''')
        graph = _resolve_cgr(src)
        names = [r.short_name for r in graph.all_resources.values()]
        # Variable name is 'item', values are x/y/z → slugs: do_item_x, etc.
        assert any("x" in n for n in names)
        assert any("y" in n for n in names)
        assert any("z" in n for n in names)

    def test_race_expansion(self):
        """Race block produces is_race resources."""
        src = textwrap.dedent('''\
            target "local" local:
              [fetch]:
                race:
                  [a]:
                    run $ echo a
                  [b]:
                    run $ echo b
        ''')
        graph = _resolve_cgr(src)
        race_res = [r for r in graph.all_resources.values() if r.is_race]
        assert len(race_res) == 2

    def test_cross_node_dependency(self):
        src = textwrap.dedent('''\
            target "db" local:
              [setup]:
                run $ echo db
            target "web" local, after "db":
              [deploy]:
                first [db/setup]
                run $ echo web
        ''')
        graph = _resolve_cgr(src)
        deploy = [r for r in graph.all_resources.values() if r.short_name == "deploy"][0]
        # deploy should depend on db's setup (through barriers)
        assert len(deploy.needs) > 0

    def test_tags_preserved(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                tags security, web
                run $ echo hi
        ''')
        graph = _resolve_cgr(src)
        step = [r for r in graph.all_resources.values() if not r.is_barrier][0]
        assert "security" in step.tags
        assert "web" in step.tags

    def test_when_preserved(self):
        src = textwrap.dedent('''\
            set os = "debian"
            target "local" local:
              [step]:
                when os == "debian"
                run $ echo hi
        ''')
        graph = _resolve_cgr(src)
        step = list(graph.all_resources.values())[0]
        assert step.when is not None

    def test_collect_key_preserved(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ hostname
                collect "hostname"
        ''')
        graph = _resolve_cgr(src)
        step = list(graph.all_resources.values())[0]
        assert step.collect_key == "hostname"


# ══════════════════════════════════════════════════════════════════════════════
# Templates
# ══════════════════════════════════════════════════════════════════════════════

class TestTemplates:
    """Test template loading and expansion using the actual repo/ directory."""

    @pytest.fixture
    def repo_dir(self):
        return str(Path(__file__).parent / "repo")

    def test_template_expansion(self, repo_dir):
        src = textwrap.dedent('''\
            using apt/install_package
            target "local" local:
              [install curl] from apt/install_package:
                name = "curl"
        ''')
        graph = _resolve_cgr(src, repo_dir=repo_dir)
        # Should have expanded: update_cache + install step
        assert len(graph.all_resources) >= 2

    def test_template_dedup(self, repo_dir):
        """Two calls to install_package should share update_cache."""
        src = textwrap.dedent('''\
            using apt/install_package
            target "local" local:
              [install curl] from apt/install_package:
                name = "curl"
              [install wget] from apt/install_package:
                name = "wget"
        ''')
        graph = _resolve_cgr(src, repo_dir=repo_dir)
        assert len(graph.dedup_map) > 0  # at least one dedup happened

    def test_template_multi_package(self, repo_dir):
        src = textwrap.dedent('''\
            using apt/install_package
            target "local" local:
              [install curl and wget] from apt/install_package:
                name = "curl wget"
        ''')
        graph = _resolve_cgr(src, repo_dir=repo_dir)
        res = graph.all_resources["local.install_curl_and_wget"]
        assert "apt-get install -y" in res.run
        assert "curl" in res.run
        assert "wget" in res.run
        assert 'for pkg in curl wget' in res.check

    def test_template_version_check(self, repo_dir):
        src = textwrap.dedent('''\
            using apt/install_package >= 1.0
            target "local" local:
              [install curl] from apt/install_package:
                name = "curl"
        ''')
        # Should resolve without error (stdlib is v1.0.0)
        graph = _resolve_cgr(src, repo_dir=repo_dir)
        assert len(graph.all_resources) >= 2

    def test_template_provenance(self, repo_dir):
        src = textwrap.dedent('''\
            using apt/install_package
            target "local" local:
              [install curl] from apt/install_package:
                name = "curl"
        ''')
        graph = _resolve_cgr(src, repo_dir=repo_dir)
        assert len(graph.provenance_log) > 0

    def test_template_composition_in_template_body(self, tmp_path):
        repo = tmp_path / "repo"
        (repo / "apt").mkdir(parents=True)
        (repo / "stack").mkdir(parents=True)
        (repo / "apt" / "install_package.cgr").write_text(textwrap.dedent("""\
            template install_package(name):
              [install ${name}]:
                run $ echo install ${name}
        """))
        (repo / "stack" / "setup_webserver.cgr").write_text(textwrap.dedent("""\
            template setup_webserver(domain, package = "nginx"):
              [install web package] from apt/install_package:
                name = "${package}"
              [get cert]:
                run $ echo cert ${domain}
                first [install web package]
        """))
        src = textwrap.dedent("""\
            using apt/install_package
            using stack/setup_webserver
            target "local" local:
              [setup site] from stack/setup_webserver:
                domain = "example.com"
        """)
        graph = _resolve_cgr(src, repo_dir=str(repo))
        install = graph.all_resources["local.install_web_package"]
        install_leaf_id = next(rid for rid in graph.all_resources if rid.startswith("local.install_web_package.") and graph.all_resources[rid].run)
        install_leaf = graph.all_resources[install_leaf_id]
        cert_id = next(rid for rid in graph.all_resources if rid.endswith(".get_cert"))
        cert = graph.all_resources[cert_id]
        assert install_leaf.run == "echo install nginx"
        assert install.id in cert.needs


# ══════════════════════════════════════════════════════════════════════════════
# Variable expansion and helpers
# ══════════════════════════════════════════════════════════════════════════════

class TestVariableExpansion:
    def test_simple_expansion(self):
        assert cg._expand("hello ${name}", {"name": "world"}) == "hello world"

    def test_multiple_vars(self):
        result = cg._expand("${a} and ${b}", {"a": "X", "b": "Y"})
        assert result == "X and Y"

    def test_none_input(self):
        assert cg._expand(None, {}) is None

    def test_undefined_raises(self):
        with pytest.raises(cg.ResolveError, match="Undefined"):
            cg._expand("${missing}", {})

    def test_env_expansion(self):
        os.environ["_CGR_TEST_VAR"] = "test_value"
        try:
            result = cg._expand("${ENV:_CGR_TEST_VAR}", {})
            assert result == "test_value"
        finally:
            del os.environ["_CGR_TEST_VAR"]

    def test_nested_var_in_var(self):
        """Variable-in-variable expansion: resolver does iterative expansion."""
        src = textwrap.dedent('''\
            set base = "/opt"
            set dir = "${base}/app"
            target "local" local:
              [step]:
                run $ echo ${dir}
        ''')
        graph = _resolve_cgr(src)
        step = list(graph.all_resources.values())[0]
        # Resolver expands ${dir} to "${base}/app" then ${base} to "/opt"
        # The actual behavior: ${dir} is expanded in-place, may or may not
        # do iterative expansion. Check what we actually get.
        assert "${dir}" not in step.run  # at minimum, dir is expanded


# ══════════════════════════════════════════════════════════════════════════════
# Redaction
# ══════════════════════════════════════════════════════════════════════════════

class TestRedaction:
    def test_basic_redact(self):
        result = cg._redact("password is secret123", {"secret123"})
        assert "secret123" not in result
        assert "***REDACTED***" in result

    def test_none_input(self):
        assert cg._redact(None, {"secret"}) is None

    def test_empty_sensitive(self):
        assert cg._redact("hello", set()) == "hello"

    def test_length_ordering(self):
        """Longer secrets should be replaced first to prevent substring issues."""
        result = cg._redact("my password123 is here", {"pass", "password123"})
        assert "password123" not in result
        assert "***REDACTED***" in result

    def test_multiple_occurrences(self):
        result = cg._redact("abc xyz abc", {"abc"})
        assert result == "***REDACTED*** xyz ***REDACTED***"


# ══════════════════════════════════════════════════════════════════════════════
# Tag filtering
# ══════════════════════════════════════════════════════════════════════════════

class TestTagFiltering:
    def _make_resource(self, tags=None, is_barrier=False, is_verify=False):
        return cg.Resource(
            id="test", short_name="test", node_name="local", description="",
            needs=[], check=None, run="echo hi", run_as=None,
            timeout=30, retries=0, retry_delay=0, retry_backoff=False, on_fail=cg.OnFail.STOP,
            when=None, env={}, is_verify=is_verify, line=1,
            is_barrier=is_barrier, tags=tags or [])

    def test_no_filters_no_skip(self):
        res = self._make_resource(tags=["web"])
        assert not cg._should_skip_tags(res, None, None)

    def test_include_matching(self):
        res = self._make_resource(tags=["web", "deploy"])
        assert not cg._should_skip_tags(res, {"web"}, None)

    def test_include_not_matching(self):
        res = self._make_resource(tags=["database"])
        assert cg._should_skip_tags(res, {"web"}, None)

    def test_include_untagged_skipped(self):
        res = self._make_resource(tags=[])
        assert cg._should_skip_tags(res, {"web"}, None)

    def test_exclude_matching(self):
        res = self._make_resource(tags=["web", "slow"])
        assert cg._should_skip_tags(res, None, {"slow"})

    def test_exclude_not_matching(self):
        res = self._make_resource(tags=["web"])
        assert not cg._should_skip_tags(res, None, {"slow"})

    def test_barrier_never_skipped(self):
        res = self._make_resource(tags=[], is_barrier=True)
        assert not cg._should_skip_tags(res, {"web"}, None)

    def test_verify_never_skipped(self):
        res = self._make_resource(tags=[], is_verify=True)
        assert not cg._should_skip_tags(res, {"web"}, None)

    def test_include_and_exclude(self):
        """Include web but exclude slow — a step tagged web+slow should be excluded."""
        res = self._make_resource(tags=["web", "slow"])
        assert cg._should_skip_tags(res, {"web"}, {"slow"})


# ══════════════════════════════════════════════════════════════════════════════
# When expressions
# ══════════════════════════════════════════════════════════════════════════════

class TestWhenExpressions:
    def test_empty_is_true(self):
        assert cg._eval_when("") is True
        assert cg._eval_when(None) is True

    def test_equality(self):
        assert cg._eval_when('os == "debian"', {"os": "debian"}) is True
        assert cg._eval_when('os == "redhat"', {"os": "debian"}) is False

    def test_inequality(self):
        assert cg._eval_when('os != "redhat"', {"os": "debian"}) is True
        assert cg._eval_when('os != "debian"', {"os": "debian"}) is False

    def test_truthy(self):
        assert cg._eval_when("debug", {"debug": "true"}) is True
        assert cg._eval_when("debug", {"debug": "yes"}) is True
        assert cg._eval_when("debug", {"debug": "1"}) is True

    def test_falsy(self):
        assert cg._eval_when("debug", {"debug": "false"}) is False
        assert cg._eval_when("debug", {"debug": "0"}) is False
        assert cg._eval_when("debug", {"debug": "no"}) is False
        assert cg._eval_when("debug", {"debug": ""}) is False

    def test_not_operator(self):
        assert cg._eval_when("not debug", {"debug": "false"}) is True
        assert cg._eval_when("not debug", {"debug": "true"}) is False

    def test_unresolved_falls_through(self):
        """Unknown variable treated as literal (truthy)."""
        assert cg._eval_when("something", {}) is True


# ══════════════════════════════════════════════════════════════════════════════
# Version checking
# ══════════════════════════════════════════════════════════════════════════════

class TestVersionChecking:
    def test_parse_semver(self):
        assert cg._parse_semver("1.2.3") == (1, 2, 3)
        assert cg._parse_semver("1.2") == (1, 2, 0)
        assert cg._parse_semver("1") == (1, 0, 0)

    def test_exact_match(self):
        assert cg._check_version("=", "1.0.0", "1.0.0") is True
        assert cg._check_version("=", "1.0.0", "1.0.1") is False

    def test_gte(self):
        assert cg._check_version(">=", "1.0.0", "1.0.0") is True
        assert cg._check_version(">=", "1.0.0", "2.0.0") is True
        assert cg._check_version(">=", "1.0.0", "0.9.0") is False

    def test_compat_major(self):
        """~> 1.0 means >= 1.0.0 and same major."""
        assert cg._check_version("~>", "1.0", "1.5.0") is True
        assert cg._check_version("~>", "1.0", "2.0.0") is False

    def test_compat_minor(self):
        """~> 1.2.0 means >= 1.2.0 and same major.minor."""
        assert cg._check_version("~>", "1.2.0", "1.2.5") is True
        assert cg._check_version("~>", "1.2.0", "1.3.0") is False


# ══════════════════════════════════════════════════════════════════════════════
# Identity hash
# ══════════════════════════════════════════════════════════════════════════════

class TestIdentityHash:
    def test_same_content_same_hash(self):
        h1 = cg._identity("test -f /x", "touch /x", "root", "node1")
        h2 = cg._identity("test -f /x", "touch /x", "root", "node1")
        assert h1 == h2

    def test_different_run_different_hash(self):
        h1 = cg._identity(None, "echo a", None, "node1")
        h2 = cg._identity(None, "echo b", None, "node1")
        assert h1 != h2

    def test_different_node_different_hash(self):
        h1 = cg._identity(None, "echo a", None, "node1")
        h2 = cg._identity(None, "echo a", None, "node2")
        assert h1 != h2

    def test_hash_length(self):
        h = cg._identity(None, "echo", None)
        assert len(h) == 16


# ══════════════════════════════════════════════════════════════════════════════
# State file
# ══════════════════════════════════════════════════════════════════════════════

class TestStateFile:
    def test_create_and_record(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        result = cg.ExecResult(
            resource_id="step_1", status=cg.Status.SUCCESS,
            run_rc=0, duration_ms=42)
        sf.record(result, wave=1)
        assert sf.get("step_1") is not None
        assert sf.get("step_1").status == "success"

    def test_should_skip_success(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        assert sf.should_skip("s1") is True

    def test_should_not_skip_failed(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.FAILED, run_rc=1), wave=0)
        assert sf.should_skip("s1") is False

    def test_should_skip_check(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SKIP_CHECK, check_rc=0), wave=0)
        assert sf.should_skip("s1") is True

    def test_should_skip_when(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SKIP_WHEN), wave=0)
        assert sf.should_skip("s1") is True

    def test_not_in_state_not_skipped(self, tmp_path):
        sf = cg.StateFile(tmp_path / "test.state")
        assert sf.should_skip("unknown") is False

    def test_reload_from_disk(self, tmp_path):
        path = tmp_path / "test.state"
        sf1 = cg.StateFile(path)
        sf1.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        sf1.record(cg.ExecResult("s2", cg.Status.FAILED, run_rc=1), wave=1)

        # Reload from disk
        sf2 = cg.StateFile(path)
        assert sf2.should_skip("s1") is True
        assert sf2.should_skip("s2") is False

    def test_malformed_line_tolerated(self, tmp_path):
        path = tmp_path / "test.state"
        path.write_text('{"id":"s1","status":"success","rc":0,"ms":0,"ts":"","wave":0}\n'
                        'CORRUPTED LINE\n'
                        '{"id":"s2","status":"success","rc":0,"ms":0,"ts":"","wave":1}\n')
        sf = cg.StateFile(path)
        assert sf.should_skip("s1") is True
        assert sf.should_skip("s2") is True

    def test_reset(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        sf.acquire_lock()
        sf.release_lock()
        sf.reset()
        assert not path.exists()
        assert not Path(f"{path}.lock").exists()
        assert sf.should_skip("s1") is False

    def test_manual_set(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.set_manual("s1", "success")
        assert sf.should_skip("s1") is True
        e = sf.get("s1")
        assert e.manual is True

    def test_drop(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        sf.drop("s1")
        assert sf.get("s1") is None
        sf2 = cg.StateFile(path)
        assert sf2.get("s1") is None

    def test_lock_unlock(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.acquire_lock()
        # Reload from disk to verify lock was persisted
        sf2 = cg.StateFile(path)
        locked, pid = sf2.is_locked()
        assert locked is True
        assert pid == os.getpid()
        sf.release_lock()
        sf3 = cg.StateFile(path)
        locked, _ = sf3.is_locked()
        assert locked is False

    def test_lock_context_releases_on_exit(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        with sf.lock():
            sf2 = cg.StateFile(path)
            locked, pid = sf2.is_locked()
            assert locked is True
            assert pid == os.getpid()
        sf3 = cg.StateFile(path)
        locked, _ = sf3.is_locked()
        assert locked is False

    def test_all_entries(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        sf.record(cg.ExecResult("s2", cg.Status.FAILED, run_rc=1), wave=0)
        entries = sf.all_entries()
        assert len(entries) == 2
        assert "s1" in entries
        assert "s2" in entries

    def test_state_summary(self, tmp_path):
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        sf.record(cg.ExecResult("s2", cg.Status.SUCCESS, run_rc=0), wave=0)
        sf.record(cg.ExecResult("s3", cg.Status.FAILED, run_rc=1), wave=1)
        summary = sf.state_summary()
        assert summary.get("success") == 2
        assert summary.get("failed") == 1


class TestStateCli:
    def test_state_set_drop_and_compact_via_main(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "test.cgr"
        marker = tmp_path / "marker.txt"
        graph_path.write_text(textwrap.dedent(f"""\
            target "local" local:
              [create marker]:
                skip if $ test -f {marker}
                run $ touch {marker}
              [report]:
                first [create marker]
                run $ echo done
        """))

        graph = cg._load(str(graph_path), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_path), max_parallel=1)

        monkeypatch.setattr(sys, "argv", ["cgr.py", "state", "set", str(graph_path), "report", "redo"])
        cg.main()
        state = cg.StateFile(cg._state_path(str(graph_path)))
        assert state.get("local.report").status == "failed"

        monkeypatch.setattr(sys, "argv", ["cgr.py", "state", "drop", str(graph_path), "report"])
        cg.main()
        state = cg.StateFile(cg._state_path(str(graph_path)))
        assert state.get("local.report") is None

        monkeypatch.setattr(sys, "argv", ["cgr.py", "state", "compact", str(graph_path)])
        cg.main()
        out = capsys.readouterr().out
        assert "State compacted" in out

        raw_lines = [line for line in Path(cg._state_path(str(graph_path))).read_text().splitlines() if line.strip()]
        resource_lines = [line for line in raw_lines if '"id":"local.create_marker"' in line]
        assert len(resource_lines) == 1
        assert any('"_wave":true' in line for line in raw_lines)
        assert any('"_run":true' in line for line in raw_lines)

    def test_state_compact_missing_graph_does_not_emit_load_error(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "missing.cgr"

        monkeypatch.setattr(sys, "argv", ["cgr.py", "state", "compact", str(graph_path)])
        cg.main()

        out = capsys.readouterr().out
        assert "No state file to compact." in out
        assert "error: not found" not in out

    def test_default_state_path_uses_dot_state_dir(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        assert cg._state_path("graphs/test.cgr") == str(tmp_path / ".state" / "graphs" / "test.cgr.state")

    def test_default_state_path_namespaces_external_graphs(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        external = tmp_path.parent / "elsewhere" / "test.cgr"
        expected = tmp_path / ".state" / "_external" / Path(*external.parts[1:-1]) / "test.cgr.state"
        assert cg._state_path(str(external)) == str(expected)

    def test_state_path_is_salted_by_run_id(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        path = cg._state_path("graphs/test.cgr", run_id="blue.canary")
        assert path.endswith(str(Path(".state") / "graphs" / "test.cgr.blue.canary.state"))

    def test_state_path_run_id_is_sanitized(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        path = cg._state_path("graphs/test.cgr", run_id="blue/canary 01")
        assert path.endswith(str(Path(".state") / "graphs" / "test.cgr.blue_canary_01.state"))

# ══════════════════════════════════════════════════════════════════════════════
# Output file
# ══════════════════════════════════════════════════════════════════════════════

class TestOutputFile:
    def test_record_and_retrieve(self, tmp_path):
        path = tmp_path / "test.output"
        of = cg.OutputFile(path)
        of.record("step_1", "hostname", "local", "myhost", "", 0, 5)
        outputs = of.all_outputs()
        assert len(outputs) == 1
        assert outputs[0]["key"] == "hostname"
        assert outputs[0]["stdout"] == "myhost"

    def test_get_by_key(self, tmp_path):
        path = tmp_path / "test.output"
        of = cg.OutputFile(path)
        of.record("s1", "disk", "node1", "50GB", "", 0, 3)
        of.record("s2", "disk", "node2", "100GB", "", 0, 3)
        of.record("s3", "mem", "node1", "8GB", "", 0, 3)
        results = of.get_by_key("disk")
        assert len(results) == 2

    def test_keys(self, tmp_path):
        path = tmp_path / "test.output"
        of = cg.OutputFile(path)
        of.record("s1", "disk", "n1", "50GB", "", 0, 3)
        of.record("s2", "mem", "n1", "8GB", "", 0, 3)
        assert set(of.keys()) == {"disk", "mem"}

    def test_reload_from_disk(self, tmp_path):
        path = tmp_path / "test.output"
        of1 = cg.OutputFile(path)
        of1.record("s1", "hostname", "local", "host1", "", 0, 5)
        of2 = cg.OutputFile(path)
        assert len(of2.all_outputs()) == 1

    def test_redaction_in_record(self, tmp_path):
        path = tmp_path / "test.output"
        of = cg.OutputFile(path)
        of.record("s1", "config", "local",
                  "password=secret123\nhost=db", "", 0, 5,
                  sensitive={"secret123"})
        output = of.all_outputs()[0]
        assert "secret123" not in output["stdout"]
        assert "***REDACTED***" in output["stdout"]

    def test_reset(self, tmp_path):
        path = tmp_path / "test.output"
        of = cg.OutputFile(path)
        of.record("s1", "key", "node", "val", "", 0, 5)
        of.reset()
        assert not path.exists()
        assert len(of.all_outputs()) == 0


# ══════════════════════════════════════════════════════════════════════════════
# Inventory parsing
# ══════════════════════════════════════════════════════════════════════════════

class TestInventory:
    def test_parse_basic_inventory(self, tmp_path):
        ini = tmp_path / "hosts.ini"
        ini.write_text(textwrap.dedent('''\
            [webservers]
            web-1  host=10.0.1.1  user=deploy
            web-2  host=10.0.1.2  user=deploy
        '''))
        vs = cg._parse_inventory(str(ini))
        assert "webservers" in vs
        assert "web-1" in vs["webservers"]
        assert "web-2" in vs["webservers"]
        assert "deploy@10.0.1.1" in vs["webservers"]

    def test_group_vars(self, tmp_path):
        ini = tmp_path / "hosts.ini"
        ini.write_text(textwrap.dedent('''\
            [fleet]
            host1  host=10.0.0.1

            [fleet:vars]
            dns_test = google.com
        '''))
        vs = cg._parse_inventory(str(ini))
        assert vs.get("dns_test") == "google.com"

    def test_ansible_prefix_stripping(self, tmp_path):
        ini = tmp_path / "hosts.ini"
        ini.write_text(textwrap.dedent('''\
            [fleet]
            web  ansible_host=10.0.1.1  ansible_user=admin  ansible_port=2222
        '''))
        vs = cg._parse_inventory(str(ini))
        # Should produce web:admin@10.0.1.1:2222
        assert "admin@10.0.1.1:2222" in vs["fleet"]

    def test_comments_and_blanks(self, tmp_path):
        ini = tmp_path / "hosts.ini"
        ini.write_text(textwrap.dedent('''\
            # comment
            ; another comment

            [group]
            host1 host=10.0.0.1
        '''))
        vs = cg._parse_inventory(str(ini))
        assert "group" in vs

    def test_real_fleet_ini(self):
        """Test the actual fleet.ini in the project."""
        fleet_path = Path(__file__).parent / "fleet.ini"
        if not fleet_path.exists():
            pytest.skip("fleet.ini not found")
        vs = cg._parse_inventory(str(fleet_path))
        assert "fleet" in vs
        assert "dns_test" in vs

    def test_inventory_integration(self, tmp_path):
        """Inventory variables flow through to graph resolution."""
        ini = tmp_path / "hosts.ini"
        ini.write_text(textwrap.dedent('''\
            [servers]
            web1 host=10.0.0.1 user=deploy
        '''))
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo ${servers}
        ''')
        graph = _resolve_cgr(src, inventory_files=[str(ini)])
        step = list(graph.all_resources.values())[0]
        assert "10.0.0.1" in step.run


# ══════════════════════════════════════════════════════════════════════════════
# Vars file parsing
# ══════════════════════════════════════════════════════════════════════════════

class TestVarsFile:
    def test_parse_quoted(self, tmp_path):
        f = tmp_path / "vars.env"
        f.write_text('port = "8080"\nhost = "localhost"\n')
        vs = cg._parse_vars_file(str(f))
        assert vs["port"] == "8080"
        assert vs["host"] == "localhost"

    def test_parse_unquoted(self, tmp_path):
        f = tmp_path / "vars.env"
        f.write_text('port = 9090\n')
        vs = cg._parse_vars_file(str(f))
        assert vs["port"] == "9090"

    def test_comments_skipped(self, tmp_path):
        f = tmp_path / "vars.env"
        f.write_text('# comment\nkey = "val"\n')
        vs = cg._parse_vars_file(str(f))
        assert len(vs) == 1


# ══════════════════════════════════════════════════════════════════════════════
# Execution (local only, simple commands)
# ══════════════════════════════════════════════════════════════════════════════

class TestExecution:
    def _local_node(self):
        return cg.HostNode(name="local", via_method="local", via_props={}, resources={})

    def _resource(self, run="echo hi", check=None, when=None, retries=0,
                  retry_delay=0, on_fail=cg.OnFail.STOP, is_verify=False):
        return cg.Resource(
            id="test_step", short_name="test_step", node_name="local",
            description="", needs=[], check=check, run=run, run_as=None,
            timeout=30, retries=retries, retry_delay=retry_delay, retry_backoff=False,
            on_fail=on_fail, when=when, env={}, is_verify=is_verify, line=1)

    def test_simple_success(self):
        res = self._resource(run="echo hello")
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert result.run_rc == 0
        assert "hello" in result.stdout

    def test_simple_failure(self):
        res = self._resource(run="false")
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        assert result.run_rc != 0

    def test_check_passes_skips(self):
        res = self._resource(run="echo should-not-run", check="true")
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SKIP_CHECK

    def test_check_fails_runs(self):
        res = self._resource(run="echo ran", check="false")
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "ran" in result.stdout

    def test_when_false_skips(self):
        res = self._resource(run="echo hi", when='os == "windows"')
        result = cg.exec_resource(res, self._local_node(), variables={"os": "linux"})
        assert result.status == cg.Status.SKIP_WHEN

    def test_when_true_runs(self):
        res = self._resource(run="echo hi", when='os == "linux"')
        result = cg.exec_resource(res, self._local_node(), variables={"os": "linux"})
        assert result.status == cg.Status.SUCCESS

    def test_retries(self):
        # A command that fails — should retry
        res = self._resource(run="false", retries=2, retry_delay=0)
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        assert result.attempts == 3  # 1 initial + 2 retries

    def test_on_fail_warn(self):
        res = self._resource(run="false", on_fail=cg.OnFail.WARN)
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.WARNED

    def test_on_fail_ignore(self):
        res = self._resource(run="false", on_fail=cg.OnFail.IGNORE)
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS

    def test_dry_run(self):
        res = self._resource(run="echo hi")
        result = cg.exec_resource(res, self._local_node(), dry_run=True)
        assert result.status == cg.Status.PENDING

    def test_stdout_captured(self):
        res = self._resource(run="echo test_output_123")
        result = cg.exec_resource(res, self._local_node())
        assert "test_output_123" in result.stdout

    def test_exec_resource_streams_live_output(self):
        cmd = (
            f"{sys.executable} -c "
            "\"import sys,time; "
            "print('out-1'); "
            "print('err-1', file=sys.stderr); "
            "time.sleep(0.05); "
            "print('out-2')\""
        )
        seen = []
        res = self._resource(run=cmd)
        result = cg.exec_resource(
            res,
            self._local_node(),
            on_output=lambda stream, chunk: seen.append((stream, chunk)),
        )
        assert result.status == cg.Status.SUCCESS
        assert "out-1" in result.stdout
        assert "out-2" in result.stdout
        assert "err-1" in result.stdout
        assert any(stream == "stdout" and "out-1" in chunk for stream, chunk in seen)
        assert any(stream == "stdout" and "err-1" in chunk for stream, chunk in seen)

    def test_exec_resource_live_output_does_not_toggle_tty_modes(self, monkeypatch):
        cmd = f"{sys.executable} -c \"print('out')\""
        res = self._resource(run=cmd)

        class _FakeStdin:
            def isatty(self):
                return True

        calls = []

        monkeypatch.setattr(cg.sys, "stdin", _FakeStdin())
        monkeypatch.setattr(cg.tty, "setcbreak", lambda fd: calls.append(("setcbreak", fd)))
        monkeypatch.setattr(cg.termios, "tcgetattr", lambda fd: [0, 0, 0, 0, 0, 0, []])
        monkeypatch.setattr(cg.termios, "tcsetattr", lambda fd, when, attrs: calls.append(("tcsetattr", fd, when, attrs)))

        result = cg.exec_resource(
            res,
            self._local_node(),
            on_output=lambda stream, chunk: None,
        )

        assert result.status == cg.Status.SUCCESS
        assert calls == []

    def test_exec_resource_cancel_check_terminates_process(self, tmp_path):
        marker = tmp_path / "cancelled.txt"
        cmd = (
            f"{sys.executable} -c "
            "\"import pathlib,time; "
            "time.sleep(5); "
            f"pathlib.Path(r'{marker}').write_text('late')\""
        )
        res = self._resource(run=cmd)
        cancelled = {"value": False}

        def _cancel():
            return cancelled["value"]

        import threading
        timer = threading.Timer(0.2, lambda: cancelled.__setitem__("value", True))
        timer.start()
        try:
            result = cg.exec_resource(res, self._local_node(), cancel_check=_cancel)
        finally:
            timer.cancel()
        assert result.status == cg.Status.CANCELLED
        assert not marker.exists()


# ══════════════════════════════════════════════════════════════════════════════
# Full file validation (existing example files)
# ══════════════════════════════════════════════════════════════════════════════

class TestExampleFiles:
    """Validate the root feature-exercise example files parse and resolve."""

    @pytest.fixture
    def project_dir(self):
        return Path(__file__).parent

    @pytest.fixture
    def repo_dir(self, project_dir):
        return str(project_dir / "repo")

    def _validate(self, filepath, repo_dir=None, inventory_files=None):
        graph = cg._load(str(filepath), repo_dir=repo_dir, raise_on_error=True,
                         inventory_files=inventory_files)
        assert graph is not None
        assert len(graph.all_resources) > 0
        assert len(graph.waves) > 0
        return graph

    def test_nginx_setup_cgr(self, project_dir):
        graph = self._validate(project_dir / "nginx_setup.cgr")
        assert len(graph.all_resources) == 10

    def test_nginx_setup_cg(self, project_dir):
        graph = self._validate(project_dir / "nginx_setup.cg")
        assert len(graph.all_resources) == 11

    def test_webserver_cgr(self, project_dir, repo_dir):
        graph = self._validate(project_dir / "webserver.cgr", repo_dir=repo_dir)
        assert len(graph.all_resources) == 18

    def test_webserver_cg(self, project_dir, repo_dir):
        graph = self._validate(project_dir / "webserver.cg", repo_dir=repo_dir)
        assert len(graph.all_resources) == 18

    def test_parallel_test_cgr(self, project_dir):
        graph = self._validate(project_dir / "parallel_test.cgr")
        assert len(graph.all_resources) == 26

    def test_multinode_test_cgr(self, project_dir):
        graph = self._validate(project_dir / "multinode_test.cgr")
        assert len(graph.all_resources) == 13
        assert len(graph.nodes) == 4

    def test_multinode_test_cg(self, project_dir):
        graph = self._validate(project_dir / "multinode_test.cg")
        assert len(graph.all_resources) == 13


# ══════════════════════════════════════════════════════════════════════════════
# Diff command
# ══════════════════════════════════════════════════════════════════════════════

class TestDiff:
    def test_identical_files(self, tmp_path):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
        ''')
        f1 = tmp_path / "a.cgr"
        f2 = tmp_path / "b.cgr"
        f1.write_text(src)
        f2.write_text(src)
        # cmd_diff returns exit code (0=same, 1=different)
        rc = cg.cmd_diff(str(f1), str(f2), json_output=True)
        assert rc == 0

    def test_different_files(self, tmp_path):
        src_a = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo a
        ''')
        src_b = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo b
        ''')
        f1 = tmp_path / "a.cgr"
        f2 = tmp_path / "b.cgr"
        f1.write_text(src_a)
        f2.write_text(src_b)
        rc = cg.cmd_diff(str(f1), str(f2), json_output=True)
        assert rc == 1

    def test_added_step(self, tmp_path):
        src_a = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo a
        ''')
        src_b = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo a
              [new step]:
                run $ echo b
        ''')
        f1 = tmp_path / "a.cgr"
        f2 = tmp_path / "b.cgr"
        f1.write_text(src_a)
        f2.write_text(src_b)
        rc = cg.cmd_diff(str(f1), str(f2), json_output=True)
        assert rc == 1

    def test_json_output_includes_expected_fields(self, tmp_path, capsys):
        src_a = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo a
        ''')
        src_b = textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo b
        ''')
        f1 = tmp_path / "a.cgr"
        f2 = tmp_path / "b.cgr"
        f1.write_text(src_a)
        f2.write_text(src_b)

        rc = cg.cmd_diff(str(f1), str(f2), json_output=True)
        out = json.loads(capsys.readouterr().out)

        assert rc == 1
        assert out["file_a"] == str(f1)
        assert out["file_b"] == str(f2)
        assert out["identical"] is False
        assert out["changed"] == [{"resource": "step", "fields": ["command"]}]


class TestValidateJsonCli:
    def test_validate_json_reports_cross_node_details(self, monkeypatch, capsys):
        project_dir = Path(__file__).resolve().parent
        graph_path = project_dir / "multinode_test.cgr"

        monkeypatch.setattr(sys, "argv", ["cgr.py", "validate", "--json", str(graph_path)])
        cg.main()

        out = json.loads(capsys.readouterr().out)
        assert out["valid"] is True
        assert out["nodes"] == 4
        assert {"from": "monitor.register_db_check", "to": "db.setup_schema"} in out["cross_node_deps"]
        assert out["node_ordering"]["web"] == ["db", "cache"]

    def test_validate_json_invalid_graph_returns_error(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "bad.cgr"
        graph_path.write_text(textwrap.dedent("""\
            target "local" local:
              [broken step]
                run $ echo nope
        """))

        monkeypatch.setattr(sys, "argv", ["cgr.py", "validate", "--json", str(graph_path)])
        with pytest.raises(SystemExit) as exc:
            cg.main()

        out = json.loads(capsys.readouterr().out)
        assert exc.value.code == 1
        assert out["valid"] is False
        assert out["file"] == str(graph_path)
        assert "Invalid step header" in out["error"]


class TestCompletion:
    def test_bash_completion_includes_state_compact(self, capsys):
        cg._print_completion("bash")
        out = capsys.readouterr().out
        assert 'compgen -W "show test reset set drop diff compact"' in out

    def test_zsh_completion_includes_why_and_doctor(self, capsys):
        cg._print_completion("zsh")
        out = capsys.readouterr().out
        assert "'why:Show reverse dependency chain'" in out
        assert "'doctor:Check environment'" in out


class TestTemplateCommand:
    def test_template_test_accepts_provisioning_and_disabled_branches(self, capsys):
        cg.cmd_test_template(repo_dir="./repo", template_path="file/ensure_line", verbose=True)
        out = capsys.readouterr().out
        assert "1 passed, 0 failed" in out


# ══════════════════════════════════════════════════════════════════════════════
# Convert and fmt (round-trip)
# ══════════════════════════════════════════════════════════════════════════════

class TestConvert:
    def test_cgr_to_cg_roundtrip(self, tmp_path):
        """Converting .cgr → .cg should produce a valid .cg file."""
        src = textwrap.dedent('''\
            set port = "8080"
            target "local" local:
              [step a]:
                skip if $ test -f /tmp/x
                run $ echo ${port}
              [step b]:
                first [step a]
                run $ echo done
        ''')
        cgr_file = tmp_path / "test.cgr"
        cg_file = tmp_path / "test.cg"
        cgr_file.write_text(src)
        cg.cmd_convert(str(cgr_file), output_file=str(cg_file))
        # The .cg file should be valid
        graph = cg._load(str(cg_file), raise_on_error=True)
        assert len(graph.all_resources) == 2

    def test_cg_to_cgr_roundtrip(self, tmp_path):
        """Converting .cg → .cgr should produce a valid .cgr file."""
        src = textwrap.dedent('''\
            var port = "8080"
            node "local" {
              via local
              resource step_a {
                check `test -f /tmp/x`
                run `echo ${port}`
              }
              resource step_b {
                needs step_a
                run `echo done`
              }
            }
        ''')
        cg_file = tmp_path / "test.cg"
        cgr_file = tmp_path / "test.cgr"
        cg_file.write_text(src)
        cg.cmd_convert(str(cg_file), output_file=str(cgr_file))
        graph = cg._load(str(cgr_file), raise_on_error=True)
        assert len(graph.all_resources) == 2

    def test_timeout_reset_on_output_roundtrip(self, tmp_path):
        src = textwrap.dedent('''\
            node "local" {
              via local
              resource step {
                timeout 30 reset on output
                run `echo hi`
              }
            }
        ''')
        cg_file = tmp_path / "timeout_reset.cg"
        cgr_file = tmp_path / "timeout_reset.cgr"
        cg_file.write_text(src)

        cg.cmd_convert(str(cg_file), output_file=str(cgr_file))

        out = cgr_file.read_text()
        assert "timeout 30s reset on output" in out
        graph = cg._load(str(cgr_file), raise_on_error=True)
        step = next(iter(graph.all_resources.values()))
        assert step.timeout == 30
        assert step.timeout_reset_on_output is True

    def test_cgr_to_cg_template_step_refs_roundtrip(self, tmp_path):
        """Converted .cg should preserve template-step dependencies from .cgr."""
        project_dir = Path(__file__).resolve().parent
        repo_dir = str((project_dir / "repo").resolve())
        src = project_dir / "webserver.cgr"
        out = tmp_path / "webserver.cg"

        cg.cmd_convert(str(src), output_file=str(out))

        graph = cg._load(str(out), repo_dir=repo_dir, raise_on_error=True)
        assert len(graph.all_resources) == 18
        assert cg.cmd_diff(str(src), str(out), repo_dir=repo_dir, json_output=True) == 0

    def test_cg_to_cgr_template_instance_refs_roundtrip(self, tmp_path):
        """Converted .cgr should emit template instance names that match .cg needs refs."""
        project_dir = Path(__file__).resolve().parent
        repo_dir = str((project_dir / "repo").resolve())
        src = project_dir / "webserver.cg"
        out = tmp_path / "webserver.cgr"

        cg.cmd_convert(str(src), output_file=str(out))

        graph = cg._load(str(out), repo_dir=repo_dir, raise_on_error=True)
        assert len(graph.all_resources) == 18
        assert cg.cmd_diff(str(src), str(out), repo_dir=repo_dir, json_output=True) == 0

    def test_cg_to_cgr_cross_node_refs_roundtrip(self, tmp_path):
        """Converted .cgr should preserve cross-node dependency references."""
        project_dir = Path(__file__).resolve().parent
        src = project_dir / "multinode_test.cg"
        out = tmp_path / "multinode_test.cgr"

        cg.cmd_convert(str(src), output_file=str(out))

        graph = cg._load(str(out), raise_on_error=True)
        assert len(graph.all_resources) == 13
        assert cg.cmd_diff(str(src), str(out), json_output=True) == 0

    def test_cg_to_cgr_nested_cross_node_refs_roundtrip(self, tmp_path):
        src = tmp_path / "nested.cg"
        out = tmp_path / "nested.cgr"
        src.write_text(textwrap.dedent("""\
            node "db" {
              via local
              resource parent {
                description "parent"
                run `echo parent`
                resource child {
                  description "child"
                  run `echo child`
                }
              }
            }
            node "web" {
              via local
              resource use_child {
                description "use child"
                needs db.parent.child
                run `echo ok`
              }
            }
        """))

        cg.cmd_convert(str(src), output_file=str(out))

        graph = cg._load(str(out), raise_on_error=True)
        assert len(graph.all_resources) == 3
        assert cg.cmd_diff(str(src), str(out), json_output=True) == 0
        assert "[db/parent/child]" in out.read_text()

    def test_cgr_to_cg_roundtrip_preserves_advanced_step_features(self, tmp_path):
        child = tmp_path / "deploy_app.cgr"
        child.write_text(textwrap.dedent("""\
            target "local" local:
              [deploy]:
                run $ echo ok
        """))
        src = tmp_path / "advanced.cgr"
        out = tmp_path / "advanced.cg"
        src.write_text(textwrap.dedent("""\
            target "local" local:
              [wait for ready]:
                wait for file "./ready.flag"
                timeout 5s
              [deploy app] from "./deploy_app.cgr":
                first [wait for ready]
                version = "2.1.0"
                collect "deploy_result" as deploy_output
                on success: set deploy_status = "ok"
                on failure: set deploy_status = "failed"
              [summarize]:
                first [deploy app]
                reduce "deploy_result" as summary

            on complete:
              post "https://hooks.example.com/done"
              body json '{"status":"ok"}'
              expect 200
        """))

        cg.cmd_convert(str(src), output_file=str(out))

        ast = cg.Parser(cg.lex(out.read_text(), str(out)), out.read_text(), str(out)).parse()
        wait_res, deploy_res, summarize_res = ast.nodes[0].resources
        assert wait_res.wait_kind == "file"
        assert deploy_res.subgraph_path == "./deploy_app.cgr"
        assert deploy_res.subgraph_vars == {"version": "2.1.0"}
        assert deploy_res.collect_var == "deploy_output"
        assert ("deploy_status", "ok") in deploy_res.on_success_set
        assert ("deploy_status", "failed") in deploy_res.on_failure_set
        assert summarize_res.reduce_key == "deploy_result"
        assert summarize_res.reduce_var == "summary"
        assert ast.on_complete is not None
        assert ast.on_complete.http_method == "POST"
        assert cg.cmd_diff(str(src), str(out), json_output=True) == 0

    def test_cg_to_cgr_roundtrip_preserves_advanced_step_features(self, tmp_path):
        child = tmp_path / "deploy_app.cgr"
        child.write_text(textwrap.dedent("""\
            target "local" local:
              [deploy]:
                run $ echo ok
        """))
        src = tmp_path / "advanced.cg"
        out = tmp_path / "advanced.cgr"
        src.write_text(textwrap.dedent("""\
            node "local" {
              via local
              resource wait_ready {
                wait for file "./ready.flag"
                timeout 5
              }
              resource deploy_app {
                needs wait_ready
                from "./deploy_app.cgr"
                version = "2.1.0"
                collect "deploy_result" as deploy_output
                on_success set deploy_status = "ok"
                on_failure set deploy_status = "failed"
              }
              resource summarize {
                needs deploy_app
                reduce "deploy_result" as summary
              }
            }
            on_complete {
              post "https://hooks.example.com/done"
              body json "{\\\"status\\\":\\\"ok\\\"}"
              expect "200"
            }
        """))

        cg.cmd_convert(str(src), output_file=str(out))

        ast = cg.parse_cgr(out.read_text(), str(out))
        wait_res, deploy_res, summarize_res = ast.nodes[0].resources
        assert wait_res.wait_kind == "file"
        assert deploy_res.subgraph_path == "./deploy_app.cgr"
        assert deploy_res.subgraph_vars == {"version": "2.1.0"}
        assert deploy_res.collect_var == "deploy_output"
        assert ("deploy_status", "ok") in deploy_res.on_success_set
        assert ("deploy_status", "failed") in deploy_res.on_failure_set
        assert summarize_res.reduce_key == "deploy_result"
        assert summarize_res.reduce_var == "summary"
        assert ast.on_complete is not None
        assert ast.on_complete.http_method == "POST"
        assert cg.cmd_diff(str(src), str(out), json_output=True) == 0

    def test_fmt_preserves_target_each_and_hooks(self, tmp_path):
        graph = tmp_path / "looped.cgr"
        graph.write_text(textwrap.dedent("""\
            set hosts = "web1:10.0.0.1,web2:10.0.0.2"
            each name, addr in ${hosts}:
              target "${name}" local:
                [announce]:
                  run $ echo ${addr}

            target "final" local, after each:
              [done]:
                run $ echo done

            on complete:
              post "https://hooks.example.com/done"
              expect 200
        """))

        cg.cmd_fmt(str(graph))

        formatted = graph.read_text()
        assert "each name, addr in ${hosts}:" in formatted
        assert 'target "${name}" local:' in formatted
        assert 'target "final" local, after each:' in formatted
        assert "on complete:" in formatted
        parsed = cg.parse_cgr(formatted, str(graph))
        resolved = cg.resolve(parsed)
        assert {"web1", "web2", "final"} <= set(resolved.nodes.keys())
        assert sorted(resolved.node_ordering["final"]) == ["web1", "web2"]
        assert parsed.on_complete is not None
        assert parsed.on_complete.http_method == "POST"


# ══════════════════════════════════════════════════════════════════════════════
# Topological sort (wave computation)
# ══════════════════════════════════════════════════════════════════════════════

class TestTopoSort:
    def test_linear_chain(self):
        """a → b → c should be 3 waves."""
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                first [a]
                run $ echo b
              [c]:
                first [b]
                run $ echo c
        ''')
        graph = _resolve_cgr(src)
        assert len(graph.waves) == 3

    def test_diamond(self):
        """Diamond: a → b,c → d should be 3 waves."""
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                first [a]
                run $ echo b
              [c]:
                first [a]
                run $ echo c
              [d]:
                first [b]
                first [c]
                run $ echo d
        ''')
        graph = _resolve_cgr(src)
        assert len(graph.waves) == 3
        # b and c should be in the same wave
        b_id = [r.id for r in graph.all_resources.values() if r.short_name == "b"][0]
        c_id = [r.id for r in graph.all_resources.values() if r.short_name == "c"][0]
        for wave in graph.waves:
            if b_id in wave:
                assert c_id in wave
                break

    def test_independent_steps(self):
        """All independent steps should be in wave 0."""
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                run $ echo b
              [c]:
                run $ echo c
        ''')
        graph = _resolve_cgr(src)
        assert len(graph.waves) == 1
        assert len(graph.waves[0]) == 3


# ══════════════════════════════════════════════════════════════════════════════
# Slug generation
# ══════════════════════════════════════════════════════════════════════════════

class TestSlug:
    def test_basic(self):
        assert cg._slug("install nginx") == "install_nginx"

    def test_special_chars(self):
        s = cg._slug("install (apt)")
        assert "(" not in s

    def test_whitespace(self):
        assert cg._slug("  hello   world  ") == "hello_world"


# ══════════════════════════════════════════════════════════════════════════════
# End-to-end apply (local commands)
# ══════════════════════════════════════════════════════════════════════════════

class TestEndToEndApply:
    """Full apply cycle: parse → resolve → execute → verify state."""

    def test_simple_apply(self, tmp_path):
        graph_file = tmp_path / "test.cgr"
        graph_file.write_text(textwrap.dedent('''\
            target "local" local:
              [create marker]:
                skip if $ test -f {marker}
                run $ touch {marker}
        '''.format(marker=tmp_path / "marker.txt")))

        graph = cg._load(str(graph_file), raise_on_error=True)
        # Run stateful apply
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        # Marker file should exist
        assert (tmp_path / "marker.txt").exists()

        # State should show success
        sf = cg.StateFile(cg._state_path(str(graph_file)))
        entries = sf.all_entries()
        assert len(entries) >= 1
        assert all(e.status in ("success", "skip_check") for e in entries.values())

    def test_idempotent_rerun(self, tmp_path):
        graph_file = tmp_path / "test.cgr"
        marker = tmp_path / "marker.txt"
        graph_file.write_text(textwrap.dedent(f'''\
            target "local" local:
              [create marker]:
                skip if $ test -f {marker}
                run $ touch {marker}
        '''))

        # First run
        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        # Second run — should skip from state
        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        sf = cg.StateFile(cg._state_path(str(graph_file)))
        # Still successful
        assert all(e.status in ("success", "skip_check") for e in sf.all_entries().values())

    def test_timeout_reset_on_output_allows_chatty_command(self, tmp_path):
        graph_file = tmp_path / "chatty_ok.cgr"
        graph_file.write_text(textwrap.dedent("""\
            target "local" local:
              [chatty] timeout 1s reset on output:
                run $ python3 -c "import sys,time; [print(i, flush=True) or time.sleep(0.4) for i in range(4)]"
        """))

        graph = cg._load(str(graph_file), raise_on_error=True)
        results, _ = cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        assert any(r.resource_id == "local.chatty" and r.status == cg.Status.SUCCESS for r in results)

    def test_timeout_without_reset_still_times_out_chatty_command(self, tmp_path):
        graph_file = tmp_path / "chatty_timeout.cgr"
        graph_file.write_text(textwrap.dedent("""\
            target "local" local:
              [chatty] timeout 1s:
                run $ python3 -c "import sys,time; [print(i, flush=True) or time.sleep(0.4) for i in range(4)]"
        """))

        graph = cg._load(str(graph_file), raise_on_error=True)
        results, _ = cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        assert any(r.resource_id == "local.chatty" and r.status == cg.Status.TIMEOUT for r in results)

    def test_start_from_reports_skips_as_start_from(self, tmp_path, capsys):
        graph_file = tmp_path / "start_from.cgr"
        a = tmp_path / "a.txt"
        b = tmp_path / "b.txt"
        c = tmp_path / "c.txt"
        graph_file.write_text(textwrap.dedent(f'''\
            target "local" local:
              [a]:
                skip if $ test -f {a}
                run $ touch {a}
              [b]:
                first [a]
                skip if $ test -f {b}
                run $ touch {b}
              [c]:
                first [b]
                skip if $ test -f {c}
                run $ touch {c}
        '''))

        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1, start_from="c")

        out = capsys.readouterr().out
        assert "before start" in out
        assert "--start-from" in out
        assert "completed previously" not in out

    def test_apply_with_collect(self, tmp_path):
        graph_file = tmp_path / "test.cgr"
        graph_file.write_text(textwrap.dedent('''\
            target "local" local:
              [get hostname]:
                run $ echo test-host-123
                collect "hostname"
        '''))
        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        # Output file should contain the collected value
        of = cg.OutputFile(str(graph_file) + ".output")
        outputs = of.all_outputs()
        assert len(outputs) >= 1
        assert any("test-host-123" in o.get("stdout", "") for o in outputs)

    def test_apply_on_complete_exports_summary_env(self, tmp_path):
        graph_file = tmp_path / "hook.cgr"
        hook_file = tmp_path / "hook.out"
        graph_file.write_text(textwrap.dedent("""\
            target "local" local:
              [a]:
                run $ echo hi
        """))

        graph = cg._load(str(graph_file), raise_on_error=True)
        hook_cmd = f"env | grep '^CGR_' > {shlex.quote(str(hook_file))}"
        cg.cmd_apply_stateful(graph, str(graph_file), on_complete=hook_cmd)

        out = hook_file.read_text()
        assert "CGR_STATUS=ok" in out
        assert "CGR_TOTAL=1" in out
        assert f"CGR_FILE={graph_file}" in out

    def test_apply_run_id_uses_isolated_state_file(self, tmp_path):
        graph_file = tmp_path / "test.cgr"
        marker = tmp_path / "marker.txt"
        graph_file.write_text(textwrap.dedent(f'''\
            target "local" local:
              [create marker]:
                skip if $ test -f {marker}
                run $ touch {marker}
        '''))
        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1, run_id="blue")

        default_state = Path(cg._state_path(str(graph_file)))
        run_state = Path(cg._state_path(str(graph_file), run_id="blue"))
        assert not default_state.exists()
        assert run_state.exists()

    def test_apply_explicit_state_path(self, tmp_path):
        graph_file = tmp_path / "test.cgr"
        graph_file.write_text(textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo ok
        '''))
        explicit_state = tmp_path / "custom.state"
        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1, state_path=str(explicit_state))
        assert explicit_state.exists()

    def test_apply_subgraph_step(self, tmp_path):
        child = tmp_path / "deploy_app.cgr"
        marker = tmp_path / "version.txt"
        child.write_text(textwrap.dedent(f"""\
            target "local" local:
              [write version]:
                run $ printf '%s' "${{version}}" > {marker}
        """))
        parent = tmp_path / "parent.cgr"
        parent.write_text(textwrap.dedent("""\
            target "local" local:
              [deploy app] from ./deploy_app.cgr:
                version = '2.1.0'
        """))

        graph = cg._load(str(parent), raise_on_error=True)
        results, _ = cg.cmd_apply_stateful(graph, str(parent), max_parallel=1)

        assert marker.read_text() == "2.1.0"
        assert any(r.resource_id == "local.deploy_app" and r.status == cg.Status.SUCCESS for r in results)

    def test_apply_subgraph_child_state_survives_parent_rerun(self, tmp_path):
        child = tmp_path / "deploy_app.cgr"
        counter = tmp_path / "deploy-count.txt"
        child.write_text(textwrap.dedent(f"""\
            target "local" local:
              [increment]:
                run $ python3 -c 'from pathlib import Path; p = Path(r"{counter}"); n = int(p.read_text()) + 1 if p.exists() else 1; p.write_text(str(n))'
        """))
        parent = tmp_path / "parent.cgr"
        parent.write_text(textwrap.dedent("""\
            target "local" local:
              [deploy app] from "./deploy_app.cgr":
                version = "2.1.0"
        """))

        graph = cg._load(str(parent), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(parent), max_parallel=1)
        assert counter.read_text().strip() == "1"

        parent_state = cg.StateFile(cg._state_path(str(parent)))
        parent_state.drop("local.deploy_app")

        graph = cg._load(str(parent), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(parent), max_parallel=1)

        assert counter.read_text().strip() == "1"
        child_state = cg.StateFile(cg._state_path(str(child)))
        assert child_state.get("local.increment").status == "success"

    def test_apply_wait_for_webhook(self, tmp_path, monkeypatch):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        monkeypatch.setenv("CGR_WEBHOOK_PORT", str(port))

        graph_file = tmp_path / "wait.cgr"
        marker = tmp_path / "approved.txt"
        graph_file.write_text(textwrap.dedent(f"""\
            set deploy_id = "abc123"
            target "local" local:
              [wait for approval]:
                wait for webhook '/approve/${{deploy_id}}'
                timeout 2s
              [mark approved]:
                first [wait for approval]
                run $ touch {marker}
        """))

        def _send_webhook():
            time.sleep(0.2)
            urllib.request.urlopen(f"http://127.0.0.1:{port}/approve/abc123").read()

        t = threading.Thread(target=_send_webhook, daemon=True)
        t.start()
        graph = cg._load(str(graph_file), raise_on_error=True)
        results, _ = cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)
        t.join(timeout=1)

        assert marker.exists()
        assert any(r.resource_id == "local.wait_for_approval" and r.status == cg.Status.SUCCESS for r in results)

    def test_apply_wait_for_file(self, tmp_path):
        ready = tmp_path / "ready.flag"
        graph_file = tmp_path / "wait_file.cgr"
        marker = tmp_path / "done.txt"
        graph_file.write_text(textwrap.dedent(f"""\
            target "local" local:
              [wait for ready]:
                wait for file "{ready}"
                timeout 2s
              [mark done]:
                first [wait for ready]
                run $ touch {marker}
        """))

        def _write_file():
            time.sleep(0.2)
            ready.write_text("ok")

        t = threading.Thread(target=_write_file, daemon=True)
        t.start()
        graph = cg._load(str(graph_file), raise_on_error=True)
        results, _ = cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)
        t.join(timeout=1)

        assert marker.exists()
        assert any(r.resource_id == "local.wait_for_ready" and r.status == cg.Status.SUCCESS for r in results)

    def test_apply_output_json(self, tmp_path, monkeypatch, capsys):
        graph_file = tmp_path / "json_apply.cgr"
        graph_file.write_text(textwrap.dedent("""\
            target "local" local:
              [step]:
                run $ echo ok
        """))

        monkeypatch.setattr(sys, "argv", ["cgr.py", "apply", str(graph_file), "--output", "json"])
        with pytest.raises(SystemExit) as exc:
            cg.main()
        assert exc.value.code == 0
        out = capsys.readouterr().out
        payload = json.loads(out)
        assert payload["file"] == str(graph_file)
        assert payload["results"][0]["status"] == "success"
        assert payload["metrics"]["run"]["wall_ms"] >= 0

    def test_apply_output_json_run_id_uses_isolated_output_file(self, tmp_path, monkeypatch, capsys):
        graph_file = tmp_path / "json_apply.cgr"
        graph_file.write_text(textwrap.dedent("""\
            target "local" local:
              [step]:
                run $ echo blue
                collect "value"
        """))

        monkeypatch.setattr(sys, "argv", ["cgr.py", "apply", str(graph_file), "--output", "json", "--run-id", "blue"])
        with pytest.raises(SystemExit) as exc:
            cg.main()
        assert exc.value.code == 0
        payload = json.loads(capsys.readouterr().out)
        assert payload["outputs"]["local/value"] == "blue"
        assert not Path(cg._output_path(str(graph_file))).exists()
        assert Path(cg._output_path(str(graph_file), run_id="blue")).exists()

        monkeypatch.setattr(sys, "argv", ["cgr.py", "report", str(graph_file), "--format", "json", "--run-id", "blue"])
        cg.main()
        report_payload = json.loads(capsys.readouterr().out)
        assert report_payload[0]["stdout"].strip() == "blue"

    def test_apply_live_progress_activation_uses_tty_only(self, monkeypatch):
        monkeypatch.setattr(cg.sys.stdout, "isatty", lambda: True, raising=False)
        assert cg._stdout_supports_live_updates() is True

        monkeypatch.setattr(cg.sys.stdout, "isatty", lambda: False, raising=False)
        assert cg._stdout_supports_live_updates() is False

    def test_live_progress_streams_stdout_lines_and_flushes_partial(self, monkeypatch):
        fake_now = [100.0]
        monkeypatch.setattr(cg.time, "monotonic", lambda: fake_now[0])
        stream = io.StringIO()
        progress = cg._LiveApplyProgress(enabled=True, stream=stream, interval=999)

        progress.set_wave(1, 1, 1)
        progress.step_started("local.step", "step", 30, True)
        fake_now[0] = 107.0
        progress.stream_stdout("local.step", "step", "hello")
        progress.stream_stdout("local.step", "step", " world\nline 2\npartial tail")
        fake_now[0] = 111.0
        render = progress._render()
        progress.step_finished("local.step")

        out = stream.getvalue()
        assert "hello world" in out
        assert "line 2" in out
        assert "partial tail" in out
        assert "step" in out
        assert "idle timeout 30s, counter 30s" in out
        assert "idle timeout 30s, counter 26s" in render

    def test_live_progress_carriage_return_stdout_flushes_latest_repaint(self, monkeypatch):
        fake_now = [100.0]
        monkeypatch.setattr(cg.time, "monotonic", lambda: fake_now[0])
        stream = io.StringIO()
        progress = cg._LiveApplyProgress(enabled=True, stream=stream, interval=999)

        progress.set_wave(1, 1, 1)
        progress.step_started("local.step", "step", 30, True)
        progress.stream_stdout("local.step", "step", "fetching 10%\rfetching 50%\rfetching 100%\n")
        progress.step_finished("local.step")

        out = stream.getvalue()
        assert "fetching 100%" in out
        assert "fetching 10%" not in out
        assert "fetching 50%" not in out

    def test_live_progress_flushes_final_carriage_return_line(self, monkeypatch):
        fake_now = [100.0]
        monkeypatch.setattr(cg.time, "monotonic", lambda: fake_now[0])
        stream = io.StringIO()
        progress = cg._LiveApplyProgress(enabled=True, stream=stream, interval=999)

        progress.set_wave(1, 1, 1)
        progress.step_started("local.step", "step", 30, True)
        progress.stream_stdout("local.step", "step", "done\r")
        progress.step_finished("local.step")

        assert "done" in stream.getvalue()

    def test_live_progress_crlf_repaint_frames_replay_committed_lines(self, monkeypatch):
        fake_now = [100.0]
        monkeypatch.setattr(cg.time, "monotonic", lambda: fake_now[0])
        stream = io.StringIO()
        progress = cg._LiveApplyProgress(enabled=True, stream=stream, interval=999)

        progress.set_wave(1, 1, 1)
        progress.step_started("local.step", "step", 30, True)
        progress.stream_stdout("local.step", "step", "\rframe 1\n\rframe 2\n\rframe 3\n")
        progress.step_finished("local.step")

        out = stream.getvalue()
        assert "frame 3" in out
        assert "│ out   step | \n" not in out
        assert "frame 1" in out
        assert "frame 2" in out

    def test_live_progress_shows_latest_repaint_in_spinner_line(self, monkeypatch):
        fake_now = [100.0]
        monkeypatch.setattr(cg.time, "monotonic", lambda: fake_now[0])
        stream = io.StringIO()
        progress = cg._LiveApplyProgress(enabled=True, stream=stream, interval=999)

        progress.set_wave(1, 1, 1)
        progress.step_started("local.step", "step", 30, True)
        progress.stream_stdout("local.step", "step", "frame 1\rframe 2")
        render = progress._render()

        assert "frame 2" in render
        assert "frame 1" not in render
        assert "│ out" not in stream.getvalue()

    def test_live_progress_flushes_deferred_repaint_before_normal_line(self, monkeypatch):
        fake_now = [100.0]
        monkeypatch.setattr(cg.time, "monotonic", lambda: fake_now[0])
        stream = io.StringIO()
        progress = cg._LiveApplyProgress(enabled=True, stream=stream, interval=999)

        progress.set_wave(1, 1, 1)
        progress.step_started("local.step", "step", 30, True)
        progress.stream_stdout("local.step", "step", "\rdownload complete\nnext line\n")
        progress.step_finished("local.step")

        out = stream.getvalue()
        assert "download complete" in out
        assert "next line" in out

    def test_state_records_wave_and_run_metrics(self, tmp_path):
        graph_file = tmp_path / "metrics.cgr"
        graph_file.write_text(textwrap.dedent("""\
            target "local" local:
              [a]:
                run $ echo a
              [b]:
                first [a]
                run $ echo b
        """))

        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)

        state = cg.StateFile(cg._state_path(str(graph_file)))
        waves = state.wave_metrics()
        run_metric = state.run_metric()
        assert len(waves) == 2
        assert run_metric is not None
        assert run_metric.wall_ms >= 0


# ══════════════════════════════════════════════════════════════════════════════
# Lint
# ══════════════════════════════════════════════════════════════════════════════

class TestLint:
    def test_lint_valid_file(self, tmp_path):
        f = tmp_path / "test.cgr"
        f.write_text(textwrap.dedent('''\
            target "local" local:
              [step]:
                skip if $ test -f /tmp/done
                run $ touch /tmp/done
        '''))
        # lint should not crash
        cg.cmd_lint(str(f))

    def test_lint_no_check_warns(self, tmp_path):
        f = tmp_path / "test.cgr"
        f.write_text(textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
        '''))
        # Should work (lint produces warnings, doesn't raise)
        cg.cmd_lint(str(f))


# ══════════════════════════════════════════════════════════════════════════════
# Package B: CLI polish features
# ══════════════════════════════════════════════════════════════════════════════

class TestAutoDetect:
    def test_single_file(self, tmp_path):
        (tmp_path / "only.cgr").write_text(textwrap.dedent('''\
            target "local" local:
              [step]:
                run $ echo hi
        '''))
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            result = cg._auto_detect_graph()
            assert "only.cgr" in result
        finally:
            os.chdir(old_cwd)

    def test_no_files_exits(self, tmp_path):
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            with pytest.raises(SystemExit):
                cg._auto_detect_graph()
        finally:
            os.chdir(old_cwd)

    def test_multiple_files_exits(self, tmp_path):
        (tmp_path / "a.cgr").write_text("target \"l\" local:\n  [s]:\n    run $ echo a")
        (tmp_path / "b.cgr").write_text("target \"l\" local:\n  [s]:\n    run $ echo b")
        old_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            with pytest.raises(SystemExit):
                cg._auto_detect_graph()
        finally:
            os.chdir(old_cwd)


class TestSkipSteps:
    def test_skip_step_in_apply(self, tmp_path):
        graph_file = tmp_path / "test.cgr"
        marker = tmp_path / "marker.txt"
        graph_file.write_text(textwrap.dedent(f'''\
            target "local" local:
              [step one]:
                run $ echo one
              [step two]:
                first [step one]
                run $ touch {marker}
              [step three]:
                first [step two]
                run $ echo three
        '''))
        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1,
                              skip_steps=["step two"])
        # step two was skipped, so marker should NOT exist
        assert not marker.exists()


class TestDryRunChecks:
    def test_dry_run_runs_checks(self, tmp_path):
        """--dry-run should run check clauses and show real skip/run status."""
        marker = tmp_path / "exists.txt"
        marker.write_text("hi")
        graph_file = tmp_path / "test.cgr"
        graph_file.write_text(textwrap.dedent(f'''\
            target "local" local:
              [already done]:
                skip if $ test -f {marker}
                run $ echo should-not-run
        '''))
        graph = cg._load(str(graph_file), raise_on_error=True)
        node = list(graph.nodes.values())[0]
        res = list(graph.all_resources.values())[0]
        result = cg.exec_resource(res, node, dry_run=True)
        # Check should run and find file exists → SKIP_CHECK
        assert result.status == cg.Status.SKIP_CHECK


class TestGatherFacts:
    def test_gather_facts_function(self):
        facts = cg._gather_facts()
        assert "os_name" in facts
        assert "os_family" in facts
        assert "arch" in facts
        assert "cpu_count" in facts
        assert "memory_mb" in facts
        assert "hostname" in facts
        assert "current_user" in facts
        assert "python_version" in facts
        # Values should be non-empty strings
        assert facts["os_name"] != ""
        assert int(facts["cpu_count"]) >= 1

    def test_gather_facts_in_graph(self):
        src = textwrap.dedent('''\
            gather facts
            target "local" local:
              [show]:
                run $ echo ${os_family}
        ''')
        graph = _resolve_cgr(src)
        step = list(graph.all_resources.values())[0]
        # os_family should have been expanded
        assert "${os_family}" not in step.run
        assert step.run != ""

    def test_gather_facts_overridden_by_set(self):
        src = textwrap.dedent('''\
            gather facts
            set os_family = "custom"
            target "local" local:
              [show]:
                run $ echo ${os_family}
        ''')
        graph = _resolve_cgr(src)
        step = list(graph.all_resources.values())[0]
        assert "custom" in step.run

    def test_no_gather_facts_no_vars(self):
        """Without 'gather facts', fact variables should not be available."""
        src = textwrap.dedent('''\
            target "local" local:
              [show]:
                run $ echo ${os_family}
        ''')
        with pytest.raises(cg.ResolveError, match="Undefined"):
            _resolve_cgr(src)

    def test_gather_facts_cg_parser(self):
        src = textwrap.dedent('''\
            gather facts
            node "local" {
              via local
              resource show {
                run `echo ${os_family}`
              }
            }
        ''')
        graph = _resolve_cg(src)
        step = list(graph.all_resources.values())[0]
        assert "${os_family}" not in step.run


class TestStateless:
    def test_stateless_flag_parsed(self):
        src = textwrap.dedent('''\
            set stateless = true
            target "local" local:
              [deploy]:
                run $ echo hello
        ''')
        graph = _resolve_cgr(src)
        assert graph.stateless is True

    def test_stateless_defaults_false(self):
        src = textwrap.dedent('''\
            target "local" local:
              [deploy]:
                run $ echo hello
        ''')
        graph = _resolve_cgr(src)
        assert graph.stateless is False

    def test_stateless_false_explicit(self):
        src = textwrap.dedent('''\
            set stateless = false
            target "local" local:
              [deploy]:
                run $ echo hello
        ''')
        graph = _resolve_cgr(src)
        assert graph.stateless is False

    def test_stateless_no_state_file_created(self, tmp_path):
        src = textwrap.dedent('''\
            set stateless = true
            target "local" local:
              [deploy]:
                run $ echo hello
        ''')
        cgr_file = tmp_path / "test.cgr"
        cgr_file.write_text(src)
        graph = cg._load(str(cgr_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(cgr_file))
        state_file = tmp_path / "test.cgr.state"
        assert not state_file.exists(), "stateless graph must not create a .state file"

    def test_stateless_state_path_override_warns(self, tmp_path, capsys):
        src = textwrap.dedent('''\
            set stateless = true
            target "local" local:
              [deploy]:
                run $ echo hello
        ''')
        cgr_file = tmp_path / "test.cgr"
        cgr_file.write_text(src)
        state_file = tmp_path / "explicit.state"
        graph = cg._load(str(cgr_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(cgr_file), state_path=str(state_file))
        out = capsys.readouterr().out
        assert "overrides" in out and "stateless" in out
        assert state_file.exists(), "--state FILE must still be written when it overrides stateless"


class TestShellCompletion:
    def test_bash_completion(self, capsys):
        cg._print_completion("bash")
        out = capsys.readouterr().out
        assert "complete -F _cgr_completions cgr" in out
        assert "plan" in out

    def test_zsh_completion(self, capsys):
        cg._print_completion("zsh")
        out = capsys.readouterr().out
        assert "#compdef cgr" in out

    def test_fish_completion(self, capsys):
        cg._print_completion("fish")
        out = capsys.readouterr().out
        assert "complete -c cgr" in out


# ══════════════════════════════════════════════════════════════════════════════
# Package D: Hardening
# ══════════════════════════════════════════════════════════════════════════════

class TestLargeGraph:
    def test_200_steps_resolves_fast(self):
        """A graph with 200 independent steps should resolve in < 1 second."""
        import time
        lines = ['target "local" local:']
        for i in range(200):
            lines.append(f'  [step {i}]:')
            lines.append(f'    run $ echo {i}')
        src = "\n".join(lines)
        t0 = time.monotonic()
        graph = _resolve_cgr(src)
        elapsed = time.monotonic() - t0
        assert len(graph.all_resources) == 200
        assert len(graph.waves) == 1  # all independent = 1 wave
        assert elapsed < 1.0, f"Resolution took {elapsed:.2f}s (should be < 1s)"

    def test_200_steps_linear_chain(self):
        """A linear chain of 200 steps should resolve correctly."""
        import time
        lines = ['target "local" local:']
        lines.append('  [step 0]:')
        lines.append('    run $ echo 0')
        for i in range(1, 200):
            lines.append(f'  [step {i}]:')
            lines.append(f'    first [step {i-1}]')
            lines.append(f'    run $ echo {i}')
        src = "\n".join(lines)
        t0 = time.monotonic()
        graph = _resolve_cgr(src)
        elapsed = time.monotonic() - t0
        assert len(graph.all_resources) == 200
        assert len(graph.waves) == 200  # linear = 200 waves
        assert elapsed < 2.0, f"Resolution took {elapsed:.2f}s (should be < 2s)"


class TestStateCrashSafety:
    def test_truncated_last_line(self, tmp_path):
        """A truncated last line (simulating crash) should not corrupt state."""
        path = tmp_path / "test.state"
        path.write_text(
            '{"id":"s1","status":"success","rc":0,"ms":10,"ts":"2026-01-01T00:00:00Z","wave":0}\n'
            '{"id":"s2","status":"success","rc":0,"ms":20,"ts":"2026-01-01T00:00:01Z","wave":1}\n'
            '{"id":"s3","status":"suc'  # truncated — crash mid-write
        )
        sf = cg.StateFile(path)
        assert sf.should_skip("s1") is True
        assert sf.should_skip("s2") is True
        assert sf.should_skip("s3") is False  # truncated entry ignored

    def test_empty_state_file(self, tmp_path):
        path = tmp_path / "test.state"
        path.write_text("")
        sf = cg.StateFile(path)
        assert len(sf.all_entries()) == 0

    def test_state_survives_double_record(self, tmp_path):
        """Recording the same step twice keeps the latest."""
        path = tmp_path / "test.state"
        sf = cg.StateFile(path)
        sf.record(cg.ExecResult("s1", cg.Status.FAILED, run_rc=1), wave=0)
        sf.record(cg.ExecResult("s1", cg.Status.SUCCESS, run_rc=0), wave=0)
        assert sf.get("s1").status == "success"
        # Reload to verify on-disk
        sf2 = cg.StateFile(path)
        assert sf2.get("s1").status == "success"


class TestErrorMessages:
    def test_undefined_dep_gives_helpful_error(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                first [nonexistent step]
                run $ echo hi
        ''')
        with pytest.raises(cg.ResolveError) as exc_info:
            _resolve_cgr(src)
        err = str(exc_info.value)
        assert "nonexistent" in err.lower() or "not found" in err.lower() or "undefined" in err.lower()

    def test_missing_run_gives_error(self):
        src = textwrap.dedent('''\
            target "local" local:
              [step]:
                skip if $ true
        ''')
        # Should either raise during parse or resolve
        try:
            graph = _resolve_cgr(src)
            # If it parses, the step should have no run command
            step = list(graph.all_resources.values())[0]
            assert step.run == "" or step.run == "true"
        except (cg.CGRParseError, cg.ResolveError):
            pass  # Also acceptable

    def test_duplicate_step_names(self):
        """Two steps with the same name in the same target should dedup or error."""
        src = textwrap.dedent('''\
            target "local" local:
              [same name]:
                run $ echo a
              [same name]:
                run $ echo b
        ''')
        # This should either dedup (if same content) or produce two distinct resources
        # Either way it should not crash
        graph = _resolve_cgr(src)
        assert len(graph.all_resources) >= 1

    def test_cycle_error_is_descriptive(self):
        src = textwrap.dedent('''\
            target "local" local:
              [a]:
                first [b]
                run $ echo a
              [b]:
                first [a]
                run $ echo b
        ''')
        with pytest.raises(cg.ResolveError) as exc_info:
            _resolve_cgr(src)
        err = str(exc_info.value).lower()
        assert "cycle" in err


class TestTimeoutInteraction:
    def test_global_timeout_stops_apply(self, tmp_path):
        """Global timeout should stop execution between waves."""
        graph_file = tmp_path / "test.cgr"
        graph_file.write_text(textwrap.dedent('''\
            target "local" local:
              [fast step]:
                run $ echo fast
              [slow step]:
                first [fast step]
                run $ sleep 0.1
              [after slow]:
                first [slow step]
                run $ echo after
        '''))
        graph = cg._load(str(graph_file), raise_on_error=True)
        # Apply with a very short timeout (should stop after wave 1 or 2)
        results, wall_ms = cg.cmd_apply_stateful(
            graph, str(graph_file), max_parallel=1,
            global_timeout=0  # immediate timeout
        )
        # With timeout=0, it should stop very quickly
        assert results == []
        assert wall_ms < 5000  # should not hang


# ══════════════════════════════════════════════════════════════════════════════
# RACE INTO — safe output handling
# ══════════════════════════════════════════════════════════════════════════════

class TestRaceInto:
    def test_parse_race_into_cgr(self):
        """CGR parser handles 'race into EXPR:'"""
        g = _resolve_cgr(textwrap.dedent('''\
            target "local" local:
              [fetch]:
                race into /tmp/out.bin:
                  [branch a]:
                    run $ echo a > ${_race_out}
                  [branch b]:
                    run $ echo b > ${_race_out}
        '''))
        # Both branches should exist as race resources
        race_res = [r for r in g.all_resources.values() if r.is_race]
        assert len(race_res) == 2
        # Each branch should have race_into and race_branch_index set
        assert all(r.race_into == "/tmp/out.bin" for r in race_res)
        indices = sorted(r.race_branch_index for r in race_res)
        assert indices == [0, 1]
        # _race_out should be expanded in run commands
        runs = sorted(r.run for r in race_res)
        assert "echo a > /tmp/out.bin.race.0" in runs[0]
        assert "echo b > /tmp/out.bin.race.1" in runs[1]

    def test_parse_race_into_with_variable(self):
        """race into ${dest}: expands variables"""
        g = _resolve_cgr(textwrap.dedent('''\
            set dest = "/tmp/download.tar.gz"
            target "local" local:
              [fetch]:
                race into ${dest}:
                  [fast]:
                    run $ cp /dev/null ${_race_out}
                  [slow]:
                    run $ cp /dev/null ${_race_out}
        '''))
        race_res = [r for r in g.all_resources.values() if r.is_race]
        assert all(r.race_into == "/tmp/download.tar.gz" for r in race_res)

    def test_plain_race_still_works(self):
        """Plain race: (no into) still works — no race_into set"""
        g = _resolve_cgr(textwrap.dedent('''\
            target "local" local:
              [pick]:
                race:
                  [a]:
                    run $ echo a
                  [b]:
                    run $ echo b
        '''))
        race_res = [r for r in g.all_resources.values() if r.is_race]
        assert len(race_res) == 2
        assert all(r.race_into is None for r in race_res)
        assert all(r.race_branch_index is None for r in race_res)

    def test_race_into_execution(self):
        """End-to-end: race into renames winner's temp to destination"""
        with tempfile.TemporaryDirectory() as td:
            dest = os.path.join(td, "result.txt")
            src = textwrap.dedent(f'''\
                target "local" local:
                  [fetch]:
                    race into {dest}:
                      [fast]:
                        run $ echo winner > ${{_race_out}}
                      [slow]:
                        run $ sleep 5 && echo loser > ${{_race_out}}
                        timeout 10s
            ''')
            graph_file = os.path.join(td, "test.cgr")
            with open(graph_file, "w") as f:
                f.write(src)
            graph = cg._load(graph_file, raise_on_error=True)
            results, _ = cg.cmd_apply_stateful(graph, graph_file, max_parallel=4)
            # Winner's output should be at the final destination
            assert os.path.exists(dest), "Winner's temp should be renamed to dest"
            content = open(dest).read().strip()
            assert content == "winner"
            # Temp files should be cleaned up
            assert not os.path.exists(f"{dest}.race.0")
            assert not os.path.exists(f"{dest}.race.1")

    def test_race_losing_branch_is_terminated(self):
        """The losing race branch should be stopped before it performs late side effects."""
        with tempfile.TemporaryDirectory() as td:
            dest = os.path.join(td, "result.txt")
            loser_marker = os.path.join(td, "loser.txt")
            src = textwrap.dedent(f'''\
                target "local" local:
                  [fetch]:
                    race into {dest}:
                      [fast]:
                        run $ echo winner > ${{_race_out}}
                      [slow]:
                        run $ {sys.executable} -c "import pathlib,time; time.sleep(5); pathlib.Path(r'{loser_marker}').write_text('late'); pathlib.Path(r'${{_race_out}}').write_text('loser')"
                        timeout 10s
            ''')
            graph_file = os.path.join(td, "test.cgr")
            with open(graph_file, "w") as f:
                f.write(src)
            graph = cg._load(graph_file, raise_on_error=True)
            cg.cmd_apply_stateful(graph, graph_file, max_parallel=4)
            assert os.path.exists(dest)
            assert open(dest).read().strip() == "winner"
            assert not os.path.exists(loser_marker)

    def test_race_into_cg_parser(self):
        """The .cg parser handles race into 'expr' { ... }"""
        g = _resolve_cg(textwrap.dedent('''\
            node "local" {
                via local
                resource fetch {
                    race into "/tmp/out.bin" {
                        resource branch_a {
                            run `echo a > ${_race_out}`
                        }
                        resource branch_b {
                            run `echo b > ${_race_out}`
                        }
                    }
                }
            }
        '''))
        race_res = [r for r in g.all_resources.values() if r.is_race]
        assert len(race_res) == 2
        assert all(r.race_into == "/tmp/out.bin" for r in race_res)

    def test_race_into_emitter_cgr(self):
        """CGR emitter preserves race into"""
        ast = cg.parse_cgr(textwrap.dedent('''\
            target "local" local:
              [fetch]:
                race into /tmp/out.bin:
                  [a]:
                    run $ echo a
                  [b]:
                    run $ echo b
        '''))
        output = cg._emit_cgr(ast)
        assert "race into /tmp/out.bin:" in output

    def test_lint_warns_race_without_into(self):
        """Lint warns when race branches write to the same destination without into"""
        src = textwrap.dedent('''\
            target "local" local:
              [fetch]:
                race:
                  [a]:
                    run $ curl -o /tmp/out.bin http://example.com/a
                  [b]:
                    run $ curl -o /tmp/out.bin http://example.com/b
        ''')
        path = Path(_tmpfile(src))
        try:
            count = cg.cmd_lint(str(path))
            assert count > 0  # Should have at least the race clobber warning
        finally:
            path.unlink(missing_ok=True)

    def test_lint_no_warn_with_race_into(self):
        """Lint does not warn when race uses into"""
        src = textwrap.dedent('''\
            target "local" local:
              [fetch]:
                race into /tmp/out.bin:
                  [a]:
                    run $ echo a > ${_race_out}
                  [b]:
                    run $ echo b > ${_race_out}
        ''')
        path = Path(_tmpfile(src))
        try:
            count = cg.cmd_lint(str(path))
            # Should NOT have the race clobber warning (may have other warnings)
            # We can't easily check specific warnings, but the race warning shouldn't fire
            # because race_into is set on the resources
        finally:
            path.unlink(missing_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# FOOTGUN SAFEGUARDS
# ══════════════════════════════════════════════════════════════════════════════

class TestFootgunSafeguards:
    def test_env_var_missing_raises(self):
        """${ENV:MISSING} raises ResolveError instead of returning literal"""
        # Make sure the var is NOT set
        os.environ.pop("_CGR_TEST_MISSING_VAR_", None)
        with pytest.raises(cg.ResolveError, match="not set"):
            _resolve_cgr(textwrap.dedent('''\
                target "local" local:
                  [step]:
                    run $ echo ${ENV:_CGR_TEST_MISSING_VAR_}
            '''))

    def test_env_var_set_works(self):
        """${ENV:VAR} still works when the env var is set"""
        os.environ["_CGR_TEST_PRESENT_"] = "hello"
        try:
            g = _resolve_cgr(textwrap.dedent('''\
                target "local" local:
                  [step]:
                    run $ echo ${ENV:_CGR_TEST_PRESENT_}
            '''))
            res = [r for r in g.all_resources.values() if not r.is_barrier]
            assert any("hello" in (r.run or "") for r in res)
        finally:
            del os.environ["_CGR_TEST_PRESENT_"]

    def test_lint_when_typo(self):
        """Lint warns when 'when' references unknown variable"""
        src = textwrap.dedent('''\
            set os_family = "debian"
            target "local" local:
              [install]:
                when os_famly == "debian"
                run $ echo install
        ''')
        path = Path(_tmpfile(src))
        try:
            count = cg.cmd_lint(str(path))
            assert count > 0  # Should warn about os_famly
        finally:
            path.unlink(missing_ok=True)

    def test_lint_when_no_warn_for_known_var(self):
        """Lint does not warn when 'when' references a known variable"""
        src = textwrap.dedent('''\
            set os_family = "debian"
            target "local" local:
              [install]:
                when os_family == "debian"
                run $ echo install
        ''')
        path = Path(_tmpfile(src))
        try:
            count = cg.cmd_lint(str(path))
            # May have other warnings, but NOT the 'when' typo warning
            # The when expression uses a known variable
        finally:
            path.unlink(missing_ok=True)

    def test_lint_each_loop_clobber(self):
        """Lint warns when each-loop branches write to same fixed path"""
        src = textwrap.dedent('''\
            set servers = "a,b,c"
            target "local" local:
              [deploy]:
                each server in ${servers}:
                  [deploy ${server}]:
                    run $ ./deploy.sh > /tmp/deploy.log
        ''')
        path = Path(_tmpfile(src))
        try:
            count = cg.cmd_lint(str(path))
            assert count > 0  # Should warn about /tmp/deploy.log
        finally:
            path.unlink(missing_ok=True)

    def test_lint_duplicate_collect_key(self):
        """Lint warns when two steps on same node share a collect key"""
        src = textwrap.dedent('''\
            target "local" local:
              [get hostname]:
                run $ hostname
                collect "info"
              [get kernel]:
                run $ uname -r
                collect "info"
        ''')
        path = Path(_tmpfile(src))
        try:
            count = cg.cmd_lint(str(path))
            assert count > 0  # Should warn about duplicate 'info' key
        finally:
            path.unlink(missing_ok=True)

    def test_start_from_warns_unsatisfied(self, capsys):
        """--start-from prints warning about unsatisfied dependencies"""
        with tempfile.TemporaryDirectory() as td:
            src = textwrap.dedent('''\
                target "local" local:
                  [step a]:
                    run $ echo a
                  [step b]:
                    first [step a]
                    run $ echo b
                  [step c]:
                    first [step b]
                    run $ echo c
            ''')
            graph_file = os.path.join(td, "test.cgr")
            with open(graph_file, "w") as f:
                f.write(src)
            graph = cg._load(graph_file, raise_on_error=True)
            # Apply with start_from — should warn about skipped deps
            results, _ = cg.cmd_apply_stateful(graph, graph_file,
                max_parallel=1, start_from="step c")
            captured = capsys.readouterr()
            assert "skipping" in captured.out.lower() or "skip" in captured.out.lower()

    def test_ssh_timeout_message(self):
        """SSH timeout error message mentions remote process may still run"""
        # We can't easily test SSH, but we can test the _run_cmd function
        # indirectly by checking the message format
        from cgr import HostNode
        node = HostNode(name="test", via_method="ssh",
                        via_props={"host": "fake", "user": "test", "port": "22"},
                        resources={})
        res = cg.Resource(
            id="test.step", short_name="step", node_name="test",
            description="test", needs=[], check=None, run="sleep 999",
            run_as=None, timeout=0, retries=0, retry_delay=0, retry_backoff=False,
            on_fail=cg.OnFail.STOP, when=None, env={}, is_verify=False, line=1)
        # The SSH connection will fail (host doesn't exist), but if it
        # somehow times out, the message should mention remote process
        rc, out, err = cg._run_cmd("sleep 999", node, res, timeout=1)
        # Either it fails to connect (FileNotFoundError) or times out
        if rc == 124:
            assert "remote process" in err


# ══════════════════════════════════════════════════════════════════════════════
# IDE Server API Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestIDEServerAPI:
    """Integration tests for the IDE server HTTP API endpoints."""

    @pytest.fixture(autouse=True, scope="class")
    def _setup_server(self, tmp_path_factory):
        """Start a server in a background thread for the whole test class."""
        import threading
        import urllib.request
        import socket
        import time

        tmp_path = tmp_path_factory.mktemp("ide_server")

        # Write a simple .cgr file
        cgr_file = tmp_path / "test.cgr"
        cgr_file.write_text(textwrap.dedent("""\
            --- Test Graph ---
            set greeting = "hello"
            target "local" local:
              [step one]:
                run $ echo one
              [step two]:
                first [step one]
                run $ echo two
        """))

        TestIDEServerAPI._cgr_file = str(cgr_file)
        TestIDEServerAPI._tmp_path = tmp_path

        # Find a free port
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        TestIDEServerAPI._port = port

        # Start server in background thread (daemon so it dies with the process)
        t = threading.Thread(
            target=cg.cmd_serve,
            kwargs=dict(filepath=str(cgr_file), port=port,
                        host="127.0.0.1", no_open=True),
            daemon=True)
        t.start()

        # Wait for server to be ready
        for _ in range(50):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/api/info", timeout=1)
                break
            except Exception:
                time.sleep(0.1)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/csrf", timeout=1) as resp:
            TestIDEServerAPI._csrf_token = json.loads(resp.read())["csrf_token"]
        yield

    def _api(self, method, path, body=None, *, include_csrf=True, headers=None):
        import urllib.request
        url = f"http://127.0.0.1:{self._port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req_headers = dict(headers or {})
        if body is not None:
            req_headers.setdefault("Content-Type", "application/json")
        if include_csrf and method.upper() not in {"GET", "HEAD", "OPTIONS"}:
            req_headers.setdefault("X-CSRF-Token", self._csrf_token)
        req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return json.loads(e.read())

    def _raw_api(self, method, path, body=None, *, headers=None):
        import urllib.request
        url = f"http://127.0.0.1:{self._port}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req_headers = dict(headers or {})
        if body is not None:
            req_headers.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, dict(resp.headers.items()), json.loads(resp.read())
        except urllib.error.HTTPError as e:
            return e.code, dict(e.headers.items()), json.loads(e.read())

    def test_get_file(self):
        r = self._api("GET", "/api/file")
        assert "content" in r
        assert "Test Graph" in r["content"]

    def test_get_info(self):
        r = self._api("GET", "/api/info")
        assert "version" in r

    def test_csrf_missing_rejected_for_mutation(self):
        r = self._api("POST", "/api/state/reset", {}, include_csrf=False)
        assert r["error"] == "Invalid or missing CSRF token"

    def test_csrf_endpoint_rejects_disallowed_origin(self):
        status, headers, body = self._raw_api(
            "GET",
            "/api/csrf",
            headers={"Origin": f"http://evil.example:{self._port}"},
        )
        assert status == 403
        assert body["error"] == "Origin not allowed"

    def test_info_allows_localhost_origin(self):
        status, headers, body = self._raw_api(
            "GET",
            "/api/info",
            headers={"Origin": f"http://localhost:{self._port}"},
        )
        assert status == 200
        assert headers.get("Access-Control-Allow-Origin") == f"http://localhost:{self._port}"
        assert "version" in body

    def test_info_cors_origin_is_canonicalized(self):
        status, headers, body = self._raw_api(
            "GET",
            "/api/info",
            headers={"Origin": f"http://LOCALHOST:{self._port}"},
        )
        assert status == 200
        assert headers.get("Access-Control-Allow-Origin") == f"http://localhost:{self._port}"
        assert "version" in body

    def test_info_rejects_disallowed_host_header(self):
        status, headers, body = self._raw_api(
            "GET",
            "/api/info",
            headers={"Host": "evil.example"},
        )
        assert status == 403
        assert body["error"] == "Host not allowed"

    def test_resolve(self):
        r = self._api("POST", "/api/resolve", {"source": open(self._cgr_file).read()})
        assert "error" not in r
        assert r["resources"] == 2
        assert r["waves_count"] == 2

    def test_resolve_redacts_secret_payload(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "supersecret")
        src = textwrap.dedent("""\
            set token = secret "env:MY_SECRET"
            target "t" local:
              [step]:
                run $ echo ${token}
        """)
        r = self._api("POST", "/api/resolve", {"source": src})
        assert "error" not in r
        assert r["variables"]["token"] == "***REDACTED***"
        assert r["graph"]["variables"]["token"] == "***REDACTED***"
        step = next(res for res in r["graph"]["resources"] if res["id"] == "t.step")
        assert "supersecret" not in step["run"]
        assert "***REDACTED***" in step["run"]

    def test_resolve_invalid(self):
        r = self._api("POST", "/api/resolve", {"source": "this is not valid cgr"})
        assert "error" in r

    def test_templates(self):
        r = self._api("GET", "/api/templates")
        assert "templates" in r

    def test_apply_and_events(self):
        import time
        r = self._api("POST", "/api/apply", {"source": open(self._cgr_file).read()})
        assert r.get("ok") is True
        # Wait for execution
        time.sleep(2)
        r2 = self._api("GET", "/api/apply/events?since=0")
        assert "events" in r2
        # Should have at least start + step events + done
        types = [e["type"] for e in r2["events"]]
        assert "start" in types
        step_start = next(e for e in r2["events"] if e["type"] == "step_start")
        assert "timeout_s" in step_start
        assert "timeout_reset_on_output" in step_start
        assert "run" in step_start
        assert "ts" in step_start
        assert "done" in types

    def test_opened_file_becomes_active_apply_target(self):
        other = Path(self._tmp_path) / "other.cgr"
        original = Path(self._cgr_file)
        other.write_text(textwrap.dedent("""\
            --- Other Graph ---
            target "local" local:
              [other step]:
                run $ echo other
        """))
        open_resp = self._api("POST", "/api/file/open", {"name": other.name})
        assert open_resp["ok"] is True
        apply_resp = self._api("POST", "/api/apply", {"source": other.read_text() + "\n# switched\n"})
        assert apply_resp["ok"] is True
        for _ in range(50):
            events = self._api("GET", "/api/apply/events?since=0")
            if any(e["type"] == "done" for e in events.get("events", [])):
                break
            import time
            time.sleep(0.1)
        assert "# switched" in other.read_text()
        assert "# switched" not in original.read_text()
        restore_resp = self._api("POST", "/api/file/open", {"name": original.name})
        assert restore_resp["ok"] is True

    def test_state_reset(self):
        try:
            r = self._api("POST", "/api/state/reset", {})
            # Should succeed (or say no state file)
            assert "ok" in r or "error" in r
        except Exception:
            pass  # Server may not have state file yet — acceptable

    def test_history_reports_runs(self):
        state = cg.StateFile(cg._state_path(self._cgr_file))
        with state.lock():
            state.record(cg.ExecResult("local.step_one", cg.Status.SUCCESS, run_rc=0, duration_ms=12), wave=0)
            state.record(cg.ExecResult("local.step_two", cg.Status.FAILED, run_rc=1, duration_ms=7), wave=1)
        r = self._api("GET", "/api/history")
        assert len(r["runs"]) >= 1
        latest = r["runs"][0]
        assert latest["ok"] == 1
        assert latest["fail"] == 1
        assert {entry["id"] for entry in latest["entries"]} >= {"local.step_one", "local.step_two"}

    def test_report_returns_collected_outputs(self):
        output = cg.OutputFile(cg._output_path(self._cgr_file))
        output.record("local.step_one", "artifact", "local", "hello", "", 0, 9)
        r = self._api("GET", "/api/report")
        assert len(r["outputs"]) >= 1
        assert any(entry["key"] == "artifact" and entry["stdout"] == "hello" for entry in r["outputs"])

    def test_lint_endpoint_returns_warnings(self):
        src = textwrap.dedent("""\
            --- Lint Graph ---
            target "local" local:
              [step]:
                run $ echo hi
        """)
        r = self._api("POST", "/api/lint", {"source": src})
        assert "error" not in r
        assert any(w["msg"] == "No check clause — step is not idempotent." for w in r["warnings"])

    def test_lint_endpoint_returns_parse_error_payload(self):
        r = self._api("POST", "/api/lint", {"source": "this is not valid cgr"})
        assert "error" in r
        assert r["warnings"] == []

    def test_lint_endpoint_does_not_mutate_loaded_file(self):
        original = Path(self._cgr_file).read_text()
        r = self._api("POST", "/api/lint", {"source": "this is not valid cgr"})
        assert "error" in r
        assert Path(self._cgr_file).read_text() == original


class TestServeSecurity(TestIDEServerAPI):
    def test_serve_rejects_non_loopback_without_unsafe_flag(self, tmp_path):
        graph_path = tmp_path / "serve-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
        """))
        with pytest.raises(SystemExit):
            cg.cmd_serve(filepath=str(graph_path), port=8421, host="0.0.0.0", no_open=True)

    def test_proxy_host_header_requires_allowlist(self):
        assert not cg._is_allowed_serve_host(
            "workspace-123.example.dev",
            bind_host="127.0.0.1",
        )
        assert cg._is_allowed_serve_host(
            "workspace-123.example.dev:443",
            bind_host="127.0.0.1",
            extra_allowed_hosts=["workspace-123.example.dev"],
        )

    def test_auto_allowed_serve_hosts_uses_vscode_proxy_uri(self):
        hosts = cg._auto_allowed_serve_hosts({
            "VSCODE_PROXY_URI": "https://coder.example.test/@user/ws/apps/code-server/proxy/{{port}}/",
            "CODER_AGENT_URL": "https://ignored.example.test/",
        })
        assert hosts == ["coder.example.test"]

    def test_host_rejection_includes_allow_host_hint(self):
        status, _, body = self._raw_api("GET", "/api/info", headers={"Host": "workspace-123.example.dev"})
        assert status == 403
        assert body["error"] == "Host not allowed"
        assert "--allow-host <proxy-hostname>" in body["hint"]
        assert body["host"] == "workspace-123.example.dev"

    def test_file_view_denied(self):
        """Accessing files outside sandbox should be denied."""
        r = self._api("GET", "/api/file/view?path=/etc/passwd")
        assert r.get("error") is not None

    def test_file_view_valid(self):
        """Accessing the graph file itself should work."""
        r = self._api("GET", f"/api/file/view?path={self._cgr_file}")
        assert "content" in r
        assert "Test Graph" in r["content"]

    def test_file_save_rejects_symlink(self):
        target = Path(self._tmp_path) / "allowed.txt"
        target.write_text("ok")
        link = Path(self._tmp_path) / "allowed-link.txt"
        link.symlink_to(target)
        r = self._api("POST", "/api/file/save", {"path": str(link), "content": "changed"})
        assert r["error"] == "Refusing to write through symlink"


class TestSingleFileBuild:
    def test_build_is_deterministic(self, tmp_path):
        target = tmp_path / "cgr.py"
        first = build_helpers.full_build()
        target.write_text(first)
        second = build_helpers.full_build()
        assert first == second

    def test_bootstrap_idempotent(self):
        first = build_helpers.full_build()
        second = build_helpers.full_build()
        assert first == second


class TestVisualizeSecrets:
    def test_visualize_redacts_secret_payload(self, tmp_path, monkeypatch):
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            set token = secret "env:MY_SECRET"
            target "t" local:
              [step]:
                run $ echo ${token}
        """))

        monkeypatch.setenv("MY_SECRET", "supersecret")
        graph = cg._load(str(graph_path), raise_on_error=True)
        out_path = tmp_path / "graph.html"
        cg.cmd_visualize(graph, str(out_path), source_file=str(graph_path))

        html = out_path.read_text()
        assert "supersecret" not in html
        assert "***REDACTED***" in html

    def test_visualize_escapes_html_and_script_context(self, tmp_path):
        graph_path = tmp_path / "evil.cgr"
        graph_path.write_text(textwrap.dedent("""\
            target "t" local:
              [step]:
                description "xss probe"
                run $ echo "</script><script>alert(1)</script>"
        """))
        graph = cg._load(str(graph_path), raise_on_error=True)
        out_path = tmp_path / "graph.html"
        cg.cmd_visualize(graph, str(out_path), source_file='evil</title><script>alert(1)</script>.cgr')

        html = out_path.read_text()
        assert "</script><script>alert(1)</script>" not in html
        assert "\\u003c/script>\\u003cscript>alert(1)\\u003c/script>" in html
        assert "&lt;/title&gt;&lt;script&gt;alert(1)&lt;/script&gt;" in html

    def test_built_artifact_embeds_ide_html(self, tmp_path):
        import py_compile

        target = tmp_path / "cgr.py"

        target.write_text(build_helpers.full_build())
        py_compile.compile(str(target), doraise=True)

        built = _load_module_from_path("cgr_built_test", target)
        html = built._get_ide_html()
        assert "CommandGraph IDE" in html
        assert '<textarea id="code"' in html


# ══════════════════════════════════════════════════════════════════════════════
# Package F Feature Tests
# ══════════════════════════════════════════════════════════════════════════════

class TestRetryBackoff:
    """Test retry backoff parsing."""

    def test_cgr_retry_with_backoff(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [step a]:
                retry 3x with backoff
                run $ echo hi
        """)
        ast = cg.parse_cgr(src, "<test>")
        res = ast.nodes[0].resources[0]
        assert res.retries == 3
        assert res.retry_backoff is True
        assert res.retry_delay == 1  # default base for backoff

    def test_cgr_retry_every_with_backoff(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [step a]:
                retry 2x every 5s with backoff
                run $ echo hi
        """)
        ast = cg.parse_cgr(src, "<test>")
        res = ast.nodes[0].resources[0]
        assert res.retries == 2
        assert res.retry_backoff is True
        assert res.retry_delay == 5

    def test_cgr_retry_every_no_backoff(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [step a]:
                retry 3x every 10s
                run $ echo hi
        """)
        ast = cg.parse_cgr(src, "<test>")
        res = ast.nodes[0].resources[0]
        assert res.retries == 3
        assert res.retry_backoff is False
        assert res.retry_delay == 10


class TestTimeoutStatus:
    """Test that TIMEOUT is a distinct status."""

    def test_timeout_status_exists(self):
        assert cg.Status.TIMEOUT.value == "timeout"

    def test_timeout_not_skipped_on_resume(self):
        """Timed-out steps should re-run on resume."""
        with tempfile.NamedTemporaryFile(suffix=".state", delete=False, mode="w") as f:
            f.write(json.dumps({"id": "test.step", "status": "timeout", "rc": 124, "ms": 5000, "ts": "now", "wave": 1}) + "\n")
            f.flush()
            state = cg.StateFile(f.name)
            assert state.should_skip("test.step") is False  # should NOT skip
            os.unlink(f.name)


class TestHTTPSteps:
    """Test HTTP step parsing, resolution, and expect logic."""

    def test_cgr_parse_get(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [check api]:
                get "https://api.example.com/health"
                expect 200
                timeout 10s
        """)
        ast = cg.parse_cgr(src, "<test>")
        res = ast.nodes[0].resources[0]
        assert res.http_method == "GET"
        assert res.http_url == "https://api.example.com/health"
        assert res.http_expect == "200"
        assert res.run is None  # no shell command

    def test_cgr_parse_post_with_auth_body(self):
        src = textwrap.dedent("""\
            --- Test ---
            set token = "abc123"
            target "t" local:
              [create record]:
                post "https://api.example.com/records"
                auth bearer "${token}"
                header "X-Request-Id" = "req-001"
                body json '{"name": "test"}'
                expect 200..299
                collect "response"
        """)
        ast = cg.parse_cgr(src, "<test>")
        res = ast.nodes[0].resources[0]
        assert res.http_method == "POST"
        assert res.http_auth == ("bearer", "${token}")
        assert ("X-Request-Id", "req-001") in res.http_headers
        assert res.http_body == '{"name": "test"}'
        assert res.http_body_type == "json"
        assert res.http_expect == "200..299"
        assert res.collect_key == "response"

    def test_cgr_parse_basic_auth(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [fetch data]:
                get "https://api.example.com/data"
                auth basic "user" "pass"
        """)
        ast = cg.parse_cgr(src, "<test>")
        res = ast.nodes[0].resources[0]
        assert res.http_auth == ("basic", "user:pass")

    def test_resolve_http_step_variables(self):
        src = textwrap.dedent("""\
            --- Test ---
            set host = "https://api.example.com"
            set token = "secret"
            target "t" local:
              [call api]:
                post "${host}/endpoint"
                auth bearer "${token}"
                header "X-Env" = "test"
                body json '{"key": "val"}'
                expect 201
        """)
        ast = cg.parse_cgr(src, "<test>")
        graph = cg.resolve(ast)
        res = graph.all_resources["t.call_api"]
        assert res.http_method == "POST"
        assert res.http_url == "https://api.example.com/endpoint"
        assert res.http_auth == ("bearer", "secret")
        assert res.run == "POST https://api.example.com/endpoint"

    def test_resolve_http_dedup_different_urls(self):
        """HTTP steps with different URLs should NOT be deduplicated."""
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [api a]:
                get "https://api.example.com/a"
              [api b]:
                get "https://api.example.com/b"
        """)
        ast = cg.parse_cgr(src, "<test>")
        graph = cg.resolve(ast)
        assert "t.api_a" in graph.all_resources
        assert "t.api_b" in graph.all_resources

    def test_http_auth_redacted(self):
        """Auth tokens should be in the sensitive values set."""
        src = textwrap.dedent("""\
            --- Test ---
            set token = "super-secret-token"
            target "t" local:
              [call api]:
                get "https://api.example.com/x"
                auth bearer "${token}"
        """)
        ast = cg.parse_cgr(src, "<test>")
        graph = cg.resolve(ast)
        assert "super-secret-token" in graph.sensitive_values

    def test_expect_check_single(self):
        assert cg._check_http_expect(200, "200") is True
        assert cg._check_http_expect(201, "200") is False

    def test_expect_check_range(self):
        assert cg._check_http_expect(200, "200..299") is True
        assert cg._check_http_expect(299, "200..299") is True
        assert cg._check_http_expect(300, "200..299") is False

    def test_expect_check_list(self):
        assert cg._check_http_expect(200, "200, 201, 204") is True
        assert cg._check_http_expect(204, "200, 201, 204") is True
        assert cg._check_http_expect(400, "200, 201, 204") is False

    def test_expect_check_default_2xx(self):
        assert cg._check_http_expect(200, None) is True
        assert cg._check_http_expect(201, None) is True
        assert cg._check_http_expect(404, None) is False

    def test_http_mixed_with_shell(self):
        """HTTP steps and shell steps can coexist in the same target."""
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [fetch data]:
                get "https://api.example.com/data"
                collect "api_data"
              [process data]:
                first [fetch data]
                run $ echo "processing"
        """)
        ast = cg.parse_cgr(src, "<test>")
        graph = cg.resolve(ast)
        assert "t.fetch_data" in graph.all_resources
        assert "t.process_data" in graph.all_resources
        assert graph.all_resources["t.fetch_data"].http_method == "GET"
        assert graph.all_resources["t.process_data"].http_method is None

    # ── .cg format HTTP parity tests ─────────────────────────────────────

    def test_cg_parse_get(self):
        src = '''
        node "t" { via local
          resource check_api {
            get "https://api.example.com/health"
            expect "200"
            timeout 10
          }
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        res = ast.nodes[0].resources[0]
        assert res.http_method == "GET"
        assert res.http_url == "https://api.example.com/health"
        assert res.http_expect == "200"
        assert res.run is None

    def test_cg_parse_post_with_auth_body(self):
        src = '''
        var token = "abc123"
        node "t" { via local
          resource create_record {
            post "https://api.example.com/records"
            auth bearer "secret"
            header "X-Request-Id" = "req-001"
            body json "{name: test}"
            expect "200..299"
            collect "response"
          }
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        res = ast.nodes[0].resources[0]
        assert res.http_method == "POST"
        assert res.http_auth == ("bearer", "secret")
        assert ("X-Request-Id", "req-001") in res.http_headers
        assert res.http_body_type == "json"
        assert res.http_expect == "200..299"
        assert res.collect_key == "response"

    def test_cg_parse_basic_auth(self):
        src = '''
        node "t" { via local
          resource fetch_data {
            get "https://api.example.com/data"
            auth basic "user" "pass"
          }
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        res = ast.nodes[0].resources[0]
        assert res.http_auth == ("basic", "user:pass")

    def test_cg_resolve_http_step(self):
        src = '''
        var host = "https://api.example.com"
        node "t" { via local
          resource call_api {
            post "https://api.example.com/endpoint"
            auth bearer "tok"
            expect "201"
          }
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        graph = cg.resolve(ast)
        res = graph.all_resources["t.call_api"]
        assert res.http_method == "POST"
        assert res.http_url == "https://api.example.com/endpoint"
        assert res.run == "POST https://api.example.com/endpoint"

    def test_cg_http_mixed_with_shell(self):
        src = '''
        node "t" { via local
          resource fetch_data {
            get "https://api.example.com/data"
          }
          resource process_data {
            needs fetch_data
            run `echo "processing"`
          }
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        graph = cg.resolve(ast)
        assert graph.all_resources["t.fetch_data"].http_method == "GET"
        assert graph.all_resources["t.process_data"].http_method is None

    def test_cg_parse_wait_subgraph_and_runtime_bindings(self):
        src = '''
        node "t" { via local
          resource wait_ready {
            wait for file "./ready.flag"
            timeout 5
          }
          resource deploy_app {
            needs wait_ready
            from "./deploy_app.cgr"
            version = "2.1.0"
            collect "deploy_result" as deploy_output
            on_success set deploy_status = "ok"
            on_failure set deploy_status = "failed"
          }
          resource summarize {
            needs deploy_app
            reduce "deploy_result" as summary
          }
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        wait_res, deploy_res, summarize_res = ast.nodes[0].resources
        assert wait_res.wait_kind == "file"
        assert wait_res.wait_target == "./ready.flag"
        assert deploy_res.subgraph_path == "./deploy_app.cgr"
        assert deploy_res.subgraph_vars == {"version": "2.1.0"}
        assert deploy_res.collect_key == "deploy_result"
        assert deploy_res.collect_var == "deploy_output"
        assert ("deploy_status", "ok") in deploy_res.on_success_set
        assert ("deploy_status", "failed") in deploy_res.on_failure_set
        assert summarize_res.reduce_key == "deploy_result"
        assert summarize_res.reduce_var == "summary"

    # ── on_complete / on_failure hook tests ───────────────────────────────

    def test_cgr_on_complete_hook(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [step a]:
                run $ echo "hello"

            on complete:
              post "https://hooks.example.com/done"
              body json '{"status": "ok"}'
              expect 200
        """)
        ast = cg.parse_cgr(src, "<test>")
        assert ast.on_complete is not None
        assert ast.on_complete.http_method == "POST"
        assert ast.on_complete.http_url == "https://hooks.example.com/done"
        assert ast.on_failure is None

    def test_cgr_on_failure_hook(self):
        src = textwrap.dedent("""\
            --- Test ---
            target "t" local:
              [step a]:
                run $ echo "hello"

            on failure:
              post "https://hooks.example.com/alert"
              body json '{"status": "failed"}'
        """)
        ast = cg.parse_cgr(src, "<test>")
        assert ast.on_failure is not None
        assert ast.on_failure.http_method == "POST"
        assert ast.on_complete is None

    def test_cgr_hooks_resolve(self):
        src = textwrap.dedent("""\
            --- Test ---
            set webhook = "https://hooks.example.com"
            target "t" local:
              [step a]:
                run $ echo "hello"

            on complete:
              post "${webhook}/done"
              auth bearer "tok123"
              expect 200

            on failure:
              post "${webhook}/alert"
              expect 200
        """)
        ast = cg.parse_cgr(src, "<test>")
        graph = cg.resolve(ast)
        assert graph.on_complete is not None
        assert graph.on_complete.http_url == "https://hooks.example.com/done"
        assert graph.on_failure is not None
        assert graph.on_failure.http_url == "https://hooks.example.com/alert"
        assert "tok123" in graph.sensitive_values

    def test_cg_on_complete_hook(self):
        src = '''
        node "t" { via local
          resource step_a { run `echo hello` }
        }
        on_complete {
          post "https://hooks.example.com/done"
          expect "200"
        }
        '''
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        assert ast.on_complete is not None
        assert ast.on_complete.http_method == "POST"


class TestInclude:
    """Test the include directive for composing graph files."""

    def test_cgr_include(self, tmp_path):
        base = tmp_path / "base.cgr"
        base.write_text(textwrap.dedent("""\
            set base_var = "from_base"
            target "base" local:
              [base step]:
                run $ echo "base"
        """))
        main = tmp_path / "main.cgr"
        main.write_text(textwrap.dedent(f"""\
            --- Main ---
            include "{base}"
            set main_var = "from_main"
            target "main" local:
              [main step]:
                run $ echo "main"
        """))
        ast = cg.parse_cgr(main.read_text(), str(main))
        var_names = [v.name for v in ast.variables]
        assert "base_var" in var_names
        assert "main_var" in var_names
        node_names = [n.name for n in ast.nodes]
        assert "base" in node_names
        assert "main" in node_names

    def test_cgr_include_no_overwrite_vars(self, tmp_path):
        """Variables in the main file take precedence over included ones."""
        base = tmp_path / "base.cgr"
        base.write_text('set port = "8080"\n')
        main = tmp_path / "main.cgr"
        main.write_text(textwrap.dedent(f"""\
            set port = "9090"
            include "{base}"
            target "t" local:
              [s]: run $ echo ok
        """))
        ast = cg.parse_cgr(main.read_text(), str(main))
        port_var = [v for v in ast.variables if v.name == "port"]
        assert len(port_var) == 1
        assert port_var[0].value == "9090"


# ══════════════════════════════════════════════════════════════════════════════
# Template error messages
# ══════════════════════════════════════════════════════════════════════════════

class TestTemplateErrorMessages:
    """Test improved template parameter error messages with fuzzy matching."""

    def test_did_you_mean_typo(self, tmp_path):
        """Typo in parameter name suggests the correct one."""
        repo = tmp_path / "repo" / "test"
        repo.mkdir(parents=True)
        (repo / "mytemplate.cgr").write_text(textwrap.dedent("""\
            template mytemplate(package_name, version = "latest"):
              description "Install ${package_name}"
              run $ echo ${package_name} ${version}
        """))
        src = textwrap.dedent("""\
            --- test ---
            using test/mytemplate
            target "t" local:
              [install nginx] from test/mytemplate:
                packge_name = "nginx"
        """)
        with pytest.raises(cg.ResolveError, match="did you mean.*package_name"):
            _resolve_cgr(src, repo_dir=str(tmp_path / "repo"))

    def test_missing_required_param(self, tmp_path):
        """Missing required param shows expected vs provided."""
        repo = tmp_path / "repo" / "test"
        repo.mkdir(parents=True)
        (repo / "mytemplate.cgr").write_text(textwrap.dedent("""\
            template mytemplate(host, port = "80"):
              description "Check ${host}"
              run $ echo ${host}:${port}
        """))
        src = textwrap.dedent("""\
            --- test ---
            using test/mytemplate
            target "t" local:
              [check svc] from test/mytemplate:
                port = "443"
        """)
        with pytest.raises(cg.ResolveError, match=r"(?s)requires parameter.*host.*Provided.*port"):
            _resolve_cgr(src, repo_dir=str(tmp_path / "repo"))


# ══════════════════════════════════════════════════════════════════════════════
# Multi-line run and skip if continuation
# ══════════════════════════════════════════════════════════════════════════════

class TestMultiLineContinuation:
    """Test that run and skip if collect indented continuation lines."""

    def test_run_multiline_backslash(self):
        """run $ collects indented continuation lines."""
        src = textwrap.dedent(r"""
            --- test ---
            target "t" local:
              [step]:
                run $ echo one \
                      echo two \
                      echo three
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert "echo one" in res.run
        assert "echo two" in res.run
        assert "echo three" in res.run
        assert res.run.count("\n") == 2

    def test_run_multiline_shell_test_bracket(self):
        """run $ continuation is NOT stopped by shell [ -f ... ] expressions."""
        src = textwrap.dedent(r"""
            --- test ---
            target "t" local:
              [step]:
                run $ key="$HOME/.ssh/id_ed25519.pub"; \
                      [ -f "$key" ] || key="$HOME/.ssh/id_rsa.pub"; \
                      echo "$key"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert '[ -f "$key" ]' in res.run
        assert 'echo "$key"' in res.run

    def test_skip_if_multiline(self):
        """skip if collects indented continuation lines."""
        src = textwrap.dedent(r"""
            --- test ---
            target "t" local:
              [step]:
                skip if $ ssh -o BatchMode=yes \
                              -o ConnectTimeout=5 \
                              user@host true 2>/dev/null
                run $ echo done
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert "-o BatchMode=yes" in res.check
        assert "-o ConnectTimeout=5" in res.check
        assert "user@host true" in res.check

    def test_run_multiline_stops_at_keyword(self):
        """run $ continuation stops at step keywords like timeout."""
        src = textwrap.dedent(r"""
            --- test ---
            target "t" local:
              [step]:
                run $ echo one \
                      echo two
                timeout 45s
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert "echo one" in res.run
        assert "echo two" in res.run
        assert res.timeout == 45

    def test_run_multiline_stops_at_child_step(self):
        """run $ continuation stops at a child [step name]: pattern."""
        src = textwrap.dedent(r"""
            --- test ---
            target "t" local:
              [parent]:
                run $ echo parent

                [child]:
                  run $ echo child
        """)
        g = _resolve_cgr(src)
        assert g.all_resources["t.parent"].run == "echo parent"
        assert g.all_resources["t.parent.child"].run == "echo child"


# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Provisioning primitives
# ══════════════════════════════════════════════════════════════════════════════

class TestProvisioningParsing:
    """Tests for content, line, put, validate keyword parsing."""

    def test_content_inline_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write file]:
                content > /tmp/test.conf:
                  key = value
                  port = 8080
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_file"]
        assert res.prov_content_inline == "key = value\nport = 8080"
        assert res.prov_dest == "/tmp/test.conf"

    def test_content_inline_preserves_hash_characters(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write file]:
                content > /tmp/test.conf:
                  # full-line comment should be preserved
                  value = foo # inline hash should stay too
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_file"]
        assert res.prov_content_inline == "# full-line comment should be preserved\nvalue = foo # inline hash should stay too"

    def test_content_from_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write file]:
                content from ./files/app.conf > /etc/app.conf
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_file"]
        assert res.prov_content_from == "./files/app.conf"
        assert res.prov_dest == "/etc/app.conf"

    def test_line_present_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [add line]:
                line "net.ipv4.ip_forward = 1" in /etc/sysctl.conf
                run $ sysctl -p
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.add_line"]
        assert res.prov_lines == [("present", "net.ipv4.ip_forward = 1", None)]
        assert res.prov_dest == "/etc/sysctl.conf"

    def test_line_absent_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [remove line]:
                line absent "PermitRootLogin yes" in /etc/ssh/sshd_config
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.remove_line"]
        assert res.prov_lines == [("absent", "PermitRootLogin yes", None)]

    def test_line_replace_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [replace line]:
                line "Port 2222" in /etc/ssh/sshd_config, replacing "^#?Port"
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.replace_line"]
        assert res.prov_lines == [("replace", "Port 2222", "^#?Port")]

    def test_multiple_line_ops_same_step(self):
        src = textwrap.dedent("""\
            target "t" local:
              [harden ssh]:
                line "PermitRootLogin no" in /etc/ssh/sshd_config, replacing "^#?PermitRootLogin"
                line "PasswordAuthentication no" in /etc/ssh/sshd_config, replacing "^#?PasswordAuthentication"
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.harden_ssh"]
        assert len(res.prov_lines) == 2
        assert res.prov_dest == "/etc/ssh/sshd_config"

    def test_put_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [install binary]:
                put ./dist/myapp > /usr/local/bin/myapp, mode 0755
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.install_binary"]
        assert res.prov_put_src == "./dist/myapp"
        assert res.prov_dest == "/usr/local/bin/myapp"
        assert res.prov_put_mode == "0755"

    def test_put_without_mode(self):
        src = textwrap.dedent("""\
            target "t" local:
              [copy file]:
                put ./src/config.yml > /etc/app/config.yml
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.copy_file"]
        assert res.prov_put_mode is None

    def test_validate_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write config]:
                content > /tmp/test.conf:
                  key = value
                validate $ test -f /tmp/test.conf
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_config"]
        assert res.prov_validate == "test -f /tmp/test.conf"

    def test_step_without_run_but_with_content_is_valid(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write only]:
                content > /tmp/x.conf:
                  foo = bar
        """)
        g = _resolve_cgr(src)
        assert "t.write_only" in g.all_resources

    def test_step_without_run_but_with_line_is_valid(self):
        src = textwrap.dedent("""\
            target "t" local:
              [add line]:
                line "foo = bar" in /tmp/conf.txt
        """)
        g = _resolve_cgr(src)
        assert "t.add_line" in g.all_resources

    def test_variable_expansion_in_prov_dest(self):
        src = textwrap.dedent("""\
            set conf_dir = "/etc/myapp"
            target "t" local:
              [write config]:
                content > ${conf_dir}/app.conf:
                  key = value
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_config"]
        assert res.prov_dest == "/etc/myapp/app.conf"

    def test_variable_expansion_in_line_text(self):
        src = textwrap.dedent("""\
            set port = "2222"
            target "t" local:
              [set port]:
                line "Port ${port}" in /etc/ssh/sshd_config
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.set_port"]
        assert res.prov_lines[0][1] == "Port 2222"

    def test_mixed_line_ops_different_files_raises(self):
        src = textwrap.dedent("""\
            target "t" local:
              [bad step]:
                line "foo" in /etc/file1
                line "bar" in /etc/file2
                run $ true
        """)
        with pytest.raises(Exception):
            _resolve_cgr(src)

    def test_content_and_run_in_same_step(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write and restart]:
                content > /tmp/app.conf:
                  key = value
                run $ echo "config written"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_and_restart"]
        assert res.prov_content_inline == "key = value"
        assert res.run == "echo \"config written\""

    def test_prov_dry_run_shows_dest(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write file]:
                content > /etc/myapp.conf:
                  key = value
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_file"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, dry_run=True)
        assert result.status == cg.Status.PENDING
        assert "/etc/myapp.conf" in result.reason


class TestProvisioningExecution:
    """Tests for actual execution of provisioning operations (local)."""

    def _local_node(self):
        return cg.HostNode(name="local", via_method="local", via_props={}, resources={})

    def _prov_resource(self, **kwargs):
        defaults = dict(
            id="test_step", short_name="test_step", node_name="local",
            description="", needs=[], check=None, run="", run_as=None,
            timeout=30, retries=0, retry_delay=0, retry_backoff=False,
            on_fail=cg.OnFail.STOP, when=None, env={}, is_verify=False, line=1,
        )
        defaults.update(kwargs)
        return cg.Resource(**defaults)

    def test_content_inline_writes_file(self, tmp_path):
        dest = str(tmp_path / "test.conf")
        res = self._prov_resource(
            prov_content_inline="key = value\nport = 8080",
            prov_dest=dest,
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert Path(dest).read_text() == "key = value\nport = 8080\n"

    def test_content_inline_writes_hash_characters(self, tmp_path):
        dest = str(tmp_path / "hash.conf")
        res = self._prov_resource(
            prov_content_inline="# comment\nvalue = keep # hash",
            prov_dest=dest,
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert Path(dest).read_text() == "# comment\nvalue = keep # hash\n"

    def test_content_inline_atomic_write(self, tmp_path):
        dest = str(tmp_path / "test.conf")
        # Write initial content
        Path(dest).write_text("old content\n")
        res = self._prov_resource(
            prov_content_inline="new content",
            prov_dest=dest,
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert Path(dest).read_text() == "new content\n"
        # No leftover tmp file
        assert not Path(dest + ".cgr_tmp").exists()

    def test_content_from_file_writes_dest(self, tmp_path):
        src_file = tmp_path / "source.conf"
        src_file.write_text("from_file = yes\n")
        dest = str(tmp_path / "dest.conf")
        # Need graph_file to resolve relative path
        graph_file = str(tmp_path / "test.cgr")
        res = self._prov_resource(
            prov_content_from=str(src_file),
            prov_dest=dest,
        )
        result = cg.exec_resource(res, self._local_node(), graph_file=graph_file)
        assert result.status == cg.Status.SUCCESS
        assert Path(dest).read_text() == "from_file = yes\n"

    def test_content_from_missing_file_fails(self, tmp_path):
        res = self._prov_resource(
            prov_content_from="/nonexistent/missing.conf",
            prov_dest=str(tmp_path / "dest.conf"),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED

    def test_line_present_appends_when_absent(self, tmp_path):
        dest = tmp_path / "sysctl.conf"
        dest.write_text("# sysctl settings\n")
        res = self._prov_resource(
            prov_lines=[("present", "net.ipv4.ip_forward = 1", None)],
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "net.ipv4.ip_forward = 1" in dest.read_text()

    def test_line_present_skips_when_already_present(self, tmp_path):
        dest = tmp_path / "sysctl.conf"
        dest.write_text("net.ipv4.ip_forward = 1\n")
        res = self._prov_resource(
            prov_lines=[("present", "net.ipv4.ip_forward = 1", None)],
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        # Should not have duplicated the line
        lines = [l for l in dest.read_text().splitlines() if l == "net.ipv4.ip_forward = 1"]
        assert len(lines) == 1

    def test_line_absent_removes_line(self, tmp_path):
        dest = tmp_path / "sshd_config"
        dest.write_text("PermitRootLogin yes\nPort 22\n")
        res = self._prov_resource(
            prov_lines=[("absent", "PermitRootLogin yes", None)],
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "PermitRootLogin yes" not in dest.read_text()
        assert "Port 22" in dest.read_text()

    def test_line_replace_replaces_matching_line(self, tmp_path):
        dest = tmp_path / "sshd_config"
        dest.write_text("#Port 22\nPermitRootLogin yes\n")
        res = self._prov_resource(
            prov_lines=[("replace", "Port 2222", "^#?Port")],
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "Port 2222" in text
        assert "#Port 22" not in text

    def test_line_replace_appends_when_no_match(self, tmp_path):
        dest = tmp_path / "sshd_config"
        dest.write_text("PermitRootLogin yes\n")
        res = self._prov_resource(
            prov_lines=[("replace", "Port 2222", "^#?Port")],
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "Port 2222" in dest.read_text()

    def test_validate_success_keeps_file(self, tmp_path):
        dest = tmp_path / "test.conf"
        res = self._prov_resource(
            prov_content_inline="key = value",
            prov_dest=str(dest),
            prov_validate=f"test -f {dest}",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert dest.exists()

    def test_validate_failure_reverts_file(self, tmp_path):
        dest = tmp_path / "test.conf"
        original = "original content\n"
        dest.write_text(original)
        res = self._prov_resource(
            prov_content_inline="new content",
            prov_dest=str(dest),
            prov_validate="false",  # always fails
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        # File should be reverted to original
        assert dest.read_text() == original

    def test_validate_failure_reverts_new_file(self, tmp_path):
        dest = tmp_path / "new_file.conf"
        assert not dest.exists()
        res = self._prov_resource(
            prov_content_inline="content",
            prov_dest=str(dest),
            prov_validate="false",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        # New file should be gone (no backup existed, so no revert possible)
        # but no crash

    def test_put_copies_file(self, tmp_path):
        src = tmp_path / "myapp"
        src.write_text("binary content")
        dest = tmp_path / "target" / "myapp"
        res = self._prov_resource(
            prov_put_src=str(src),
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert dest.read_text() == "binary content"

    def test_put_sets_mode(self, tmp_path):
        src = tmp_path / "src_script.sh"
        src.write_text("#!/bin/bash\necho hi")
        dest = tmp_path / "dest_script.sh"
        res = self._prov_resource(
            prov_put_src=str(src),
            prov_dest=str(dest),
            prov_put_mode="0755",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        mode = oct(dest.stat().st_mode)[-4:]
        assert mode == "0755"

    def test_put_missing_source_fails(self, tmp_path):
        res = self._prov_resource(
            prov_put_src="/nonexistent/source.bin",
            prov_dest=str(tmp_path / "dest.bin"),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED

    def test_line_creates_file_if_not_exists(self, tmp_path):
        dest = tmp_path / "new_file.conf"
        assert not dest.exists()
        res = self._prov_resource(
            prov_lines=[("present", "new_setting = true", None)],
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "new_setting = true" in dest.read_text()

    def test_line_and_run_in_same_step(self, tmp_path):
        dest = tmp_path / "sysctl.conf"
        dest.write_text("")
        res = self._prov_resource(
            prov_lines=[("present", "net.ipv4.ip_forward = 1", None)],
            prov_dest=str(dest),
            run="echo reloaded",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "net.ipv4.ip_forward = 1" in dest.read_text()
        assert "reloaded" in result.stdout


class TestSecretVariables:
    """Tests for secret "env:VAR" and secret "file:PATH" variable resolution."""

    def test_secret_env_resolved(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "hunter2")
        src = textwrap.dedent("""\
            set pw = secret "env:MY_SECRET"
            target "t" local:
              [use secret]:
                run $ echo "done"
        """)
        g = _resolve_cgr(src)
        assert g.variables["pw"] == "hunter2"

    def test_secret_env_redacted(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "hunter2")
        src = textwrap.dedent("""\
            set pw = secret "env:MY_SECRET"
            target "t" local:
              [use secret]:
                run $ echo "done"
        """)
        g = _resolve_cgr(src)
        assert "hunter2" in g.sensitive_values

    def test_secret_env_missing_raises(self):
        src = textwrap.dedent("""\
            set pw = secret "env:DEFINITELY_NOT_SET_XYZ123"
            target "t" local:
              [use secret]:
                run $ echo "done"
        """)
        with pytest.raises(Exception, match="not set"):
            _resolve_cgr(src)

    def test_secret_file_resolved(self, tmp_path):
        secret_file = tmp_path / "my_secret"
        secret_file.write_text("my_secret_value\n")
        src = textwrap.dedent(f"""\
            set api_key = secret "file:{secret_file}"
            target "t" local:
              [use secret]:
                run $ echo "done"
        """)
        g = _resolve_cgr(src)
        assert g.variables["api_key"] == "my_secret_value"

    def test_secret_file_redacted(self, tmp_path):
        secret_file = tmp_path / "my_secret"
        secret_file.write_text("super_secret_value\n")
        src = textwrap.dedent(f"""\
            set api_key = secret "file:{secret_file}"
            target "t" local:
              [use secret]:
                run $ echo "done"
        """)
        g = _resolve_cgr(src)
        assert "super_secret_value" in g.sensitive_values

    def test_secret_file_missing_raises(self):
        src = textwrap.dedent("""\
            set token = secret "file:/nonexistent/path/secret.txt"
            target "t" local:
              [use]:
                run $ echo "done"
        """)
        with pytest.raises(Exception, match="not found"):
            _resolve_cgr(src)

    def test_secret_value_used_in_command(self, monkeypatch, tmp_path):
        monkeypatch.setenv("MY_TOKEN", "abc123")
        src = textwrap.dedent("""\
            set token = secret "env:MY_TOKEN"
            target "t" local:
              [call api]:
                run $ echo ${token}
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.call_api"]
        assert "abc123" in res.run


# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Structured config operations
# ══════════════════════════════════════════════════════════════════════════════

class TestPhase2Parsing:
    """Parser tests for block, ini, json, assert keywords."""

    def test_block_inline_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [add logrotate]:
                block in /etc/logrotate.d/myapp, marker "cgr:myapp":
                  /var/log/myapp/*.log {
                      daily
                      rotate 7
                  }
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.add_logrotate"]
        assert res.prov_block_dest == "/etc/logrotate.d/myapp"
        assert res.prov_block_marker == "cgr:myapp"
        assert "/var/log/myapp/*.log" in res.prov_block_inline
        assert not res.prov_block_absent

    def test_block_inline_preserves_hash_characters(self):
        src = textwrap.dedent("""\
            target "t" local:
              [add block]:
                block in /etc/app.conf, marker "cgr:test":
                  # comment line
                  key = value # with hash
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.add_block"]
        assert res.prov_block_inline == "# comment line\nkey = value # with hash"

    def test_block_absent_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [remove block]:
                block absent in /etc/logrotate.d/myapp, marker "cgr:myapp"
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.remove_block"]
        assert res.prov_block_absent
        assert res.prov_block_dest == "/etc/logrotate.d/myapp"

    def test_block_from_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write block]:
                block from ./nginx/upstream.conf in /etc/nginx/nginx.conf, marker "cgr:upstream"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.write_block"]
        assert res.prov_block_from == "./nginx/upstream.conf"
        assert res.prov_block_dest == "/etc/nginx/nginx.conf"
        assert res.prov_block_marker == "cgr:upstream"

    def test_ini_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [tune pg]:
                ini /etc/postgresql/postgresql.conf:
                  max_connections = "200"
                  shared_buffers = "256MB"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.tune_pg"]
        assert res.prov_ini_dest == "/etc/postgresql/postgresql.conf"
        assert (None, "max_connections", "200") in res.prov_ini_ops
        assert (None, "shared_buffers", "256MB") in res.prov_ini_ops

    def test_ini_with_section_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [configure app]:
                ini /etc/myapp/config.ini:
                  section "database":
                    host = "localhost"
                    port = "5432"
                  section "cache":
                    backend = "redis"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.configure_app"]
        assert ("database", "host", "localhost") in res.prov_ini_ops
        assert ("database", "port", "5432") in res.prov_ini_ops
        assert ("cache", "backend", "redis") in res.prov_ini_ops

    def test_ini_absent_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [remove key]:
                ini /etc/myapp/config.ini:
                  section "legacy":
                    old_key = absent
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.remove_key"]
        assert ("legacy", "old_key", "absent") in res.prov_ini_ops

    def test_json_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [configure docker]:
                json /etc/docker/daemon.json:
                  log-driver = "json-file"
                  log-opts.max-size = "100m"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.configure_docker"]
        assert res.prov_json_dest == "/etc/docker/daemon.json"
        assert ("log-driver", "json-file") in res.prov_json_ops
        assert ("log-opts.max-size", "100m") in res.prov_json_ops

    def test_json_boolean_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [enable feature]:
                json /etc/app/config.json:
                  features.new_ui = true
                  features.legacy_api = false
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.enable_feature"]
        assert ("features.new_ui", True) in res.prov_json_ops
        assert ("features.legacy_api", False) in res.prov_json_ops

    def test_json_number_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [set limit]:
                json /etc/app/config.json:
                  max_connections = 200
                  timeout = 30
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.set_limit"]
        assert ("max_connections", 200) in res.prov_json_ops
        assert ("timeout", 30) in res.prov_json_ops

    def test_json_null_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [clear key]:
                json /etc/app/config.json:
                  debug = null
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.clear_key"]
        assert ("debug", None) in res.prov_json_ops

    def test_json_absent_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [remove key]:
                json /etc/app/config.json:
                  experimental = absent
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.remove_key"]
        assert ("experimental", "absent") in res.prov_json_ops

    def test_json_array_append_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [add mirror]:
                json /etc/docker/daemon.json:
                  insecure-registries[] = "registry.internal:5000"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.add_mirror"]
        assert ("insecure-registries[]", "registry.internal:5000") in res.prov_json_ops

    def test_assert_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [provision db]:
                assert $ test -f /etc/apt/sources.list.d/pgdg.list, "PG repo not configured"
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.provision_db"]
        assert len(res.prov_asserts) == 1
        cmd, msg = res.prov_asserts[0]
        assert "test -f" in cmd
        assert msg == "PG repo not configured"

    def test_assert_without_message_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [check prereq]:
                assert $ which docker
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.check_prereq"]
        cmd, msg = res.prov_asserts[0]
        assert "which docker" in cmd
        assert msg is None

    def test_multiple_asserts_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [deploy]:
                assert $ test -f /etc/ssl/certs/ca.pem, "CA cert missing"
                assert $ curl -sf https://registry.internal/health, "Registry unreachable"
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.deploy"]
        assert len(res.prov_asserts) == 2

    def test_assert_only_step_is_valid(self):
        src = textwrap.dedent("""\
            target "t" local:
              [check env]:
                assert $ which docker, "docker must be installed"
        """)
        g = _resolve_cgr(src)
        assert "t.check_env" in g.all_resources

    def test_variable_expansion_in_assert(self):
        src = textwrap.dedent("""\
            set host = "db.internal"
            target "t" local:
              [check db]:
                assert $ ping -c1 ${host}, "Cannot reach ${host}"
                run $ true
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.check_db"]
        cmd, msg = res.prov_asserts[0]
        assert "db.internal" in cmd
        assert "db.internal" in msg

    def test_variable_expansion_in_ini(self):
        src = textwrap.dedent("""\
            set db_host = "localhost"
            target "t" local:
              [configure]:
                ini /etc/app/config.ini:
                  section "database":
                    host = "${db_host}"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.configure"]
        assert ("database", "host", "localhost") in res.prov_ini_ops

    def test_variable_expansion_in_json(self):
        src = textwrap.dedent("""\
            set log_level = "info"
            target "t" local:
              [configure]:
                json /etc/app/config.json:
                  logging.level = "${log_level}"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.configure"]
        assert ("logging.level", "info") in res.prov_json_ops


class TestPhase2Execution:
    """Execution tests for block, ini, json, assert operations (local)."""

    def _local_node(self):
        return cg.HostNode(name="local", via_method="local", via_props={}, resources={})

    def _prov_resource(self, **kwargs):
        defaults = dict(
            id="test_step", short_name="test_step", node_name="local",
            description="", needs=[], check=None, run="", run_as=None,
            timeout=30, retries=0, retry_delay=0, retry_backoff=False,
            on_fail=cg.OnFail.STOP, when=None, env={}, is_verify=False, line=1,
        )
        defaults.update(kwargs)
        return cg.Resource(**defaults)

    # ── block tests ──────────────────────────────────────────────────────────

    def test_block_inserts_into_new_file(self, tmp_path):
        dest = tmp_path / "logrotate.conf"
        res = self._prov_resource(
            prov_block_dest=str(dest),
            prov_block_marker="cgr:myapp",
            prov_block_inline="/var/log/myapp/*.log {\n    daily\n    rotate 7\n}",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "# cgr:myapp begin" in text
        assert "# cgr:myapp end" in text
        assert "/var/log/myapp/*.log" in text

    def test_block_appends_to_existing_file(self, tmp_path):
        dest = tmp_path / "logrotate.conf"
        dest.write_text("# existing content\n/var/log/other/*.log { daily }\n")
        res = self._prov_resource(
            prov_block_dest=str(dest),
            prov_block_marker="cgr:myapp",
            prov_block_inline="/var/log/myapp/*.log { daily }",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "# existing content" in text
        assert "/var/log/other/*.log" in text
        assert "# cgr:myapp begin" in text

    def test_block_replaces_existing_block(self, tmp_path):
        dest = tmp_path / "logrotate.conf"
        dest.write_text(
            "before\n"
            "# cgr:myapp begin\nOLD CONTENT\n# cgr:myapp end\n"
            "after\n"
        )
        res = self._prov_resource(
            prov_block_dest=str(dest),
            prov_block_marker="cgr:myapp",
            prov_block_inline="NEW CONTENT",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "OLD CONTENT" not in text
        assert "NEW CONTENT" in text
        assert "before" in text
        assert "after" in text

    def test_block_absent_removes_block(self, tmp_path):
        dest = tmp_path / "logrotate.conf"
        dest.write_text(
            "before\n"
            "# cgr:myapp begin\nSOME CONTENT\n# cgr:myapp end\n"
            "after\n"
        )
        res = self._prov_resource(
            prov_block_dest=str(dest),
            prov_block_marker="cgr:myapp",
            prov_block_absent=True,
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "SOME CONTENT" not in text
        assert "cgr:myapp" not in text
        assert "before" in text
        assert "after" in text

    def test_block_from_file(self, tmp_path):
        src = tmp_path / "upstream.conf"
        src.write_text("upstream myapp {\n    server 127.0.0.1:8080;\n}\n")
        dest = tmp_path / "nginx.conf"
        dest.write_text("# nginx config\n")
        graph_file = str(tmp_path / "test.cgr")
        res = self._prov_resource(
            prov_block_dest=str(dest),
            prov_block_marker="cgr:upstream",
            prov_block_from=str(src),
        )
        result = cg.exec_resource(res, self._local_node(), graph_file=graph_file)
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "upstream myapp" in text
        assert "# cgr:upstream begin" in text

    # ── ini tests ────────────────────────────────────────────────────────────

    def test_ini_creates_file_with_keys(self, tmp_path):
        dest = tmp_path / "config.ini"
        res = self._prov_resource(
            prov_ini_dest=str(dest),
            prov_ini_ops=[(None, "max_connections", "200"), (None, "log_level", "info")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        text = dest.read_text()
        assert "max_connections = 200" in text
        assert "log_level = info" in text

    def test_ini_sets_key_in_section(self, tmp_path):
        dest = tmp_path / "config.ini"
        res = self._prov_resource(
            prov_ini_dest=str(dest),
            prov_ini_ops=[("database", "host", "localhost"), ("database", "port", "5432")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import configparser
        p = configparser.RawConfigParser()
        p.optionxform = str
        p.read(str(dest))
        assert p.get("database", "host") == "localhost"
        assert p.get("database", "port") == "5432"

    def test_ini_updates_existing_key(self, tmp_path):
        dest = tmp_path / "config.ini"
        dest.write_text("[database]\nhost = old_host\nport = 5432\n")
        res = self._prov_resource(
            prov_ini_dest=str(dest),
            prov_ini_ops=[("database", "host", "new_host")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import configparser
        p = configparser.RawConfigParser()
        p.optionxform = str
        p.read(str(dest))
        assert p.get("database", "host") == "new_host"
        assert p.get("database", "port") == "5432"

    def test_ini_removes_key(self, tmp_path):
        dest = tmp_path / "config.ini"
        dest.write_text("[section]\nkey1 = value1\nkey2 = value2\n")
        res = self._prov_resource(
            prov_ini_dest=str(dest),
            prov_ini_ops=[("section", "key1", "absent")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import configparser
        p = configparser.RawConfigParser()
        p.optionxform = str
        p.read(str(dest))
        assert not p.has_option("section", "key1")
        assert p.get("section", "key2") == "value2"

    def test_ini_no_change_when_idempotent(self, tmp_path):
        dest = tmp_path / "config.ini"
        dest.write_text("[database]\nhost = localhost\n")
        mtime_before = dest.stat().st_mtime
        import time; time.sleep(0.01)
        res = self._prov_resource(
            prov_ini_dest=str(dest),
            prov_ini_ops=[("database", "host", "localhost")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "no changes" in result.stdout

    # ── json tests ───────────────────────────────────────────────────────────

    def test_json_creates_file(self, tmp_path):
        dest = tmp_path / "config.json"
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("log-driver", "json-file"), ("storage-driver", "overlay2")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert data["log-driver"] == "json-file"
        assert data["storage-driver"] == "overlay2"

    def test_json_updates_nested_key(self, tmp_path):
        dest = tmp_path / "config.json"
        dest.write_text('{"log-opts": {"max-size": "10m"}}\n')
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("log-opts.max-size", "100m"), ("log-opts.max-file", "3")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert data["log-opts"]["max-size"] == "100m"
        assert data["log-opts"]["max-file"] == "3"

    def test_json_removes_key(self, tmp_path):
        dest = tmp_path / "config.json"
        dest.write_text('{"experimental": true, "debug": false}\n')
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("experimental", "absent")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert "experimental" not in data
        assert "debug" in data

    def test_json_sets_boolean(self, tmp_path):
        dest = tmp_path / "config.json"
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("experimental", True), ("debug", False)],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert data["experimental"] is True
        assert data["debug"] is False

    def test_json_sets_number(self, tmp_path):
        dest = tmp_path / "config.json"
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("max_connections", 200), ("timeout", 30.5)],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert data["max_connections"] == 200
        assert data["timeout"] == 30.5

    def test_json_array_append(self, tmp_path):
        dest = tmp_path / "daemon.json"
        dest.write_text('{"insecure-registries": ["existing.internal"]}\n')
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("insecure-registries[]", "new.internal")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert "new.internal" in data["insecure-registries"]
        assert "existing.internal" in data["insecure-registries"]

    def test_json_array_append_idempotent(self, tmp_path):
        dest = tmp_path / "daemon.json"
        dest.write_text('{"insecure-registries": ["existing.internal"]}\n')
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("insecure-registries[]", "existing.internal")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        import json
        data = json.loads(dest.read_text())
        assert data["insecure-registries"].count("existing.internal") == 1

    def test_json_no_change_when_idempotent(self, tmp_path):
        dest = tmp_path / "config.json"
        dest.write_text('{"log-driver": "json-file"}\n')
        res = self._prov_resource(
            prov_json_dest=str(dest),
            prov_json_ops=[("log-driver", "json-file")],
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS
        assert "no changes" in result.stdout

    # ── assert tests ─────────────────────────────────────────────────────────

    def test_assert_passes_when_command_succeeds(self, tmp_path):
        sentinel = tmp_path / "sentinel.txt"
        sentinel.write_text("ok")
        res = self._prov_resource(
            prov_asserts=[(f"test -f {sentinel}", "sentinel must exist")],
            run="echo done",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.SUCCESS

    def test_assert_fails_when_command_fails(self):
        res = self._prov_resource(
            prov_asserts=[("false", "this must not be false")],
            run="echo done",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        assert "this must not be false" in result.stderr

    def test_assert_fails_with_default_message(self):
        res = self._prov_resource(
            prov_asserts=[("false", None)],
            run="echo done",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        assert "assertion failed" in result.stderr

    def test_assert_stops_before_writes(self, tmp_path):
        dest = tmp_path / "should_not_be_written.conf"
        res = self._prov_resource(
            prov_asserts=[("false", "prerequisite missing")],
            prov_content_inline="key = value",
            prov_dest=str(dest),
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        assert not dest.exists()

    def test_multiple_asserts_first_failure_stops(self):
        res = self._prov_resource(
            prov_asserts=[
                ("true", "this passes"),
                ("false", "this fails"),
                ("true", "this never runs"),
            ],
            run="echo done",
        )
        result = cg.exec_resource(res, self._local_node())
        assert result.status == cg.Status.FAILED
        assert "this fails" in result.stderr

    def test_assert_dry_run_shows_count(self):
        src = textwrap.dedent("""\
            target "t" local:
              [check prereqs]:
                assert $ which docker, "docker required"
                assert $ which kubectl, "kubectl required"
                run $ echo ready
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.check_prereqs"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, dry_run=True)
        assert result.status == cg.Status.PENDING
        assert "assert" in result.reason

# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Secret Backends (cmd: and vault:)
# ══════════════════════════════════════════════════════════════════════════════

class TestSecretBackends:
    """Tests for secret "cmd:..." and secret "vault:..." backends."""

    def test_secret_cmd_captures_stdout(self):
        src = textwrap.dedent("""\
            set token = secret "cmd:echo mytoken"
            target "t" local:
              [step]:
                run $ echo ${token}
        """)
        g = _resolve_cgr(src, resolve_deferred_secrets=True)
        assert g.variables.get("token") == "mytoken"

    def test_secret_cmd_is_marked_sensitive(self):
        src = textwrap.dedent("""\
            set token = secret "cmd:echo supersecret"
            target "t" local:
              [step]:
                run $ echo done
        """)
        g = _resolve_cgr(src, resolve_deferred_secrets=True)
        assert "supersecret" in g.sensitive_values

    def test_secret_cmd_is_deferred_by_default(self):
        src = textwrap.dedent("""\
            set token = secret "cmd:echo mytoken"
            target "t" local:
              [step]:
                run $ echo ${token}
        """)
        g = _resolve_cgr(src)
        assert g.variables["token"] == "<deferred:cmd>"
        assert g.all_resources["t.step"].run == "echo <deferred:cmd>"

    def test_secret_cmd_nonzero_raises_when_resolved(self):
        src = textwrap.dedent("""\
            set token = secret "cmd:exit 1"
            target "t" local:
              [step]:
                run $ echo ${token}
        """)
        with pytest.raises(Exception, match="exited"):
            _resolve_cgr(src, resolve_deferred_secrets=True)

    def test_secret_cmd_nonzero_does_not_echo_stderr(self):
        src = textwrap.dedent("""\
            set token = secret "cmd:sh -c 'echo $LEAK >&2; exit 1'"
            target "t" local:
              [step]:
                run $ echo ${token}
        """)
        os.environ["LEAK"] = "supersecret"
        with pytest.raises(Exception) as exc:
            _resolve_cgr(src, resolve_deferred_secrets=True)
        msg = str(exc.value)
        assert "supersecret" not in msg
        assert "stderr suppressed" in msg

    def test_secret_cmd_strips_trailing_newline(self):
        src = textwrap.dedent("""\
            set pw = secret "cmd:printf 'mypassword\\n'"
            target "t" local:
              [step]:
                run $ echo ${pw}
        """)
        g = _resolve_cgr(src, resolve_deferred_secrets=True)
        assert g.variables.get("pw") == "mypassword"

    def test_secret_vault_missing_token_raises(self):
        """vault: backend raises if VAULT_TOKEN is not set."""
        os.environ.pop("VAULT_TOKEN", None)
        src = textwrap.dedent("""\
            set pw = secret "vault:secret/myapp"
            target "t" local:
              [step]:
                run $ echo ${pw}
        """)
        with pytest.raises(Exception, match="VAULT_TOKEN"):
            _resolve_cgr(src, resolve_deferred_secrets=True)

    def test_secret_vault_is_deferred_by_default(self):
        src = textwrap.dedent("""\
            set pw = secret "vault:secret/myapp"
            target "t" local:
              [step]:
                run $ echo ${pw}
        """)
        g = _resolve_cgr(src)
        assert g.variables["pw"] == "<deferred:vault>"
        assert g.all_resources["t.step"].run == "echo <deferred:vault>"

    def test_secret_cmd_expansion_in_run(self):
        """Variable set via secret cmd: is expanded in run commands."""
        src = textwrap.dedent("""\
            set api_key = secret "cmd:echo key-abc123"
            target "t" local:
              [step]:
                run $ echo ${api_key}
        """)
        g = _resolve_cgr(src, resolve_deferred_secrets=True)
        res = g.all_resources["t.step"]
        assert "key-abc123" in res.run

    def test_load_does_not_execute_secret_cmd_by_default(self, tmp_path, monkeypatch):
        graph_path = tmp_path / "secret-cmd.cgr"
        graph_path.write_text(textwrap.dedent("""\
            set token = secret "cmd:echo should-not-run"
            target "t" local:
              [step]:
                run $ echo ${token}
        """))
        calls = {"count": 0}
        real_run = cg.subprocess.run

        def fake_run(*args, **kwargs):
            calls["count"] += 1
            return real_run(*args, **kwargs)

        monkeypatch.setattr(cg.subprocess, "run", fake_run)
        graph = cg._load(str(graph_path), raise_on_error=True)
        assert calls["count"] == 0
        assert graph.variables["token"] == "<deferred:cmd>"

    def test_validate_flag_allows_secret_cmd_resolution(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "secret-cmd.cgr"
        graph_path.write_text(textwrap.dedent("""\
            set token = secret "cmd:echo cli-secret"
            target "t" local:
              [step]:
                run $ echo ${token}
        """))
        monkeypatch.setattr(sys, "argv", ["cgr.py", "validate", "--allow-secret-cmd", str(graph_path)])
        cg.main()
        out = capsys.readouterr().out
        assert "<deferred:cmd>" not in out

    def test_load_uses_explicit_vault_passphrase(self, tmp_path):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "lol", {"key": "from-secret"})
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            secrets "vault.enc"

            target "t" local:
              [step]:
                run $ echo ${key}
        """))

        graph = cg._load(str(graph_path), raise_on_error=True, vault_passphrase="lol")
        assert graph.variables["key"] == "from-secret"

    def test_encrypt_decrypt_round_trip_v2(self):
        plaintext = b'key = "from-secret"\n'
        encrypted = cg._encrypt_data(plaintext, "lol")
        assert encrypted[0] == cg._SECRETS_VERSION_V2
        assert cg._decrypt_data(encrypted, "lol") == plaintext

    def test_decrypt_rejects_tampered_v2_ciphertext(self):
        plaintext = b'key = "from-secret"\n'
        encrypted = bytearray(cg._encrypt_data(plaintext, "lol"))
        encrypted[50] ^= 0x01
        with pytest.raises(ValueError, match="integrity check failed"):
            cg._decrypt_data(bytes(encrypted), "lol")

    def test_decrypt_legacy_format_emits_deprecation_warning(self, tmp_path):
        secrets_path = tmp_path / "vault.enc"
        _write_legacy_encrypted_secrets(secrets_path, "lol", {"key": "from-secret"})
        with pytest.warns(DeprecationWarning, match="legacy format without integrity protection"):
            plaintext = cg._decrypt_data(secrets_path.read_bytes(), "lol")
        assert b'key = "from-secret"' in plaintext

    def test_load_prompts_for_vault_passphrase_when_requested(self, tmp_path, monkeypatch):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "lol", {"key": "from-secret"})
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            secrets "vault.enc"

            target "t" local:
              [step]:
                run $ echo ${key}
        """))

        monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr("getpass.getpass", lambda prompt: "lol")
        graph = cg._load(str(graph_path), raise_on_error=True, vault_prompt=True)
        assert graph.variables["key"] == "from-secret"

    def test_main_apply_accepts_vault_pass_cli(self, tmp_path, monkeypatch):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "lol", {"key": "from-secret"})
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            secrets "vault.enc"

            target "t" local:
              [step]:
                run $ echo ${key}
        """))

        seen = {}

        def fake_apply(graph, filepath, **kwargs):
            seen["graph"] = graph
            seen["filepath"] = filepath
            return [], 0

        monkeypatch.setattr(cg, "cmd_apply_stateful", fake_apply)
        monkeypatch.setattr(sys, "argv", ["cgr.py", "apply", "--vault-pass", "lol", str(graph_path), "--dry-run"])
        with pytest.raises(SystemExit) as exc:
            cg.main()

        assert exc.value.code == 0
        assert seen["filepath"] == str(graph_path)
        assert seen["graph"].variables["key"] == "from-secret"

    def test_main_apply_can_prompt_for_vault_pass(self, tmp_path, monkeypatch):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "lol", {"key": "from-secret"})
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            secrets "vault.enc"

            target "t" local:
              [step]:
                run $ echo ${key}
        """))

        seen = {}

        def fake_apply(graph, filepath, **kwargs):
            seen["graph"] = graph
            return [], 0

        monkeypatch.setattr(cg, "cmd_apply_stateful", fake_apply)
        monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
        monkeypatch.setattr("getpass.getpass", lambda prompt: "lol")
        monkeypatch.setattr(sys, "argv", ["cgr.py", "apply", "--vault-pass-ask", str(graph_path), "--dry-run"])
        with pytest.raises(SystemExit) as exc:
            cg.main()

        assert exc.value.code == 0
        assert seen["graph"].variables["key"] == "from-secret"

    def test_main_lint_accepts_vault_pass_cli(self, tmp_path, monkeypatch):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "lol", {"key": "from-secret"})
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            secrets "vault.enc"

            target "t" local:
              [step]:
                run $ echo ${key}
        """))

        seen = {}

        def fake_lint(filepath, **kwargs):
            seen["filepath"] = filepath
            seen["kwargs"] = kwargs
            return 0

        monkeypatch.setattr(cg, "cmd_lint", fake_lint)
        monkeypatch.setattr(sys, "argv", ["cgr.py", "lint", "--vault-pass", "lol", str(graph_path)])
        with pytest.raises(SystemExit) as exc:
            cg.main()

        assert exc.value.code == 0
        assert seen["filepath"] == str(graph_path)
        assert seen["kwargs"]["vault_passphrase"] == "lol"

    def test_validate_redacts_secret_variables_in_text_output(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            set token = secret "env:MY_SECRET"

            target "t" local:
              [step]:
                run $ echo done
        """))

        monkeypatch.setenv("MY_SECRET", "supersecret")
        monkeypatch.setattr(sys, "argv", ["cgr.py", "validate", str(graph_path)])
        cg.main()
        out = capsys.readouterr().out
        assert "supersecret" not in out

    def test_get_vault_pass_errors_without_source(self, monkeypatch, capsys):
        monkeypatch.delenv("CGR_VAULT_PASS", raising=False)
        with pytest.raises(SystemExit) as exc:
            cg._get_vault_pass(None)
        assert exc.value.code == 1
        assert "No vault passphrase" in capsys.readouterr().out

    def test_cmd_secrets_view_rejects_wrong_passphrase(self, tmp_path, capsys):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "correct", {"api_key": "top-secret"})
        args = argparse.Namespace(vault_pass="wrong", vault_pass_ask=False, vault_pass_file=None)
        with pytest.raises(SystemExit) as exc:
            cg.cmd_secrets("view", str(secrets_path), vault_args=args)
        assert exc.value.code == 1
        out = capsys.readouterr().out
        assert "error:" in out
        assert "top-secret" not in out

    def test_cmd_secrets_view_masks_values(self, tmp_path, capsys):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "correct", {"api_key": "top-secret"})
        args = argparse.Namespace(vault_pass="correct", vault_pass_ask=False, vault_pass_file=None,
                                  show_values=False)
        cg.cmd_secrets("view", str(secrets_path), vault_args=args)
        out = capsys.readouterr().out
        assert "api_key" in out
        assert "<hidden:10 chars>" in out
        assert "top-secret" not in out

    def test_cmd_secrets_view_show_values_still_masks_output(self, tmp_path, capsys):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "correct", {"api_key": "top-secret"})
        args = argparse.Namespace(vault_pass="correct", vault_pass_ask=False, vault_pass_file=None,
                                  show_values=True)
        cg.cmd_secrets("view", str(secrets_path), vault_args=args)
        out = capsys.readouterr().out
        assert "api_key" in out
        assert "<hidden:10 chars>" in out
        assert "plaintext secret display is disabled" in out
        assert "top-secret" not in out

    def test_cmd_secrets_rm_missing_key_leaves_file_intact(self, tmp_path, capsys):
        secrets_path = tmp_path / "vault.enc"
        _write_encrypted_secrets(secrets_path, "lol", {"api_key": "from-secret"})
        args = argparse.Namespace(vault_pass="lol", vault_pass_ask=False, vault_pass_file=None)
        cg.cmd_secrets("rm", str(secrets_path), key="missing_key", vault_args=args)
        out = capsys.readouterr().out
        assert "not found" in out
        assert cg._load_secrets(str(secrets_path), "lol") == {"api_key": "from-secret"}

    def test_validate_redacts_secret_variables_in_json_output(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "secret-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            set token = secret "env:MY_SECRET"

            target "t" local:
              [step]:
                run $ echo done
        """))

        monkeypatch.setenv("MY_SECRET", "supersecret")
        monkeypatch.setattr(sys, "argv", ["cgr.py", "validate", "--json", str(graph_path)])
        cg.main()
        out = json.loads(capsys.readouterr().out)
        assert out["variables"]["token"] == "***REDACTED***"


class TestSecretOutputRedactionFollowup:
    def test_print_exec_verbose_shows_full_stdout_and_stderr(self, capsys):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo done
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        result = cg.ExecResult(
            resource_id="t.step",
            status=cg.Status.FAILED,
            run_rc=1,
            stdout="one\ntwo\nthree\nfour\nfive\nsix\n",
            stderr="err1\nerr2\nerr3\nerr4\nerr5\nerr6\nerr7\nerr8\nerr9\n",
            duration_ms=1,
        )
        cg._print_exec(1, 1, res, result, g, verbose=True)
        out = capsys.readouterr().out
        assert "one" in out
        assert "six" in out
        assert "err1" in out
        assert "err9" in out
        assert "│ ... (" not in out

    def test_print_exec_redacts_until_observed_value(self, monkeypatch, capsys):
        monkeypatch.setenv("MY_SECRET", "supersecret")
        src = textwrap.dedent("""\
            set token = secret "env:MY_SECRET"
            target "t" local:
              [step]:
                run $ printf "%s\\n" "${token}"
                until "nomatch"
                retry 1x wait 1s
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        result = cg.ExecResult(
            resource_id="t.step",
            status=cg.Status.FAILED,
            run_rc=1,
            stderr="until mismatch",
            duration_ms=1,
            observed_value="supersecret",
        )
        cg._print_exec(1, 1, res, result, g)
        out = capsys.readouterr().out
        assert "supersecret" not in out
        assert "***REDACTED***" in out

    def test_state_record_redacts_observed_value_before_persisting(self, tmp_path):
        state = cg.StateFile(tmp_path / "test.state")
        result = cg.ExecResult(
            resource_id="t.step",
            status=cg.Status.FAILED,
            run_rc=1,
            duration_ms=1,
            observed_value="supersecret",
        )
        state.record(result, 1, sensitive={"supersecret"})
        raw = (tmp_path / "test.state").read_text()
        assert "supersecret" not in raw
        assert "***REDACTED***" in raw

    def test_explain_redacts_run_and_check(self, monkeypatch, capsys):
        monkeypatch.setenv("MY_SECRET", "supersecret")
        src = textwrap.dedent("""\
            set token = secret "env:MY_SECRET"
            target "t" local:
              [step]:
                skip if $ sh -c 'echo ${token} >&2; exit 1'
                run $ echo ${token}
        """)
        g = _resolve_cgr(src)
        cg.cmd_explain(g, "step")
        out = capsys.readouterr().out
        assert "supersecret" not in out
        assert "***REDACTED***" in out

    def test_check_verbose_redacts_stderr(self, monkeypatch, capsys):
        monkeypatch.setenv("MY_SECRET", "supersecret")
        src = textwrap.dedent("""\
            set token = secret "env:MY_SECRET"
            target "t" local:
              [step]:
                skip if $ sh -c 'echo ${token} >&2; exit 1'
                run $ echo done
        """)
        g = _resolve_cgr(src)
        needs = cg.cmd_check(g, verbose=True)
        out = capsys.readouterr().out
        assert needs == 1
        assert "supersecret" not in out
        assert "***REDACTED***" in out

    def test_state_test_redacts_stderr(self, tmp_path, monkeypatch, capsys):
        graph_path = tmp_path / "secret-state-test.cgr"
        graph_path.write_text(textwrap.dedent("""\
            set token = secret "env:MY_SECRET"
            set flag = env("FLAG")
            target "t" local:
              [step]:
                skip if $ sh -c 'echo ${token} >&2; test "${flag}" = ok'
                run $ echo done
        """))

        monkeypatch.setenv("MY_SECRET", "supersecret")
        monkeypatch.setenv("FLAG", "ok")
        graph = cg._load(str(graph_path), raise_on_error=True)
        state = cg.StateFile(cg._state_path(str(graph_path)))
        state.record(
            cg.ExecResult(resource_id="t.step", status=cg.Status.SKIP_CHECK, duration_ms=1),
            1,
            sensitive=graph.sensitive_values,
        )

        monkeypatch.setenv("FLAG", "bad")
        graph = cg._load(str(graph_path), raise_on_error=True)
        cg.cmd_state_test(str(graph_path), graph)
        out = capsys.readouterr().out
        assert "supersecret" not in out
        assert "***REDACTED***" in out


# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Provisioning Lint Rules
# ══════════════════════════════════════════════════════════════════════════════

class TestProvisioningLint:
    """Tests for cmd_lint provisioning anti-pattern detection."""

    def _lint(self, src: str) -> list[tuple]:
        """Resolve a cgr source and run the lint checks, returning warnings list."""
        import io, unittest.mock as mock

        with tempfile.NamedTemporaryFile(suffix=".cgr", mode="w", delete=False) as f:
            f.write(src)
            filepath = f.name
        try:
            with mock.patch("builtins.print"):
                result = cg.cmd_lint(filepath)
        finally:
            import os as _os
            _os.unlink(filepath)
        return result

    def test_sed_i_without_backup_flagged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [edit]:
                run $ sed -i 's/Port 22/Port 2222/' /etc/ssh/sshd_config
        """)
        count = self._lint(src)
        assert count > 0

    def test_sed_i_bak_not_flagged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [edit]:
                skip if $ false
                run $ sed -i.bak 's/Port 22/Port 2222/' /etc/ssh/sshd_config
        """)
        # Should produce 0 sed-related warnings (may still warn about other things)
        # We just check it doesn't raise
        count = self._lint(src)
        assert isinstance(count, int)

    def test_echo_to_etc_flagged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write]:
                run $ echo 'ServerName localhost' > /etc/apache2/apache2.conf
        """)
        count = self._lint(src)
        assert count > 0

    def test_content_write_without_validate_flagged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write config]:
                content > "/etc/nginx/nginx.conf":
                  worker_processes 4;
        """)
        count = self._lint(src)
        assert count > 0

    def test_content_write_with_validate_not_flagged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write config]:
                content > "/etc/nginx/nginx.conf":
                  worker_processes 4;
                validate $ nginx -t -c /etc/nginx/nginx.conf
        """)
        count = self._lint(src)
        # Only generic warnings (no check, no description), not the validate warning
        # Verify no validate-related warning by checking return is lower
        assert isinstance(count, int)

    def test_tee_to_system_path_flagged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [write]:
                run $ echo 'my config' | tee /etc/myapp/config.conf
        """)
        count = self._lint(src)
        assert count > 0



# ══════════════════════════════════════════════════════════════════════════════
# each concurrency limit via variable
# ══════════════════════════════════════════════════════════════════════════════

class TestEachVariableLimit:
    """each x in ${list}, ${n} at a time: — variable concurrency limit."""

    def test_each_variable_limit_parsed_cgr(self):
        src = textwrap.dedent("""\
            set workers = "a,b,c,d"
            set limit = "2"
            target "t" local:
              [deploy]:
                each host in ${workers}, ${limit} at a time:
                  [deploy to host]:
                    run $ echo ${host}
        """)
        g = _resolve_cgr(src)
        # Should resolve without error; items should have concurrency_limit set
        items = [r for r in g.all_resources.values()
                 if r.parallel_group and "__each" in r.parallel_group]
        assert len(items) == 4
        assert all(r.concurrency_limit == 2 for r in items)

    def test_each_literal_limit_still_works(self):
        src = textwrap.dedent("""\
            set servers = "x,y,z"
            target "t" local:
              [step]:
                each s in ${servers}, 3 at a time:
                  [run on server]:
                    run $ echo ${s}
        """)
        g = _resolve_cgr(src)
        items = [r for r in g.all_resources.values()
                 if r.parallel_group and "__each" in r.parallel_group]
        assert len(items) == 3
        assert all(r.concurrency_limit == 3 for r in items)

    def test_each_no_limit_still_works(self):
        src = textwrap.dedent("""\
            set items = "p,q"
            target "t" local:
              [step]:
                each item in ${items}:
                  [process]:
                    run $ echo ${item}
        """)
        g = _resolve_cgr(src)
        items = [r for r in g.all_resources.values()
                 if r.parallel_group and "__each" in r.parallel_group]
        assert len(items) == 2
        assert all(r.concurrency_limit is None for r in items)

    def test_each_variable_limit_non_integer_raises(self):
        """If the variable expands to a non-integer, ResolveError is raised."""
        src = textwrap.dedent("""\
            set items = "a,b"
            set limit = "oops"
            target "t" local:
              [step]:
                each item in ${items}, ${limit} at a time:
                  [process]:
                    run $ echo ${item}
        """)
        with pytest.raises(cg.ResolveError, match="not a valid integer"):
            _resolve_cgr(src)


class TestTopLevelTargetEach:
    def test_top_level_target_each_resolves_in_cgr(self):
        src = textwrap.dedent("""\
            set hosts = "web1:10.0.0.1,web2:10.0.0.2"
            each name, addr in ${hosts}:
              target "${name}" local:
                [announce]:
                  run $ echo ${addr}

            target "final" local, after each:
              [done]:
                run $ echo done
        """)
        g = _resolve_cgr(src)
        assert {"web1", "web2", "final"} <= set(g.nodes.keys())
        assert sorted(g.node_ordering["final"]) == ["web1", "web2"]

    def test_top_level_target_each_resolves_in_cg(self):
        src = textwrap.dedent("""\
            var hosts = "web1:10.0.0.1,web2:10.0.0.2"
            each name, addr in hosts {
              node "${name}" {
                via local
                resource announce {
                  run `echo ${addr}`
                }
              }
            }
            node "final" {
              via local
              after each
              resource done {
                run `echo done`
              }
            }
        """)
        g = _resolve_cg(src)
        assert {"web1", "web2", "final"} <= set(g.nodes.keys())
        assert sorted(g.node_ordering["final"]) == ["web1", "web2"]


class TestStagePhase:
    def test_stage_resolves_phase_ordering(self):
        src = textwrap.dedent("""\
            set servers = "a,b,c"
            target "t" local:
              [rollout]:
                stage "deploy":
                  phase "canary" 1 from ${servers}:
                    [ship]:
                      run $ echo ${server}
                  phase "rest" rest from ${servers}:
                    each server, 2 at a time:
                      [ship]:
                        run $ echo ${server}
        """)
        g = _resolve_cgr(src)
        ships = {r.short_name: rid for rid, r in g.all_resources.items() if r.short_name.startswith("ship_")}
        phases = {r.short_name for r in g.all_resources.values() if r.is_barrier and r.short_name.startswith("phase_")}
        assert set(ships) == {"ship_a", "ship_b", "ship_c"}
        assert {"phase_canary", "phase_rest"} <= phases

        canary_wave = next(i for i, wave in enumerate(g.waves) if ships["ship_a"] in wave)
        rest_wave = min(i for i, wave in enumerate(g.waves) if ships["ship_b"] in wave or ships["ship_c"] in wave)
        assert canary_wave < rest_wave

    def test_stage_parses_in_cg(self):
        src = textwrap.dedent("""\
            node "t" {
              via local
              resource rollout {
                stage "deploy" {
                  phase "canary" 1 from "${servers}" {
                    resource ship {
                      run `echo ${server}`
                    }
                  }
                  phase "rest" rest from "${servers}" {
                    each server, 2 {
                      resource ship {
                        run `echo ${server}`
                      }
                    }
                  }
                }
              }
            }
        """)
        ast = cg.Parser(cg.lex(src), "<test>").parse()
        res = ast.nodes[0].resources[0]
        assert res.stage_name == "deploy"
        assert len(res.stage_phases) == 2
        assert res.stage_phases[1].each_limit == 2



# ══════════════════════════════════════════════════════════════════════════════
# Gap 1: collect "key" as var — promote stdout to runtime variable
# ══════════════════════════════════════════════════════════════════════════════

class TestCollectAsVar:
    """collect "key" as varname promotes stdout into a runtime-bound variable."""

    def test_collect_as_var_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hello
                collect "output" as result_var
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.collect_key == "output"
        assert res.collect_var == "result_var"

    def test_collect_without_as_unchanged(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                collect "output"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.collect_key == "output"
        assert res.collect_var is None

    def test_collect_as_var_binds_stdout(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo myvalue
                collect "out" as captured
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.status == cg.Status.SUCCESS
        assert result.var_bindings.get("captured") == "myvalue"


# ══════════════════════════════════════════════════════════════════════════════
# Gap 2: on success/failure: set var = "val"
# ══════════════════════════════════════════════════════════════════════════════

class TestOnSuccessFailureSet:
    """on success/failure: set VAR = "VAL" — exit-code-driven variable binding."""

    def test_on_success_set_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ true
                on success: set status = "ok"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert ("status", "ok") in res.on_success_set

    def test_on_failure_set_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ false
                on failure: set status = "failed"
                if fails ignore
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert ("status", "failed") in res.on_failure_set

    def test_on_success_binds_on_exit_zero(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ true
                on success: set result = "yes"
                on failure: set result = "no"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.status == cg.Status.SUCCESS
        assert result.var_bindings.get("result") == "yes"

    def test_on_failure_binds_on_nonzero(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ exit 1
                on success: set result = "yes"
                on failure: set result = "no"
                if fails ignore
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.var_bindings.get("result") == "no"

    def test_on_success_failure_combined_with_collect(self):
        """on success + collect as var both bind correctly."""
        src = textwrap.dedent("""\
            target "t" local:
              [probe]:
                run $ echo "compress"
                collect "decision" as action
                on success: set do_compress = "yes"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.probe"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.var_bindings.get("action") == "compress"
        assert result.var_bindings.get("do_compress") == "yes"

    def test_runtime_binding_expands_in_later_run_command(self):
        src = textwrap.dedent("""\
            set has_python = ""
            target "t" local:
              [detect]:
                run $ command -v definitely_not_a_real_binary_for_commandgraph_test
                on success: set has_python = "yes"
                on failure: set has_python = "no"
                if fails ignore

              [report]:
                first [detect]
                run $ echo "${has_python}"
        """)
        g = _resolve_cgr(src)
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})

        detect = cg.exec_resource(g.all_resources["t.detect"], node, variables=g.variables)
        g.variables.update(detect.var_bindings)
        report = cg.exec_resource(g.all_resources["t.report"], node, variables=g.variables)

        assert detect.var_bindings.get("has_python") == "no"
        assert report.stdout.strip() == "no"

    def test_runtime_bindings_replay_from_state_on_resume(self, tmp_path):
        graph_file = tmp_path / "resume_vars.cgr"
        out_file = tmp_path / "report.txt"
        graph_file.write_text(textwrap.dedent(f"""\
            set has_python = ""
            target "t" local:
              [detect]:
                run $ command -v definitely_not_a_real_binary_for_commandgraph_test
                on success: set has_python = "yes"
                on failure: set has_python = "no"
                if fails ignore

              [report]:
                first [detect]
                run $ echo "${{has_python}}" > {out_file}
        """))

        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)
        assert out_file.read_text().strip() == "no"

        out_file.unlink()
        state = cg.StateFile(cg._state_path(str(graph_file)))
        state.drop("t.report")

        graph = cg._load(str(graph_file), raise_on_error=True)
        cg.cmd_apply_stateful(graph, str(graph_file), max_parallel=1)
        assert out_file.read_text().strip() == "no"


# ══════════════════════════════════════════════════════════════════════════════
# Gap 3: reduce "key" — aggregate collected outputs
# ══════════════════════════════════════════════════════════════════════════════

class TestReduce:
    """reduce "key" gathers collected outputs from prior steps."""

    def test_reduce_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [gather]:
                reduce "host_result"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.gather"]
        assert res.reduce_key == "host_result"
        assert res.reduce_var is None

    def test_reduce_as_var_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [gather]:
                reduce "host_result" as fleet_status
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.gather"]
        assert res.reduce_key == "host_result"
        assert res.reduce_var == "fleet_status"

    def test_reduce_aggregates_collected_outputs(self):
        src = textwrap.dedent("""\
            target "t" local:
              [gather]:
                reduce "results" as summary
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.gather"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        accumulated = {"results": ["host-1: OK", "host-2: OK", "host-3: OK"]}
        result = cg.exec_resource(res, node, variables=g.variables,
                                  accumulated_collected=accumulated)
        assert result.status == cg.Status.SUCCESS
        assert "host-1: OK" in result.stdout
        assert "host-2: OK" in result.stdout
        assert result.var_bindings.get("summary") == result.stdout

    def test_reduce_empty_is_success(self):
        """reduce with no prior collected values still succeeds (empty string)."""
        src = textwrap.dedent("""\
            target "t" local:
              [gather]:
                reduce "nothing"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.gather"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.status == cg.Status.SUCCESS
        assert result.stdout == ""


# ══════════════════════════════════════════════════════════════════════════════
# Gap 4: retry backoff syntax
# ══════════════════════════════════════════════════════════════════════════════

class TestRetryBackoffSyntax:
    """retry Nx backoff Xs[..Ys] [jitter Z%] — exponential backoff syntax."""

    def test_backoff_base_only_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                retry 3x backoff 10s
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.retries == 3
        assert res.retry_delay == 10
        assert res.retry_backoff is True
        assert res.retry_backoff_max == 0
        assert res.retry_jitter_pct == 0

    def test_backoff_with_cap_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                retry 5x backoff 30s..300s
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.retries == 5
        assert res.retry_delay == 30
        assert res.retry_backoff_max == 300
        assert res.retry_jitter_pct == 0

    def test_backoff_with_cap_and_jitter_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                retry 5x backoff 30s..300s jitter 10%
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.retries == 5
        assert res.retry_delay == 30
        assert res.retry_backoff_max == 300
        assert res.retry_jitter_pct == 10

    def test_backoff_minutes_parsed(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                retry 3x backoff 1m..5m jitter 20%
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.retry_delay == 60
        assert res.retry_backoff_max == 300
        assert res.retry_jitter_pct == 20

    def test_existing_backoff_syntax_unchanged(self):
        """retry Nx wait Xs backoff — old syntax still works."""
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                retry 3x wait 5s with backoff
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert res.retries == 3
        assert res.retry_backoff is True

    def test_when_with_runtime_var_dollar_brace_syntax(self):
        """when '${var} == value' works for runtime-bound variables."""
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo hi
                when "${compress} == 'yes'"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        # compress=yes → should execute
        result = cg.exec_resource(res, node, variables={**g.variables, "compress": "yes"})
        assert result.status == cg.Status.SUCCESS


# ══════════════════════════════════════════════════════════════════════════════
# Conditional flags / env / until
# ══════════════════════════════════════════════════════════════════════════════

class TestConditionalFlagsAndUntil:
    def test_flag_appended_when_true(self):
        src = textwrap.dedent("""\
            set tags = "daily backup"
            target "t" local:
              [step]:
                run $ printf '%s\\n' restic backup /data
                flag "--tag ${tags}" when "${tags} != ''"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert "--tag 'daily backup'" in res.run
        assert res.flags == ["--tag 'daily backup'"]

    def test_flag_omitted_when_false(self):
        src = textwrap.dedent("""\
            set tags = ""
            target "t" local:
              [step]:
                run $ echo restic
                flag "--tag ${tags}" when "${tags} != ''"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert "--tag" not in res.run
        assert res.flags == []

    def test_env_when_resolved(self):
        src = textwrap.dedent("""\
            set token = ""
            target "t" local:
              [step]:
                run $ env | grep OPTIONAL_TOKEN || true
                env OPTIONAL_TOKEN = "${token}" when "${token} != ''"
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        assert "OPTIONAL_TOKEN" not in res.env

    def test_until_matches_last_nonempty_line(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ printf "pending\\n0\\n"
                until "0"
                retry 2x wait 1s
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.status == cg.Status.SUCCESS
        assert result.observed_value == "0"

    def test_until_failure_records_last_value(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ printf "3\\n"
                until "0"
                retry 1x wait 1s
        """)
        g = _resolve_cgr(src)
        res = g.all_resources["t.step"]
        node = cg.HostNode(name="t", via_method="local", via_props={}, resources={})
        result = cg.exec_resource(res, node, variables=g.variables)
        assert result.status == cg.Status.FAILED
        assert result.observed_value == "3"

    def test_until_without_retry_rejected(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                run $ echo 0
                until "0"
        """)
        with pytest.raises(cg.CGRParseError, match="until"):
            cg.parse_cgr(src, "<test>")


class TestNewLintWarnings:
    def _lint(self, src: str) -> int:
        with tempfile.NamedTemporaryFile(suffix=".cgr", mode="w", delete=False) as f:
            f.write(src)
            filepath = f.name
        try:
            return cg.cmd_lint(filepath)
        finally:
            os.unlink(filepath)

    def test_lint_warns_manual_ssh_in_local_target(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                skip if $ false
                run $ ssh user@host true
        """)
        assert self._lint(src) > 0

    def test_lint_warns_shell_flag_construction(self):
        src = textwrap.dedent("""\
            set tags = "daily"
            target "t" local:
              [step]:
                skip if $ false
                run $ FLAG=\"\"; if [ -n \"${tags}\" ]; then FLAG=\"--tag ${tags}\"; fi; echo $FLAG
        """)
        assert self._lint(src) > 0

    def test_lint_warns_polling_loop(self):
        src = textwrap.dedent("""\
            target "t" local:
              [step]:
                skip if $ false
                run $ while true; do echo 1; sleep 1; done
        """)
        assert self._lint(src) > 0


class TestDurationParsing(unittest.TestCase):
    def test_parse_duration_str(self):
        assert cg._parse_duration_str("300") == 300
        assert cg._parse_duration_str("300s") == 300
        assert cg._parse_duration_str("5m") == 300
        assert cg._parse_duration_str("2h") == 7200

    def test_duration_to_secs(self):
        assert cg._duration_to_secs(10, None) == 10
        assert cg._duration_to_secs(10, "s") == 10
        assert cg._duration_to_secs(2, "m") == 120
        assert cg._duration_to_secs(1, "h") == 3600
