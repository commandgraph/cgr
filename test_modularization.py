from __future__ import annotations

import dataclasses
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

import cgr as built
import cgr_src as dev


def _ast_dict(ast_obj):
    return dataclasses.asdict(ast_obj)


def _graph_summary(graph):
    id_map = {
        rid: f"{res.node_name}.{res.short_name}" for rid, res in graph.all_resources.items()
    }

    def _norm_id(value):
        return id_map.get(value, value)

    return {
        "variables": dict(sorted(graph.variables.items())),
        "nodes": sorted(graph.nodes.keys()),
        "resources": sorted(
            (
                {
                    "id": id_map[rid],
                    "node": res.node_name,
                    "short_name": res.short_name,
                    "needs": sorted(_norm_id(dep) for dep in res.needs),
                    "check": res.check,
                    "run": res.run,
                    "is_barrier": res.is_barrier,
                    "barrier_kind": getattr(res, "barrier_kind", None),
                    "tags": list(res.tags),
                }
                for rid, res in sorted(graph.all_resources.items())
            ),
            key=lambda item: item["id"],
        ),
        "waves": [[_norm_id(rid) for rid in wave] for wave in graph.waves],
        "dedup_groups": sorted(
            sorted(_norm_id(rid) for rid in rids)
            for rids in graph.dedup_map.values()
        ),
        "node_ordering": {k: list(v) for k, v in sorted(graph.node_ordering.items())},
    }


def _cross_format_summary(graph):
    return {
        "variables": dict(sorted(graph.variables.items())),
        "nodes": sorted(graph.nodes.keys()),
        "resource_count": len(graph.all_resources),
        "wave_count": len(graph.waves),
        "resources": sorted(
            (
                res.node_name,
                res.run,
                res.check,
                res.is_barrier,
                tuple(sorted(res.tags)),
            )
            for res in graph.all_resources.values()
        ),
    }


def test_dev_entrypoint_reports_version():
    proc = subprocess.run(
        [sys.executable, "cgr_dev.py", "version"],
        cwd=Path(__file__).resolve().parent,
        capture_output=True,
        text=True,
        check=True,
    )
    assert built.__version__ in proc.stdout


def test_built_artifact_has_no_source_imports():
    content = Path("cgr.py").read_text()
    assert not any(
        line.startswith("from cgr_src") or line.startswith("import cgr_src")
        for line in content.splitlines()
    )


def test_parser_pairs_match_in_built_and_dev_modules():
    repo_root = Path(__file__).resolve().parent
    cg_source = (repo_root / "parallel_test.cg").read_text()
    cgr_source = (repo_root / "parallel_test.cgr").read_text()

    built_cg_ast = built.Parser(
        built.lex(cg_source, "parallel_test.cg"), cg_source, "parallel_test.cg"
    ).parse()
    built_cgr_ast = built.parse_cgr(cgr_source, "parallel_test.cgr")
    dev_cg_ast = dev.Parser(
        dev.lex(cg_source, "parallel_test.cg"), cg_source, "parallel_test.cg"
    ).parse()
    dev_cgr_ast = dev.parse_cgr(cgr_source, "parallel_test.cgr")

    assert _ast_dict(built_cg_ast) == _ast_dict(dev_cg_ast)
    assert _ast_dict(built_cgr_ast) == _ast_dict(dev_cgr_ast)

    built_cg_graph = built.resolve(built_cg_ast)
    built_cgr_graph = built.resolve(built_cgr_ast)
    dev_cg_graph = dev.resolve(dev_cg_ast)
    dev_cgr_graph = dev.resolve(dev_cgr_ast)

    assert _graph_summary(built_cg_graph) == _graph_summary(dev_cg_graph)
    assert _graph_summary(built_cgr_graph) == _graph_summary(dev_cgr_graph)
    assert _cross_format_summary(built_cg_graph) == _cross_format_summary(built_cgr_graph)
    assert _cross_format_summary(dev_cg_graph) == _cross_format_summary(dev_cgr_graph)


def test_resolve_and_state_round_trip_match_between_built_and_dev():
    source = textwrap.dedent(
        """\
        set app = "demo"
        target "local" local:
          [prepare]:
            run $ echo prep
          [deploy]:
            first [prepare]
            run $ echo ${app}
        """
    )

    built_graph = built.resolve(built.parse_cgr(source, "inline.cgr"))
    dev_graph = dev.resolve(dev.parse_cgr(source, "inline.cgr"))
    assert _graph_summary(built_graph) == _graph_summary(dev_graph)

    with tempfile.TemporaryDirectory() as tmpdir:
        state_path = Path(tmpdir) / "inline.state"
        sf_built = built.StateFile(state_path)
        sf_built.set_manual("local.prepare", "success")
        sf_built.record_wave(1, 42, to_run=2, skipped=1)
        sf_built.record_run(
            99,
            total_results=2,
            bottleneck_id="local.deploy",
            bottleneck_ms=55,
        )
        baseline = state_path.read_text()

        sf_dev = dev.StateFile(state_path)
        assert {
            k: dataclasses.asdict(v) for k, v in sf_dev.all_entries().items()
        } == {k: dataclasses.asdict(v) for k, v in sf_built.all_entries().items()}
        assert [dataclasses.asdict(v) for v in sf_dev.wave_metrics()] == [
            dataclasses.asdict(v) for v in sf_built.wave_metrics()
        ]
        assert dataclasses.asdict(sf_dev.run_metric()) == dataclasses.asdict(
            sf_built.run_metric()
        )

        sf_dev.compact()
        compacted = state_path.read_text()
        sf_built_reloaded = built.StateFile(state_path)
        sf_dev_reloaded = dev.StateFile(state_path)

        assert compacted
        assert baseline
        assert {
            k: dataclasses.asdict(v)
            for k, v in sf_built_reloaded.all_entries().items()
        } == {
            k: dataclasses.asdict(v) for k, v in sf_dev_reloaded.all_entries().items()
        }
