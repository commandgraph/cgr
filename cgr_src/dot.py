"""Graphviz DOT rendering."""
from __future__ import annotations
from cgr_src.common import *
from cgr_src.resolver import *

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

__all__ = [
    name for name in globals()
    if name not in {"__all__", "__annotations__", "__builtins__", "__cached__", "__doc__", "__file__", "__loader__", "__name__", "__package__", "__spec__"}
]
