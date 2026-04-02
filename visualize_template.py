#!/usr/bin/env python3
"""
visualize.py — Generates a self-contained interactive HTML visualization
of a CommandGraph dependency graph.

This gets integrated into cgr.py as cmd_visualize().
"""

def generate_html(graph_json: str, title: str = "CommandGraph", source_file: str = "") -> str:
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
'''
'''
