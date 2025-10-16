let config = null;
let meta = null;

const state = {
  page: 1,
  pageSize: 25,
  filters: {
    start: null,
    end: null,
    q: "",
    actors: new Set(),
    countries: new Set(),
    tools: new Set()
  }
};
// --- Color helpers
const TOOL_ACCENTS = [
  "accent_orange","accent_yellow","accent_green","accent_teal",
  "accent_lightblue","accent_blue","accent_purple","accent_pink"
];

const toolColorMap = {};
const actorColorMap = {};
let sortedTools = []; // Cache for alphabetically sorted tools

function actorColor(a){
  // Prefer explicit config colors (Russia/China predefined)
  const base = (config && config.actor_palette) || {};
  if (base[a]) return base[a];
  if (!actorColorMap[a]) {
    const idx = Object.keys(actorColorMap).length;
    actorColorMap[a] = d3.schemeTableau10[idx % d3.schemeTableau10.length];
  }
  return actorColorMap[a];
}

function initializeToolColors(allTools) {
  // Sort tools alphabetically and assign colors based on that order
  sortedTools = [...allTools].sort();
  sortedTools.forEach((tool, index) => {
    if (!toolColorMap[tool]) {
      const colorIndex = index % TOOL_ACCENTS.length;
      toolColorMap[tool] = config.colors[TOOL_ACCENTS[colorIndex]];
    }
  });
}

function toolColor(t){
  if (toolColorMap[t]) return toolColorMap[t];
  
  // If not initialized, assign based on alphabetical position among known tools
  if (sortedTools.length === 0 || !sortedTools.includes(t)) {
    // Fallback: add to sorted list and reinitialize
    const currentTools = new Set([...sortedTools, t]);
    initializeToolColors(currentTools);
  }
  
  return toolColorMap[t];
}

function hashCode(s){ let h = 0; for (let i=0; i<s.length; i++) h = (h*31 + s.charCodeAt(i))|0; return h; }

// Blend from white -> base to encode intensity
function blendToBase(baseHex, frac){
  const interp = d3.interpolateRgb("#ffffff", baseHex);
  // keep a little saturation at low counts
  return interp(Math.max(0.15, Math.min(1, frac)));
}
// --- End color helpers
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

async function init() {
  config = await (await fetch("/api/config")).json();
  meta = await (await fetch("/api/meta")).json();

  // UI bindings
  $("#start").addEventListener("change", () => { state.filters.start = $("#start").value || null; state.page=1; refresh(); });
  $("#end").addEventListener("change", () => { state.filters.end = $("#end").value || null; state.page=1; refresh(); });
  $("#search").addEventListener("input", (e) => { state.filters.q = e.target.value.trim(); state.page=1; debounceRefresh(); });
  $("#clearAll").addEventListener("click", clearAll);
  $("#prev").addEventListener("click", () => { if (state.page > 1) { state.page--; refresh(); }});
  $("#next").addEventListener("click", () => { state.page++; refresh(); });

  await refresh();
}

let debounceTimer = null;
function debounceRefresh() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(refresh, 300);
}

function currentParams() {
  const p = new URLSearchParams();
  if (state.filters.start) p.set("start", state.filters.start);
  if (state.filters.end) p.set("end", state.filters.end);
  if (state.filters.q) p.set("q", state.filters.q);
  if (state.filters.actors.size) p.set("actors", Array.from(state.filters.actors).join(","));
  if (state.filters.countries.size) p.set("countries", Array.from(state.filters.countries).join(","));
  if (state.filters.tools.size) p.set("tools", Array.from(state.filters.tools).join(","));
  p.set("page", state.page);
  p.set("page_size", state.pageSize);
  return p.toString();
}

async function refresh() {
  const res = await fetch(`/api/incidents?${currentParams()}`);
  const data = await res.json();

  $("#total-count").textContent = data.total;
  $("#pageinfo").textContent = `Page ${data.page} • ${Math.min(data.page*data.page_size, data.total)} of ${data.total}`;
  renderApplied();

  renderHeatmap(data.heatmap);
  renderStacked(data.stacked);
  renderMap(data.country_actor, data.country_meta);
  renderList(data.incidents);
}

/* ---------------- Applied filters chips ---------------- */
function renderApplied() {
  const wrap = $("#applied");
  wrap.innerHTML = "";

  function chip(label, val, group) {
    const d = document.createElement("div");
    d.className = "chip";
    d.innerHTML = `<span>${label}: <strong>${val}</strong></span> <span class="x" title="remove">✕</span>`;
    d.querySelector(".x").addEventListener("click", () => {
      if (group === "actors") state.filters.actors.delete(val);
      if (group === "countries") state.filters.countries.delete(val);
      if (group === "tools") state.filters.tools.delete(val);
      if (group === "start") state.filters.start = null, $("#start").value="";
      if (group === "end") state.filters.end = null, $("#end").value="";
      if (group === "q") state.filters.q = "", $("#search").value="";
      state.page = 1; refresh();
    });
    wrap.appendChild(d);
  }

  if (state.filters.start) chip("Start", state.filters.start, "start");
  if (state.filters.end) chip("End", state.filters.end, "end");
  if (state.filters.q) chip("Search", state.filters.q, "q");
  state.filters.actors.forEach(v => chip("Actor", v, "actors"));
  state.filters.countries.forEach(v => chip("Country", v, "countries"));
  state.filters.tools.forEach(v => chip("Type", v, "tools"));
}

function clearAll() {
  state.filters.start = null; $("#start").value="";
  state.filters.end = null; $("#end").value="";
  state.filters.q = ""; $("#search").value="";
  state.filters.actors.clear();
  state.filters.countries.clear();
  state.filters.tools.clear();
  state.page = 1;
  refresh();
}

/* ---------------- Heatmap (Actor x Year) ---------------- */
function renderHeatmap(rows) {
  const el = d3.select("#heatmap");
  el.selectAll("*").remove();

  if (!rows.length) { el.append("div").text("No data."); return; }

  const years = Array.from(new Set(rows.map(d => d.year))).sort((a,b) => a-b);
  const actors = Array.from(new Set(rows.map(d => d.actor))).sort();

  const gridW = Math.min(680, document.querySelector(".heat").clientWidth - 24);
  const cellSize = 22, gap = 4;
  const gridH = actors.length * (cellSize + gap) + 4;

  const svg = el.append("svg")
    .attr("width", gridW)
    .attr("height", gridH + 30);

  const x = d3.scaleBand().domain(years).range([80, gridW]).paddingInner(0.1);
  const y = d3.scaleBand().domain(actors).range([0, gridH]).paddingInner(0.12);

  const max = d3.max(rows, d => d.count) || 1;

  // Actor labels — styled like tags with light background and dark text
  const actorLabelGroups = svg.selectAll(".actor-lab-group").data(actors).enter().append("g")
    .attr("class", "actor-lab-group axis-click")
    .on("click", (_, d) => {
      toggleSet(state.filters.actors, d);
      state.page = 1; refresh();
    });

  actorLabelGroups.append("rect")
    .attr("x", 6).attr("y", d => (y(d) ?? 0) + 1)
    .attr("width", 68).attr("height", y.bandwidth() - 2)
    .attr("rx", 3)
    .style("fill", d => blendToBase(actorColor(d), 0.25))
    .style("stroke", d => actorColor(d))
    .style("stroke-width", 1);

  actorLabelGroups.append("text")
    .attr("x", 40).attr("y", d => (y(d) ?? 0) + (y.bandwidth()/2) + 4)
    .attr("text-anchor", "middle").attr("class", "lab-actor axis-click")
    .style("font-size", "11px")
    .style("fill", "#111")
    .text(d => d);

  // Year labels: show every ~5th
  const yearLabels = years.filter((_, i) => i % 5 === 0);
  svg.selectAll(".lab-year").data(yearLabels).enter().append("text")
    .attr("x", d => (x(d) ?? 0) + (x.bandwidth()/2))
    .attr("y", gridH + 20).attr("text-anchor","middle")
    .attr("class", "lab-year axis-click")
    .style("font-size", "11px")
    .text(d => d)
    .on("click", (_, yr) => {
      $("#start").value = `${yr}-01-01`; state.filters.start = $("#start").value;
      $("#end").value = `${yr}-12-31`;   state.filters.end = $("#end").value;
      state.page = 1; refresh();
    });

  // Cells colored by actor hue, intensity by count
  svg.selectAll(".hm-cell")
    .data(rows)
    .enter()
    .append("rect")
    .attr("class", "hm-cell")
    .attr("x", d => x(d.year))
    .attr("y", d => y(d.actor))
    .attr("width", x.bandwidth())
    .attr("height", y.bandwidth())
    .attr("fill", d => blendToBase(actorColor(d.actor), d.count / max))
    .on("click", (_, d) => {
      toggleSet(state.filters.actors, d.actor);
      $("#start").value = `${d.year}-01-01`; state.filters.start = $("#start").value;
      $("#end").value = `${d.year}-12-31`;   state.filters.end = $("#end").value;
      state.page = 1; refresh();
    })
    .append("title").text(d => `${d.actor} • ${d.year}: ${d.count}`);
}

/* ---------------- Stacked Bar (Tools x Actor) ---------------- */function renderStacked(rows) {
  const el = d3.select("#stackedbar");
  el.selectAll("*").remove();

  if (!rows.length) { el.append("div").text("No data."); return; }

  const tools = Array.from(new Set(rows.map(d => d.tool))).sort();
  const actors = Array.from(new Set(rows.map(d => d.actor))).sort();

  // Initialize tool colors based on alphabetical order
  initializeToolColors(tools);

  const nested = d3.rollup(rows, v => {
    const byActor = d3.rollup(v, vv => d3.sum(vv, d => d.count), d => d.actor);
    return Object.fromEntries(byActor);
  }, d => d.tool);
  const seriesData = tools.map(t => ({ tool: t, ...nested.get(t) }));

  const width = Math.min(680, document.querySelector(".stacked").clientWidth - 24);
  const height = Math.max(220, tools.length * 26 + 60);

  const svg = el.append("svg").attr("width", width).attr("height", height);

  const x = d3.scaleLinear()
    .domain([0, d3.max(seriesData, d => d3.sum(actors, a => d[a] || 0)) || 1])
    .range([160, width - 12]);

  const y = d3.scaleBand().domain(tools).range([12, height - 24]).paddingInner(0.2);

  // Tool labels — styled like tags with light background and dark text
  const labelGroups = svg.selectAll(".tool-lab-group").data(tools).enter().append("g")
    .attr("class", "tool-lab-group axis-click")
    .on("click", (_, tool) => { toggleSet(state.filters.tools, tool); state.page=1; refresh(); });

  labelGroups.append("rect")
    .attr("x", 10).attr("y", d => (y(d) ?? 0) + 2)
    .attr("width", 135).attr("height", y.bandwidth() - 4)
    .attr("rx", 3)
    .style("fill", d => blendToBase(toolColor(d), 0.25))
    .style("stroke", d => toolColor(d))
    .style("stroke-width", 1);

  labelGroups.append("text")
    .attr("x", 77).attr("y", d => (y(d) ?? 0) + y.bandwidth()/2 + 4)
    .attr("text-anchor", "middle").attr("class", "axis-click")
    .style("font-size", "11px")
    .style("fill", "#111")
    .text(d => d);

  tools.forEach(tool => {
    let x0 = 160;
    actors.forEach(actor => {
      const value = (seriesData.find(s => s.tool === tool)?.[actor]) || 0;
      if (!value) return;
      svg.append("rect")
        .attr("class", "sb-seg")
        .attr("x", x0)
        .attr("y", y(tool))
        .attr("width", x(value) - x(0))
        .attr("height", y.bandwidth())
        .attr("fill", actorColor(actor))
        .on("click", () => {
          toggleSet(state.filters.tools, tool);
          toggleSet(state.filters.actors, actor);
          state.page = 1; refresh();
        })
        .append("title").text(`${tool} • ${actor}: ${value}`);
      x0 += (x(value) - x(0));
    });
  });
}


/* ---------------- Map (Donut markers + clusters) ---------------- */
let map, clusterLayer;

function initMap() {
  if (map) return map;
  map = L.map("map", { scrollWheelZoom: true, worldCopyJump: true }).setView([25, 10], 2);

  L.tileLayer(
    "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
    { maxZoom: 18, attribution: '&copy; OpenStreetMap &copy; CARTO' }
  ).addTo(map);

  clusterLayer = L.markerClusterGroup({
    iconCreateFunction: cluster => {
      // Aggregate actor counts from children
      const counts = {};
      cluster.getAllChildMarkers().forEach(m => {
        const parts = m.options.actorCounts || {};
        for (const [a, c] of Object.entries(parts)) counts[a] = (counts[a]||0) + c;
      });
      const tot = Object.values(counts).reduce((a,b)=>a+b,0) || 1;
      const donutData = donutSVG(counts, tot);
      const size = donutData.containerSize;
      const anchor = size / 2;
      return L.divIcon({
        html: `<div style="position:relative;width:${size}px;height:${size}px;display:flex;align-items:center;justify-content:center;">${donutData.svg}<div class="lmc-label" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);">${tot}</div></div>`,
        className: "",
        iconAnchor: [anchor, anchor]
      });
    }
  });
  map.addLayer(clusterLayer);
  return map;
}

function renderMap(countryRows, meta) {
  initMap();
  clusterLayer.clearLayers();

  // Transform to country -> {actor:count}
  const byCountry = {};
  countryRows.forEach(r => {
    if (!byCountry[r.country]) byCountry[r.country] = {};
    byCountry[r.country][r.actor] = (byCountry[r.country][r.actor] || 0) + r.count;
  });

  for (const [country, counts] of Object.entries(byCountry)) {
    const m = meta[country];
    if (!m) continue;
    const tot = Object.values(counts).reduce((a,b)=>a+b,0);
    const donutData = donutSVG(counts, tot);
    const size = donutData.containerSize;
    const anchor = size / 2;
    const icon = L.divIcon({
      html: `<div style="position:relative;width:${size}px;height:${size}px;display:flex;align-items:center;justify-content:center;">${donutData.svg}<div class="lmc-label" style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);">${tot}</div></div>`,
      className: "",
      iconAnchor: [anchor, anchor]
    });
    const marker = L.marker([m.lat, m.lon], {
      icon,
      title: `${country}: ${tot} incidents`,
      actorCounts: counts
    }).on("click", () => {
      toggleSet(state.filters.countries, country);
      state.page = 1; refresh();
    });
    clusterLayer.addLayer(marker);
  }
}

function actorColorScale(actors) {
  const base = config.actor_palette || {};
  const palette = d3.schemeTableau10;
  const assigned = {};
  let i = 0;
  return (a) => {
    if (base[a]) return base[a];
    if (!assigned[a]) assigned[a] = palette[i++ % palette.length];
    return assigned[a];
  };
}

function calculateDonutRadius(total, isCluster = false) {
  // Base size for single incidents, scale up with more incidents
  const minRadius = isCluster ? 20 : 16;
  const maxRadius = isCluster ? 40 : 32;
  
  // Logarithmic scaling for better visual distribution
  const scale = Math.log(total + 1) / Math.log(101); // normalized to 0-1 for up to 100 incidents
  return minRadius + (maxRadius - minRadius) * Math.min(scale, 1);
}

function donutSVG(counts, total = null) {
  const actors = Object.keys(counts);
  total = total || actors.reduce((s,a)=>s+counts[a],0) || 1;
  
  // Calculate dynamic radius based on total incidents
  const r = calculateDonutRadius(total);
  const containerSize = Math.max(44, r * 2 + 8); // Ensure minimum container size with padding
  
  const arc = d3.arc().innerRadius(r*0.55).outerRadius(r);
  let start = 0;
  const color = actorColorScale(actors);

  const paths = actors.map(a => {
    const val = counts[a];
    const angle = (val/total) * 2 * Math.PI;
    const d = arc({ startAngle: start, endAngle: start + angle });
    const fill = color(a);
    start += angle;
    return `<path d="${d}" fill="${fill}" class="donut-slice" data-actor="${a}"></path>`;
  }).join("");

  return {
    svg: `<svg viewBox="${-r} ${-r} ${r*2} ${r*2}" width="${r*2}" height="${r*2}">${paths}</svg>`,
    radius: r,
    containerSize: containerSize
  };
}

/* ---------------- Incident list ---------------- */
function renderList(incidents) {
  const el = $("#list");
  el.innerHTML = "";
  incidents.forEach(inc => {
    const d = document.createElement("div");
    d.className = "incident";
    const dt = (inc.start_date || inc.date_text || "").split("T")[0];
    const link = inc.link ? `<a href="${inc.link}" target="_blank" rel="noopener">Source ↗</a>` : "";
    d.innerHTML = `
      <div class="title">${inc.title || "(untitled)"} ${link}</div>
      <div class="meta">
        <span>${dt || "n.d."}</span>
        <span>POST# ${inc.post_id ?? "-"}</span>
        <span>ID ${inc.incident_id}</span>
      </div>
      <div class="excerpt">${(inc.excerpt_clean || inc.content_clean || "").slice(0, 320)}</div>
      <div class="tags">
        ${inc.actors.map(a => `<span class="tag tag-actor" data-v="${a}" style="background:${blendToBase(actorColor(a),0.25)};border-color:${actorColor(a)};color:#111;">${a}</span>`).join("")}
        ${inc.countries.map(c => `<span class="tag tag-country" data-v="${c}">${c}</span>`).join("")}
        ${inc.tools.map(t => `<span class="tag tag-tool" data-v="${t}" style="background:${blendToBase(toolColor(t),0.25)};border-color:${toolColor(t)};color:#111;">${t}</span>`).join("")}
      </div>
    `;
    el.appendChild(d);
  });

  // tag clicks -> filters
  $$(".tag-actor").forEach(t => {
    t.addEventListener("click", (e) => { toggleSet(state.filters.actors, e.target.dataset.v); state.page=1; refresh(); });
    if (state.filters.actors.has(t.dataset.v)) t.classList.add("selected");
  });
  $$(".tag-country").forEach(t => {
    t.addEventListener("click", (e) => { toggleSet(state.filters.countries, e.target.dataset.v); state.page=1; refresh(); });
    if (state.filters.countries.has(t.dataset.v)) t.classList.add("selected");
  });
  $$(".tag-tool").forEach(t => {
    t.addEventListener("click", (e) => { toggleSet(state.filters.tools, e.target.dataset.v); state.page=1; refresh(); });
    if (state.filters.tools.has(t.dataset.v)) t.classList.add("selected");
  });
}

function toggleSet(set, val) {
  if (set.has(val)) set.delete(val); else set.add(val);
}

document.addEventListener("DOMContentLoaded", init);
