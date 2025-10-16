import json
import os
import sqlite3
from datetime import datetime, date
from dateutil import parser
from collections import defaultdict, Counter
from flask import Flask, jsonify, render_template, request, send_from_directory

DB_PATH = os.environ.get("TRACKER_DB", "./data/incidents.sqlite")  # SQLite file containing the VIEW
COUNTRY_CENTROIDS_PATH = os.environ.get("COUNTRY_CENTROIDS", "static/data/country_centroids.json")

app = Flask(__name__)

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ---------- Utilities ----------
def to_date(s):
    if not s:
        return None
    try:
        return parser.parse(s).date()
    except Exception:
        return None

def split_csv(s):
    """Accept comma-separated string OR list; always return a clean list."""
    if s is None:
        return []
    if isinstance(s, list):
        return [str(x).strip() for x in s if str(x).strip()]
    # string path
    s = str(s)
    if not s.strip():
        return []
    return [part.strip() for part in s.split(",") if part.strip()]


def load_centroids_from_db():
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cur = conn.execute("SELECT name, lat, lon FROM countries WHERE lat IS NOT NULL AND lon IS NOT NULL")
    data = {}
    for row in cur.fetchall():
        # ensure floats
        try:
            lat = float(row["lat"])
            lon = float(row["lon"])
        except (TypeError, ValueError):
            continue
        data[row["name"]] = {"lat": lat, "lon": lon}
    conn.close()
    return data

def filter_incident(inc, filters):
    """Apply in-Python filters because VIEW returns CSV fields."""
    # Date range
    start = filters.get("start")
    end = filters.get("end")
    d = to_date(inc.get("start_date")) or to_date(inc.get("date_text"))
    if start and d and d < start: return False
    if end and d and d > end: return False

    # Actors / Countries / Tools (incident types)
    if filters.get("actors"):
        if not set(split_csv(inc.get("actors"))).intersection(filters["actors"]):
            return False
    if filters.get("countries"):
        if not set(split_csv(inc.get("countries"))).intersection(filters["countries"]):
            return False
    if filters.get("tools"):
        if not set(split_csv(inc.get("tools"))).intersection(filters["tools"]):
            return False

    # Search text (title + content + excerpt)
    q = filters.get("q")
    if q:
        hay = " ".join([
            (inc.get("title") or ""), (inc.get("content_clean") or ""), (inc.get("excerpt_clean") or "")
        ]).lower()
        if q.lower() not in hay:
            return False
    return True

def incident_to_dict(row):
    d = dict(row)
    # normalize for frontend
    d["countries"] = split_csv(d.get("countries"))
    d["actors"]    = split_csv(d.get("actors"))
    d["tools"]     = split_csv(d.get("tools"))
    return d

def collect_meta(incidents):
    actors = Counter()
    countries = Counter()
    tools = Counter()
    years = Counter()
    for inc in incidents:
        for a in inc["actors"]: actors[a] += 1
        for c in inc["countries"]: countries[c] += 1
        for t in inc["tools"]: tools[t] += 1
        dy = to_date(inc.get("start_date")) or to_date(inc.get("date_text"))
        if dy: years[dy.year] += 1
    return {
        "actors": sorted(actors.items(), key=lambda x: (-x[1], x[0])),
        "countries": sorted(countries.items(), key=lambda x: (-x[1], x[0])),
        "tools": sorted(tools.items(), key=lambda x: (-x[1], x[0])),
        "years": sorted(years.items())
    }

# ---------- Routes ----------
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/config")
def api_config():
    # Color tokens + actor palette (extend as needed)
    config = {
        "colors": {
            "primary": "#cf2e2e",
            "accent_orange": "#ff6900",
            "accent_yellow": "#fcb900",
            "accent_green": "#7bdcb5",
            "accent_teal": "#00d084",
            "accent_lightblue": "#8ed1fc",
            "accent_blue": "#0693e3",
            "accent_purple": "#9b51e0",
            "accent_pink": "#f78da7",
            "ta_russia": "#0d47a1",
            "ta_china": "#8b0000"
        },
        # default actor colors; add more at will
        "actor_palette": {
            "Russia": "#0d47a1",
            "China": "#8b0000",
            "Iran": "#9b51e0",
            "Other": "#444444",
            "Unknown": "#7f7f7f"
        }
    }
    return jsonify(config)

def read_all_incidents():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT incident_id, post_id, title, link, content_clean, excerpt_clean,
               date_text, start_date, end_date, display,
               countries, actors, tools
        FROM incidents_denorm
        WHERE display IS NULL OR display <> 'hidden'
    """)
    rows = [incident_to_dict(r) for r in cur.fetchall()]
    conn.close()
    return rows

@app.route("/api/meta")
def api_meta():
    incidents = read_all_incidents()
    meta = collect_meta(incidents)
    return jsonify(meta)

@app.route("/api/incidents")
def api_incidents():
    # Parse filters
    def parse_multi(name):
        v = request.args.get(name, "").strip()
        return [s for s in v.split(",") if s] if v else []

    start = request.args.get("start")
    end = request.args.get("end")
    filters = {
        "start": to_date(start) if start else None,
        "end": to_date(end) if end else None,
        "actors": parse_multi("actors"),
        "countries": parse_multi("countries"),
        "tools": parse_multi("tools"),  # "incident types" from tools field
        "q": request.args.get("q", "").strip() or None
    }
    page = max(1, int(request.args.get("page", 1)))
    page_size = min(100, int(request.args.get("page_size", 25)))

    incidents = read_all_incidents()
    filtered = [inc for inc in incidents if filter_incident(inc, filters)]

    # Aggregations for widgets
    # heatmap: counts by (year, actor)
    heatmap = defaultdict(lambda: defaultdict(int))
    for inc in filtered:
        y = to_date(inc.get("start_date")) or to_date(inc.get("date_text"))
        if not y: continue
        year = y.year
        for a in (inc["actors"] or ["Unknown"]):
            heatmap[year][a] += 1
    heatmap_rows = []
    for year, bucket in heatmap.items():
        for actor, count in bucket.items():
            heatmap_rows.append({"year": year, "actor": actor, "count": count})

    # stacked bar: tools x actor
    tba = defaultdict(lambda: defaultdict(int))
    for inc in filtered:
        tools = inc["tools"] or ["Unspecified"]
        actors = inc["actors"] or ["Unknown"]
        for t in tools:
            for a in actors:
                tba[t][a] += 1
    stacked_rows = []
    for tool, bucket in tba.items():
        for actor, count in bucket.items():
            stacked_rows.append({"tool": tool, "actor": actor, "count": count})

    # country x actor counts (for map donuts)
    cxa = defaultdict(lambda: defaultdict(int))
    for inc in filtered:
        cs = inc["countries"] or ["Unassigned"]
        as_ = inc["actors"] or ["Unknown"]
        for c in cs:
            for a in as_:
                cxa[c][a] += 1
    country_rows = []
    total_by_country = {}
    for country, bucket in cxa.items():
        tot = sum(bucket.values())
        total_by_country[country] = tot
        for actor, count in bucket.items():
            country_rows.append({"country": country, "actor": actor, "count": count})

    # paging
    total = len(filtered)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_items = filtered[start_idx:end_idx]

    # attach country metadata (lat/lon/region/ subregion)
    centroids = load_centroids_from_db()

    return jsonify({
        "total": total,
        "page": page,
        "page_size": page_size,
        "incidents": page_items,
        "heatmap": heatmap_rows,
        "stacked": stacked_rows,
        "country_actor": country_rows,
        "country_meta": centroids
    })

# Static helper to serve the centroids stub if needed
@app.route("/static/data/<path:filename>")
def static_data(filename):
    return send_from_directory("static/data", filename)

if __name__ == "__main__":
    app.run(debug=True)