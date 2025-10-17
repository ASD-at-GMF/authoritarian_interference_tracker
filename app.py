import json
import os
import sqlite3
from datetime import datetime, date
from dateutil import parser
from collections import defaultdict, Counter
from flask import Flask, jsonify, render_template, request, send_from_directory, session, redirect, url_for, flash
import re
from functools import wraps
# remove: import requests
from geopy.geocoders import Nominatim


app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")  # required for sessions
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme")          # set in env for prod

ALLOW_EXTERNAL_GEOCODING = os.environ.get("ALLOW_EXTERNAL_GEOCODING", "0") == "1"
NOMINATIM_USER_AGENT = os.environ.get("NOMINATIM_USER_AGENT", "ait-admin/1.0")
DB_PATH = os.environ.get("DB_PATH", "./data/incidents.sqlite")

# init geopy (lazy-safe)
_geocoder = None
def geocoder():
    global _geocoder
    if _geocoder is None:
        _geocoder = Nominatim(user_agent=NOMINATIM_USER_AGENT, timeout=8)
    return _geocoder

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

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    return s.strip("-")

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("admin_login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper

def split_and_clean_csv(val):
    if not val:
        return []
    if isinstance(val, list):
        vals = val
    else:
        vals = re.split(r"[;,]", str(val))
    return sorted({v.strip() for v in vals if v and v.strip()})

def geocode_country_external(name: str):
    """Optional Nominatim via geopy. Returns (lat, lon) or (None, None)."""
    if not ALLOW_EXTERNAL_GEOCODING or not name:
        return None, None
    try:
        loc = geocoder().geocode(name, exactly_one=True)
        if loc:
            return float(loc.latitude), float(loc.longitude)
    except Exception:
        pass
    return None, None

def get_or_create_country(conn, name: str):
    cur = conn.cursor()
    row = cur.execute("SELECT id, lat, lon FROM countries WHERE name = ?", (name,)).fetchone()
    if row:
        cid = row["id"]
        if (row["lat"] is None or row["lon"] is None):
            lat, lon = geocode_country_external(name)
            if lat is not None and lon is not None:
                cur.execute("UPDATE countries SET lat = ?, lon = ? WHERE id = ?", (lat, lon, cid))
        return cid
    lat, lon = geocode_country_external(name)
    cur.execute("INSERT INTO countries (name, lat, lon) VALUES (?, ?, ?)", (name, lat, lon))
    return cur.lastrowid

def get_or_create_actor(conn, name: str):
    cur = conn.cursor()
    row = cur.execute("SELECT id FROM actors WHERE name = ?", (name,)).fetchone()
    if row:
        return row["id"]
    # generate a unique term_id
    next_term = cur.execute("SELECT COALESCE(MAX(term_id), 0) + 1 FROM actors").fetchone()[0]
    cur.execute(
        "INSERT INTO actors (term_id, name, slug, taxonomy, description) VALUES (?, ?, ?, ?, ?)",
        (next_term, name, slugify(name), "threat_actor", None),
    )
    return cur.lastrowid

def get_or_create_tool(conn, name: str):
    cur = conn.cursor()
    row = cur.execute("SELECT id FROM tools WHERE name = ?", (name,)).fetchone()
    if row:
        return row["id"]
    next_term = cur.execute("SELECT COALESCE(MAX(term_id), 0) + 1 FROM tools").fetchone()[0]
    cur.execute(
        "INSERT INTO tools (term_id, name, slug, taxonomy, description) VALUES (?, ?, ?, ?, ?)",
        (next_term, name, slugify(name), "incident_type", None),
    )
    return cur.lastrowid


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

@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    err = None
    if request.method == "POST":
        pw = request.form.get("password", "")
        if pw == ADMIN_PASSWORD:
            session["admin"] = True
            flash("Logged in.", "ok")
            return redirect(request.args.get("next") or url_for("admin_new_incident"))
        err = "Incorrect password."
    return render_template("admin_login.html", err=err)

@app.route("/admin/logout")
def admin_logout():
    session.clear()
    flash("Logged out.", "ok")
    return redirect(url_for("index"))

@app.route("/admin/new-incident", methods=["GET", "POST"])
@login_required
def admin_new_incident():
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # preload lists for the form
    countries = cur.execute("SELECT name FROM countries ORDER BY name").fetchall()
    actors = cur.execute("SELECT name FROM actors ORDER BY name").fetchall()
    tools = cur.execute("SELECT name FROM tools ORDER BY name").fetchall()

    if request.method == "POST":
        try:
            post_id = int(request.form["post_id"])
            title = request.form["title"].strip()
            link = request.form.get("link") or None
            content = request.form.get("content_clean") or None
            excerpt = request.form.get("excerpt_clean") or None
            date_text = request.form.get("date_text") or None
            start_date = request.form.get("start_date") or None
            end_date = request.form.get("end_date") or None
            display = 1 if request.form.get("display", "on") == "on" else 0

            # selections from multi-selects
            sel_actors = request.form.getlist("actors_sel")  # multi-select of existing actors
            sel_tools  = request.form.getlist("tools_sel")   # multi-select of existing tools

            # countries still from CSV box (kept as-is)
            sel_countries = split_and_clean_csv(request.form.get("countries_csv"))

            # optional new items (comma/semicolon)
            new_actors = split_and_clean_csv(request.form.get("new_actors"))
            new_tools  = split_and_clean_csv(request.form.get("new_tools"))

            # union and de-dup
            sel_actors = sorted(set(sel_actors) | set(new_actors))
            sel_tools  = sorted(set(sel_tools)  | set(new_tools))

            # insert incident
            cur.execute("""
                INSERT INTO incidents (post_id, title, link, content_clean, excerpt_clean, date_text, start_date, end_date, display)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (post_id, title, link, content, excerpt, date_text, start_date, end_date, display))
            incident_id = cur.lastrowid

            # relate countries (create if missing; auto-geocode if allowed)
            for c in sel_countries:
                if not c: continue
                cid = get_or_create_country(conn, c)
                cur.execute("INSERT OR IGNORE INTO incident_countries (incident_id, country_id) VALUES (?, ?)", (incident_id, cid))

            # relate actors (create if missing)
            for a in sel_actors:
                if not a: continue
                aid = get_or_create_actor(conn, a)
                cur.execute("INSERT OR IGNORE INTO incident_actors (incident_id, actor_id) VALUES (?, ?)", (incident_id, aid))

            # relate tools (create if missing)
            for t in sel_tools:
                if not t: continue
                tid = get_or_create_tool(conn, t)
                cur.execute("INSERT OR IGNORE INTO incident_tools (incident_id, tool_id) VALUES (?, ?)", (incident_id, tid))

            conn.commit()
            flash(f"Incident #{incident_id} created.", "ok")
            return redirect(url_for("admin_new_incident"))

        except sqlite3.IntegrityError as e:
            conn.rollback()
            # likely duplicate post_id or FK issue
            flash(f"DB error: {e}", "err")
        except Exception as e:
            conn.rollback()
            flash(f"Unexpected error: {e}", "err")

    return render_template(
        "admin_new_incident.html",
        countries=[r["name"] for r in countries],
        actors=[r["name"] for r in actors],
        tools=[r["name"] for r in tools],
        allow_external_geocoding=ALLOW_EXTERNAL_GEOCODING
    )



if __name__ == "__main__":
    app.run(debug=True)