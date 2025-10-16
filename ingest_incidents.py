"""
ingest_authoritarian_incidents.py

Usage:
  python ingest_authoritarian_incidents.py --json input.json

- Creates the SQLite database automatically in data/incidents.sqlite
- Creates the SQLite schema (if not present)
- Ingests GeoJSON (FeatureCollection of countries -> incidents[])
- Strips HTML and WordPress shortcodes from content/excerpt
- Deduplicates incidents by post_id
- Supports multi-country, multi-actor, multi-tool
"""

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from typing import Optional

# ---------- Cleaning helpers ----------

SHORTCODE_RE = re.compile(
    r"""
    \[                                  # opening bracket
    (?:/?[a-zA-Z0-9_]+)                 # shortcode name (optionally closing)
    (?:\s+[^\]]+)?                      # optional attributes
    \]                                  # closing bracket
    (?:                                 # optional enclosed content + closing tag
        (?!\s)                          # not followed by whitespace-only (minor perf guard)
        .*?
        \[/[a-zA-Z0-9_]+\]
    )?
    """,
    re.DOTALL | re.VERBOSE,
)

TAG_RE = re.compile(r"<[^>]+>")  # basic HTML tag stripper as a fallback


class TextExtractor(HTMLParser):
    """Robust-ish HTML → text extractor using stdlib only."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []

    def handle_data(self, data):
        if data:
            self.parts.append(data)

    def handle_entityref(self, name):
        self.parts.append(unescape(f"&{name};"))

    def handle_charref(self, name):
        self.parts.append(unescape(f"&#{name};"))

    def get_text(self):
        return "".join(self.parts)


def strip_shortcodes(text: str) -> str:
    if not text:
        return ""
    return SHORTCODE_RE.sub("", text)


def strip_html(text: str) -> str:
    if not text:
        return ""
    # Prefer HTMLParser for nested tag safety
    parser = TextExtractor()
    try:
        parser.feed(text)
        return parser.get_text().strip()
    except Exception:
        # Fallback
        return TAG_RE.sub("", text).strip()


def clean_rich_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    # 1) unescape entities early (helps shortcode regex sometimes)
    s = unescape(raw)
    # 2) remove WordPress shortcodes (e.g., [fusion_*]...[/fusion_*])
    s = strip_shortcodes(s)
    # 3) strip any remaining HTML
    s = strip_html(s)
    # 4) collapse whitespace
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s).strip()
    return s


# ---------- Date normalization ----------

def normalize_date(s: Optional[str]) -> Optional[str]:
    """
    Accepts strings like '20100101', '201407', '2014', '' and returns ISO 'YYYY-MM-DD' or None.
    """
    if not s:
        return None
    s = s.strip()
    if not s:
        return None

    # Only digits?
    if not s.isdigit():
        return None

    try:
        if len(s) == 8:
            # YYYYMMDD
            dt = datetime.strptime(s, "%Y%m%d")
            return dt.strftime("%Y-%m-%d")
        elif len(s) == 6:
            # YYYYMM
            dt = datetime.strptime(s, "%Y%m")
            return dt.strftime("%Y-%m-01")
        elif len(s) == 4:
            # YYYY
            dt = datetime.strptime(s, "%Y")
            return dt.strftime("%Y-01-01")
    except ValueError:
        return None
    return None


# ---------- SQLite schema ----------

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS countries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL UNIQUE,
    lat             REAL,
    lon             REAL,
    dataset_count_hint INTEGER
);

CREATE TABLE IF NOT EXISTS incidents (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id         INTEGER NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    link            TEXT,
    content_clean   TEXT,
    excerpt_clean   TEXT,
    date_text       TEXT,
    start_date      TEXT,      -- ISO 'YYYY-MM-DD' or NULL
    end_date        TEXT,      -- ISO 'YYYY-MM-DD' or NULL
    display         INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS actors (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    term_id             INTEGER NOT NULL UNIQUE,
    name                TEXT NOT NULL,
    slug                TEXT,
    taxonomy            TEXT,
    description         TEXT
);

CREATE TABLE IF NOT EXISTS tools (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    term_id             INTEGER NOT NULL UNIQUE,
    name                TEXT NOT NULL,
    slug                TEXT,
    taxonomy            TEXT,
    description         TEXT
);

-- Many-to-many joins
CREATE TABLE IF NOT EXISTS incident_countries (
    incident_id     INTEGER NOT NULL,
    country_id      INTEGER NOT NULL,
    PRIMARY KEY (incident_id, country_id),
    FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE,
    FOREIGN KEY (country_id)  REFERENCES countries(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS incident_actors (
    incident_id     INTEGER NOT NULL,
    actor_id        INTEGER NOT NULL,
    role            TEXT,             -- optional future use
    confidence      TEXT,             -- optional future use
    PRIMARY KEY (incident_id, actor_id),
    FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE,
    FOREIGN KEY (actor_id)    REFERENCES actors(id)    ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS incident_tools (
    incident_id     INTEGER NOT NULL,
    tool_id         INTEGER NOT NULL,
    PRIMARY KEY (incident_id, tool_id),
    FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE,
    FOREIGN KEY (tool_id)     REFERENCES tools(id)     ON DELETE CASCADE
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_incidents_start ON incidents(start_date);
CREATE INDEX IF NOT EXISTS idx_incidents_end   ON incidents(end_date);
CREATE INDEX IF NOT EXISTS idx_countries_name  ON countries(name);
CREATE INDEX IF NOT EXISTS idx_actors_name     ON actors(name);
CREATE INDEX IF NOT EXISTS idx_tools_name      ON tools(name);

-- Dashboard-friendly denormalized VIEW
DROP VIEW IF EXISTS incidents_denorm;
CREATE VIEW incidents_denorm AS
SELECT
    i.id AS incident_id,
    i.post_id,
    i.title,
    i.link,
    i.content_clean,
    i.excerpt_clean,
    i.date_text,
    i.start_date,
    i.end_date,
    i.display,
    -- comma-separated distinct lists for easy crossfiltering
    (SELECT GROUP_CONCAT(DISTINCT c.name)
       FROM incident_countries ic
       JOIN countries c ON c.id = ic.country_id
      WHERE ic.incident_id = i.id) AS countries,
    (SELECT GROUP_CONCAT(DISTINCT a.name)
       FROM incident_actors ia
       JOIN actors a ON a.id = ia.actor_id
      WHERE ia.incident_id = i.id) AS actors,
    (SELECT GROUP_CONCAT(DISTINCT t.name)
       FROM incident_tools it
       JOIN tools t ON t.id = it.tool_id
      WHERE it.incident_id = i.id) AS tools
FROM incidents i;
"""

# ---------- Upsert helpers ----------

def upsert_country(cur, name: str, lat: float, lon: float, count_hint: Optional[int]) -> int:
    cur.execute("""
        INSERT INTO countries(name, lat, lon, dataset_count_hint)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            lat=excluded.lat,
            lon=excluded.lon,
            dataset_count_hint=excluded.dataset_count_hint
    """, (name, lat, lon, count_hint))
    cur.execute("SELECT id FROM countries WHERE name = ?", (name,))
    return cur.fetchone()[0]


def upsert_actor(cur, term) -> int:
    cur.execute("""
        INSERT INTO actors(term_id, name, slug, taxonomy, description)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(term_id) DO UPDATE SET
            name=excluded.name,
            slug=excluded.slug,
            taxonomy=excluded.taxonomy,
            description=COALESCE(NULLIF(excluded.description,''), actors.description)
    """, (
        int(term.get("term_id")),
        term.get("name") or "",
        term.get("slug"),
        term.get("taxonomy"),
        term.get("description") or ""
    ))
    cur.execute("SELECT id FROM actors WHERE term_id = ?", (int(term.get("term_id")),))
    return cur.fetchone()[0]


def upsert_tool(cur, term) -> int:
    cur.execute("""
        INSERT INTO tools(term_id, name, slug, taxonomy, description)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(term_id) DO UPDATE SET
            name=excluded.name,
            slug=excluded.slug,
            taxonomy=excluded.taxonomy,
            description=COALESCE(NULLIF(excluded.description,''), tools.description)
    """, (
        int(term.get("term_id")),
        term.get("name") or "",
        term.get("slug"),
        term.get("taxonomy"),
        term.get("description") or ""
    ))
    cur.execute("SELECT id FROM tools WHERE term_id = ?", (int(term.get("term_id")),))
    return cur.fetchone()[0]


def upsert_incident(cur, inc: dict) -> int:
    post_id = int(inc.get("post_id"))
    title = (inc.get("title") or "").strip()
    link = inc.get("link") or None
    content_clean = clean_rich_text(inc.get("content") or "")
    excerpt_clean = clean_rich_text(inc.get("excerpt") or "")
    date_text = (inc.get("date_text") or "").strip()

    # start_date / end_date in array form (first element)
    start_raw = None
    end_raw = None
    try:
        arr = inc.get("start_date") or []
        if isinstance(arr, list) and arr:
            start_raw = arr[0]
        arr2 = inc.get("end_date") or []
        if isinstance(arr2, list) and arr2:
            end_raw = arr2[0]
    except Exception:
        pass

    start_iso = normalize_date(start_raw)
    end_iso = normalize_date(end_raw)

    display = 1 if inc.get("display", True) else 0

    cur.execute("""
        INSERT INTO incidents(post_id, title, link, content_clean, excerpt_clean, date_text, start_date, end_date, display)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_id) DO UPDATE SET
            title=excluded.title,
            link=excluded.link,
            content_clean=excluded.content_clean,
            excerpt_clean=excluded.excerpt_clean,
            date_text=excluded.date_text,
            start_date=COALESCE(excluded.start_date, incidents.start_date),
            end_date=COALESCE(excluded.end_date, incidents.end_date),
            display=excluded.display
    """, (post_id, title, link, content_clean, excerpt_clean, date_text, start_iso, end_iso, display))
    cur.execute("SELECT id FROM incidents WHERE post_id = ?", (post_id,))
    return cur.fetchone()[0]


def link_incident_country(cur, incident_id: int, country_id: int):
    cur.execute("""
        INSERT OR IGNORE INTO incident_countries(incident_id, country_id)
        VALUES (?, ?)
    """, (incident_id, country_id))


def link_incident_actor(cur, incident_id: int, actor_id: int, role: Optional[str] = None, confidence: Optional[str] = None):
    cur.execute("""
        INSERT OR IGNORE INTO incident_actors(incident_id, actor_id, role, confidence)
        VALUES (?, ?, ?, ?)
    """, (incident_id, actor_id, role, confidence))


def link_incident_tool(cur, incident_id: int, tool_id: int):
    cur.execute("""
        INSERT OR IGNORE INTO incident_tools(incident_id, tool_id)
        VALUES (?, ?)
    """, (incident_id, tool_id))


# ---------- Database initialization ----------

def initialize_database(db_path: str = "data/incidents.sqlite"):
    """Initialize the SQLite database with the required schema."""
    import os
    
    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        cur = conn.cursor()
        cur.executescript(SCHEMA_SQL)
        conn.commit()
    
    print(f"✅ Database initialized: {db_path}")
    return db_path


# ---------- Ingest pipeline ----------

def ingest_geojson(db_path: str, json_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        cur = conn.cursor()
        cur.executescript(SCHEMA_SQL)

        # Load JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        features = data.get("features", [])
        for ftr in features:
            props = ftr.get("properties", {}) or {}
            geom = ftr.get("geometry", {}) or {}
            coords = geom.get("coordinates") or [None, None]
            lon = float(coords[0]) if coords and len(coords) >= 2 and coords[0] is not None else None
            lat = float(coords[1]) if coords and len(coords) >= 2 and coords[1] is not None else None

            country_name = (props.get("country") or "").strip()
            count_hint = props.get("count")
            try:
                count_hint = int(count_hint) if count_hint is not None else None
            except Exception:
                count_hint = None

            if not country_name:
                # Skip features missing a country name
                continue

            country_id = upsert_country(cur, country_name, lat, lon, count_hint)

            for inc in props.get("incidents", []) or []:
                incident_id = upsert_incident(cur, inc)

                # link to country (supports multi-country if same post_id appears in other features)
                link_incident_country(cur, incident_id, country_id)

                # actors (multi)
                for a in inc.get("actors", []) or []:
                    actor_id = upsert_actor(cur, a)
                    # JSON doesn't carry role/confidence; keep nulls for now
                    link_incident_actor(cur, incident_id, actor_id)

                # tools (multi)
                for t in inc.get("tools", []) or []:
                    tool_id = upsert_tool(cur, t)
                    link_incident_tool(cur, incident_id, tool_id)

        conn.commit()


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Ingest authoritarian interference incidents into SQLite.")
    ap.add_argument("--json", help="Path to the input GeoJSON file.", default="./data/incidents.json")
    args = ap.parse_args()

    # Initialize database with default path
    db_path = initialize_database()

    try:
        ingest_geojson(db_path, args.json)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)

    print(f"✅ Ingest complete.\n→ SQLite DB: {db_path}\nTry:\n  sqlite3 {db_path} \"SELECT incident_id, title, countries, actors, tools FROM incidents_denorm LIMIT 10;\"")


if __name__ == "__main__":
    main()