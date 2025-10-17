#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_incidents_merge.py

Usage:
  python ingest_incidents_merge.py --db incidents.sqlite [--geojson geo_incidents.json] [--wpjson incidents_10000.json]

- Creates/updates the SQLite schema
- Ingests:
    (A) GeoJSON FeatureCollection (features[].properties.incidents[])
    (B) WP JSON array (incidents_10000.json-style)
- Strips HTML + WP shortcodes from content/excerpt
- Dedupes incidents by post_id / id
- Links multi-country, multi-actor, multi-tool, and multi-source
"""

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from typing import Optional, Iterable
from urllib.parse import urlparse

# ---------- Cleaning helpers ----------
# Removes WP-style shortcodes like [shortcode]...[/shortcode], but not whats between the tags
SHORTCODE_RE = re.compile(
    r"""
    # Match opening shortcode tag and capture content, then closing tag
    \[                          # opening bracket
    ([a-zA-Z0-9_]+)             # capture shortcode name (group 1)
    (?:                         # optional attributes group
        \s+                     # whitespace before attributes
        [^\]]*                  # any characters except closing bracket (more flexible than [^\]]+)
    )?                          # end optional attributes
    \]                          # closing bracket
    (.*?)                       # capture content between tags (group 2) - non-greedy
    \[/\1\]                     # closing tag with same name as opening (backreference to group 1)
    |                           # OR
    # Match self-closing shortcodes (no content to preserve)
    \[                          # opening bracket
    [a-zA-Z0-9_]+               # shortcode name
    (?:                         # optional attributes group
        \s+                     # whitespace before attributes
        [^\]]*                  # any characters except closing bracket
    )?                          # end optional attributes
    /?                          # optional forward slash for self-closing
    \]                          # closing bracket
    """,
    re.DOTALL | re.VERBOSE,
)
TAG_RE = re.compile(r"<[^>]+>")

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
    def handle_data(self, data): 
        if data: self.parts.append(data)
    def handle_entityref(self, name): self.parts.append(unescape(f"&{name};"))
    def handle_charref(self, name):   self.parts.append(unescape(f"&#{name};"))
    def get_text(self): return "".join(self.parts)

# Removes WP-style shortcodes like [shortcode]...[/shortcode] but preserves content between the tags
def strip_shortcodes(text: str) -> str:
    if not text:
        return ""
    
    # Handle nested shortcodes by repeatedly processing until no more changes
    prev_text = None
    current_text = text
    
    def replace_shortcode(match):
        # If group 2 exists, it means we matched a paired shortcode with content
        if match.group(2) is not None:
            return match.group(2)  # Return just the content between the tags
        else:
            return ""  # Self-closing shortcode, remove entirely
    
    # Keep processing until no more shortcodes are found (handles nested structures)
    max_iterations = 50  # Safety limit to prevent infinite loops
    iteration = 0
    
    while prev_text != current_text and iteration < max_iterations:
        prev_text = current_text
        current_text = SHORTCODE_RE.sub(replace_shortcode, current_text)
        iteration += 1
    
    return current_text

def strip_html(text: str) -> str:
    if not text: 
        return ""
    parser = TextExtractor()
    try:
        parser.feed(text)
        return parser.get_text().strip()
    except Exception:
        return TAG_RE.sub("", text).strip()

def clean_rich_text(raw: Optional[str]) -> str:
    if not raw: return ""
    s = unescape(raw)
    s = strip_shortcodes(s)
    s = strip_html(s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\n{2,}", "\n", s).strip()
    return s

# ---------- Date normalization ----------

def normalize_date(s: Optional[str]) -> Optional[str]:
    """
    Accepts 'YYYYMMDD', 'YYYYMM', 'YYYY', 'MM/DD/YYYY', 'M/D/YYYY', '' -> 'YYYY-MM-DD' or None.
    """
    if not s: return None
    s = s.strip()
    if not s: return None

    # common numeric formats
    if s.isdigit():
        try:
            if len(s) == 8:
                return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
            if len(s) == 6:
                return datetime.strptime(s, "%Y%m").strftime("%Y-%m-01")
            if len(s) == 4:
                return datetime.strptime(s, "%Y").strftime("%Y-%m-%01")
        except ValueError:
            return None

    # mm/dd/yyyy (or m/d/yyyy)
    for fmt in ("%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None

# ---------- URL / domain helpers ----------

def clean_url(u: Optional[str]) -> Optional[str]:
    if not u: return None
    u = u.strip()
    if not u: return None
    # Some fields may contain NULs or whitespace junk
    u = u.replace("\x00", "")
    return u

def domain_of(u: str) -> Optional[str]:
    try:
        netloc = urlparse(u).netloc.lower()
        if not netloc: return None
        if netloc.startswith("www."): netloc = netloc[4:]
        return netloc
    except Exception:
        return None

# ---------- SQLite schema (adds slug/published_at; adds sources) ----------

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
    slug            TEXT,
    title           TEXT NOT NULL,
    link            TEXT,
    content_clean   TEXT,
    excerpt_clean   TEXT,
    date_text       TEXT,
    start_date      TEXT,
    end_date        TEXT,
    display         INTEGER NOT NULL DEFAULT 1,
    published_at    TEXT   -- WP post datetime (ISO 8601) if available
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

-- New: sources
CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT NOT NULL UNIQUE,
    domain          TEXT
);

CREATE TABLE IF NOT EXISTS incident_sources (
    incident_id     INTEGER NOT NULL,
    source_id       INTEGER NOT NULL,
    ordinal         INTEGER,         -- 1..5 for WP ACF source slots (or null)
    label           TEXT,            -- future-proof if you want to name slots
    PRIMARY KEY (incident_id, source_id),
    FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE CASCADE,
    FOREIGN KEY (source_id)  REFERENCES sources(id)   ON DELETE CASCADE
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
    role            TEXT,
    confidence      TEXT,
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
CREATE INDEX IF NOT EXISTS idx_incidents_slug  ON incidents(slug);
CREATE INDEX IF NOT EXISTS idx_countries_name  ON countries(name);
CREATE INDEX IF NOT EXISTS idx_actors_name     ON actors(name);
CREATE INDEX IF NOT EXISTS idx_tools_name      ON tools(name);
CREATE INDEX IF NOT EXISTS idx_sources_domain  ON sources(domain);

-- Denormalized VIEW with sources aggregated
DROP VIEW IF EXISTS incidents_denorm;
CREATE VIEW incidents_denorm AS
SELECT
    i.id AS incident_id,
    i.post_id,
    i.slug,
    i.title,
    i.link,
    i.content_clean,
    i.excerpt_clean,
    i.date_text,
    i.start_date,
    i.end_date,
    i.display,
    i.published_at,
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
      WHERE it.incident_id = i.id) AS tools,
    (SELECT GROUP_CONCAT(DISTINCT s.domain)
       FROM incident_sources xis
       JOIN sources s ON s.id = xis.source_id
      WHERE xis.incident_id = i.id) AS source_domains,
    (SELECT GROUP_CONCAT(DISTINCT s.url)
       FROM incident_sources xis
       JOIN sources s ON s.id = xis.source_id
      WHERE xis.incident_id = i.id) AS source_urls,
    (SELECT COUNT(*)
       FROM incident_sources xis
      WHERE xis.incident_id = i.id) AS source_count
FROM incidents i;
"""

# ---------- DB helpers ----------

def exec_schema(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_SQL)

def upsert_country(cur, name: str, lat: Optional[float], lon: Optional[float], count_hint: Optional[int]) -> int:
    cur.execute("""
        INSERT INTO countries(name, lat, lon, dataset_count_hint)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            lat=COALESCE(excluded.lat, countries.lat),
            lon=COALESCE(excluded.lon, countries.lon),
            dataset_count_hint=COALESCE(excluded.dataset_count_hint, countries.dataset_count_hint)
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

def upsert_source(cur, url: str) -> int:
    dom = domain_of(url)
    cur.execute("""
        INSERT INTO sources(url, domain)
        VALUES (?, ?)
        ON CONFLICT(url) DO UPDATE SET
            domain=COALESCE(excluded.domain, sources.domain)
    """, (url, dom))
    cur.execute("SELECT id FROM sources WHERE url = ?", (url,))
    return cur.fetchone()[0]

def upsert_incident(cur, *, post_id: int, title: str, link: Optional[str],
                    slug: Optional[str], content_clean: Optional[str],
                    excerpt_clean: Optional[str], date_text: Optional[str],
                    start_iso: Optional[str], end_iso: Optional[str],
                    display: int, published_at: Optional[str]) -> int:
    # Only overwrite string fields if the new value is non-empty (avoid clobbering rich content with empty WP fields)
    cur.execute("""
        INSERT INTO incidents(post_id, slug, title, link, content_clean, excerpt_clean, date_text, start_date, end_date, display, published_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(post_id) DO UPDATE SET
            slug        = CASE WHEN excluded.slug        IS NOT NULL AND excluded.slug        != '' THEN excluded.slug        ELSE incidents.slug        END,
            title       = CASE WHEN excluded.title       IS NOT NULL AND excluded.title       != '' THEN excluded.title       ELSE incidents.title       END,
            link        = CASE WHEN excluded.link        IS NOT NULL AND excluded.link        != '' THEN excluded.link        ELSE incidents.link        END,
            content_clean=CASE WHEN excluded.content_clean IS NOT NULL AND excluded.content_clean != '' THEN excluded.content_clean ELSE incidents.content_clean END,
            excerpt_clean=CASE WHEN excluded.excerpt_clean IS NOT NULL AND excluded.excerpt_clean != '' THEN excluded.excerpt_clean ELSE incidents.excerpt_clean END,
            date_text   = CASE WHEN excluded.date_text   IS NOT NULL AND excluded.date_text   != '' THEN excluded.date_text   ELSE incidents.date_text   END,
            start_date  = COALESCE(excluded.start_date, incidents.start_date),
            end_date    = COALESCE(excluded.end_date,   incidents.end_date),
            display     = excluded.display,
            published_at= COALESCE(excluded.published_at, incidents.published_at)
    """, (post_id, slug, title, link, content_clean, excerpt_clean, date_text, start_iso, end_iso, display, published_at))
    cur.execute("SELECT id FROM incidents WHERE post_id = ?", (post_id,))
    return cur.fetchone()[0]

def link_incident_country(cur, incident_id: int, country_id: int):
    cur.execute("INSERT OR IGNORE INTO incident_countries(incident_id, country_id) VALUES (?, ?)", (incident_id, country_id))

def link_incident_actor(cur, incident_id: int, actor_id: int, role: Optional[str] = None, confidence: Optional[str] = None):
    cur.execute("INSERT OR IGNORE INTO incident_actors(incident_id, actor_id, role, confidence) VALUES (?, ?, ?, ?)",
                (incident_id, actor_id, role, confidence))

def link_incident_tool(cur, incident_id: int, tool_id: int):
    cur.execute("INSERT OR IGNORE INTO incident_tools(incident_id, tool_id) VALUES (?, ?)", (incident_id, tool_id))

def link_incident_source(cur, incident_id: int, source_id: int, ordinal: Optional[int]):
    cur.execute("INSERT OR IGNORE INTO incident_sources(incident_id, source_id, ordinal) VALUES (?, ?, ?)",
                (incident_id, source_id, ordinal))

# ---------- Ingestors ----------

def ingest_geojson(conn: sqlite3.Connection, json_path: str):
    cur = conn.cursor()
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
        count_hint = None
        try:
            if props.get("count") is not None:
                count_hint = int(props.get("count"))
        except Exception:
            count_hint = None
        if not country_name:
            continue

        country_id = upsert_country(cur, country_name, lat, lon, count_hint)

        for inc in props.get("incidents", []) or []:
            post_id = int(inc.get("post_id"))
            title = (inc.get("title") or "").strip()
            link = inc.get("link") or None
            content_clean = clean_rich_text(inc.get("content") or "")
            excerpt_clean = clean_rich_text(inc.get("excerpt") or "")
            date_text = (inc.get("date_text") or "").strip()
            start_raw = (inc.get("start_date") or [None])[0]
            end_raw   = (inc.get("end_date")   or [None])[0]
            start_iso = normalize_date(start_raw)
            end_iso   = normalize_date(end_raw)
            display   = 1 if inc.get("display", True) else 0

            incident_id = upsert_incident(
                cur,
                post_id=post_id, title=title, link=link, slug=None,
                content_clean=content_clean, excerpt_clean=excerpt_clean,
                date_text=date_text, start_iso=start_iso, end_iso=end_iso,
                display=display, published_at=None
            )

            # country link (multi-country handled if this post_id appears in multiple features)
            link_incident_country(cur, incident_id, country_id)

            # actors
            for a in inc.get("actors", []) or []:
                actor_id = upsert_actor(cur, a)
                link_incident_actor(cur, incident_id, actor_id)

            # tools
            for t in inc.get("tools", []) or []:
                tool_id = upsert_tool(cur, t)
                link_incident_tool(cur, incident_id, tool_id)

    conn.commit()

def _iter_wp_sources(acf: dict) -> Iterable[tuple[int, Optional[str]]]:
    # yields (ordinal, url or None)
    for idx, key in enumerate(["source", "source_2", "source_3", "source_4", "source_5"], start=1):
        u = clean_url(acf.get(key))
        yield (idx, u if u else None)

def ingest_wpjson(conn: sqlite3.Connection, json_path: str):
    cur = conn.cursor()
    with open(json_path, "r", encoding="utf-8") as f:
        items = json.load(f)

    for item in items:
        post_id = int(item.get("id"))
        slug = (item.get("slug") or "").strip() or None
        title = clean_rich_text((item.get("title", {}).get("rendered") or "").strip())
        # Build a canonical link if you want; or leave NULL if unknown
        link = None
        if slug:
            link = f"https://securingdemocracy.gmfus.org/incident/{slug}/"

        published_at = item.get("date") or None
        acf = item.get("acf", {}) or {}

        date_text = (acf.get("date_text") or "").strip() or None
        start_iso = normalize_date(acf.get("start_date"))
        end_iso   = normalize_date(acf.get("end_date"))

        # WP JSON here has no content/excerpt for incidents; avoid clobbering richer data already stored
        incident_id = upsert_incident(
            cur,
            post_id=post_id, slug=slug, title=title, link=link,
            content_clean=None, excerpt_clean=None,
            date_text=date_text, start_iso=start_iso, end_iso=end_iso,
            display=1, published_at=published_at
        )

        # country from ACF
        # Prefer acf.country, else acf.location.country/name, and lat/lng if present
        country_name = (acf.get("country") or
                        (acf.get("location") or {}).get("country") or
                        (acf.get("location") or {}).get("name") or "").strip()
        lat = None
        lon = None
        try:
            lat = float(acf.get("latitude")) if acf.get("latitude") not in (None, "") else None
            lon = float(acf.get("longitude")) if acf.get("longitude") not in (None, "") else None
        except Exception:
            lat = lat or None
            lon = lon or None

        if country_name:
            country_id = upsert_country(cur, country_name, lat, lon, None)
            link_incident_country(cur, incident_id, country_id)

        # sources
        for ordinal, u in _iter_wp_sources(acf):
            if not u: 
                continue
            try:
                sid = upsert_source(cur, u)
                link_incident_source(cur, incident_id, sid, ordinal)
            except Exception:
                # swallow bad URLs to keep ingest resilient
                pass

    conn.commit()

# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Merge ingest of GeoJSON + WP JSON into SQLite (with sources).")
    ap.add_argument("--db", help="Path to the SQLite DB file (created/updated).", default="./data/incidents.sqlite")
    ap.add_argument("--geojson", help="Path to GeoJSON FeatureCollection (country → incidents[]).", default="./data/incidents.json")
    ap.add_argument("--wpjson", help="Path to incidents_10000.json (WordPress posts).", default="./data/incidents_10000.json")
    args = ap.parse_args()

    with sqlite3.connect(args.db) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        exec_schema(conn)

        if args.geojson:
            ingest_geojson(conn, args.geojson)
        if args.wpjson:
            ingest_wpjson(conn, args.wpjson)

    print("✅ Ingest complete.")
    print(f"→ DB: {args.db}")
    print('Try:\n  sqlite3 %s "SELECT incident_id, title, countries, tools, source_domains, source_count FROM incidents_denorm ORDER BY incident_id DESC LIMIT 10;"' % args.db)

if __name__ == "__main__":
    main()
