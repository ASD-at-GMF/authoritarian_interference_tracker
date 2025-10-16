# Authoritarian Interference Tracker (Flask)

Dark‑themed, modern replacement for the aging tracker. Features:

- Heatmap timeline split by threat actor and year, with a manual date‑range selector.
- Horizontal stacked bar chart by incident type and threat actor.
- Leaflet map with clustering; each country (and cluster) uses donut‑chart markers sized by incident count with the label in the middle.
- Cross‑filtering across all elements: clicking the heatmap, a stacked‑bar segment, or a map marker filters the others. A “Reset Filters” button clears the state.

## Quick Start

```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python app.py
# Open http://127.0.0.1:5000 in your browser
```

## Data

Place your full GeoJSON in `data/incidents.json`. The expected schema matches your excerpt:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [lng, lat] },
      "properties": {
        "country": "Canada",
        "count": 32,
        "incidents": [
          {
            "title": "PRC state-affiliated hackers target Canadian telecommunications sector",
            "link": "https://...",
            "date_text": "June 23, 2025",
            "start_date": ["20250215"],
            "tools": [{"name": "Cyber Operations"}],
            "actors": [{"name": "China"}],
            "display": true
          }
        ]
      }
    }
  ]
}
```

Optional keys `region` and `subregion` (under `properties`) can be added later to support region filters. The app will gracefully fall back if missing.

## Theming

Colors come from your provided CSS variables. Threat‑actor color mapping is defined in `templates/index.html` under `window.ACTOR_COLORS`. Update that mapping as needed; `.ta-russia` and `.ta-china` are reflected there.

## Notes

- The map uses Leaflet + MarkerCluster with custom HTML/SVG donut markers for countries and clusters.
- The heatmap uses a per‑actor hue (from `ACTOR_COLORS`) with opacity scaled by cell counts.
- The stacked bars use D3’s stack with actor series; clicking a segment filters to the pair.
- The dual‑handle year slider uses noUiSlider.
- All interactions call a single `renderAll()` that re‑computes filtered views.
