"""Minimal search webpage for the spectra database — single-star search by
Gaia source_id or name, plus a batch upload for a list of either.

Reads a read-only DuckDB view over a Parquet snapshot instead of a live
Postgres connection — this process has no DATABASE_URL and never touches
Postgres at all, not even to write. The snapshot is written by
scripts.export_to_parquet from the real Postgres database (wherever that
runs) directly into morgan's ~/public_html, which joy's Apache (mod_userdir)
already serves publicly — morgan and joy share the same NFS home directory,
so nothing needs to explicitly sync/publish anything. This app reads it
straight over HTTP via DuckDB's httpfs extension (SPECTRA_DATA_URL, what the
hosted Cloud Run service uses), or from a local directory (SPECTRA_DATA_DIR)
for local dev.

The one exception is /triage's classification submissions, which do need to
persist somewhere: rather than opening a write path from this public,
unauthenticated web tier to Postgres, they're appended as JSON lines to
another public file on joy over a narrowly-scoped SSH connection (see
_append_triage_submission / _joy_ssh_client below) and only actually land in
skip_classifications the next time scripts.export_to_parquet runs and
imports them.

Run locally against a local export:
    python3 -m scripts.export_to_parquet --out-dir ./data
    SPECTRA_DATA_DIR=./data python3 -m webapp.app

Run against the hosted snapshot (what Cloud Run does):
    SPECTRA_DATA_URL=http://joy.chara.gsu.edu/~way/spectra_data python3 -m webapp.app
"""

from __future__ import annotations

import base64
import csv
import io
import json
import math
import os
import random
import re
import secrets
import threading
import time
import urllib.error
import urllib.request
import zlib
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlencode, urlsplit

import astropy.units as u
import duckdb
import numpy as np
import paramiko
import requests
from astropy.coordinates import SkyCoord
from flask import Flask, Response, redirect, render_template_string, request
from pyvo.dal.exceptions import DALServiceError

from ingest.add_star import _launch_gaia_job, resolve_bsc_hr_number, resolve_gaia_source_id, resolve_stellar_gaia_ids_batch

app = Flask(__name__)

# Set on the old renamed-away service (e.g. the original spectra-database
# Cloud Run URL) so every request there shows a moved notice with a link to
# the current site, instead of just going dark or silently redirecting --
# keeps old bookmarks/links understandable during the decommission window.
_REDIRECT_BASE_URL = os.environ.get("REDIRECT_BASE_URL", "").rstrip("/")
_MOVED_NOTICE_DEADLINE = "September 5, 2026"

if _REDIRECT_BASE_URL:
    @app.before_request
    def _show_moved_notice():
        target = _REDIRECT_BASE_URL + request.path
        if request.query_string:
            target += "?" + request.query_string.decode()
        return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer has moved</title>
  <style>{SHARED_STYLE}</style>
</head>
<body>
  <h1>This site has moved</h1>
  <p>Spectra Database has been renamed <strong>The Spectra Pointer</strong>
    and now lives at a new address:</p>
  <p><a href="{target}">{target}</a></p>
  <p class="note">This address ({_REDIRECT_BASE_URL}) will be taken offline
    around {_MOVED_NOTICE_DEADLINE} -- please update any bookmarks or links.</p>
</body>
</html>
"""

# Source_id lookups are one indexed query regardless of list size — no cap
# needed. Name lookups each cost a SIMBAD round trip (batched, but still),
# so cap the list to keep a single upload from turning into a huge SIMBAD
# query — per project to-do, laptop/small-server scale, not a bulk pipeline.
MAX_NAME_LOOKUPS = 2000

DATA_TABLES = (
    "stars", "archives", "spectroscopy_holdings", "archive_sync_state",
    "leaderboard", "cmd_stars", "archive_status", "instruments", "instrument_sky_sample",
    "sky_sample", "triage_queue", "star_name_index",
    "archive_overlap", "archive_overlap_triple", "instrument_overlap", "instrument_overlap_triple",
    "needs_review", "skipped_by_archive", "skipped",
)


def _resolve_data_source() -> str:
    """Base path or URL containing the DATA_TABLES parquet files."""
    url = os.environ.get("SPECTRA_DATA_URL")
    if url:
        return url.rstrip("/")
    local_dir = os.environ.get("SPECTRA_DATA_DIR")
    if local_dir:
        return local_dir.rstrip("/")
    raise RuntimeError(
        "Set SPECTRA_DATA_URL (e.g. http://joy.chara.gsu.edu/~way/spectra_data "
        "— what the hosted service uses) or SPECTRA_DATA_DIR (local export) — "
        "see webapp.app's module docstring."
    )


def _make_connection() -> duckdb.DuckDBPyConnection:
    source = _resolve_data_source()
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL json")
    con.execute("LOAD json")
    if source.startswith("http://") or source.startswith("https://"):
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
    for table in DATA_TABLES:
        path = f"{source}/{table}.parquet"
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    # /stats' summary numbers -- precomputed by scripts.export_to_parquet
    # (see its module for why) as one JSON object with mixed scalar/list
    # fields, rather than one table per field like everything else here.
    con.execute(f"CREATE VIEW stats_summary AS SELECT * FROM read_json_auto('{source}/stats_summary.json')")
    return con


# One shared connection, loaded once at process startup — re-reading the
# Parquet snapshot per request would be wasteful and it only changes when
# scripts.export_to_parquet publishes a new one anyway. DuckDB connections
# aren't safe for concurrent execute() calls from multiple threads, so each
# request pulls its own cursor off this rather than sharing it directly —
# cursors share the parent's views/data and are safe to use concurrently.
_con = _make_connection()


def get_cursor() -> duckdb.DuckDBPyConnection:
    return _con.cursor()


def _rows_as_dicts(cur: duckdb.DuckDBPyConnection) -> list[dict]:
    columns = [c[0] for c in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _csv_response(fieldnames: list[str], rows: list[dict], filename: str) -> Response:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _aitoff_project(ra_deg: list[float], dec_deg: list[float]) -> tuple[list[float], list[float]]:
    """RA/Dec (degrees) -> Aitoff-projection x/y, for an all-sky map. Flips
    RA so it increases right-to-left, matching the conventional sky-map
    view (looking up/out at the sky, not down at a map of it)."""
    ra = np.radians(np.array(ra_deg, dtype=float))
    dec = np.radians(np.array(dec_deg, dtype=float))
    lam = np.where(ra > np.pi, ra - 2 * np.pi, ra)
    lam = -lam
    alpha = np.arccos(np.cos(dec) * np.cos(lam / 2))
    sinc_alpha = np.where(alpha == 0, 1.0, np.sin(alpha) / np.where(alpha == 0, 1.0, alpha))
    x = 2 * np.cos(dec) * np.sin(lam / 2) / sinc_alpha
    y = np.sin(dec) / sinc_alpha
    return x.tolist(), y.tolist()


def _galactic_plane_xy() -> tuple[list[float | None], list[float | None]]:
    """Points along the Galactic plane (b=0), Aitoff-projected, for a
    computed Milky Way overlay on the sky map. A real astropy coordinate
    transform, not a raster image — sourcing a photographic all-sky image
    and warping it pixel-for-pixel into this exact Aitoff parameterization
    to align with the star coordinates would be a lot of extra work (and
    licensing to sort out) for the same visual payoff.
    """
    lon = np.linspace(0, 360, 361)
    gal = SkyCoord(l=lon * u.deg, b=np.zeros_like(lon) * u.deg, frame="galactic").icrs
    x, y = _aitoff_project(gal.ra.deg.tolist(), gal.dec.deg.tolist())

    # Break the line wherever consecutive points jump discontinuously (the
    # RA wrap-around in the projection), so Plotly doesn't draw a spurious
    # line straight across the plot connecting the two edges.
    x_out, y_out = [x[0]], [y[0]]
    for i in range(1, len(x)):
        if (x[i] - x[i - 1]) ** 2 + (y[i] - y[i - 1]) ** 2 > 0.25:
            x_out.append(None)
            y_out.append(None)
        x_out.append(x[i])
        y_out.append(y[i])
    return x_out, y_out


def _group_holdings(holdings: list[dict]) -> list[dict]:
    """Collapse repeat observations (common for multi-epoch archives) into
    one group per (archive, instrument) pair — the raw per-row table was
    unreadable for stars with many visits."""
    groups: dict[tuple, dict] = {}
    order = []
    for h in holdings:
        key = (h["display_name"], h["instrument"])
        if key not in groups:
            groups[key] = {
                "display_name": h["display_name"],
                "instrument": h["instrument"],
                # Same hand-maintained lookup the Instruments tab uses --
                # see INSTRUMENT_RESOLVING_POWER's own comment for why it's
                # keyed by (archive, instrument) rather than instrument alone.
                "resolving_power": INSTRUMENT_RESOLVING_POWER.get(key, "—"),
                "observations": [],
            }
            order.append(key)
        groups[key]["observations"].append(h)
    return [groups[k] for k in order]


# Categorical palette for the wavelength-coverage chart below (dataviz
# skill's default 8-slot categorical order -- validated for adjacent-pair
# CVD/contrast under the "bars" pairlist, see references/palette.md).
# Assigned per-star (not globally across every archive_code) since no star
# in practice has holdings from more than a handful of archives at once.
WAVELENGTH_CHART_PALETTE = [
    '#2a78d6', '#eb6834', '#1baf7a', '#eda100',
    '#e87ba4', '#008300', '#4a3aa7', '#e34948',
]


def _pack_wavelength_rows(intervals: list[tuple[float, float]]) -> list[int]:
    """Greedy interval-graph-coloring row assignment for the wavelength
    chart below: sort by start, place each interval in the first existing
    row whose last-placed bar already ended (row_end <= this start), else
    open a new row. First-fit-by-start-order is optimal for interval
    graphs -- the row count it produces equals the maximum simultaneous
    overlap depth, which is exactly "share a row whenever two bars don't
    overlap, split only when they do" -- the chart's whole point."""
    order = sorted(range(len(intervals)), key=lambda i: intervals[i][0])
    row_end: list[float] = []
    rows = [0] * len(intervals)
    for i in order:
        start, end = intervals[i]
        for r, e in enumerate(row_end):
            if e <= start:
                row_end[r] = end
                rows[i] = r
                break
        else:
            row_end.append(end)
            rows[i] = len(row_end) - 1
    return rows


def _wavelength_coverage_bars(holdings_groups: list[dict]) -> dict | None:
    """Build the search page's wavelength-coverage chart data: one bar per
    (archive, instrument) group with a published range in
    INSTRUMENT_WAVELENGTH_RANGE_NM, packed onto as few y-rows as possible
    (see _pack_wavelength_rows). Groups with no known range -- BeSS's
    hundreds of free-text amateur setups, a handful of obscure retired
    instruments, see INSTRUMENT_WAVELENGTH_RANGE_NM's own comment -- are
    silently skipped; returns None if nothing here could be plotted at all
    (so the template can omit the chart entirely rather than show an empty
    plot)."""
    bars = []
    for g in holdings_groups:
        coverage = INSTRUMENT_WAVELENGTH_RANGE_NM.get((g["display_name"], g["instrument"]))
        if coverage is None:
            continue
        bars.append({
            "archive": g["display_name"],
            "label": f"{g['display_name']} — {g['instrument']}",
            "resolving_power": g["resolving_power"],
            "wave_min": coverage[0],
            "wave_max": coverage[1],
        })
    if not bars:
        return None

    rows = _pack_wavelength_rows([(b["wave_min"], b["wave_max"]) for b in bars])
    archive_order: list[str] = []
    for b in bars:
        if b["archive"] not in archive_order:
            archive_order.append(b["archive"])

    for b, row in zip(bars, rows):
        b["row"] = row
        b["color"] = WAVELENGTH_CHART_PALETTE[archive_order.index(b["archive"]) % len(WAVELENGTH_CHART_PALETTE)]

    return {"bars": bars, "n_rows": max(rows) + 1}


# How many stars the CMD plots as individually-clickable points. The
# underlying list (the CMD_SAMPLE_SIZE most-observed stars with valid
# photometry) is precomputed by scripts.export_to_parquet, not sampled here
# — this constant is just for the page's descriptive text; the actual cap is
# baked into that export's LIMIT.
CMD_SAMPLE_SIZE = 30000

# Sky Map still uses a genuine random sample (unlike CMD) — the catalog is
# 1.4M+ and growing toward several million, so shipping every star to the
# browser would mean an ever-growing multi-MB payload and more points than
# any charting library renders interactively without WebGL trouble. USING
# SAMPLE applies after the WHERE filter, not before, so this is a sample of
# valid points, not valid points among a sample of everything.
#
# Descriptive text only -- the actual sampling is precomputed by
# scripts.export_to_parquet's SKY_SAMPLE_QUERY into sky_sample.parquet (same
# "duplicated constant, just for the caption" pattern as CMD_SAMPLE_SIZE).
# Used to be a live `USING SAMPLE n` against the full `stars` table on every
# /sky request -- confirmed live as ~27s per request (DuckDB's remote-parquet
# reader has to scan nearly the whole ~500MB+ file to sample from a 9.8M-row
# table with no filter pushdown available), the dominant cost in "webapp is
# sluggish switching tabs".
SKY_SAMPLE_SIZE = 30000

# Radial (cone) search by sky position, below the name search box. The
# webapp has no q3c/spatial index available (that's Postgres-side, used
# only by sync.matcher during ingest — see this module's docstring for why
# the webapp reads a plain Parquet snapshot instead), so this is a
# straight-up great-circle-distance computation over the whole `stars`
# table rather than an indexed query. Same cost profile as /sky's existing
# live full-table read (1.4M+ rows and growing) — DuckDB vectorizes the trig
# over the whole column fast enough for interactive use; the dec-band
# pre-filter below cuts most of that work for a typical small-radius search.
RADIAL_SEARCH_DEFAULT_RADIUS_ARCMIN = 5.0
RADIAL_SEARCH_MAX_RADIUS_ARCMIN = 300.0  # 5 degrees
RADIAL_SEARCH_MAX_RESULTS = 200


def _radial_search(ra_str: str, dec_str: str, radius_str: str, export_csv: bool):
    try:
        ra_val = float(ra_str) % 360.0
        dec_val = float(dec_str)
    except ValueError:
        return _render_radial(ra_str, dec_str, radius_str, radial_error="RA and Dec must be decimal degrees.")
    if not (-90.0 <= dec_val <= 90.0):
        return _render_radial(ra_str, dec_str, radius_str, radial_error="Dec must be between -90 and 90 degrees.")

    radius_arcmin = RADIAL_SEARCH_DEFAULT_RADIUS_ARCMIN
    if radius_str:
        try:
            radius_arcmin = float(radius_str)
        except ValueError:
            return _render_radial(ra_str, dec_str, radius_str, radial_error="Radius must be a number of arcminutes.")
    radius_arcmin = max(0.01, min(radius_arcmin, RADIAL_SEARCH_MAX_RADIUS_ARCMIN))
    radius_deg = radius_arcmin / 60.0

    cur = get_cursor()
    cur.execute(
        """
        SELECT gaia_source_id, bsc_hr_number, ra, dec, phot_g_mean_mag, name_aliases, input_name, sep_deg
        FROM (
            SELECT gaia_source_id, bsc_hr_number, ra, dec, phot_g_mean_mag, name_aliases, input_name,
                degrees(acos(least(1.0, greatest(-1.0,
                    sin(radians(dec)) * sin(radians(?)) +
                    cos(radians(dec)) * cos(radians(?)) * cos(radians(ra - ?))
                )))) AS sep_deg
            FROM stars
            WHERE ra IS NOT NULL AND dec IS NOT NULL AND dec BETWEEN ? AND ?
        ) t
        WHERE sep_deg <= ?
        ORDER BY sep_deg
        LIMIT ?
        """,
        [dec_val, dec_val, ra_val, dec_val - radius_deg, dec_val + radius_deg, radius_deg, RADIAL_SEARCH_MAX_RESULTS],
    )
    rows = _rows_as_dicts(cur)
    for r in rows:
        r["known_as"] = _known_as(r)
        r["sep_arcsec"] = r["sep_deg"] * 3600.0
        # A BSC5 star with no credible Gaia counterpart has no gaia_source_id
        # for the click-through ?q= link above to be -- fall back to its HR
        # number, which this route's own lookup now understands too.
        r["search_id"] = r["gaia_source_id"] if r["gaia_source_id"] is not None else r["bsc_hr_number"]

    if export_csv:
        return _csv_response(
            ["gaia_source_id", "known_as", "ra", "dec", "sep_arcsec", "phot_g_mean_mag"],
            rows,
            f"spectra_pointer_radial_ra{ra_val:.5f}_dec{dec_val:.5f}_r{radius_arcmin:g}arcmin.csv",
        )

    return _render_radial(ra_str, dec_str, str(radius_arcmin), radial_results=rows, radius_display=radius_arcmin)


def _render_radial(ra_str, dec_str, radius_str, radial_error=None, radial_results=None, radius_display=None):
    return render_template_string(
        PAGE_TEMPLATE, query=None, star=None, holdings=None, wavelength_chart=None,
        error=None, resolved_source_id=None,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
        active_tab="search",
        ra=ra_str, dec=dec_str, radius=radius_str,
        radial_searched=True, radial_error=radial_error, radial_results=radial_results,
        radius_display=radius_display if radius_display is not None else radius_str,
    )

NAV_HTML = """
  <nav class="tabs">
    <a href="/" class="{{ 'active' if active_tab == 'search' else '' }}">Search</a>
    <a href="/cmd" class="{{ 'active' if active_tab == 'cmd' else '' }}">Color-Magnitude Diagram</a>
    <a href="/timeplots" class="{{ 'active' if active_tab == 'timeplots' else '' }}">Leaderboard</a>
    <a href="/instruments" class="{{ 'active' if active_tab == 'instruments' else '' }}">Instruments</a>
    <a href="/status" class="{{ 'active' if active_tab == 'archive_status' else '' }}">Archive Status</a>
    <a href="/triage" class="{{ 'active' if active_tab == 'triage' else '' }}">Triage</a>
    <a href="/info" class="{{ 'active' if active_tab == 'info' else '' }}">More Info</a>
    <a href="/citation" class="{{ 'active' if active_tab == 'citation' else '' }}">Citation</a>
  </nav>
"""

SHARED_STYLE = """
    body { font-family: monospace; max-width: 800px; margin: 2rem auto; padding: 0 1rem; color: #000; background: #fff; }
    dl { display: grid; grid-template-columns: max-content 1fr; gap: 0.2rem 1rem; }
    dt { font-weight: bold; }
    dd { margin: 0; }
    table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
    th, td { text-align: left; padding: 0.3rem 0.5rem; border-bottom: 1px solid #000; }
    a { color: #000; }
    .error { font-weight: bold; border: 1px solid #000; padding: 0.5rem; }
    .note { font-style: italic; }
    textarea { width: 100%; font-family: monospace; }
    .search-input { width: 70%; max-width: 500px; font-family: monospace; font-size: 1rem; padding: 0.3rem; }
    hr { margin: 2rem 0; border: none; border-top: 1px solid #000; }
    details { border: 1px solid #000; margin-top: 0.5rem; padding: 0.3rem 0.5rem; }
    details table { margin-top: 0.3rem; }
    summary { cursor: pointer; font-weight: bold; }
    summary.summary-row { display: flex; justify-content: space-between; align-items: baseline; gap: 1rem; }
    summary.summary-row .summary-count { flex-shrink: 0; white-space: nowrap; font-weight: normal; }
    nav.tabs { display: flex; gap: 0; border-bottom: 1px solid #000; margin-bottom: 1.5rem; }
    nav.tabs a { text-decoration: none; padding: 0.5rem 1rem; border: 1px solid #000; border-bottom: none;
                 margin-right: 0.3rem; color: #000; }
    nav.tabs a.active { font-weight: bold; background: #000; color: #fff; }
    .site-header { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: 1.5rem; }
    .site-header h1 { margin: 0; }
    .logo-placeholder { flex-shrink: 0; width: 48px; height: 48px; border: 1px solid #000;
                         border-radius: 4px; object-fit: cover; }
"""

PAGE_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer</title>
  <style>""" + SHARED_STYLE + """
    #wavelength-plot { width: 100%; margin-top: 0.5rem; }
  </style>
  {% if wavelength_chart %}<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>{% endif %}
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <p class="note">A numeric search is interpreted as a Gaia source_id or a Bright Star Catalogue (HR) number.</p>
  <form method="get" action="">
    <input type="text" name="q" class="search-input" placeholder="Gaia source_id or star name, e.g. Proxima Centauri" value="{{ query or '' }}" autofocus>
    <button type="submit">Search</button>
  </form>

  <p class="note">Or search by sky position:</p>
  <form method="get" action="" class="radial-form">
    <input type="text" name="ra" placeholder="RA (deg)" value="{{ ra or '' }}" size="10">
    <input type="text" name="dec" placeholder="Dec (deg)" value="{{ dec or '' }}" size="10">
    <input type="text" name="radius" placeholder="Radius (arcmin, default {{ '%g'|format(5) }})" value="{{ radius or '' }}" size="20">
    <button type="submit">Search radius</button>
  </form>

  {% if radial_searched %}
    {% if radial_error %}
      <p class="error">Error: {{ radial_error }}</p>
    {% else %}
      <p>{{ radial_results|length }} star{{ "s" if radial_results|length != 1 else "" }} found within {{ '%g'|format(radius_display|float) }}&#39; of RA {{ ra }}, Dec {{ dec }}.
        {% if radial_results %} <a href="?ra={{ ra }}&amp;dec={{ dec }}&amp;radius={{ radius_display }}&amp;format=csv">Download as CSV</a>{% endif %}
      </p>
      {% if radial_results %}
      <table>
        <tr><th>Star</th><th>RA</th><th>Dec</th><th>Separation</th><th>G mag</th></tr>
        {% for r in radial_results %}
        <tr>
          <td><a href="?q={{ r.search_id }}">{{ r.known_as }}</a></td>
          <td>{{ "%.5f"|format(r.ra) }}</td>
          <td>{{ "%.5f"|format(r.dec) }}</td>
          <td>{{ '%.1f"'|format(r.sep_arcsec) }}</td>
          <td>{{ r.phot_g_mean_mag if r.phot_g_mean_mag is not none else "—" }}</td>
        </tr>
        {% endfor %}
      </table>
      {% endif %}
    {% endif %}
  {% endif %}

  {% if resolved_source_id %}
    <p>"{{ query }}" resolved via SIMBAD to source_id {{ resolved_source_id }}.</p>
  {% endif %}

  {% if error %}
    <p class="error">Error: {{ error }}</p>
  {% endif %}

  {% if star %}
    <dl>
      {% if star.gaia_source_id is not none %}
      <dt>Gaia source_id</dt><dd>{{ star.gaia_source_id }}</dd>
      <dt>SIMBAD</dt><dd><a href="https://simbad.cds.unistra.fr/simbad/sim-id?Ident=Gaia+DR3+{{ star.gaia_source_id }}" target="_blank" rel="noopener">open</a></dd>
      {% else %}
      <dt>Gaia source_id</dt><dd>— (no credible Gaia counterpart; tracked via Bright Star Catalogue HR {{ star.bsc_hr_number }})</dd>
      <dt>SIMBAD</dt><dd><a href="https://simbad.cds.unistra.fr/simbad/sim-id?Ident=HR+{{ star.bsc_hr_number }}" target="_blank" rel="noopener">open</a></dd>
      {% endif %}
      <dt>RA, Dec</dt><dd>{{ "%.6f"|format(star.ra) }}, {{ "%.6f"|format(star.dec) }}</dd>
      <dt>G mag</dt><dd>{{ star.phot_g_mean_mag if star.phot_g_mean_mag is not none else "—" }}</dd>
      <dt>Gaia XP continuous</dt><dd>{{ "yes" if star.has_xp_continuous else "no" }}</dd>
      <dt>Known as</dt><dd>{{ (star.name_aliases | join(", ")) if star.name_aliases else (star.input_name or "—") }}</dd>
    </dl>

    {% if wavelength_chart %}
      <h3>Wavelength coverage</h3>
      <p class="note">Each bar is one archive/instrument's published wavelength range, packed onto as few rows as possible -- hover a bar for its name and resolving power.</p>
      <div id="wavelength-plot"></div>
      <script>
        (function() {
          var bars = {{ wavelength_chart.bars | tojson }};
          var nRows = {{ wavelength_chart.n_rows }};
          var trace = {
            type: 'bar',
            orientation: 'h',
            base: bars.map(function(b) { return b.wave_min; }),
            x: bars.map(function(b) { return b.wave_max - b.wave_min; }),
            y: bars.map(function(b) { return b.row; }),
            width: 0.7,
            marker: { color: bars.map(function(b) { return b.color; }) },
            hovertext: bars.map(function(b) {
              return b.label + '<br>' + b.resolving_power + '<br>' +
                b.wave_min + '–' + b.wave_max + ' nm';
            }),
            hoverinfo: 'text',
            showlegend: false,
          };
          Plotly.newPlot('wavelength-plot', [trace], {
            barmode: 'overlay',
            height: Math.max(60, 20 + nRows * 22) + 20,
            margin: { l: 8, r: 8, t: 4, b: 48 },
            xaxis: { title: { text: 'Wavelength (nm)', standoff: 12 }, type: 'log', automargin: true },
            yaxis: { visible: false, range: [-0.7, nRows - 0.3] },
          }, { responsive: true, displayModeBar: false });
        })();
      </script>
    {% endif %}

    {% if holdings %}
      <p><a href="?q={{ star_search_id }}&amp;format=csv">Download holdings as CSV</a></p>
      {% for g in holdings %}
      <details{% if holdings|length == 1 %} open{% endif %}>
        <summary class="summary-row">
          <span>{{ g.display_name }} — {{ g.instrument or "—" }}{% if g.instrument and g.resolving_power != "—" %} ({{ g.resolving_power }}){% endif %}</span>
          <span class="summary-count">{{ g.observations|length }} observation{{ "s" if g.observations|length != 1 else "" }}</span>
        </summary>
        <table>
          <tr><th>Date</th><th>Match</th><th>Method</th><th>Reduction</th><th>Link</th></tr>
          {% for h in g.observations %}
          <tr>
            <td>{{ h.obs_date or "—" }}</td>
            <td>{{ h.match_status }}</td>
            <td>{{ h.match_method }}</td>
            <td>{{ h.reduction_status }}</td>
            <td><a href="{{ h.archive_url }}" target="_blank" rel="noopener">open</a></td>
          </tr>
          {% endfor %}
        </table>
      </details>
      {% endfor %}
    {% else %}
      <p>No spectroscopy holdings found for this star yet.</p>
    {% endif %}
  {% endif %}

  <hr>

  <h2>Batch lookup</h2>
  <p class="note">Paste or upload a list of Gaia source_ids and/or star names, one per line. Name lookups (anything non-numeric) are capped at {{ max_name_lookups }} per batch; source_id lookups are not.</p>
  <form method="post" action="batch" enctype="multipart/form-data">
    <textarea name="names" rows="8" placeholder="4472832130942575872&#10;Proxima Centauri&#10;Barnard's Star"></textarea>
    <p><input type="file" name="file" accept=".txt,.csv"></p>
    <button type="submit">Look up list</button>
    <button type="submit" name="format" value="csv">Look up and download CSV</button>
  </form>

  {% if batch_error %}
    <p class="error">Error: {{ batch_error }}</p>
  {% endif %}

  {% if batch_note %}
    <p class="note">{{ batch_note }}</p>
  {% endif %}

  {% if batch_results %}
    <table>
      <tr><th>Query</th><th>source_id</th><th>Tracked</th><th>Known as</th><th>Holdings</th></tr>
      {% for r in batch_results %}
      <tr>
        <td>{{ r.query }}</td>
        <td>{% if r.source_id %}<a href="/?q={{ r.source_id }}">{{ r.source_id }}</a>{% else %}—{% endif %}</td>
        <td>{{ r.status }}</td>
        <td>{{ r.known_as or "—" }}</td>
        <td>{{ r.holdings_count if r.holdings_count is not none else "—" }}</td>
      </tr>
      {% endfor %}
    </table>
  {% endif %}

</body>
</html>
"""


def _blank(query=None, error=None, resolved_source_id=None):
    return render_template_string(
        PAGE_TEMPLATE, query=query, star=None, holdings=None, wavelength_chart=None,
        error=error, resolved_source_id=resolved_source_id,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
        active_tab="search",
    )


def _blank_batch(batch_error=None, batch_note=None, batch_results=None):
    return render_template_string(
        PAGE_TEMPLATE, query=None, star=None, holdings=None, wavelength_chart=None,
        error=None, resolved_source_id=None,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=batch_error, batch_note=batch_note, batch_results=batch_results,
        active_tab="search",
    )


# SIMBAD's own "ids" field -- what a source_catalog='bsc5' star added via
# add_bsc_star gets its name_aliases from verbatim (see ingest.add_star) --
# doesn't use bare common names: Arcturus shows up as "NAME Arcturus", and
# its Bayer designation as "* alf Boo". It's also inconsistently spaced --
# "HR  5340", two spaces, not "HR 5340" -- unlike the Gaia-path seeding in
# scripts/seed_bright_star_catalog.py, which does add an exact "HR <n>"
# alias but only for stars resolved to a gaia_source_id. Confirmed live:
# without this normalization, searching "Arcturus" (a real production BSC5
# star) fell through to external SIMBAD/Gaia resolution and 404'd, because
# neither of its cached aliases match that string exactly.
_NAME_PREFIX_RE = re.compile(r"^(NAME|\*)\s+", re.IGNORECASE)


def _normalize_star_name(s: str) -> str:
    return re.sub(r"\s+", " ", _NAME_PREFIX_RE.sub("", s.strip())).lower()


def _lookup_local_star(cur: duckdb.DuckDBPyConnection, query: str) -> dict | None:
    """Match `query` against a star already tracked locally -- by
    gaia_source_id/bsc_hr_number for a numeric query, or by any cached name
    alias (case- and formatting-insensitive, see _normalize_star_name)
    otherwise -- before ever going out to SIMBAD.

    This is the only way to find a source_catalog='bsc5' star by name at
    all: those have no gaia_source_id for resolve_gaia_source_id to resolve
    to (see db/migrations/0001_star_id_surrogate_key.sql), and for the ~18
    with zero Gaia sources within 30" (e.g. Arcturus), the external
    resolution path fails outright rather than just returning a different
    star.
    """
    if query.isdigit():
        # Two separate single-column queries, not one `OR`-joined query --
        # confirmed live that combining them defeats DuckDB's Parquet
        # predicate pushdown entirely (bsc_hr_number has no usable row-group
        # stats, being NULL for all but a handful of bsc5 rows, so the
        # planner can't rule either branch out per row group and falls back
        # to scanning the whole file). Splitting them lets the
        # gaia_source_id lookup -- the overwhelmingly common case -- use the
        # export's sort order for row-group pruning (see
        # scripts/export_to_parquet.py's `stars` ORDER BY) instead of
        # pulling the entire multi-hundred-MB file into memory and OOMing
        # the Cloud Run container.
        n = int(query)
        cur.execute("SELECT * FROM stars WHERE gaia_source_id = ?", [n])
        rows = _rows_as_dicts(cur)
        if not rows:
            cur.execute("SELECT * FROM stars WHERE bsc_hr_number = ?", [n])
            rows = _rows_as_dicts(cur)
        if rows:
            return rows[0]

    # Goes through star_name_index (a precomputed normalized-name ->
    # identifier table, see scripts/export_to_parquet.py's
    # STAR_NAME_INDEX_QUERY) rather than filtering `stars` directly on
    # normalize(input_name)/name_aliases -- confirmed live that wrapping the
    # filtered columns in a function defeats Parquet's row-group pruning
    # entirely, so *every* name search (i.e. nearly every real query) pulled
    # nearly the entire multi-hundred-MB stars.parquet over HTTP. The index
    # is small enough that a full scan of it is cheap regardless of
    # pruning; this just resolves the name to an identifier, then reuses
    # the numeric branch's already-pruning-friendly lookup above.
    normalized_query = _normalize_star_name(query)
    cur.execute(
        "SELECT gaia_source_id, bsc_hr_number FROM star_name_index WHERE normalized_name = ? LIMIT 1",
        [normalized_query],
    )
    idx_row = cur.fetchone()
    if idx_row is None:
        return None
    gaia_source_id, bsc_hr_number = idx_row
    if gaia_source_id is not None:
        cur.execute("SELECT * FROM stars WHERE gaia_source_id = ?", [gaia_source_id])
    else:
        cur.execute("SELECT * FROM stars WHERE bsc_hr_number = ?", [bsc_hr_number])
    rows = _rows_as_dicts(cur)
    return rows[0] if rows else None


@app.route("/")
def search():
    query = request.args.get("q", "").strip()
    export_csv = request.args.get("format", "").strip().lower() == "csv"

    ra_str = request.args.get("ra", "").strip()
    dec_str = request.args.get("dec", "").strip()
    if not query and (ra_str or dec_str):
        return _radial_search(ra_str, dec_str, request.args.get("radius", "").strip(), export_csv)

    if not query:
        return _blank()

    cur = get_cursor()
    resolved_source_id = None
    star = _lookup_local_star(cur, query)
    if star is None:
        if query.isdigit():
            return _blank(query=query, error=f"No tracked star with source_id {query}.")
        try:
            source_id = resolve_gaia_source_id(query)
        except DALServiceError:
            # Confirmed live during this project: SIMBAD's TAP service goes
            # down periodically. Say so plainly rather than a generic error
            # or (worse) a misleading "not found".
            return _blank(query=query, error="SIMBAD is currently unavailable — try again in a bit.")
        except ValueError as e:
            return _blank(query=query, error=str(e))
        resolved_source_id = source_id

        cur.execute("SELECT * FROM stars WHERE gaia_source_id = ?", [source_id])
        rows = _rows_as_dicts(cur)
        star = rows[0] if rows else None
        if star is None:
            return _blank(
                query=query,
                error=f"No tracked star with source_id {source_id}.",
                resolved_source_id=resolved_source_id,
            )

    # gaia_source_id is purely a display value from here on -- it can
    # legitimately be NULL for a source_catalog='bsc5' star (same reasoning
    # as timeplots', see 751327c). star_search_id is what round-trips back
    # through this same route's own ?q= lookup (bsc_hr_number for a BSC5
    # star, since it has no gaia_source_id for that to be).
    source_id = star["gaia_source_id"]
    star_search_id = source_id if source_id is not None else star["bsc_hr_number"]

    cur.execute(
        """
        SELECT h.*, a.display_name
        FROM spectroscopy_holdings h
        JOIN archives a ON a.archive_code = h.archive_code
        WHERE h.star_id = ?
        ORDER BY a.display_name, h.instrument, h.obs_date
        """,
        [star["star_id"]],
    )
    raw_holdings = _rows_as_dicts(cur)

    if export_csv:
        known_as = ", ".join(star["name_aliases"]) if star["name_aliases"] else star["input_name"]
        for h in raw_holdings:
            h["query"] = query
            h["source_id"] = source_id
            h["status"] = "tracked"
            h["known_as"] = known_as
            h["archive"] = h["display_name"]
        return _csv_response(
            ["query", "source_id", "status", "known_as",
             "archive", "instrument", "obs_date", "match_status", "match_method", "reduction_status", "archive_url"],
            raw_holdings,
            f"spectra_pointer_holdings_{source_id if source_id is not None else star['star_id']}.csv",
        )

    holdings = _group_holdings(raw_holdings)
    wavelength_chart = _wavelength_coverage_bars(holdings)

    return render_template_string(
        PAGE_TEMPLATE, query=query, star=star, holdings=holdings, star_search_id=star_search_id,
        wavelength_chart=wavelength_chart,
        error=None, resolved_source_id=resolved_source_id,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
        active_tab="search",
    )


CMD_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Color-Magnitude Diagram</title>
  <style>""" + SHARED_STYLE + """
    #cmd-plot { width: 100%; height: 700px; margin-top: 1rem; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <p class="note">Gaia color-magnitude diagram — the {{ "{:,}".format(sample_size) }} most-observed tracked stars with valid BP-RP color and a positive parallax (needed for absolute magnitude). Click a point to see that star's holdings.</p>
  {% if bp_rp %}
    <div id="cmd-plot"></div>
    <script>
      const bpRp = {{ bp_rp | tojson }};
      const absGMag = {{ abs_g_mag | tojson }};
      // Gaia source_ids are 19-digit integers, well past JS's 53-bit safe-
      // integer range — serialized as strings (never as JSON numbers) so
      // they can't get silently rounded by the browser.
      const sourceIds = {{ source_ids | tojson }};
      const labels = {{ labels | tojson }};

      // Spectral-class boundaries (Bp-Rp) from Mamajek's dwarf color/Teff
      // table -- https://github.com/emamajek/SpectralType/blob/master/EEM_dwarf_UBVIJHK_colors_Teff.txt,
      // 2024.05.15. O5V (not a true class start -- Gaia BP/RP has no
      // earlier calibration, same reasoning as before) and B0V (no
      // tabulated Bp-Rp for any B0-B8 dwarf) are extrapolated from the
      // B9V-F0V rows via a linear B-V -> Bp-Rp fit (slope 1.36, intercept
      // -0.028); A0V/F0V/G0V/K0V/M0V/M9V are read straight from the table.
      const SPECTRAL_TYPE_BOUNDS = [-0.47, -0.44, -0.037, 0.377, 0.784, 0.983, 1.84, 4.78];

      // Maps a raw Bp-Rp value to a position in [0, 1] with each of the 7
      // classes above (the gaps between consecutive bounds) getting an
      // equal 1/7 share, linearly interpolated within its own real Bp-Rp
      // span -- rather than the whole colormap being spent proportionally
      // to raw Bp-Rp, which is what a plain cmin/cmax mapping does and
      // which starved O-B-A-F (barely 1.3 mag wide combined) of almost all
      // visible color change while K-M (nearly 3 mag wide) ate most of the
      // ramp. Bp-Rp outside this domain clamps to the nearest end rather
      // than inventing color for spectral types this table doesn't
      // usefully cover (see the CMD colorscale PR history for why L/T/Y
      // aren't included).
      function stretchBpRpBySpectralType(bpRp) {
        const n = SPECTRAL_TYPE_BOUNDS.length - 1;
        if (bpRp <= SPECTRAL_TYPE_BOUNDS[0]) return 0;
        if (bpRp >= SPECTRAL_TYPE_BOUNDS[n]) return 1;
        for (let i = 0; i < n; i++) {
          const lo = SPECTRAL_TYPE_BOUNDS[i], hi = SPECTRAL_TYPE_BOUNDS[i + 1];
          if (bpRp <= hi) return (i + (bpRp - lo) / (hi - lo)) / n;
        }
        return 1;
      }
      const stretchedColor = bpRp.map(stretchBpRpBySpectralType);

      Plotly.newPlot('cmd-plot', [
        {
          x: bpRp,
          y: absGMag,
          text: labels,
          hovertemplate: '%{text}<extra></extra>',
          mode: 'markers',
          type: 'scattergl',
          marker: {
            size: 5, opacity: 0.75, color: stretchedColor,
            // ColorBrewer "Spectral" reversed (violet/blue -> red) -- a
            // smooth, continuous rainbow rather than a handful of
            // hand-picked anchor colors, applied to stretchedColor (see
            // above) rather than raw bpRp so the smoothness is spent where
            // spectral types actually are instead of where raw Bp-Rp
            // magnitude happens to be.
            colorscale: [
              [0, '#5e4fa2'], [0.1, '#3288bd'], [0.2, '#66c2a5'], [0.3, '#abdda4'],
              [0.4, '#e6f598'], [0.5, '#ffffbf'], [0.6, '#fee08b'], [0.7, '#fdae61'],
              [0.8, '#f46d43'], [0.9, '#d53e4f'], [1, '#9e0142'],
            ],
            cmin: 0, cmax: 1,
            line: { width: 0.3, color: 'rgba(0,0,0,0.4)' },
          },
        },
        {
          // Invisible dummy trace bound to xaxis2 -- Plotly never draws an
          // axis that no trace references, even one declared purely for its
          // tick labels. x/y values are irrelevant since xaxis2's `matches:
          // 'x'` below pins its range to the primary axis regardless of
          // what this trace itself would otherwise autorange to.
          x: [0], y: [0], xaxis: 'x2', yaxis: 'y',
          mode: 'markers', marker: { opacity: 0 }, showlegend: false, hoverinfo: 'skip',
        },
      ], {
        xaxis: { title: 'BP - RP (mag)' },
        // Same Mamajek Bp-Rp-at-mid-type anchors as the marker colorscale
        // above, just three more of them (B/A/F) for a legible top axis --
        // O5V/B5V extrapolated (see the colorscale comment), A5V/F5V/G5V/
        // K5V/M5V tabulated directly.
        xaxis2: {
          overlaying: 'x', matches: 'x', side: 'top',
          tickmode: 'array',
          tickvals: [-0.47, -0.24, 0.194, 0.587, 0.85, 1.43, 3.35],
          ticktext: ['O', 'B', 'A', 'F', 'G', 'K', 'M'],
          title: 'Spectral type (dwarfs, Mamajek)',
        },
        yaxis: { title: 'Absolute G magnitude', autorange: 'reversed' },
        hovermode: 'closest',
      }, { responsive: true });
      document.getElementById('cmd-plot').on('plotly_click', function(data) {
        if (data.points[0].curveNumber !== 0) return;
        const idx = data.points[0].pointIndex;
        window.location.href = '/?q=' + sourceIds[idx];
      });
    </script>
  {% else %}
    <p>No stars with both BP/RP photometry and a positive parallax yet.</p>
  {% endif %}
</body>
</html>
"""


@app.route("/cmd")
def cmd():
    # cmd_stars is precomputed by scripts.export_to_parquet — see that
    # module for why (same reasoning as the Leaderboard: ranking by
    # observation count needs a join against the ever-growing holdings
    # table, which shouldn't happen on every request in a memory-capped
    # container). Already the CMD_SAMPLE_SIZE most-observed stars, in no
    # particular order beyond that.
    cur = get_cursor()
    cur.execute("SELECT gaia_source_id, bp_rp, abs_g_mag, label FROM cmd_stars")
    rows = _rows_as_dicts(cur)
    return render_template_string(
        CMD_TEMPLATE,
        bp_rp=[r["bp_rp"] for r in rows],
        abs_g_mag=[r["abs_g_mag"] for r in rows],
        source_ids=[str(r["gaia_source_id"]) for r in rows],
        labels=[r["label"] for r in rows],
        sample_size=CMD_SAMPLE_SIZE,
        active_tab="cmd",
    )


SKY_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Sky Map</title>
  <style>""" + SHARED_STYLE + """
    #sky-plot { width: 100%; height: 700px; margin-top: 1rem; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <p class="note">An Aitoff-projection all-sky map of a random sample of up to {{ "{:,}".format(sample_size) }} tracked stars — brighter stars (lower G mag) drawn larger, like a real star chart. The gray band is the Galactic plane (computed, not a photograph — see the note in the page source). Scroll to zoom, click a point to see that star's holdings.</p>
  {% if x %}
    <div id="sky-plot"></div>
    <script>
      const x = {{ x | tojson }};
      const y = {{ y | tojson }};
      const sizes = {{ sizes | tojson }};
      const sourceIds = {{ source_ids | tojson }};
      const labels = {{ labels | tojson }};
      const galX = {{ galactic_x | tojson }};
      const galY = {{ galactic_y | tojson }};
      Plotly.newPlot('sky-plot', [
        {
          x: galX, y: galY,
          mode: 'lines',
          line: { color: 'rgba(120,120,120,0.5)', width: 14 },
          hoverinfo: 'skip',
          showlegend: false,
        },
        {
          x: x, y: y,
          text: labels,
          hovertemplate: '%{text}<extra></extra>',
          mode: 'markers',
          type: 'scattergl',
          marker: { size: sizes, opacity: 0.85, color: '#000' },
        },
      ], {
        xaxis: { showticklabels: false, zeroline: false, title: 'Right Ascension', scaleanchor: 'y' },
        yaxis: { showticklabels: false, zeroline: false, title: 'Declination' },
        hovermode: 'closest',
      }, { responsive: true, scrollZoom: true });
      document.getElementById('sky-plot').on('plotly_click', function(data) {
        const idx = data.points[0].pointIndex;
        if (data.points[0].curveNumber !== 1) return;
        window.location.href = '/?q=' + sourceIds[idx];
      });
    </script>
  {% else %}
    <p>No stars with position and G magnitude yet.</p>
  {% endif %}
</body>
</html>
"""


@app.route("/sky")
def sky():
    cur = get_cursor()
    cur.execute("SELECT gaia_source_id, ra, dec, phot_g_mean_mag, known_as FROM sky_sample")
    rows = _rows_as_dicts(cur)
    x, y = _aitoff_project([r["ra"] for r in rows], [r["dec"] for r in rows])
    # Brighter (lower mag) stars drawn bigger, clipped to a sane pixel range.
    sizes = [max(1.5, min(10.0, 12.0 - r["phot_g_mean_mag"])) for r in rows]
    galactic_x, galactic_y = _galactic_plane_xy()
    return render_template_string(
        SKY_TEMPLATE,
        x=x, y=y, sizes=sizes,
        source_ids=[str(r["gaia_source_id"]) for r in rows],
        labels=[r["known_as"] for r in rows],
        galactic_x=galactic_x, galactic_y=galactic_y,
        sample_size=SKY_SAMPLE_SIZE,
        active_tab="sky",
    )


TIMEPLOTS_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Leaderboard</title>
  <style>""" + SHARED_STYLE + """
    #cumulative-plot, #period-plot { width: 100%; height: 500px; margin-top: 1rem; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <p class="note">Fixed 6-month periods. At each period, two top-10 lists are computed: the 10 stars with the most cumulative (all-time-so-far) observations, and the 10 with the most observations within that period alone. Every star that ever broke into either list, at any period, gets a line in both charts below — so there can be more than 10 lines total, and a line can start partway through the timeline (whenever that star first qualified) and stop appearing again once it drops out of that period's top 10, rather than dragging a stale line across the whole chart. Only counts holdings with a known observation date — some archives (DESI, SDSS-V) don't report per-observation dates at all, so a star's true total (see Stats below) can be higher than what's reflected here. Log scale, so a period with zero observations for a star just leaves a gap rather than a dip to zero.</p>
  <h2>Cumulative observations</h2>
  {% if cumulative_traces %}
    <div id="cumulative-plot"></div>
    <script>
      const periodLabels = {{ period_labels | tojson }};
      const cumulativeSourceIds = {{ cumulative_traces | tojson }}.map(t => t.source_id);
      const cumulativeTraces = {{ cumulative_traces | tojson }}.map(t => ({
        x: periodLabels, y: t.counts, name: t.label,
        mode: 'lines+markers', line: { shape: 'spline' }, marker: { size: 4 }, type: 'scatter',
        connectgaps: false,
        hovertemplate: '%{fullData.name}<extra></extra>',
      }));
      Plotly.newPlot('cumulative-plot', cumulativeTraces, {
        xaxis: { title: 'Period' },
        yaxis: { title: 'Cumulative observations (log scale)', type: 'log' },
        hovermode: 'closest',
        showlegend: false,
      }, { responsive: true });
      document.getElementById('cumulative-plot').on('plotly_click', function(data) {
        const idx = data.points[0].curveNumber;
        window.location.href = '/?q=' + cumulativeSourceIds[idx];
      });
    </script>
  {% else %}
    <p>No dated observations yet.</p>
  {% endif %}

  <hr>
  <h2>Observations within each 6-month period</h2>
  {% if period_traces %}
    <div id="period-plot"></div>
    <script>
      const periodSourceIds = {{ period_traces | tojson }}.map(t => t.source_id);
      const periodTracesData = {{ period_traces | tojson }}.map(t => ({
        x: periodLabels, y: t.counts, name: t.label,
        mode: 'lines+markers', line: { shape: 'spline' }, marker: { size: 4 }, type: 'scatter',
        connectgaps: false,
        hovertemplate: '%{fullData.name}<extra></extra>',
      }));
      Plotly.newPlot('period-plot', periodTracesData, {
        xaxis: { title: 'Period' },
        yaxis: { title: 'Observations in period (log scale)', type: 'log' },
        hovermode: 'closest',
        showlegend: false,
      }, { responsive: true });
      document.getElementById('period-plot').on('plotly_click', function(data) {
        const idx = data.points[0].curveNumber;
        window.location.href = '/?q=' + periodSourceIds[idx];
      });
    </script>
  {% else %}
    <p>No dated observations yet.</p>
  {% endif %}

  <hr>
  <h2>Stats</h2>
  <dl>
    <dt>Tracked stars</dt><dd>{{ "{:,}".format(total_stars) }}</dd>
    <dt>Spectroscopy holdings</dt><dd>{{ "{:,}".format(total_holdings) }}</dd>
  </dl>

  <h3>Most observed stars</h3>
  <table>
    <tr><th>Star</th><th>Observations</th></tr>
    {% for r in most_observed %}
    <tr><td><a href="/?q={{ r.gaia_source_id if r.gaia_source_id is not none else r.bsc_hr_number }}">{{ r.known_as }}</a></td><td>{{ r.n }}</td></tr>
    {% endfor %}
  </table>

  <h3>Trending — most observed in the last {{ trending_years }} years</h3>
  {% if trending %}
    <table>
      <tr><th>Star</th><th>Observations</th></tr>
      {% for r in trending %}
      <tr><td><a href="/?q={{ r.gaia_source_id if r.gaia_source_id is not none else r.bsc_hr_number }}">{{ r.known_as }}</a></td><td>{{ r.n }}</td></tr>
      {% endfor %}
    </table>
  {% else %}
    <p class="note">Nothing in the last {{ trending_years }} years yet — most tracked holdings are decades-old archival spectra, and the bulk direct-Gaia-column archives (DESI, SDSS-V) don't carry per-observation dates at all, so "trending" will stay sparse until enough recently-dated archives (ESO, MAST, KOA, NOIRLab) are synced.</p>
  {% endif %}

  <h3>Holdings by archive</h3>
  <table>
    <tr><th>Archive</th><th>Holdings</th></tr>
    {% for r in by_archive %}
    <tr><td>{{ r.display_name }}</td><td>{{ "{:,}".format(r.n) }}</td></tr>
    {% endfor %}
  </table>

  <h3>Matches by method</h3>
  <table>
    <tr><th>Method</th><th>Count</th></tr>
    {% for r in by_method %}
    <tr><td>{{ r.match_method }}</td><td>{{ "{:,}".format(r.n) }}</td></tr>
    {% endfor %}
  </table>

  <h3>Nearest tracked stars</h3>
  <p class="note">By parallax (distance = 1000 / parallax_mas, no error cut applied — treat as approximate).</p>
  <table>
    <tr><th>Star</th><th>Distance (pc)</th></tr>
    {% for r in nearest %}
    <tr><td><a href="/?q={{ r.gaia_source_id if r.gaia_source_id is not none else r.bsc_hr_number }}">{{ r.known_as }}</a></td><td>{{ "%.2f"|format(r.distance_pc) }}</td></tr>
    {% endfor %}
  </table>

  <h3>Fastest movers</h3>
  <p class="note">By total proper motion. For reference, Barnard's Star (the fastest known) moves ~10,358 mas/yr.</p>
  <table>
    <tr><th>Star</th><th>Proper motion (mas/yr)</th></tr>
    {% for r in fastest_movers %}
    <tr><td><a href="/?q={{ r.gaia_source_id if r.gaia_source_id is not none else r.bsc_hr_number }}">{{ r.known_as }}</a></td><td>{{ "%.1f"|format(r.total_pm) }}</td></tr>
    {% endfor %}
  </table>

  <h3>Rough spectral-type distribution</h3>
  <p class="note">A simple BP-RP color bucketing, not real spectral classification — that needs actual spectroscopy, not one color index. Illustrative only.</p>
  <table>
    {% for r in spectral_types %}
    <tr>
      <td style="width: 4rem;">{{ r.bucket }}</td>
      <td><div style="background: #000; height: 1rem; width: {{ r.pct }}%;"></div></td>
      <td style="width: 6rem; text-align: right;">{{ "{:,}".format(r.n) }}</td>
    </tr>
    {% endfor %}
  </table>
</body>
</html>
"""


@app.route("/timeplots")
def timeplots():
    cur = get_cursor()

    # scripts.export_to_parquet precomputes the full top-5-per-period
    # selection (not just the raw counts) against live Postgres on morgan —
    # this table is already just "cast" stars x all periods, with within/
    # cumulative values already nulled out for periods a star isn't top-5
    # in. See that module for why: an earlier version of this route did the
    # top-5 selection here in Python, which meant sorted() over the full
    # (multi-million-star) population once per period — confirmed live as
    # what was actually OOMing the Cloud Run container, not the raw GROUP BY.
    cur.execute("SELECT star_id, gaia_source_id, bsc_hr_number, label, yr, half, within_n, cumulative_n FROM leaderboard ORDER BY star_id, yr, half")
    rows = _rows_as_dicts(cur)

    period_labels: list[str] = []
    cumulative_traces: list[dict] = []
    period_traces: list[dict] = []

    if rows:
        period_keys = sorted({(r["yr"], r["half"]) for r in rows})
        period_labels = [f"{yr} H{half}" for yr, half in period_keys]

        # Grouped by star_id, not gaia_source_id: a small number of BSC5-
        # sourced stars (bright naked-eye stars with no credible Gaia
        # counterpart -- see db/migrations/0001_star_id_surrogate_key.sql)
        # have a NULL gaia_source_id, which broke both of these -- sorted()
        # can't order None against int (confirmed live, 500ing every request
        # once one such star cracked a top-N list), and even fixed up, every
        # NULL-gaia_source_id star would collide on the same dict key and
        # stomp each other's data. star_id is the one identifier every
        # tracked star always has.
        by_star: dict[int, dict] = defaultdict(dict)
        labels_by_id: dict[int, str] = {}
        gaia_id_by_star: dict[int, int | None] = {}
        bsc_hr_by_star: dict[int, int | None] = {}
        for r in rows:
            by_star[r["star_id"]][(r["yr"], r["half"])] = r
            labels_by_id[r["star_id"]] = r["label"]
            gaia_id_by_star[r["star_id"]] = r["gaia_source_id"]
            bsc_hr_by_star[r["star_id"]] = r["bsc_hr_number"]

        for sid in sorted(by_star):
            by_period = by_star[sid]
            # Gaia source_ids are 19-digit integers, well past JS's 53-bit
            # safe-integer range — serialized as a string so a click-through
            # can't get silently rounded by the browser (same issue fixed
            # for the CMD/Sky Map click-throughs). BSC5 stars have no
            # gaia_source_id at all, so their click-through source_id falls
            # back to bsc_hr_number -- the same fallback the "Most observed"/
            # "Trending"/"Nearest"/"Fastest movers" tables and the `/` search
            # route itself (star_search_id) already use -- rather than
            # str(None), which used to send the click-through to a literal
            # "/?q=None" search.
            gaia_id = gaia_id_by_star[sid]
            source_id = str(gaia_id) if gaia_id is not None else str(bsc_hr_by_star[sid])
            cumulative_traces.append(
                {
                    "label": labels_by_id[sid],
                    "source_id": source_id,
                    "counts": [by_period[k]["cumulative_n"] if k in by_period else None for k in period_keys],
                }
            )
            period_traces.append(
                {
                    "label": labels_by_id[sid],
                    "source_id": source_id,
                    "counts": [by_period[k]["within_n"] if k in by_period else None for k in period_keys],
                }
            )

    # stats_summary is precomputed by scripts.export_to_parquet — most-
    # observed, trending, total_holdings, by-archive, by-method, nearest,
    # fastest-movers and spectral-type-histogram all used to be separate live
    # queries here, each scanning some or all of the ever-growing
    # spectroscopy_holdings/stars tables on every request. See that module
    # for the full reasoning (same OOM/full-scan-per-request risk as the
    # top-5-per-period selection above; nearest/fastest-movers/spectral-types
    # specifically confirmed live at multiple seconds each against `stars`
    # over HTTP, since ORDER BY/GROUP BY over an unfiltered remote Parquet
    # table can't skip any row groups).
    cur.execute("SELECT * FROM stats_summary")
    summary = _rows_as_dicts(cur)[0]
    most_observed = summary["most_observed"]
    trending = summary["trending"]
    total_stars = summary["total_stars"]
    total_holdings = summary["total_holdings"]
    by_archive = summary["by_archive"]
    by_method = summary["by_method"]
    trending_years = summary["trending_years"]
    nearest = summary["nearest"]
    fastest_movers = summary["fastest_movers"]

    counts_by_bucket = {r["bucket"]: r["n"] for r in summary["spectral_types"]}
    max_bucket_n = max(counts_by_bucket.values()) if counts_by_bucket else 0
    spectral_types = [
        {
            "bucket": b,
            "n": counts_by_bucket.get(b, 0),
            "pct": (counts_by_bucket.get(b, 0) / max_bucket_n * 100) if max_bucket_n else 0,
        }
        for b in SPECTRAL_BUCKETS
    ]

    return render_template_string(
        TIMEPLOTS_TEMPLATE,
        period_labels=period_labels,
        cumulative_traces=cumulative_traces,
        period_traces=period_traces,
        most_observed=most_observed, trending=trending, trending_years=trending_years,
        total_stars=total_stars, total_holdings=total_holdings,
        by_archive=by_archive, by_method=by_method,
        nearest=nearest, fastest_movers=fastest_movers, spectral_types=spectral_types,
        active_tab="timeplots",
    )


# Natural OBAFGKM order — GROUP BY doesn't preserve it, so the display order
# is applied in Python after querying.
SPECTRAL_BUCKETS = ["O/B (hot)", "A", "F", "G", "K", "M (cool)"]


def _known_as(row: dict) -> str:
    if row.get("name_aliases"):
        return row["name_aliases"][0]
    return row.get("input_name") or str(row["gaia_source_id"])


# Descriptive text only -- the actual sampling is baked into
# scripts.export_to_parquet's INSTRUMENT_SKY_SAMPLE_QUERY (same "duplicated
# constant, just for the caption" pattern as CMD_SAMPLE_SIZE above).
INSTRUMENT_SKY_SAMPLE_TOP_N = 12
INSTRUMENT_SKY_SAMPLE_PER_INSTRUMENT = 2000

# Resolving power (R = lambda/delta-lambda) per (archive display_name,
# instrument) -- like NOT_YET_TRACKED below, this can't be derived from the
# database (holdings don't carry it) so it's hand-maintained from each
# instrument's published specs. Keyed by (display_name, instrument) rather
# than instrument name alone because a few names collide across archives
# (e.g. "OSIRIS" is a different instrument at Keck vs. GTC). Many real
# spectrographs offer several gratings/modes with different R -- shown as a
# range where that's the common case rather than picking one mode
# arbitrarily. "n/a" marks instruments that are primarily imagers with no
# (or only a fixed low-R grism) spectroscopic mode; "—" marks instruments
# not yet looked up, mostly retired/obscure ones lacking an easily
# confirmed spec.
INSTRUMENT_RESOLVING_POWER: dict[tuple[str, str], str] = {
    ('Asiago Observatory (Echelle)', 'Echelle + Andor iKon DW436-BV'): 'R ≈ 20,000',
    ('Asiago Observatory (Echelle)', 'echelle hi-res Spectrograph'): 'R ≈ 20,000',
    ('Asiago Observatory (Echelle)', 'Echelle Hi-Res Spectrograph'): 'R ≈ 20,000',
    ('Asiago Observatory (Echelle)', 'ECHELLE REOSC'): 'R ≈ 20,000',
    ('CARMENES', 'CARMENES VIS'): 'R ≈ 94,600',
    ('CARMENES (CAHA archive, VIS+NIR)', 'CARMENES NIR'): 'R ≈ 80,600',
    ('CARMENES (CAHA archive, VIS+NIR)', 'CARMENES VIS'): 'R ≈ 94,600',
    ('CFHT / CADC', 'SPIRou'): 'R ≈ 70,000',
    ('CFHT / CADC', 'ESPaDOnS'): 'R ≈ 68,000 (spectroscopy mode)',
    ('CFHT / CADC', 'MegaPrime'): 'n/a (wide-field imager)',
    ('CFHT / CADC', 'UH8K'): 'n/a (imaging mosaic camera)',
    ('CFHT / CADC', 'HRCAM'): 'n/a (imaging camera)',
    ('CFHT / CADC', 'FTS'): 'R ≈ 100,000+ (Fourier Transform Spectrometer, variable)',
    ('CFHT / CADC', 'GECKO'): 'R ≈ 120,000 (fiber-fed echelle)',
    ('CFHT / CADC', 'TIGER'): 'R ≈ 1,600–3,600 (integral-field, grism-dependent)',
    ('CFHT / CADC', 'MOS'): 'R ≈ 400–3,000 (grism-dependent, retired)',
    ('CFHT / CADC', 'SIS'): 'R ≈ 400–3,000 (grism-dependent, retired)',
    ('CFHT / CADC', 'OSIS'): '—',
    ('CFHT / CADC', 'PUMA'): '—',
    ('CFHT / CADC', 'SISFP'): '—',
    ('CFHT / CADC', 'HERZBERG'): '—',
    ('CFHT / CADC', 'ISIS'): '—',
    ('CFHT / CADC', 'PYTHIAS'): '—',
    ('DAO (Dominion Astrophysical Observatory)', 'McKellar Spectrograph'): 'R ≈ 10,000–20,000 (grating-dependent)',
    ('DAO (Dominion Astrophysical Observatory)', 'Cassegrain Spectrograph'): 'R ≈ 1,000–12,000 (grating-dependent)',
    ('DAO (Dominion Astrophysical Observatory)', 'Cassegrain Spectropolarimeter'): 'R ≈ 1,000–5,000 (grating-dependent)',
    ('DAO (Dominion Astrophysical Observatory)', 'Radial-Velocity Scanner'): '—',
    ('DESI', 'DESI'): 'R ≈ 2,000–5,500 (wavelength-dependent)',
    ('ELODIE (OHP)', 'ELODIE'): 'R ≈ 42,000',
    ('ESO Science Archive', 'GIRAFFE'): 'R ≈ 6,000–30,000 (mode-dependent)',
    ('ESO Science Archive', 'HARPS'): 'R ≈ 115,000',
    ('ESO Science Archive', 'XSHOOTER'): 'R ≈ 3,000–17,000 (arm/slit-dependent)',
    ('ESO Science Archive', 'VIMOS'): 'R ≈ 200–2,500 (grism-dependent)',
    ('ESO Science Archive', 'FORS2'): 'R ≈ 260–2,600 (grism-dependent)',
    ('ESO Science Archive', 'UVES'): 'R ≈ 40,000–110,000 (slit-dependent)',
    ('ESO Science Archive', 'FEROS'): 'R ≈ 48,000',
    ('ESO Science Archive', 'NIRPS'): 'R ≈ 100,000',
    ('ESO Science Archive', 'ESPRESSO'): 'R ≈ 70,000–190,000 (mode-dependent)',
    ('ESO Science Archive', 'EFOSC'): 'R ≈ 600–2,500 (grism-dependent)',
    ('ESO Science Archive', 'KMOS'): 'R ≈ 1,500–4,000 (grating-dependent)',
    ('ESO Science Archive', 'CRIRES'): 'R ≈ 50,000–100,000+ (slit-dependent)',
    ('ESO Science Archive', 'SOFI'): 'R ≈ 600–1,800 (grism-dependent)',
    ('ESO Science Archive', 'APEXHET'): '—',
    ('ESO Science Archive', 'FORS1'): 'R ≈ 260–2,600 (grism-dependent, retired)',
    ('ESO Science Archive', ''): '—',
    ('ESO Science Archive', 'MUSE'): 'R ≈ 1,700–4,000',
    ('ESO Science Archive', 'SINFONI'): 'R ≈ 1,500–4,000 (grating-dependent)',
    ('FEROS Public Spectra (GAVO)', 'FEROS'): 'R ≈ 48,000',
    ('Flash/Heros Public Spectra (GAVO)', 'Flash/Heros'): 'R ≈ 20,000',
    ('GALAH', 'GALAH (HERMES)'): 'R ≈ 28,000',
    ('GTC (Gran Telescopio CANARIAS)', 'EMIR'): 'R ≈ 4,000–5,000 (MOS mode)',
    ('GTC (Gran Telescopio CANARIAS)', 'OSIRIS'): 'R ≈ 300–2,500 (grism-dependent)',
    ('GTC (Gran Telescopio CANARIAS)', 'MEGARA'): 'R ≈ 6,000–20,000 (LR/MR/HR modes)',
    ('GTC (Gran Telescopio CANARIAS)', 'HORuS'): 'R ≈ 25,000',
    ('GTC (Gran Telescopio CANARIAS)', 'CANARICAM'): 'R ≈ 175–1,300 (mode-dependent)',
    ('Gaia RVS', 'Gaia RVS'): 'R ≈ 11,500',
    ('Gemini Observatory Archive', 'GNIRS'): 'R ≈ 500–18,000 (mode-dependent)',
    ('Gemini Observatory Archive', 'GMOS-N'): 'R ≈ 400–5,000 (grating-dependent)',
    ('Gemini Observatory Archive', 'GMOS-S'): 'R ≈ 400–5,000 (grating-dependent)',
    ('Gemini Observatory Archive', 'PHOENIX'): 'R ≈ 50,000',
    ('Gemini Observatory Archive', 'GPI'): 'R ≈ 35–90 (IFS mode)',
    ('Gemini Observatory Archive', 'NIRI'): 'R ≈ 500–1,300 (grism mode)',
    ('Gemini Observatory Archive', 'NIFS'): 'R ≈ 5,290',
    ('Gemini Observatory Archive', 'F2'): 'R ≈ 900–3,600 (grating-dependent)',
    ('Gemini Observatory Archive', 'GRACES'): 'R ≈ 40,000–67,500 (mode-dependent)',
    ('Gemini Observatory Archive', 'MAROON-X'): 'R ≈ 85,000',
    ('Gemini Observatory Archive', 'michelle'): 'R ≈ up to 30,000 (echelle mode, retired)',
    ('Gemini Observatory Archive', 'TEXES'): 'R ≈ up to 100,000',
    ('Gemini Observatory Archive', 'TReCS'): 'R ≈ 100–1,000 (retired)',
    ('Gemini Observatory Archive', 'GHOST'): 'R ≈ 50,000 / 75,000 (standard/high-res mode)',
    ('Gemini Observatory Archive', 'FLAMINGOS'): 'R ≈ 1,300 (low-res, retired)',
    ('Gemini Observatory Archive', 'CIRPASS'): '—',
    ('Gemini Observatory Archive', 'bHROS'): 'R ≈ 150,000 (retired)',
    ('Gemini Observatory Archive', 'OSCIR'): '—',
    ('Gemini Observatory Archive — GHOST', 'GHOST'): 'R ≈ 50,000 / 75,000 (standard/high-res mode)',
    ('Gemini Observatory Archive — IGRINS', 'IGRINS'): 'R ≈ 45,000',
    ('HARPS-N (TNG)', 'HARPS-N'): 'R ≈ 115,000',
    ('HERMES (Mercator Telescope, KU Leuven)', 'HERMES'): 'R ≈ 25,000–86,000 (mode-dependent)',
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS red arm'): 'R ≈ 600–8,000 (grating-dependent)',
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS blue arm'): 'R ≈ 600–8,000 (grating-dependent)',
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS RED ARM'): 'R ≈ 600–8,000 (grating-dependent)',
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS BLUE ARM'): 'R ≈ 600–8,000 (grating-dependent)',
    ('IRTF SpeX (via IRSA)', 'SpeX'): 'R ≈ 200 (prism) – 2,500 (cross-dispersed)',
    ('IRTF iSHELL (via IRSA)', 'iSHELL'): 'R ≈ 80,000 (0.375" slit)',
    ('IRTF Legacy Archive', 'SpeX'): 'R ≈ 200 (prism) – 2,500 (cross-dispersed)',
    ('IRTF Legacy Archive', 'CSHELL'): '—',
    ('Keck Observatory Archive', 'NIRSPEC'): 'R ≈ 2,000–25,000 (mode-dependent)',
    ('Keck Observatory Archive', 'HIRES'): 'R ≈ 25,000–85,000 (slit-dependent)',
    ('Keck Observatory Archive', 'MOSFIRE'): 'R ≈ 3,600',
    ('Keck Observatory Archive', 'LRIS'): 'R ≈ 300–2,500 (grism/grating-dependent)',
    ('Keck Observatory Archive', 'NIRES'): 'R ≈ 2,700',
    ('Keck Observatory Archive', 'OSIRIS'): 'R ≈ 3,800 (near-IR IFU)',
    ('Keck Observatory Archive', 'DEIMOS'): 'R ≈ 1,000–6,000 (grating-dependent)',
    ('Keck Observatory Archive', 'KPF'): 'R ≈ 98,000 (also 35,000 simultaneous mode)',
    ('Keck Observatory Archive', 'ESI'): 'R ≈ 1,000–8,000 (mode-dependent)',
    ('LAMOST', 'LAMOST'): 'R ≈ 1,800',
    ('LAMOST — MRS', 'LAMOST-MRS'): 'R ≈ 7,500',
    ('LBT — PEPSI', 'MODS'): 'R ≈ 1,000–3,000 (grating-dependent)',
    ('LBT — PEPSI', 'LUCI'): 'R ≈ 4,000–8,000 (mode-dependent)',
    ('LBT — PEPSI', 'PEPSI'): 'R ≈ 43,000–320,000 (mode-dependent)',
    ('Las Cumbres Observatory -- FLOYDS', 'FLOYDS'): 'R ≈ 400–700 (order-dependent)',
    ('Las Cumbres Observatory -- NRES', 'NRES'): 'R ≈ 48,000–53,000',
    ('Lick / Mt. Hamilton (Shane + APF)', 'Lick APF'): 'R ≈ 100,000',
    ('Lick / Mt. Hamilton (Shane + APF)', 'Lick shane'): 'R ≈ 600–2,000 (Kast, grating-dependent)',
    ('MAST', 'WFC3/IR'): 'R ≈ 130 (grism mode)',
    ('MAST', 'COS/FUV'): 'R ≈ 2,400–24,000 (grating-dependent)',
    ('MAST', 'STIS/CCD'): 'R ≈ 500–114,000 (mode-dependent)',
    ('MAST', 'NICMOS/NIC3'): 'R ≈ 200 (grism mode)',
    ('MAST', 'HRS/2'): 'R ≈ 2,000–100,000 (grating-dependent, retired GHRS)',
    ('MAST', 'FOS/RD'): 'R ≈ 250–1,300 (grating-dependent, retired)',
    ('MAST', 'STIS/FUV-MAMA'): 'R ≈ 500–114,000 (mode-dependent)',
    ('MAST', 'FOS/BL'): 'R ≈ 250–1,300 (grating-dependent, retired)',
    ('MAST', 'COS/NUV'): 'R ≈ 2,400–24,000 (grating-dependent)',
    ('MAST', 'STIS/NUV-MAMA'): 'R ≈ 500–114,000 (mode-dependent)',
    ('MAST', 'COS'): 'R ≈ 2,400–24,000 (grating-dependent)',
    ('MAST', 'STIS'): 'R ≈ 500–114,000 (mode-dependent)',
    ('MAST', 'WFC3/UVIS'): 'n/a (imaging, no grism)',
    ('MAST', 'ACS/HRC'): 'R ≈ 100 (grism/prism mode, retired)',
    ('MAST', 'HRS/1'): 'R ≈ 2,000–100,000 (grating-dependent, retired GHRS)',
    ('MAST', 'ACS/WFC'): 'R ≈ 100 (grism mode)',
    ('MAST', 'ACS/SBC'): 'R ≈ 100 (grism mode)',
    ('MAST', 'COS-STIS'): '—',
    ('MAST', 'FOC/96'): 'n/a (imaging camera)',
    ('MAST', 'FOC/48'): 'n/a (imaging camera)',
    ('MAST', 'FOC/288'): 'n/a (imaging camera)',
    ('MAST — JWST', 'NIRSPEC/MSA'): 'R ≈ 100–2,700 (mode-dependent)',
    ('MAST — JWST', 'NIRCAM/GRISM'): 'R ≈ 1,600',
    ('MAST — JWST', 'NIRSPEC/SLIT'): 'R ≈ 100–2,700 (mode-dependent)',
    ('MAST — JWST', 'NIRISS/WFSS'): 'R ≈ 150',
    ('MAST — JWST', 'MIRI/SLIT'): 'R ≈ 40–160 (LRS)',
    ('MAST — JWST', 'NIRSPEC'): 'R ≈ 100–2,700 (mode-dependent)',
    ('MAST — JWST', 'MIRI/SLITLESS'): 'R ≈ 40–160 (LRS)',
    ('MAST — JWST', 'NIRCAM/IMAGE'): 'n/a (imaging)',
    ('MAST — JWST', 'NIRCAM/TARGACQ'): 'n/a (target acquisition)',
    ('MAST — JWST', 'MIRI/IMAGE'): 'n/a (imaging)',
    ('MAST — JWST', 'NIRISS/SOSS'): 'R ≈ 700',
    ('NAOJ (Subaru HDS, via JVO)', 'HDS'): 'R ≈ 45,000–160,000 (slit-dependent)',
    ('NOIRLab Astro Data Archive', 'goodman'): 'R ≈ 300–4,500 (grating-dependent)',
    ('NOIRLab Astro Data Archive', 'echelle'): 'R ≈ 40,000–45,000 (CTIO echelle)',
    ('NOIRLab Astro Data Archive', 'chiron'): 'R ≈ 28,000–90,000 (mode-dependent)',
    ('NOIRLab Astro Data Archive', 'triplespec'): 'R ≈ 2,700–3,500',
    ('NOIRLab Astro Data Archive', 'sami'): '—',
    ('NOIRLab Astro Data Archive', 'ghts_red'): 'R ≈ 300–4,500 (grating-dependent)',
    ('NOIRLab Astro Data Archive', 'arcoiris'): 'R ≈ 3,500',
    ('NOIRLab Astro Data Archive', 'cosmos'): 'R ≈ 300–3,000 (grating-dependent)',
    ('NOIRLab Astro Data Archive', 'kosmos'): 'R ≈ 500–5,000 (grating-dependent)',
    ('NOIRLab Astro Data Archive', 'ghts_blue'): 'R ≈ 300–4,500 (grating-dependent)',
    ('OIRSA (CfA)', 'Hectospec'): 'R ≈ 1,000–2,500 (grating-dependent)',
    ('OIRSA (CfA)', 'Hectochelle'): 'R ≈ 20,000–40,000 (order-dependent)',
    ('OIRSA (CfA)', 'echelle'): 'R ≈ 25,000–44,000 (FLWO 1.5m echelle)',
    ('OIRSA (CfA)', 'FAST'): 'R ≈ 1,000–4,000 (grating-dependent)',
    ('RAVE', 'RAVE'): 'R ≈ 7,500',
    ('SALT HRS (SAAO SSDA)', 'HRS'): 'R ≈ 15,000–65,000 (LR/MR/HR/HS mode)',
    ('SDSS Legacy Optical', 'SDSS/BOSS'): 'R ≈ 1,300–2,600 (wavelength-dependent)',
    ('SDSS-V — APOGEE', 'APOGEE'): 'R ≈ 22,500',
    ('SDSS-V — Optical', 'SDSS-V/BOSS'): 'R ≈ 1,300–2,600 (wavelength-dependent)',
    ('SOPHIE (OHP)', 'SOPHIE'): 'R ≈ 39,000–75,000 (HE/HR mode)',
}

# Wavelength coverage (nm, vacuum/air distinction not tracked -- published
# specs quoted at whatever precision the instrument's own documentation
# uses) per (archive display_name, instrument) -- same hand-maintained,
# same-key shape as INSTRUMENT_RESOLVING_POWER above (same reasoning: not
# derivable from the database, no per-observation column for it). Powers the
# /?q=... search page's wavelength-coverage chart -- see
# _wavelength_coverage_bars. Deliberately a strict subset of
# INSTRUMENT_RESOLVING_POWER's keys: n/a (imaging-only) entries are omitted
# outright, and a handful of obscure/retired instruments this project
# couldn't confirm a real published range for (e.g. CFHT's GECKO, PYTHIAS,
# HERZBERG, OSIS, PUMA, SISFP, ISIS; Gemini's CIRPASS/OSCIR; NOIRLab's sami;
# ESO's APEXHET, a submm heterodyne receiver with no meaningful nm range;
# HST's COS-STIS combined mode) are left out rather than guessed -- a
# missing key just means that instrument's bar doesn't render, the same
# graceful-degradation shape as INSTRUMENT_RESOLVING_POWER's own "—". A few
# instruments (GALAH/HERMES, LAMOST-MRS) are non-contiguous multi-band
# spectrographs -- the tuple here is the outer envelope (first band's blue
# edge to last band's red edge), not literal continuous coverage; the chart
# doesn't attempt to render the internal gap.
INSTRUMENT_WAVELENGTH_RANGE_NM: dict[tuple[str, str], tuple[float, float]] = {
    ('Asiago Observatory (Echelle)', 'Echelle + Andor iKon DW436-BV'): (360, 730),
    ('Asiago Observatory (Echelle)', 'echelle hi-res Spectrograph'): (360, 730),
    ('Asiago Observatory (Echelle)', 'Echelle Hi-Res Spectrograph'): (360, 730),
    ('Asiago Observatory (Echelle)', 'ECHELLE REOSC'): (360, 730),
    ('CARMENES', 'CARMENES VIS'): (520, 960),
    ('CARMENES (CAHA archive, VIS+NIR)', 'CARMENES NIR'): (960, 1710),
    ('CARMENES (CAHA archive, VIS+NIR)', 'CARMENES VIS'): (520, 960),
    ('CFHT / CADC', 'SPIRou'): (980, 2350),
    ('CFHT / CADC', 'ESPaDOnS'): (370, 1050),
    ('CFHT / CADC', 'FTS'): (450, 1100),
    ('CFHT / CADC', 'TIGER'): (400, 700),
    ('CFHT / CADC', 'MOS'): (370, 900),
    ('CFHT / CADC', 'SIS'): (370, 1000),
    ('DAO (Dominion Astrophysical Observatory)', 'McKellar Spectrograph'): (350, 900),
    ('DAO (Dominion Astrophysical Observatory)', 'Cassegrain Spectrograph'): (350, 900),
    ('DAO (Dominion Astrophysical Observatory)', 'Cassegrain Spectropolarimeter'): (350, 900),
    ('DESI', 'DESI'): (360, 980),
    ('ELODIE (OHP)', 'ELODIE'): (390, 680),
    ('ESO Science Archive', 'GIRAFFE'): (370, 900),
    ('ESO Science Archive', 'HARPS'): (378, 691),
    ('ESO Science Archive', 'XSHOOTER'): (300, 2480),
    ('ESO Science Archive', 'VIMOS'): (360, 1000),
    ('ESO Science Archive', 'FORS2'): (330, 1100),
    ('ESO Science Archive', 'UVES'): (300, 1100),
    ('ESO Science Archive', 'FEROS'): (350, 920),
    ('ESO Science Archive', 'NIRPS'): (980, 1800),
    ('ESO Science Archive', 'ESPRESSO'): (380, 788),
    ('ESO Science Archive', 'EFOSC'): (330, 1100),
    ('ESO Science Archive', 'KMOS'): (800, 2500),
    ('ESO Science Archive', 'CRIRES'): (950, 5300),
    ('ESO Science Archive', 'SOFI'): (950, 2500),
    ('ESO Science Archive', 'FORS1'): (330, 1100),
    ('ESO Science Archive', 'MUSE'): (480, 930),
    ('ESO Science Archive', 'SINFONI'): (1100, 2450),
    ('FEROS Public Spectra (GAVO)', 'FEROS'): (350, 920),
    ('Flash/Heros Public Spectra (GAVO)', 'Flash/Heros'): (350, 870),
    ('GALAH', 'GALAH (HERMES)'): (471, 789),
    ('GTC (Gran Telescopio CANARIAS)', 'EMIR'): (900, 2500),
    ('GTC (Gran Telescopio CANARIAS)', 'OSIRIS'): (365, 1000),
    ('GTC (Gran Telescopio CANARIAS)', 'MEGARA'): (365, 1000),
    ('GTC (Gran Telescopio CANARIAS)', 'HORuS'): (383, 690),
    ('GTC (Gran Telescopio CANARIAS)', 'CANARICAM'): (8000, 25000),
    ('Gaia RVS', 'Gaia RVS'): (846, 870),
    ('Gemini Observatory Archive', 'GNIRS'): (900, 2500),
    ('Gemini Observatory Archive', 'GMOS-N'): (360, 1000),
    ('Gemini Observatory Archive', 'GMOS-S'): (360, 1000),
    ('Gemini Observatory Archive', 'PHOENIX'): (1000, 5000),
    ('Gemini Observatory Archive', 'GPI'): (900, 2400),
    ('Gemini Observatory Archive', 'NIRI'): (1000, 2500),
    ('Gemini Observatory Archive', 'NIFS'): (940, 2500),
    ('Gemini Observatory Archive', 'F2'): (900, 2500),
    ('Gemini Observatory Archive', 'GRACES'): (500, 1050),
    ('Gemini Observatory Archive', 'MAROON-X'): (500, 920),
    ('Gemini Observatory Archive', 'michelle'): (7900, 25300),
    ('Gemini Observatory Archive', 'TEXES'): (5000, 25000),
    ('Gemini Observatory Archive', 'TReCS'): (8000, 25000),
    ('Gemini Observatory Archive', 'GHOST'): (363, 1000),
    ('Gemini Observatory Archive', 'FLAMINGOS'): (1000, 2500),
    ('Gemini Observatory Archive', 'bHROS'): (350, 1050),
    ('Gemini Observatory Archive — GHOST', 'GHOST'): (363, 1000),
    ('Gemini Observatory Archive — IGRINS', 'IGRINS'): (1450, 2450),
    ('HARPS-N (TNG)', 'HARPS-N'): (383, 693),
    ('HERMES (Mercator Telescope, KU Leuven)', 'HERMES'): (377, 900),
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS red arm'): (500, 1000),
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS blue arm'): (300, 550),
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS RED ARM'): (500, 1000),
    ('ING Archive (WHT/ISIS)', 'WHT/ISIS BLUE ARM'): (300, 550),
    ('IRTF SpeX (via IRSA)', 'SpeX'): (700, 5300),
    ('IRTF iSHELL (via IRSA)', 'iSHELL'): (1060, 5300),
    ('IRTF Legacy Archive', 'SpeX'): (700, 5300),
    ('Keck Observatory Archive', 'NIRSPEC'): (950, 5500),
    ('Keck Observatory Archive', 'HIRES'): (300, 1000),
    ('Keck Observatory Archive', 'MOSFIRE'): (970, 2450),
    ('Keck Observatory Archive', 'LRIS'): (300, 1100),
    ('Keck Observatory Archive', 'NIRES'): (940, 2450),
    ('Keck Observatory Archive', 'OSIRIS'): (1000, 2400),
    ('Keck Observatory Archive', 'DEIMOS'): (410, 1100),
    ('Keck Observatory Archive', 'KPF'): (445, 870),
    ('Keck Observatory Archive', 'ESI'): (390, 1090),
    ('LAMOST', 'LAMOST'): (370, 900),
    ('LAMOST — MRS', 'LAMOST-MRS'): (495, 685),
    ('LBT — PEPSI', 'MODS'): (320, 1000),
    ('LBT — PEPSI', 'LUCI'): (850, 2500),
    ('LBT — PEPSI', 'PEPSI'): (383, 907),
    ('Las Cumbres Observatory -- FLOYDS', 'FLOYDS'): (320, 1000),
    ('Las Cumbres Observatory -- NRES', 'NRES'): (380, 860),
    ('Lick / Mt. Hamilton (Shane + APF)', 'Lick APF'): (374, 970),
    ('Lick / Mt. Hamilton (Shane + APF)', 'Lick shane'): (330, 1000),
    ('MAST', 'WFC3/IR'): (800, 1700),
    ('MAST', 'COS/FUV'): (90, 205),
    ('MAST', 'STIS/CCD'): (164, 1030),
    ('MAST', 'NICMOS/NIC3'): (1400, 2500),
    ('MAST', 'HRS/2'): (115, 320),
    ('MAST', 'FOS/RD'): (160, 850),
    ('MAST', 'STIS/FUV-MAMA'): (115, 170),
    ('MAST', 'FOS/BL'): (130, 550),
    ('MAST', 'COS/NUV'): (165, 320),
    ('MAST', 'STIS/NUV-MAMA'): (165, 310),
    ('MAST', 'COS'): (90, 320),
    ('MAST', 'STIS'): (115, 1030),
    ('MAST', 'HRS/1'): (105, 320),
    ('MAST — JWST', 'NIRSPEC/MSA'): (600, 5300),
    ('MAST — JWST', 'NIRCAM/GRISM'): (2400, 5000),
    ('MAST — JWST', 'NIRSPEC/SLIT'): (600, 5300),
    ('MAST — JWST', 'NIRISS/WFSS'): (800, 2200),
    ('MAST — JWST', 'MIRI/SLIT'): (5000, 12000),
    ('MAST — JWST', 'NIRSPEC'): (600, 5300),
    ('MAST — JWST', 'MIRI/SLITLESS'): (5000, 12000),
    ('MAST — JWST', 'NIRISS/SOSS'): (600, 2800),
    ('NAOJ (Subaru HDS, via JVO)', 'HDS'): (300, 1000),
    ('NOIRLab Astro Data Archive', 'goodman'): (320, 900),
    ('NOIRLab Astro Data Archive', 'echelle'): (350, 900),
    ('NOIRLab Astro Data Archive', 'chiron'): (410, 870),
    ('NOIRLab Astro Data Archive', 'triplespec'): (950, 2460),
    ('NOIRLab Astro Data Archive', 'ghts_red'): (500, 900),
    ('NOIRLab Astro Data Archive', 'arcoiris'): (700, 2450),
    ('NOIRLab Astro Data Archive', 'cosmos'): (350, 950),
    ('NOIRLab Astro Data Archive', 'kosmos'): (330, 1000),
    ('NOIRLab Astro Data Archive', 'ghts_blue'): (320, 700),
    ('OIRSA (CfA)', 'Hectospec'): (370, 920),
    ('OIRSA (CfA)', 'Hectochelle'): (500, 900),
    ('OIRSA (CfA)', 'echelle'): (350, 900),
    ('OIRSA (CfA)', 'FAST'): (350, 750),
    ('RAVE', 'RAVE'): (841, 879),
    ('SALT HRS (SAAO SSDA)', 'HRS'): (370, 890),
    ('SDSS Legacy Optical', 'SDSS/BOSS'): (360, 1040),
    ('SDSS-V — APOGEE', 'APOGEE'): (1514, 1696),
    ('SDSS-V — Optical', 'SDSS-V/BOSS'): (360, 1040),
    ('SOPHIE (OHP)', 'SOPHIE'): (387, 694),
}

INSTRUMENTS_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Instruments</title>
  <style>""" + SHARED_STYLE + """
    #instrument-treemap, #instrument-sky { width: 100%; height: 700px; margin-top: 1rem; }
    #overlap-heatmap { width: 100%; height: 650px; margin-top: 1rem; }
    .overlap-controls { display: flex; gap: 1rem; align-items: center; flex-wrap: wrap; margin: 1rem 0; }
    .overlap-controls select { font-family: monospace; padding: 0.3rem; }
    .granularity-btn { font-family: monospace; padding: 0.3rem 0.8rem; border: 1px solid #000; background: #fff; cursor: pointer; }
    .granularity-btn.active { background: #000; color: #fff; }
    #venn-svg-wrap svg { max-width: 100%; height: auto; }
    #venn-legend { margin-top: 0.8rem; }
    #venn-legend table { width: auto; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <h2>Holdings by archive and instrument</h2>
  <p class="note">Size = number of holdings, log-scaled so smaller instruments stay visible next to the largest archives. Click a box to zoom into an archive's instruments.</p>
  {% if treemap_labels %}
    <div id="instrument-treemap"></div>
    <script>
      Plotly.newPlot('instrument-treemap', [{
        type: 'treemap',
        branchvalues: 'total',
        labels: {{ treemap_labels | tojson }},
        parents: {{ treemap_parents | tojson }},
        values: {{ treemap_values | tojson }},
        customdata: {{ treemap_counts | tojson }},
        texttemplate: '%{label}<br>%{customdata:,}',
        hovertemplate: '%{label}<br>%{customdata:,} holdings<extra></extra>',
      }], { margin: { t: 10, l: 10, r: 10, b: 10 } }, { responsive: true });
    </script>
  {% else %}
    <p>No instrument data yet.</p>
  {% endif %}

  <hr>
  <h2>Tracked instruments</h2>
  <p class="note">Every distinct instrument name seen in current holdings, grouped by archive, with an approximate resolving power (R = &lambda;/&Delta;&lambda;) for each -- hand-maintained from published instrument specs, not derived from the database. A star can have no spectrum from a listed instrument and still be correctly tracked -- this only says the instrument is covered by the sync, not that every star has data from it. Many spectrographs offer several gratings or modes with different R; a range is shown where that's the common case rather than picking one mode arbitrarily. "n/a" marks instruments that are primarily imagers; "&mdash;" marks ones not yet looked up.</p>
  {% for a in instruments %}
  <details>
    <summary class="summary-row">
      <span>{{ a.display_name }}</span>
      <span class="summary-count">{{ a.instruments|length }} instrument{{ "s" if a.instruments|length != 1 else "" }}</span>
    </summary>
    <table>
      <tr><th>Instrument</th><th>Holdings</th><th>Resolving power</th></tr>
      {% for i in a.instruments %}
      <tr><td>{{ i.instrument }}</td><td>{{ "{:,}".format(i.n) }}</td><td>{{ i.resolving_power }}</td></tr>
      {% endfor %}
    </table>
  </details>
  {% endfor %}

  <hr>
  <h2>Where each instrument points</h2>
  <p class="note">A sample of up to {{ "{:,}".format(per_instrument_cap) }} position-tagged observations for each of the {{ top_n }} instruments with the most of them, Aitoff-projected -- a rough fingerprint of each instrument's sky coverage (northern vs. southern observatories, survey footprints, pointed vs. all-sky programs). Click a legend entry to isolate one instrument.</p>
  {% if sky_traces %}
    <div id="instrument-sky"></div>
    <script>
      const skyTraces = {{ sky_traces | tojson }};
      Plotly.newPlot('instrument-sky', skyTraces.map(t => ({
        x: t.x, y: t.y, name: t.instrument, mode: 'markers', type: 'scattergl',
        marker: { size: 3, opacity: 0.6 },
        hovertemplate: t.instrument + '<extra></extra>',
      })), {
        xaxis: { showticklabels: false, zeroline: false, title: 'Right Ascension', scaleanchor: 'y' },
        yaxis: { showticklabels: false, zeroline: false, title: 'Declination' },
        hovermode: 'closest',
        legend: { orientation: 'h' },
      }, { responsive: true, scrollZoom: true });
    </script>
  {% else %}
    <p>No position-tagged instrument data yet.</p>
  {% endif %}

  <hr>
  <h2>Star overlap between archives</h2>
  {% if archive_items|length >= 2 %}
  <p class="note">How many stars have spectra from more than one source. "Archives" is the coarse view (which observatories/surveys share targets); "Instruments" breaks archives that host several instruments (e.g. Gemini, KOA) apart -- e.g. HARPS vs. HARPS-N vs. ELODIE. The heatmap shows every pair at once (color = # shared stars; the diagonal, unshaded, is each set's own total). Below it, pick 2 or 3 sets for an exact, proportionally-sized Venn diagram.</p>
  <div class="overlap-controls">
    <button type="button" class="granularity-btn active" id="granularity-archives" onclick="setOverlapGranularity('archives')">Archives</button>
    <button type="button" class="granularity-btn" id="granularity-instruments" onclick="setOverlapGranularity('instruments')">Instruments</button>
  </div>
  <div id="overlap-heatmap"></div>

  <h3>Venn diagram</h3>
  <div class="overlap-controls">
    <select id="venn-select-a"></select>
    <select id="venn-select-b"></select>
    <label><input type="checkbox" id="venn-add-third" onchange="onVennThirdToggle()"> add a third set</label>
    <select id="venn-select-c" disabled></select>
  </div>
  <div id="venn-svg-wrap"></div>
  <div id="venn-legend"></div>

  <script>
    const overlapData = {
      archives: { items: {{ archive_items | tojson }}, pairs: {{ archive_pairs | tojson }}, triples: {{ archive_triples | tojson }} },
      instruments: { items: {{ instrument_items | tojson }}, pairs: {{ instrument_pairs | tojson }}, triples: {{ instrument_triples | tojson }} },
    };
    const INSTRUMENT_HEATMAP_TOP_N = {{ instrument_heatmap_top_n }};
    let overlapGranularity = 'archives';
    const VENN_COLORS = ['#2a78d6', '#eb6834', '#1baf7a'];

    function pairKey(a, b) { return [a, b].sort().join('\\u0000'); }
    function tripleKey(a, b, c) { return [a, b, c].sort().join('\\u0000'); }
    function pairMap(pairs) {
      const m = new Map();
      pairs.forEach(p => m.set(pairKey(p.a, p.b), p.n));
      return m;
    }
    function tripleMap(triples) {
      const m = new Map();
      triples.forEach(t => m.set(tripleKey(t.a, t.b, t.c), t.n));
      return m;
    }
    function itemByCode(code) {
      return overlapData[overlapGranularity].items.find(it => it.code === code);
    }
    function pairOverlap(sets, i, j) {
      return pairMap(overlapData[overlapGranularity].pairs).get(pairKey(sets[i].code, sets[j].code)) || 0;
    }

    function renderHeatmap() {
      const data = overlapData[overlapGranularity];
      const topN = overlapGranularity === 'archives' ? data.items.length : INSTRUMENT_HEATMAP_TOP_N;
      const items = data.items.slice(0, topN);
      const pmap = pairMap(data.pairs);
      const labels = items.map(it => it.display_name);
      const z = [], customdata = [];
      const annotations = [];
      let maxOverlap = 0;
      for (let i = 0; i < items.length; i++) {
        const zRow = [], cdRow = [];
        for (let j = 0; j < items.length; j++) {
          if (i === j) {
            zRow.push(null);
            cdRow.push(null);
            annotations.push({ x: labels[j], y: labels[i], text: items[i].n.toLocaleString(), showarrow: false, font: { size: 10, color: '#52514e' } });
          } else {
            const n = pmap.get(pairKey(items[i].code, items[j].code)) || 0;
            maxOverlap = Math.max(maxOverlap, n);
            zRow.push(Math.log10(n + 1));
            cdRow.push(n);
          }
        }
        z.push(zRow);
        customdata.push(cdRow);
      }

      // Real shared-star counts between pairs span orders of magnitude too
      // (a handful up to hundreds of thousands) -- same reasoning as the
      // Venn circle sizing above. A linear color scale makes every cell but
      // the brightest few look the same near-white shade, so cells are
      // colored by log10(n+1) instead; the colorbar's ticks are remapped
      // back to real counts (powers of ten, plus the true max so the scale's
      // top isn't a rounded-off lie) since the underlying log values aren't
      // meaningful to a reader on their own. hovertemplate reads customdata
      // (the real count) rather than z (the log-transformed color value).
      const tickVals = [], tickText = [];
      for (let t = 1; t <= maxOverlap; t *= 10) {
        tickVals.push(Math.log10(t + 1));
        tickText.push(t.toLocaleString());
      }
      if (maxOverlap > 0 && tickVals[tickVals.length - 1] < Math.log10(maxOverlap + 1)) {
        tickVals.push(Math.log10(maxOverlap + 1));
        tickText.push(maxOverlap.toLocaleString());
      }
      const colorbar = { title: { text: 'shared stars' } };
      if (tickVals.length > 0) {
        colorbar.tickvals = tickVals;
        colorbar.ticktext = tickText;
      }

      Plotly.newPlot('overlap-heatmap', [{
        type: 'heatmap', x: labels, y: labels, z: z, customdata: customdata,
        colorscale: [[0, '#cde2fb'], [0.25, '#6da7ec'], [0.5, '#2a78d6'], [0.75, '#1c5cab'], [1, '#0d366b']],
        hoverongaps: false,
        hovertemplate: '%{y} \\u2229 %{x}: %{customdata:,} stars<extra></extra>',
        colorbar: colorbar,
      }], {
        margin: { t: 10, l: 150, r: 20, b: 150 },
        xaxis: { tickangle: -45, automargin: true },
        yaxis: { automargin: true },
        annotations: annotations,
      }, { responsive: true });
    }

    function populateSelects() {
      const items = overlapData[overlapGranularity].items;
      const selects = ['venn-select-a', 'venn-select-b', 'venn-select-c'].map(id => document.getElementById(id));
      selects.forEach((sel, idx) => {
        sel.innerHTML = '';
        items.forEach(it => {
          const opt = document.createElement('option');
          opt.value = it.code;
          opt.textContent = it.display_name + ' (' + it.n.toLocaleString() + ')';
          sel.appendChild(opt);
        });
        sel.selectedIndex = Math.min(idx, items.length - 1);
      });
    }

    // Solves for the center-to-center distance between two circles that
    // makes their overlap (lens) area equal targetArea -- lens area shrinks
    // monotonically as distance grows (from full containment down to 0 at
    // r1+r2 apart), so a plain bisection over that range converges cleanly.
    // Same approach matplotlib_venn uses for its 2/3-circle proportional
    // Venn diagrams: fit each pairwise distance independently from that
    // pair's own overlap area, then triangulate the third circle's position
    // from the three (independently-fit) distances -- the resulting middle
    // region is usually close to, but not exactly, the true triple-overlap
    // count, so the actual count is always shown as text rather than relied
        // on to fall out of the geometry.
    function lensArea(r1, r2, d) {
      if (d >= r1 + r2) return 0;
      if (d <= Math.abs(r1 - r2)) return Math.PI * Math.min(r1, r2) ** 2;
      const clamp = (v) => Math.max(-1, Math.min(1, v));
      const alpha = Math.acos(clamp((d * d + r1 * r1 - r2 * r2) / (2 * d * r1)));
      const beta = Math.acos(clamp((d * d + r2 * r2 - r1 * r1) / (2 * d * r2)));
      const tri = 0.5 * Math.sqrt(Math.max(0, (-d + r1 + r2) * (d + r1 - r2) * (d - r1 + r2) * (d + r1 + r2)));
      return r1 * r1 * alpha + r2 * r2 * beta - tri;
    }
    function solveDistance(r1, r2, targetArea) {
      const maxArea = Math.PI * Math.min(r1, r2) ** 2;
      if (targetArea <= 0) return r1 + r2;
      if (targetArea >= maxArea) return Math.abs(r1 - r2);
      let lo = Math.abs(r1 - r2), hi = r1 + r2;
      for (let i = 0; i < 60; i++) {
        const mid = (lo + hi) / 2;
        if (lensArea(r1, r2, mid) > targetArea) lo = mid; else hi = mid;
      }
      return (lo + hi) / 2;
    }

    // Real archive/instrument totals span several orders of magnitude
    // (LAMOST: millions: ELODIE: tens of thousands) -- sizing circles so
    // area is exactly proportional to count (radius ~ sqrt(n)) makes the
    // smallest set collapse to a sub-pixel sliver next to the largest one,
    // confirmed live once this ran against real production data instead of
    // the small synthetic test set used during development. log10(n+1)
    // compresses that range so every set stays visible; it's a legibility
    // choice, not a correctness one -- the geometry no longer represents
    // true proportions, which is why renderVenn always labels every region
    // with its real count (never relies on the drawn area to convey it).
    function sizeMetric(n) { return Math.log10(n + 1); }

    function computeVennLayout(sets) {
      const pmap = pairMap(overlapData[overlapGranularity].pairs);
      const tmap = tripleMap(overlapData[overlapGranularity].triples);
      const maxMetric = Math.max(...sets.map(s => sizeMetric(s.n)));
      const R_MAX = 150;
      const scale = R_MAX / Math.sqrt(maxMetric);
      const radii = sets.map(s => scale * Math.sqrt(sizeMetric(s.n)));
      const areaPerMetric = Math.PI * scale * scale;
      const overlapN = (i, j) => i === j ? sets[i].n : (pmap.get(pairKey(sets[i].code, sets[j].code)) || 0);
      const overlapArea = (i, j) => sizeMetric(overlapN(i, j)) * areaPerMetric;

      if (sets.length === 2) {
        const d = solveDistance(radii[0], radii[1], overlapArea(0, 1));
        return { centers: [{ x: 0, y: 0 }, { x: d, y: 0 }], radii, tripleN: null };
      }

      const dAB = solveDistance(radii[0], radii[1], overlapArea(0, 1));
      const dAC = solveDistance(radii[0], radii[2], overlapArea(0, 2));
      const dBC = solveDistance(radii[1], radii[2], overlapArea(1, 2));
      const cx = (dAC * dAC - dBC * dBC + dAB * dAB) / (2 * dAB);
      const cy = Math.sqrt(Math.max(0, dAC * dAC - cx * cx));
      const tripleN = tmap.get(tripleKey(sets[0].code, sets[1].code, sets[2].code)) || 0;
      return { centers: [{ x: 0, y: 0 }, { x: dAB, y: 0 }, { x: cx, y: cy }], radii, tripleN };
    }

    // Every region's count used to be drawn as text inside the SVG, positioned
    // by an approximate geometric heuristic (near each region's rough
    // centroid). That works for modestly-sized, well-separated circles, but
    // confirmed live twice against real production archive sizes: once
    // circles are large and heavily mutually overlapping (common once real
    // archives share most of their stars, not just a synthetic test slice),
    // their centers end up close together, so *any* "offset in some
    // direction" heuristic -- for singles, pairs, or the triple -- crowds
    // multiple labels into the same small area and renders overlapping,
    // unreadable text. There's no in-diagram position guaranteed clear of
    // every other label once circles can overlap arbitrarily, so every
    // count now lists in a breakdown table below the diagram instead (color
    // swatches tie each row back to which set(s) it's the intersection of)
    // -- legible regardless of how squeezed the real geometry gets. The SVG
    // itself only needs to convey the visual impression of overlap now, not
    // carry any text.
    function swatchesHtml(colorIndices) {
      return colorIndices.map(ci =>
        '<span style="display:inline-block;width:10px;height:10px;margin-right:2px;' +
        'border-radius:2px;background:' + VENN_COLORS[ci] + ';"></span>'
      ).join('');
    }

    function regionLabel(sets, cis) {
      const names = cis.map(ci => sets[ci].display_name);
      return names.join(' ∩ ') + (names.length === 1 ? ' only' : '');
    }

    function renderVenn() {
      const selA = document.getElementById('venn-select-a').value;
      const selB = document.getElementById('venn-select-b').value;
      const thirdEnabled = document.getElementById('venn-add-third').checked;
      const selC = thirdEnabled ? document.getElementById('venn-select-c').value : null;
      const codes = [selA, selB].concat(selC ? [selC] : []);

      if (new Set(codes).size !== codes.length || codes.some(c => !c)) {
        document.getElementById('venn-svg-wrap').innerHTML = '<p class="note">Pick distinct sets.</p>';
        document.getElementById('venn-legend').innerHTML = '';
        return;
      }
      const sets = codes.map(itemByCode);
      if (sets.some(s => !s)) return;

      const layout = computeVennLayout(sets);
      let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
      layout.centers.forEach((c, i) => {
        minX = Math.min(minX, c.x - layout.radii[i]);
        maxX = Math.max(maxX, c.x + layout.radii[i]);
        minY = Math.min(minY, c.y - layout.radii[i]);
        maxY = Math.max(maxY, c.y + layout.radii[i]);
      });
      const pad = 40;
      minX -= pad; maxX += pad; minY -= pad; maxY += pad;

      let svg = '<svg viewBox="' + minX + ' ' + minY + ' ' + (maxX - minX) + ' ' + (maxY - minY) +
        '" xmlns="http://www.w3.org/2000/svg">';
      layout.centers.forEach((c, i) => {
        svg += '<circle cx="' + c.x + '" cy="' + c.y + '" r="' + layout.radii[i] + '" fill="' + VENN_COLORS[i] +
          '" fill-opacity="0.5" stroke="' + VENN_COLORS[i] + '" stroke-width="2" />';
      });
      svg += '</svg>';
      document.getElementById('venn-svg-wrap').innerHTML = svg;

      let breakdown;
      if (sets.length === 2) {
        const n01 = pairOverlap(sets, 0, 1);
        breakdown = [
          { cis: [0], val: sets[0].n - n01 },
          { cis: [1], val: sets[1].n - n01 },
          { cis: [0, 1], val: n01 },
        ];
      } else {
        const n01 = pairOverlap(sets, 0, 1), n02 = pairOverlap(sets, 0, 2), n12 = pairOverlap(sets, 1, 2);
        const n012 = layout.tripleN;
        breakdown = [
          { cis: [0], val: sets[0].n - n01 - n02 + n012 },
          { cis: [1], val: sets[1].n - n01 - n12 + n012 },
          { cis: [2], val: sets[2].n - n02 - n12 + n012 },
          { cis: [0, 1], val: n01 - n012 },
          { cis: [0, 2], val: n02 - n012 },
          { cis: [1, 2], val: n12 - n012 },
          { cis: [0, 1, 2], val: n012 },
        ];
      }

      let legend = '<table><tr><th></th><th>Region</th><th>Stars</th></tr>';
      breakdown.forEach(row => {
        legend += '<tr><td>' + swatchesHtml(row.cis) + '</td><td>' + regionLabel(sets, row.cis) +
          '</td><td>' + row.val.toLocaleString() + '</td></tr>';
      });
      legend += '</table>';
      document.getElementById('venn-legend').innerHTML = legend;
    }

    function setOverlapGranularity(g) {
      overlapGranularity = g;
      document.getElementById('granularity-archives').classList.toggle('active', g === 'archives');
      document.getElementById('granularity-instruments').classList.toggle('active', g === 'instruments');
      document.getElementById('venn-add-third').checked = false;
      document.getElementById('venn-select-c').disabled = true;
      populateSelects();
      renderHeatmap();
      renderVenn();
    }
    function onVennThirdToggle() {
      document.getElementById('venn-select-c').disabled = !document.getElementById('venn-add-third').checked;
      renderVenn();
    }

    populateSelects();
    renderHeatmap();
    ['venn-select-a', 'venn-select-b', 'venn-select-c'].forEach(id =>
      document.getElementById(id).addEventListener('change', renderVenn)
    );
    renderVenn();
  </script>
  {% else %}
    <p>Not enough archives with matched holdings yet to compute overlap.</p>
  {% endif %}
</body>
</html>
"""


INSTRUMENT_OVERLAP_HEATMAP_TOP_N = 20


def _split_overlap_rows(rows: list[dict], a_key: str, b_key: str, display_key: str | None = None):
    """archive_overlap/instrument_overlap rows are a<=b pairs including the
    a==b self-pair (see export_to_parquet's comment on why) -- split that
    into per-item totals (from the self-pairs) and strict a<b pairs (the
    actual overlaps), the two things the heatmap and Venn picker need
    separately."""
    totals: dict[str, dict] = {}
    pairs: list[dict] = []
    for r in rows:
        a, b, n = r[a_key], r[b_key], r["n_overlap"]
        if a == b:
            totals[a] = {"code": a, "display_name": r[display_key] if display_key else a, "n": n}
        else:
            pairs.append({"a": a, "b": b, "n": n})
    items = sorted(totals.values(), key=lambda x: -x["n"])
    return items, pairs


@app.route("/instruments")
def instruments_page():
    # instruments (display_name, instrument, n) is precomputed by
    # scripts.export_to_parquet -- see INSTRUMENTS_QUERY there.
    cur = get_cursor()
    cur.execute("SELECT display_name, instrument, n FROM instruments ORDER BY display_name, n DESC")
    rows = _rows_as_dicts(cur)

    # Treemap: one root-level node per archive, one leaf per (archive,
    # instrument). Box area is driven by log10(n+1) rather than raw n --
    # holdings counts span orders of magnitude, and a linear area scale
    # makes the long tail of small instruments render as invisible slivers.
    # Real counts are carried separately in treemap_counts for the
    # label/hover text so the log transform never reaches the reader.
    #
    # Each archive node gets its own explicit log10(archive_total+1) value,
    # and the trace uses branchvalues:'total' (see the JS below) rather than
    # the default 'remainder' mode with a value of 0. Under 'remainder', an
    # archive's box size is the *sum of its children's log values*, which is
    # not the same as the log of the archive's total (log(a)+log(b) !=
    # log(a+b)) -- that made an archive's box size track its instrument
    # *count* almost as much as its holdings total, so an archive with many
    # small instruments could out-size one dominated by a single huge
    # instrument.
    #
    # 'total' mode requires each node's declared value to equal the sum of
    # its children's declared values -- Plotly doesn't renormalize a
    # mismatch itself, it just breaks the layout (an archive with a lot of
    # small instruments can have its children's raw log10(n+1) values sum to
    # many times the archive's own log10(total+1), which blanked the whole
    # chart). So each instrument leaf's value is its log10(n+1) weight
    # rescaled to its share of the archive's own value -- this keeps
    # instruments' relative sizes within an archive exactly as before
    # (still log-scaled, so small ones stay visible next to big ones) while
    # making the leaves sum to precisely the archive's true-total value.
    archive_totals: dict[str, int] = defaultdict(int)
    for r in rows:
        archive_totals[r["display_name"]] += r["n"]
    leaf_weights_by_archive: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        leaf_weights_by_archive[r["display_name"]].append(math.log10(r["n"] + 1))
    weight_sum_by_archive = {a: sum(ws) for a, ws in leaf_weights_by_archive.items()}

    treemap_labels, treemap_parents, treemap_values, treemap_counts = [], [], [], []
    seen_archives = set()
    for r in rows:
        archive = r["display_name"]
        archive_value = math.log10(archive_totals[archive] + 1)
        if archive not in seen_archives:
            treemap_labels.append(archive)
            treemap_parents.append("")
            treemap_values.append(archive_value)
            treemap_counts.append(archive_totals[archive])
            seen_archives.add(archive)
        weight_sum = weight_sum_by_archive[archive]
        leaf_weight = math.log10(r["n"] + 1)
        leaf_value = archive_value * leaf_weight / weight_sum if weight_sum > 0 else 0
        treemap_labels.append(f"{archive} / {r['instrument']}")
        treemap_parents.append(archive)
        treemap_values.append(leaf_value)
        treemap_counts.append(r["n"])

    instruments_by_archive: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        instruments_by_archive[r["display_name"]].append({
            "instrument": r["instrument"],
            "n": r["n"],
            "resolving_power": INSTRUMENT_RESOLVING_POWER.get((r["display_name"], r["instrument"]), "—"),
        })
    instruments = [
        {"display_name": name, "instruments": insts}
        for name, insts in instruments_by_archive.items()
    ]

    # instrument_sky_sample is precomputed by scripts.export_to_parquet --
    # see INSTRUMENT_SKY_SAMPLE_QUERY there for why (a live per-request
    # ROW_NUMBER()/random() sample over the full holdings table has the same
    # OOM-shaped risk documented for the Leaderboard elsewhere in this file).
    cur.execute("SELECT instrument, raw_ra, raw_dec FROM instrument_sky_sample")
    sky_by_instrument: dict[str, list[dict]] = defaultdict(list)
    for r in _rows_as_dicts(cur):
        sky_by_instrument[r["instrument"]].append(r)

    sky_traces = []
    for instrument, pts in sky_by_instrument.items():
        x, y = _aitoff_project([p["raw_ra"] for p in pts], [p["raw_dec"] for p in pts])
        sky_traces.append({"instrument": instrument, "x": x, "y": y})

    # Star overlap between archives/instruments -- archive_overlap(_triple)
    # and instrument_overlap(_triple) are precomputed by
    # scripts.export_to_parquet (see the queries there for why: this needs
    # a per-star array_agg + self-cross rather than a live self-join over
    # the full, ever-growing holdings table). Backs the overlap heatmap and
    # the 2/3-set Venn picker below.
    cur.execute("SELECT archive_a, display_a, archive_b, display_b, n_overlap FROM archive_overlap")
    archive_items, archive_pairs = _split_overlap_rows(
        _rows_as_dicts(cur), "archive_a", "archive_b", "display_a"
    )

    # Self-triples (a==b==c) duplicate archive_overlap's diagonal -- not
    # needed here, only the genuine 3-distinct-set combinations the Venn
    # picker looks up.
    cur.execute(
        "SELECT archive_a, archive_b, archive_c, n_overlap FROM archive_overlap_triple "
        "WHERE archive_a != archive_b AND archive_b != archive_c"
    )
    archive_triples = [
        {"a": r["archive_a"], "b": r["archive_b"], "c": r["archive_c"], "n": r["n_overlap"]}
        for r in _rows_as_dicts(cur)
    ]

    cur.execute("SELECT instrument_a, instrument_b, n_overlap FROM instrument_overlap")
    instrument_items, instrument_pairs = _split_overlap_rows(
        _rows_as_dicts(cur), "instrument_a", "instrument_b"
    )

    cur.execute(
        "SELECT instrument_a, instrument_b, instrument_c, n_overlap FROM instrument_overlap_triple "
        "WHERE instrument_a != instrument_b AND instrument_b != instrument_c"
    )
    instrument_triples = [
        {"a": r["instrument_a"], "b": r["instrument_b"], "c": r["instrument_c"], "n": r["n_overlap"]}
        for r in _rows_as_dicts(cur)
    ]

    return render_template_string(
        INSTRUMENTS_TEMPLATE,
        treemap_labels=treemap_labels, treemap_parents=treemap_parents, treemap_values=treemap_values,
        treemap_counts=treemap_counts,
        instruments=instruments,
        sky_traces=sky_traces,
        top_n=INSTRUMENT_SKY_SAMPLE_TOP_N, per_instrument_cap=INSTRUMENT_SKY_SAMPLE_PER_INSTRUMENT,
        archive_items=archive_items, archive_pairs=archive_pairs, archive_triples=archive_triples,
        instrument_items=instrument_items, instrument_pairs=instrument_pairs, instrument_triples=instrument_triples,
        instrument_heatmap_top_n=INSTRUMENT_OVERLAP_HEATMAP_TOP_N,
        active_tab="instruments",
    )


@app.route("/stats")
def stats():
    return redirect("/timeplots")


# (category db value, display label) -- fixed order so every archive's row
# lines up under the same columns regardless of which categories it
# actually has rows in. Matches the match-method/status names described on
# the /info page.
ARCHIVE_STATUS_CATEGORIES = [
    ("direct_gaia_column", "Direct Gaia"),
    ("name_resolved", "Name resolved"),
    ("positional_easy_match", "Positional"),
    ("needs_review", "Needs review"),
    ("skipped", "Skipped"),
]

# Known instrument-coverage gaps -- unlike everything else on this page,
# this can't be derived from the database (by definition, nothing not
# tracked shows up in holdings), so it's hand-maintained here rather than
# precomputed. Kept in sync with each archive module's docstring; update
# both when a gap gets closed. (archive display_name, what's missing, why)
NOT_YET_TRACKED = [
    ("CARMENES", "co-added template library (TAC)", "carmenes_caha.py covers per-observation raw spectra, both channels; the co-added templates are a separate product"),
    ("—", "ARIES DOT (3.6m Devasthal)", "no public archive; the one data endpoint is PI-login only"),
    ("—", "WEAVE, 4MOST", "surveys not yet public"),
    ("—", "JUST (Lenghu, China)", "not yet public -- site's own Data page still reads \"Coming soon\""),
    ("—", "BeSS (Be Star Spectra, France)", "not yet investigated; only a web query form found, no confirmed API"),
    ("—", "IAO Hanle (India), SAO RAS BTA/SCORPIO (Russia), McDonald Tull Coude, OAN-SPM (Mexico)", "investigated -- no public bulk/API archive found for any of these"),
]

ARCHIVE_STATUS_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Archive Status</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <p class="note">Per-archive sync status, observation date coverage, and match breakdown, precomputed at export time (see the Leaderboard tab note on why) -- refreshed whenever the hosted snapshot is next published, not live. "Last synced" is when this archive's sync last completed a page here, not when the data itself was observed -- for an archive mid-resync when this snapshot was taken, treat its numbers as a work-in-progress, not a final count. "Needs review" and "Skipped" are not dropped -- see More Info for what those mean and how to help resolve them. See the Instruments tab for the per-archive instrument breakdown, including resolving power.</p>
  <table>
    <tr>
      <th>Archive</th><th>Last synced</th><th>Status</th><th>Observations span</th><th>Total</th>
      {% for label in category_labels %}<th>{{ label }}</th>{% endfor %}
    </tr>
    {% for a in archives %}
    <tr>
      <td>{{ a.display_name }}</td>
      <td>{{ a.last_run_at or "never" }}</td>
      <td>{{ a.last_run_status or "—" }}</td>
      <td>{{ a.obs_span or "—" }}</td>
      <td>{{ "{:,}".format(a.total) }}</td>
      {% for c in a.counts %}<td>{{ "{:,}".format(c) }}</td>{% endfor %}
    </tr>
    {% endfor %}
  </table>

  <hr>
  <h2>Known gaps</h2>
  <p class="note">Spectrographs known to exist at an already-implemented archive (or whole archives) that aren't tracked yet -- hand-maintained, not derived from the database. See More Info for the broader "pointer database" scope note.</p>
  <table>
    <tr><th>Archive</th><th>Not yet tracked</th><th>Why</th></tr>
    {% for archive, missing, why in not_yet_tracked %}
    <tr><td>{{ archive }}</td><td>{{ missing }}</td><td>{{ why }}</td></tr>
    {% endfor %}
  </table>
</body>
</html>
"""


@app.route("/status")
def archive_status():
    # Precomputed by scripts.export_to_parquet -- see its module for why
    # (same reasoning as the Leaderboard/Stats: the per-category counts
    # need a GROUP BY over the full, ever-growing holdings table).
    cur = get_cursor()
    cur.execute(
        "SELECT archive_code, display_name, last_run_at, last_run_status, rows_seen_last_run, "
        "min_obs_date, max_obs_date, category, n "
        "FROM archive_status ORDER BY display_name"
    )
    rows = _rows_as_dicts(cur)

    by_archive: dict[str, dict] = {}
    order: list[str] = []
    for r in rows:
        code = r["archive_code"]
        if code not in by_archive:
            by_archive[code] = {
                "display_name": r["display_name"],
                # Just a date -- the exact time this ran isn't useful and
                # made an archive mid-resync look like a finished, precise
                # measurement rather than a snapshot of work in progress.
                "last_run_at": r["last_run_at"].date().isoformat() if r["last_run_at"] else None,
                "last_run_status": r["last_run_status"],
                "min_obs_date": r["min_obs_date"],
                "max_obs_date": r["max_obs_date"],
                "counts": {},
                "total": 0,
            }
            order.append(code)
        if r["category"] is not None:
            by_archive[code]["counts"][r["category"]] = r["n"]
            by_archive[code]["total"] += r["n"]

    archives = [
        {
            "display_name": by_archive[code]["display_name"],
            "last_run_at": by_archive[code]["last_run_at"],
            "last_run_status": by_archive[code]["last_run_status"],
            "obs_span": (
                f"{by_archive[code]['min_obs_date']} to {by_archive[code]['max_obs_date']}"
                if by_archive[code]["min_obs_date"]
                else None
            ),
            "total": by_archive[code]["total"],
            "counts": [by_archive[code]["counts"].get(cat, 0) for cat, _ in ARCHIVE_STATUS_CATEGORIES],
        }
        for code in order
    ]

    return render_template_string(
        ARCHIVE_STATUS_TEMPLATE,
        archives=archives,
        category_labels=[label for _, label in ARCHIVE_STATUS_CATEGORIES],
        not_yet_tracked=NOT_YET_TRACKED,
        active_tab="archive_status",
    )


INFO_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — More Info</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <h2>How matching works</h2>
  <p>Every archive record goes through up to three match methods, tried in this order, and the first one that succeeds wins:</p>
  <ol>
    <li><b>direct_gaia_column</b> — the archive already reports a Gaia DR3 source_id for the record (e.g. DESI, LAMOST, GALAH, SDSS-V). This is just a lookup against the tracked-star list, not a positional or name match, so it's the most reliable method.</li>
    <li><b>name_resolved</b> — no Gaia column, but the archive's reported target name matches one of a tracked star's cached SIMBAD aliases. Tried <i>before</i> position deliberately: Gaia's single-star astrometric solution can be biased for close visual binaries, which can break a positional match even with otherwise-correct proper motion — an identifier match sidesteps that failure mode entirely. Still sanity-checked against the record's own reported position when one is present (within 10 arcmin) — a name match whose own position is nowhere near that star falls through to positional matching instead of being trusted blindly.</li>
    <li><b>positional_easy_match</b> — no Gaia column and no name match (or a name match that failed the sanity check above). The record's reported RA/Dec is checked against tracked stars only (not the full Gaia catalog), each candidate's proper motion propagated to the observation's epoch, within a fixed 1.0 arcsecond radius. Exactly one candidate within radius → matched. More than one → <b>needs_review</b> (ambiguous, gaia_source_id left unassigned). Zero → recorded as <b>skipped</b> (see below) rather than dropped — unless a name match was rejected just above, in which case it's <b>needs_review</b> instead: a rejected name match is often still correct (e.g. the archive's own logged position for that one exposure is simply wrong), so it's kept visible for confirmation rather than dropped with no candidate at all.</li>
  </ol>
  <p class="note">The 1.0" match radius is the same for every archive and instrument. Some instruments have a real, documented systematic offset between their reported pointing and the true catalog position (e.g. finder-camera-derived coordinates) — if that offset ever exceeds 1.0", the record ends up in the skipped queue rather than getting mismatched (the tight radius protects against false positives, at the cost of some real holdings not surfacing automatically).</p>

  <h2>What's likely missing</h2>
  <p>This is a "pointer" database, not a spectra archive — it tracks whether an archive has a spectrum for a star and links to it, not the spectrum data (flux/wavelength arrays) itself. A few concrete, known gaps beyond that:</p>
  <ul>
    <li><b>Archives and instruments not yet tracked</b>: see the Archive Status tab's Known gaps table (whole archives not yet public or investigated, like WEAVE/4MOST/JUST, and specific instruments at already-implemented archives like CARMENES's co-added template library).</li>
    <li><b>Name resolution gaps</b>: not every archive-reported target name resolves to a tracked star via SIMBAD, and it varies a lot by archive — some archives (e.g. NOIRLab) report a much higher fraction of unresolvable names than others, often because the reported name is a survey-internal field ID or calibration marker rather than an actual star name. These records aren't dropped: they're persisted with match_status <b>skipped</b> so they can be manually or crowd-sourced attached to a real Gaia source later. See the Skipped records section below for live, per-archive counts.</li>
    <li><b>Gaia XP continuous spectra</b>: flagged as available per-star (see the "Gaia XP continuous" field on a star's page) but not ingested as data — same lean-pointer tradeoff as everything else here.</li>
    <li><b>SDSS legacy vs. SDSS-V</b>: legacy optical spectroscopy is capped at MJD 58932 (~2020); anything after that boundary lives in the separate SDSS-V optical archive instead, on a different pipeline.</li>
  </ul>

  <p class="note">See the Archive Status tab for when each archive was last synced, a per-archive match breakdown, and the known-gaps table; the Instruments tab for the tracked-instruments/resolving-power breakdown; and the Leaderboard tab for catalog-wide holdings-by-archive and matches-by-method breakdowns.</p>

  <h2>How reduction status is tracked</h2>
  <p>Each holding carries a coarse <b>raw</b> / <b>reduced</b> / <b>unknown</b> label in its "Reduction" column (see the tables below), deliberately simplified from the underlying IVOA ObsCore <code>calib_level</code> scale (0=raw telemetry, 1=instrument-signature-removed, 2=calibrated to standard units, 3=enhanced/combined) — anything above level 1 is bucketed as "reduced" here. This is only set when an archive's sync module has good grounds to know it; otherwise it's left as <b>unknown</b> rather than guessed. Three ways it gets set:</p>
  <ul>
    <li><b>From a real calib_level column</b>: for archives queried via ObsCore/TAP (ESO, CFHT/CADC, DAO, Gemini, OIRSA, MAST, MAST/JWST), the archive reports calib_level per record and it's mapped directly.</li>
    <li><b>Hardcoded, because the archive's access path only ever serves one kind of product</b>: survey pipeline archives with no raw counterpart (SDSS legacy/V, LAMOST, LAMOST MRS, GALAH, RAVE, DESI, Gemini GHOST/IGRINS via GOA) are marked <b>reduced</b>. KOA and <b>NOIRLab</b> are marked <b>raw</b> — NOIRLab's sync query explicitly filters to <code>proc_type = "raw"</code> at the source, so every NOIRLab record this project has ever ingested is an unreduced exposure; there are no reduced NOIRLab spectra to find here, by construction of the query rather than a gap in tracking.</li>
    <li><b>Derived from the file itself</b>: NAOJ inspects the access URL/format of each product to infer raw vs. reduced.</li>
  </ul>
  <p class="note">A handful of archives don't set this field yet, so their holdings sit at <b>unknown</b> even where the true status is actually known with confidence — most notably <b>HARPS-N (TNG)</b>: every record synced from it is a raw exposure (the sync module already dedupes on the raw, unprocessed FITS filename specifically to avoid double-counting each DRS pipeline data product as a separate observation), but that fact isn't yet propagated into the reduction_status field. BeSS is a softer case worth flagging in the other direction: it's marked <b>reduced</b>, but that only means wavelength-calibrated, not flux-calibrated — a real but weaker claim than the "reduced" label implies for e.g. an ESO calib_level-2 spectrum. Treat "unknown" as "not yet recorded," not as "confirmed unclassifiable."</p>

  <h2>Needs-review queue</h2>
  <p class="note">Either an ambiguous positional match (2+ tracked stars fell within the 1.0" radius of the archive's reported position) or a name match rejected as implausible with no positional candidate to fall back on (see How matching works above) — in both cases no single star was assigned automatically. Most recent {{ needs_review|length }} shown{% if needs_review_total > needs_review|length %} of {{ "{:,}".format(needs_review_total) }} total{% endif %}.</p>
  {% if needs_review %}
    <table>
      <tr><th>Archive</th><th>Reported name</th><th>Reported RA, Dec</th><th>Date</th><th>Best separation</th><th>Reduction</th></tr>
      {% for r in needs_review %}
      <tr>
        <td>{{ r.display_name }}</td>
        <td>{{ r.raw_target_name or "—" }}</td>
        <td>{{ "%.4f, %.4f"|format(r.raw_ra, r.raw_dec) if r.raw_ra is not none and r.raw_dec is not none else "—" }}</td>
        <td>{{ r.obs_date or "—" }}</td>
        <td>{{ '%.2f"'|format(r.theta_arcsec) if r.theta_arcsec is not none else "—" }}</td>
        <td>{{ r.reduction_status }}</td>
      </tr>
      {% endfor %}
    </table>
  {% else %}
    <p>None yet.</p>
  {% endif %}

  <h2>Skipped records</h2>
  <p class="note">No candidate at all — nothing within the match radius, an untracked direct Gaia id, or missing/invalid position data. Persisted with the raw reported name/position specifically so they can be reviewed later (e.g. manually or crowd-sourced attachment to a Gaia source), not discarded.</p>
  <table>
    <tr><th>Archive</th><th>Skipped</th></tr>
    {% for r in skipped_by_archive %}
    <tr><td><a href="/info?archive={{ r.archive_code }}#skipped-list">{{ r.display_name }}</a></td><td>{{ "{:,}".format(r.n) }}</td></tr>
    {% endfor %}
  </table>

  <h3 id="skipped-list">{% if archive_filter %}{{ archive_filter }} — {% endif %}Most recent skipped{% if archive_filter %} <a href="/info">(clear filter)</a>{% endif %}</h3>
  {% if skipped %}
    <table>
      <tr><th>Archive</th><th>Reported name</th><th>Reported RA, Dec</th><th>Date</th><th>Reduction</th></tr>
      {% for r in skipped %}
      <tr>
        <td>{{ r.display_name }}</td>
        <td>{{ r.raw_target_name or "—" }}</td>
        <td>{{ "%.4f, %.4f"|format(r.raw_ra, r.raw_dec) if r.raw_ra is not none and r.raw_dec is not none else "—" }}</td>
        <td>{{ r.obs_date or "—" }}</td>
        <td>{{ r.reduction_status }}</td>
      </tr>
      {% endfor %}
    </table>
  {% else %}
    <p>None yet.</p>
  {% endif %}
</body>
</html>
"""


CITATION_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Citation</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <p>This page is currently under development and does not have a citable DOI. Once created, this page will link to the direct citation.</p>
  <p>If you make use of this page for your research, please use the following acknowledgement:</p>
  <p>Source code: <a href="https://github.com/zachway/spectra_pointer" target="_blank" rel="noopener">github.com/zachway/spectra_pointer</a></p>
</body>
</html>
"""


@app.route("/citation")
def citation():
    return render_template_string(CITATION_TEMPLATE, active_tab="citation")


@app.route("/info")
def info():
    # needs_review/skipped_by_archive/skipped (and needs_review_total, in
    # stats_summary) are precomputed by scripts.export_to_parquet -- this
    # route used to run these as four separate live queries against
    # spectroscopy_holdings (a 1GB+ remote Parquet file) on every request,
    # the same slow-page shape already fixed for /sky, /timeplots, and
    # /triage. See export_to_parquet's NEEDS_REVIEW_QUERY/SKIPPED_QUERY for
    # the full reasoning.
    cur = get_cursor()
    cur.execute("SELECT needs_review_total FROM stats_summary")
    needs_review_total = cur.fetchone()[0]

    cur.execute(
        "SELECT display_name, raw_target_name, raw_ra, raw_dec, obs_date, theta_arcsec, reduction_status "
        "FROM needs_review"
    )
    needs_review = _rows_as_dicts(cur)

    cur.execute("SELECT archive_code, display_name, n FROM skipped_by_archive ORDER BY n DESC")
    skipped_by_archive = _rows_as_dicts(cur)

    # The per-archive filter is a rare, deliberate user action (not the
    # default page load), and cheap once narrowed to one archive_code -- kept
    # as a live query rather than precomputing one skipped-records table per
    # archive_code up front.
    archive_filter = request.args.get("archive", "").strip()
    if archive_filter:
        cur.execute(
            """
            SELECT a.display_name, h.raw_target_name, h.raw_ra, h.raw_dec, h.obs_date, h.reduction_status
            FROM spectroscopy_holdings h
            JOIN archives a ON a.archive_code = h.archive_code
            WHERE h.match_status = 'skipped' AND h.archive_code = ?
            ORDER BY h.updated_at DESC
            LIMIT 20
            """,
            [archive_filter],
        )
        skipped = _rows_as_dicts(cur)
    else:
        cur.execute(
            "SELECT display_name, raw_target_name, raw_ra, raw_dec, obs_date, reduction_status FROM skipped"
        )
        skipped = _rows_as_dicts(cur)

    return render_template_string(
        INFO_TEMPLATE, active_tab="info",
        needs_review=needs_review, needs_review_total=needs_review_total,
        skipped=skipped, skipped_by_archive=skipped_by_archive, archive_filter=archive_filter,
    )


def _parse_batch_lines(text: str) -> list[str]:
    seen = set()
    entries = []
    for raw_line in text.splitlines():
        entry = raw_line.strip()
        if not entry or entry in seen:
            continue
        seen.add(entry)
        entries.append(entry)
    return entries


@app.route("/batch", methods=["POST"])
def batch_search():
    export_csv = request.form.get("format", "").strip().lower() == "csv"
    uploaded = request.files.get("file")
    if uploaded and uploaded.filename:
        text = uploaded.read().decode("utf-8", errors="replace")
    else:
        text = request.form.get("names", "")

    entries = _parse_batch_lines(text)
    if not entries:
        return _blank_batch(batch_error="No names or source_ids found in the upload.")

    id_entries = [e for e in entries if e.isdigit()]
    name_entries = [e for e in entries if not e.isdigit()]

    truncated = 0
    if len(name_entries) > MAX_NAME_LOOKUPS:
        truncated = len(name_entries) - MAX_NAME_LOOKUPS
        name_entries = name_entries[:MAX_NAME_LOOKUPS]
        kept = set(id_entries) | set(name_entries)
        entries = [e for e in entries if e in kept]

    name_to_source_id: dict[str, int] = {}
    batch_error = None
    if name_entries:
        try:
            name_to_source_id = resolve_stellar_gaia_ids_batch(name_entries)
        except DALServiceError:
            batch_error = "SIMBAD is currently unavailable — name lookups skipped, source_id lookups below are unaffected."

    all_source_ids = sorted({int(e) for e in id_entries} | set(name_to_source_id.values()))

    tracked: dict[int, dict] = {}
    holdings_counts: dict[int, int] = {}
    if all_source_ids:
        cur = get_cursor()
        # A literal IN (?, ?, ...) list, not list_contains(?, gaia_source_id)
        # -- gaia_source_id is bare on one side of a real comparison operator
        # this way, so DuckDB's Parquet row-group min/max pruning can still
        # apply (stars.parquet is exported sorted by gaia_source_id
        # specifically for this). Wrapping the column inside a function call
        # like list_contains() hides it from that pruning entirely -- the
        # same failure mode already diagnosed and fixed once for
        # STAR_NAME_INDEX_QUERY's normalize()-wrapped filter; confirmed live
        # here too (a 50-id lookup ran ~19x slower via list_contains than the
        # IN-list equivalent against the real stars.parquet).
        id_placeholders = ", ".join("?" for _ in all_source_ids)
        cur.execute(
            f"SELECT gaia_source_id, name_aliases, input_name FROM stars WHERE gaia_source_id IN ({id_placeholders})",
            all_source_ids,
        )
        tracked = {row["gaia_source_id"]: row for row in _rows_as_dicts(cur)}

        cur.execute(
            f"""
            SELECT s.gaia_source_id, COUNT(*) AS n
            FROM spectroscopy_holdings h
            JOIN stars s ON s.star_id = h.star_id
            WHERE s.gaia_source_id IN ({id_placeholders})
            GROUP BY s.gaia_source_id
            """,
            all_source_ids,
        )
        holdings_counts = {row["gaia_source_id"]: row["n"] for row in _rows_as_dicts(cur)}

    results = []
    for entry in entries:
        if entry.isdigit():
            source_id = int(entry)
        else:
            source_id = name_to_source_id.get(entry)

        if source_id is None:
            results.append({
                "query": entry, "source_id": None,
                "status": "not resolved via SIMBAD", "known_as": None, "holdings_count": None,
            })
            continue

        star = tracked.get(source_id)
        if star is None:
            results.append({
                "query": entry, "source_id": source_id,
                "status": "not tracked", "known_as": None, "holdings_count": None,
            })
            continue

        known_as = ", ".join(star["name_aliases"]) if star["name_aliases"] else star["input_name"]
        results.append({
            "query": entry, "source_id": source_id,
            "status": "tracked", "known_as": known_as,
            "holdings_count": holdings_counts.get(source_id, 0),
        })

    if export_csv:
        holdings_by_source_id: dict[int, list[dict]] = {}
        if all_source_ids:
            cur.execute(
                f"""
                SELECT s.gaia_source_id, a.display_name, h.instrument, h.obs_date,
                       h.match_status, h.match_method, h.reduction_status, h.archive_url
                FROM spectroscopy_holdings h
                JOIN stars s ON s.star_id = h.star_id
                JOIN archives a ON a.archive_code = h.archive_code
                WHERE s.gaia_source_id IN ({id_placeholders})
                ORDER BY s.gaia_source_id, a.display_name, h.instrument, h.obs_date
                """,
                all_source_ids,
            )
            for row in _rows_as_dicts(cur):
                holdings_by_source_id.setdefault(row["gaia_source_id"], []).append(row)

        # One row per holding (not per query) so the CSV is the actual list
        # of spectra behind each star, not just a count -- matches what the
        # single-star "download holdings" CSV already does. Queries with no
        # holdings (or that didn't resolve/aren't tracked) still get one row
        # so they aren't silently dropped from the export.
        csv_rows = []
        for r in results:
            base = {"query": r["query"], "source_id": r["source_id"], "status": r["status"], "known_as": r["known_as"]}
            star_holdings = holdings_by_source_id.get(r["source_id"], []) if r["source_id"] is not None else []
            if not star_holdings:
                csv_rows.append({**base, "archive": None, "instrument": None, "obs_date": None,
                                  "match_status": None, "match_method": None, "reduction_status": None,
                                  "archive_url": None})
            else:
                for h in star_holdings:
                    csv_rows.append({
                        **base,
                        "archive": h["display_name"], "instrument": h["instrument"], "obs_date": h["obs_date"],
                        "match_status": h["match_status"], "match_method": h["match_method"],
                        "reduction_status": h["reduction_status"],
                        "archive_url": h["archive_url"],
                    })

        return _csv_response(
            ["query", "source_id", "status", "known_as",
             "archive", "instrument", "obs_date", "match_status", "match_method", "reduction_status", "archive_url"],
            csv_rows,
            "spectra_pointer_batch_lookup.csv",
        )

    note = f"{len(entries)} entries looked up."
    if truncated:
        note += f" {truncated} additional name(s) beyond the {MAX_NAME_LOOKUPS} cap were skipped entirely."

    return _blank_batch(batch_error=batch_error, batch_note=note, batch_results=results)


# =============================================================================
# Crowdsourced triage for match_status = 'skipped' rows (design sketch).
#
# Every other route in this file only reads the DuckDB/Parquet snapshot (see
# the module docstring) -- this is the app's first genuine write path. An
# earlier version of this opened a live psycopg connection via DATABASE_URL
# straight to Postgres, but DATABASE_URL is deliberately never set on the
# hosted Cloud Run deployment: this is a public, unauthenticated web tier,
# and giving it direct write access to the real database is a bigger blast
# radius than this feature is worth. Submissions are appended instead as
# JSON lines to a public file on joy (same host/directory
# scripts.export_to_parquet already publishes the Parquet snapshot to) over
# a narrowly-scoped SSH connection, and only actually land in
# skip_classifications the next time scripts.export_to_parquet runs and
# imports them (see its TRIAGE_QUEUE_QUERY-adjacent import_triage_submissions).
# =============================================================================

TRIAGE_SUBMISSIONS_FILENAME = "triage_submissions.jsonl"


def _joy_ssh_client() -> paramiko.SSHClient:
    """A dedicated, narrowly-scoped SSH key -- never committed to this repo,
    configured entirely via env vars (Cloud Run Secret Manager in
    production) -- connects to joy to append one classification submission.
    The corresponding authorized_keys entry on joy MUST use a forced
    `command=` restriction (see scripts/joy_triage_append.py's setup
    docstring) so this key can only ever run that one append script, never
    an arbitrary shell command -- confirmed live during development that a
    session requesting an arbitrary command string still only ever runs the
    forced command.

    JOY_SSH_HOST_KEY pins the expected host key rather than trusting
    on first use (paramiko's AutoAddPolicy) -- format is a single
    "<keytype> <base64>" pair, e.g. one line copied from
    /etc/ssh/ssh_host_ed25519_key.pub on joy itself (more trustworthy than
    `ssh-keyscan`, which is itself a first-use trust decision).
    """
    host = os.environ.get("JOY_SSH_HOST")
    user = os.environ.get("JOY_SSH_USER")
    key_path = os.environ.get("JOY_SSH_KEY_PATH")
    port = int(os.environ.get("JOY_SSH_PORT", "22"))
    host_key_line = os.environ.get("JOY_SSH_HOST_KEY")
    if not (host and user and key_path and host_key_line):
        raise RuntimeError(
            "JOY_SSH_HOST, JOY_SSH_USER, JOY_SSH_KEY_PATH, and JOY_SSH_HOST_KEY "
            "must all be set -- the /triage submission route needs a live SSH "
            "connection to joy to append a classification (see the comment "
            "above _joy_ssh_client)."
        )

    key_type, key_b64 = host_key_line.split(None, 1)
    host_key = paramiko.PKey.from_type_string(key_type, base64.b64decode(key_b64))

    client = paramiko.SSHClient()
    # Matches paramiko's own internal lookup-key format (SSHClient.connect):
    # bare hostname on the default port, "[host]:port" otherwise -- getting
    # this wrong makes host key verification silently fail to match and
    # raise "not found in known_hosts" even though the right key was added.
    lookup_name = host if port == 22 else f"[{host}]:{port}"
    client.get_host_keys().add(lookup_name, key_type, host_key)
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    client.connect(
        host, port=port, username=user, key_filename=key_path,
        timeout=10, look_for_keys=False, allow_agent=False,
    )
    return client


# A fresh call to _joy_ssh_client() pays a full TCP + SSH key-exchange +
# auth round trip to joy over the public internet -- confirmed live this was
# the dominant chunk of /triage/submit's latency, often 1-3s on its own.
# Cached and reused across requests within this process instead: paramiko
# lets multiple exec_command() calls open independent channels over one
# already-authenticated transport (the server's authorized_keys `command=`
# forced-command applies per channel, not per TCP connection, so each call
# still independently re-runs joy_triage_append.py), which turns every
# submission after the first into just a channel open, no handshake. Guarded
# by a lock since app.run(threaded=True) serves requests concurrently and a
# paramiko SSHClient/Transport isn't safe to drive from multiple threads at
# once.
_joy_ssh_lock = threading.Lock()
_joy_ssh_client_cache: paramiko.SSHClient | None = None


def _append_triage_submission(payload: dict) -> None:
    global _joy_ssh_client_cache
    data = json.dumps(payload, separators=(",", ":")) + "\n"

    for attempt in (1, 2):
        with _joy_ssh_lock:
            client = _joy_ssh_client_cache
            transport = client.get_transport() if client is not None else None
            if transport is None or not transport.is_active():
                if client is not None:
                    client.close()
                client = _joy_ssh_client()
                _joy_ssh_client_cache = client

        try:
            # The remote end's authorized_keys `command=` forced-command
            # ignores whatever we ask to exec here and always runs the
            # append script -- see scripts/joy_triage_append.py. The literal
            # string doesn't matter, but exec_command requires one.
            stdin, stdout, stderr = client.exec_command("append-triage-submission", timeout=10)
            stdin.write(data)
            stdin.channel.shutdown_write()
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                err = stderr.read().decode("utf-8", "replace").strip()
                raise RuntimeError(f"joy rejected this submission (exit {exit_status}): {err}")
            return
        except RuntimeError:
            raise  # a real rejection from the script, not a connection problem -- don't retry
        except Exception:
            # Connection-shaped failure (dropped transport, reset, etc.) --
            # drop the cached client and, on the first attempt, retry once
            # against a freshly-opened connection before giving up.
            with _joy_ssh_lock:
                if _joy_ssh_client_cache is client:
                    _joy_ssh_client_cache = None
            try:
                client.close()
            except Exception:
                pass
            if attempt == 2:
                raise


# Every /triage page load (and the single-record view now means one load
# per skip/submit, not one per 20-row batch) re-fetches and re-parses this
# whole file -- fine when it's small, but it only ever grows, and every
# submit immediately triggers a fresh page load that fetches it again. A
# short TTL cache keeps rapid skip/submit clicks from each paying a full
# fetch+parse; a few seconds of staleness here just means "prior submission"
# annotations can lag slightly behind your own just-submitted vote, which is
# harmless -- the submission itself already landed on joy regardless.
_TRIAGE_SUBMISSIONS_CACHE_TTL_SECONDS = 10.0
_triage_submissions_cache: dict = {"result": None, "fetched_at": 0.0}


def _fetch_triage_submissions() -> tuple[list[dict], str | None]:
    """Reads the public triage_submissions.jsonl file joy's Apache serves
    (appended to by _append_triage_submission, imported into
    skip_classifications by scripts.export_to_parquet).

    Deliberately NOT read via the DATA_TABLES/CREATE VIEW mechanism in
    _make_connection -- that mechanism assumes every file already exists at
    process startup (a missing one there fails every route, not just
    /triage), and this file doesn't exist at all until the first submission
    is ever made. A 404 here is a normal, expected state.

    Since this reads the raw append log rather than skip_classifications
    directly, it shows every submission ever made under a name, not just
    ones not yet applied (this process has no way to know
    skip_classifications.applied_at) -- good enough for "does this
    identifier already have votes", which is all /triage uses it for.

    Cached for _TRIAGE_SUBMISSIONS_CACHE_TTL_SECONDS -- see the comment above
    the cache dict.
    """
    now = time.monotonic()
    cached = _triage_submissions_cache["result"]
    if cached is not None and now - _triage_submissions_cache["fetched_at"] < _TRIAGE_SUBMISSIONS_CACHE_TTL_SECONDS:
        return cached

    result = _fetch_triage_submissions_uncached()
    _triage_submissions_cache["result"] = result
    _triage_submissions_cache["fetched_at"] = now
    return result


def _fetch_triage_submissions_uncached() -> tuple[list[dict], str | None]:
    source = _resolve_data_source()
    if source.startswith("http://") or source.startswith("https://"):
        try:
            with urllib.request.urlopen(f"{source}/{TRIAGE_SUBMISSIONS_FILENAME}", timeout=10) as resp:
                body = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return [], None
            return [], str(exc)
        except (urllib.error.URLError, OSError) as exc:
            return [], str(exc)
    else:
        # SPECTRA_DATA_DIR local-dev mode -- source is a plain directory.
        local_path = os.path.join(source, TRIAGE_SUBMISSIONS_FILENAME)
        if not os.path.exists(local_path):
            return [], None
        try:
            with open(local_path, encoding="utf-8") as f:
                body = f.read()
        except OSError as exc:
            return [], str(exc)

    submissions = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            submissions.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # tolerate a stray malformed line rather than 500ing the whole page
    return submissions, None


# Generous compared to ingest.add_star.resolve_gaia_source_id's 2" ambiguity-
# check radius (see add_star.py:117-149) -- deliberately wide (10-30" per the
# design notes) because a bright star's old catalog position, plus real high
# proper motion, can leave real separation from the true Gaia-epoch position.
# A human still reviews the actual result before confirming, so a wider
# radius costs nothing but a few more candidate rows to look at.
TRIAGE_CONE_SEARCH_RADIUS_ARCSEC = 20.0

# Matches sync.matcher.NAME_MATCH_SANITY_RADIUS_ARCSEC (600" / 10') -- the
# same "is this plausibly one star" cutoff used there for name-matched
# records, reused here to warn when a triage group's own member positions
# (see export_to_parquet.py's position_spread_deg) don't actually agree with
# each other. Duplicated rather than imported so webapp.app doesn't have to
# pull in sync.matcher's live-sync dependencies for one constant.
TRIAGE_POSITION_SPREAD_WARN_DEG = 600.0 / 3600.0

# Same TAP pattern as ingest.add_star's GAIA_CONE_QUERY (see add_star.py:70-77),
# but also pulls phot_g_mean_mag and orders by it -- the design notes call for
# showing the *actual* query result (nothing found, or only much-fainter
# spurious sources), not just a count, so a contributor/reviewer can judge
# "fainter" at a glance instead of re-querying Gaia themselves.
TRIAGE_GAIA_CONE_QUERY = """
SELECT source_id, phot_g_mean_mag
FROM gaiadr3.gaia_source
WHERE 1=CONTAINS(
    POINT('ICRS', ra, dec),
    CIRCLE('ICRS', {ra}, {dec}, {radius_deg})
)
ORDER BY phot_g_mean_mag ASC
"""


def _esasky_url(ra: float, dec: float) -> str:
    return f"https://sky.esa.int/esasky/?target={ra}%20{dec}&fov=0.2&hips=DSS2+color&cooframe=J2000&sci=true&lang=en"


def _simbad_coord_url(ra: float, dec: float) -> str:
    return f"https://simbad.cds.unistra.fr/simbad/sim-coo?Coord={ra}+{dec}&Radius=2&Radius.unit=arcmin"


# =============================================================================
# "View headers" on a triage record -- reads just the primary FITS header off
# an archive_url via a bounded HTTP GET, instead of a reviewer downloading the
# whole spectrum to see e.g. OBJECT/RA/DEC/INSTRUME/DATE-OBS. Only works when
# archive_url actually points at a raw FITS file rather than a landing page or
# a resolver stub that needs an extra hop (ESO, CADC DataLink, LBT's portal,
# etc.) -- _fetch_fits_header detects that case and reports it rather than
# guessing.
#
# archive_url is data this app already trusts enough to render as an outbound
# <a href>, but here the *server* is the one making the request, off a
# visitor-supplied query-string value -- on a public, unauthenticated Cloud
# Run service that turns an unrestricted URL param into an SSRF proxy against
# anything reachable from the container (notably the GCP metadata server).
# The fix is an explicit hostname allowlist, not just a scheme check --
# it's the exact host set observed across every archive_url in production
# (see spectroscopy_holdings, one entry per sync/archives/*.py module), so it
# doesn't reject any real archive link while still refusing everything else.
_ARCHIVE_URL_ALLOWED_HOSTS = {
    "archives.ia2.inaf.it", "caha.sdc.cab.inta-csic.es",
    "ws.cadc-ccda.hia-iha.nrc-cnrc.gc.ca", "atlas.obs-hp.fr",
    "archive.eso.org", "dc.g-vo.org", "datacentral.org.au",
    "archive.gemini.edu", "gtc.sdc.cab.inta-csic.es",
    "mercatorvo.ster.kuleuven.be", "casu.ast.cam.ac.uk",
    "koa.ipac.caltech.edu", "www.lamost.org", "archive.lbto.org",
    "mthamilton.ucolick.org", "mast.stsci.edu", "jvo.nao.ac.jp",
    "astroarchive.noirlab.edu", "oirsa.cfa.harvard.edu:8080",
    "ssda.saao.ac.za", "dr19.sdss.org", "skyserver.sdss.org",
    "data.sdss.org",
}

_FITS_BLOCK_SIZE = 2880  # FITS header cards come in fixed 80-char x 36-card blocks
_FITS_MAX_HEADER_BLOCKS = 128  # 368,640 bytes -- covers even HARPS-N e2ds headers (~213 KB,
# unusually large because they carry per-order wavelength-solution coefficients as keywords),
# confirmed against a real archives.ia2.inaf.it file, while still tiny next to the actual data


class _HeaderUnavailable(Exception):
    """Raised when archive_url can't be read as a bare FITS header -- not a
    real error, just something to display to the reviewer in place of one."""


def _is_headerable_url(url: str) -> bool:
    """Whether a "headers" link is even worth offering for this archive_url
    -- same allowlist _fetch_fits_header enforces server-side, checked again
    here just so the triage page doesn't dangle a link that's guaranteed to
    fail."""
    try:
        parsed = urlsplit(url)
    except ValueError:
        return False
    return parsed.scheme in ("http", "https") and parsed.netloc in _ARCHIVE_URL_ALLOWED_HOSTS


def _fetch_fits_header(url: str) -> list[str]:
    """Best-effort read of just a FITS primary header, via a single bounded
    GET rather than downloading the whole file. Raises _HeaderUnavailable if
    the URL isn't allowlisted, doesn't look like FITS, or the archive doesn't
    cooperate (a landing page, a DataLink/resolver stub, an embargoed 403,
    etc.) -- all of which are real outcomes given how differently each
    archive's archive_url is shaped (see the module comment above).
    """
    parsed = urlsplit(url)
    if parsed.scheme not in ("http", "https") or parsed.netloc not in _ARCHIVE_URL_ALLOWED_HOSTS:
        raise _HeaderUnavailable("This archive link isn't one we can read a header from directly.")

    range_cap = _FITS_MAX_HEADER_BLOCKS * _FITS_BLOCK_SIZE
    try:
        resp = requests.get(
            url, headers={"Range": f"bytes=0-{range_cap - 1}"},
            stream=True, timeout=15,
        )
    except requests.RequestException as exc:
        raise _HeaderUnavailable(f"Couldn't reach the archive: {exc}") from exc

    with resp:
        if resp.status_code not in (200, 206):
            raise _HeaderUnavailable(f"Archive returned HTTP {resp.status_code}.")

        cards: list[str] = []
        buf = b""
        gunzip = None  # set once we see a gzip magic number on the first chunk
        try:
            for raw_chunk in resp.iter_content(chunk_size=_FITS_BLOCK_SIZE):
                if gunzip is None and raw_chunk[:2] == b"\x1f\x8b":
                    # Some archives (e.g. LAMOST) serve .fits as a gzip
                    # stream and ignore the Range header entirely -- inflate
                    # on the fly rather than treating the compressed bytes
                    # as if they were the FITS header themselves.
                    gunzip = zlib.decompressobj(wbits=zlib.MAX_WBITS | 16)
                chunk = gunzip.decompress(raw_chunk) if gunzip is not None else raw_chunk
                buf += chunk
                while len(buf) >= _FITS_BLOCK_SIZE:
                    block, buf = buf[:_FITS_BLOCK_SIZE], buf[_FITS_BLOCK_SIZE:]
                    if not cards and block[:6] not in (b"SIMPLE", b"XTENSI"):
                        raise _HeaderUnavailable(
                            "This doesn't look like a FITS file -- the link is probably "
                            "a landing page or resolver rather than the spectrum itself."
                        )
                    for i in range(0, _FITS_BLOCK_SIZE, 80):
                        card = block[i:i + 80].decode("ascii", errors="replace").rstrip()
                        cards.append(card)
                        if card == "END":
                            return cards
                if len(buf) + len(cards) * 80 > range_cap:
                    break
        except requests.RequestException as exc:
            raise _HeaderUnavailable(f"Connection dropped while reading: {exc}") from exc

    raise _HeaderUnavailable(
        "Didn't find the header's END card within the first "
        f"{range_cap // 1024} KB -- giving up rather than downloading further."
    )


TRIAGE_HEADER_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — FITS header</title>
  <style>""" + SHARED_STYLE + """
    pre.fits-header { background: #f4f4f4; padding: 0.8rem; overflow-x: auto; font-size: 0.85rem; }
  </style>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>
  <h2>FITS header</h2>
  <p class="note">Read directly from <a href="{{ url }}" target="_blank" rel="noopener">{{ url }}</a>
    via a bounded range request -- the file itself was never downloaded.</p>
  {% if error %}
    <p class="error">Couldn't read a header: {{ error }}</p>
  {% else %}
    <pre class="fits-header">{% for card in cards %}{{ card }}
{% endfor %}</pre>
  {% endif %}
</body>
</html>
"""


@app.route("/triage/header")
def triage_header():
    url = request.args.get("url", "")
    try:
        cards = _fetch_fits_header(url)
        error = None
    except _HeaderUnavailable as exc:
        cards = []
        error = str(exc)
    return render_template_string(TRIAGE_HEADER_TEMPLATE, url=url, cards=cards, error=error)


TRIAGE_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Triage</title>
  <style>""" + SHARED_STYLE + """
    .triage-row { border: 1px solid #000; padding: 0.6rem 0.8rem; margin-top: 1rem; }
    .triage-row form { margin-top: 0.5rem; }
    .triage-row label { display: block; margin: 0.2rem 0; }
    .triage-row input[type=text] { font-family: monospace; }
    .finder-links a { margin-right: 1rem; }
    .prior-submissions { font-style: italic; }
    .cone-result { display: block; margin: 0.2rem 0 0.2rem 1.4rem; }
    .record-list { font-size: 0.9rem; }
    .record-entries { display: flex; flex-wrap: wrap; gap: 0.3rem 0.8rem; margin-top: 0.3rem; }
    .record-entry { white-space: nowrap; }
    .record-entry a { margin-right: 0.3rem; }
    .header-link { font-size: 0.85em; color: #555; }
    .mood-image { float: right; max-width: 140px; margin: 0 0 0.5rem 1rem; }
    .triage-progress { display: flex; justify-content: space-between; align-items: baseline; }
    .skip-link { white-space: nowrap; margin-left: 1rem; }
    .spread-warning { color: #a00; font-weight: bold; }
  </style>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <h2>Triage: skipped records</h2>
  <img class="mood-image" src="/static/triage_mood.jpg" alt="how the triage queue feels sometimes">
  <p class="note">Triaging as <b>{{ submitter }}</b> (<a href="/triage?change_submitter=1">not you?</a>) --
    records you've already submitted a classification for are filtered out of your queue below.</p>
  <p class="note">
    These are spectroscopy_holdings rows with match_status = 'skipped' -- the
    automated matcher (see <a href="/info">More Info</a>) found no name or
    positional candidate at all for them. Rows are grouped by (archive,
    reported target name) rather than shown one-per-record, so the same
    identifier doesn't resurface over and over -- a classification is
    submitted once and recorded against every underlying record sharing that
    name. Shown one at a time, shuffled per-visitor (named/high-record-count
    groups still weighted to surface earlier) rather than a fixed queue, so
    different contributors aren't all working through the identical
    sequence. Submissions below do <b>not</b> update the database directly:
    they accumulate as independent votes (recorded to a public file,
    imported into the real skip_classifications table the next time this
    project's export job runs) and only get applied once a quorum of
    contributors agree (design sketch -- the apply step is a documented
    stub, not wired up yet).
  </p>

  {% if error %}
    <p class="error">Error: {{ error }}</p>
  {% endif %}
  {% if note %}
    <p class="note">{{ note }}</p>
  {% endif %}
  {% if submissions_error %}
    <p class="note">Prior-submission history unavailable ({{ submissions_error }}) -- showing the skipped queue without it.</p>
  {% endif %}

  {% if total %}
    <p class="triage-progress">
      <span>Record {{ offset + 1 }} of {{ total }} in your shuffled queue.</span>
      <a class="skip-link" href="/triage?{{ skip_query }}">Skip this one, show me another &rarr;</a>
    </p>
  {% endif %}

  {% for r in rows %}
  <div class="triage-row">
    <p>
      <b>{{ r.display_name }}</b> —
      {{ r.raw_target_name or "(no reported name)" }}
      {% if r.raw_ra is not none and r.raw_dec is not none %}
        at RA {{ "%.5f"|format(r.raw_ra) }}, Dec {{ "%.5f"|format(r.raw_dec) }}
        {% if r.position_spread_deg is not none and r.position_spread_deg > position_spread_warn_deg %}
          <span class="spread-warning" title="This name's own records don't all report the same position -- the coordinate above is just one record's, not necessarily representative of the group">&#9888; records under this name disagree by {{ "%.1f"|format(r.position_spread_deg) }}&deg; -- may not be one star</span>
        {% endif %}
      {% else %}
        (no reported position)
      {% endif %}
      {% if r.obs_date %} — earliest {{ r.obs_date }}{% endif %}
      {% if r.instrument %} — {{ r.instrument }}{% endif %}
      — {{ r.n_records }} record{{ "s" if r.n_records != 1 else "" }}
    </p>

    <details class="record-list">
      <summary>{{ r.n_records }} archive record{{ "s" if r.n_records != 1 else "" }} under this name</summary>
      <div class="record-entries">
        {% for rec in r.records %}<span class="record-entry"><a href="{{ rec.url }}" target="_blank" rel="noopener">{{ rec.oid }}</a>{% if rec.header_url %} <a href="{{ rec.header_url }}" target="_blank" rel="noopener" class="header-link">headers</a>{% endif %}</span>{% endfor %}
        {% if r.records_truncated %}<span class="note">…and more (showing first {{ r.records|length }})</span>{% endif %}
      </div>
    </details>

    {% if r.sky_finder_url %}
    <p class="finder-links">
      <a href="{{ r.sky_finder_url }}" target="_blank" rel="noopener">ESASky finder chart</a>
      <a href="{{ r.simbad_url }}" target="_blank" rel="noopener">SIMBAD at this position</a>
    </p>
    {% endif %}

    {% if r.prior_submissions %}
    <p class="prior-submissions">{{ r.prior_submissions|length }} prior submission{{ "s" if r.prior_submissions|length != 1 else "" }} under this name:
      {% for s in r.prior_submissions %}{{ s.outcome }} ({{ s.submitter }}){% if not loop.last %}; {% endif %}{% endfor %}
    </p>
    {% endif %}

    <form method="post" action="/triage/submit">
      <input type="hidden" name="offset" value="{{ offset }}">
      <input type="hidden" name="archive_code" value="{{ r.archive_code }}">
      {% if r.raw_target_name %}
        <input type="hidden" name="raw_target_name" value="{{ r.raw_target_name }}">
      {% else %}
        <input type="hidden" name="archive_obs_id" value="{{ r.archive_obs_ids[0] }}">
      {% endif %}

      <label><input type="radio" name="outcome" value="attach_gaia_source" required>
        Attach to Gaia source:
        <input type="text" name="gaia_target" placeholder="Gaia source_id or star name" size="28">
      </label>

      <label><input type="radio" name="outcome" value="attach_bright_star">
        Attach to bright star (too bright for Gaia to have detected at all):
        <input type="text" name="bright_star_target" placeholder="Bright Star (HR) number or star name" size="28">
      </label>

      <label><input type="radio" name="outcome" value="not_a_real_target">
        Confirmed — not a real target (calibration frame, engineering exposure, etc.)
      </label>

      <label><input type="radio" name="outcome" value="not_a_star">
        Not a star (galaxy, quasar, Solar System object, or other non-stellar target)
      </label>

      <label>
        <input type="radio" name="outcome" value="confirmed_absent_from_gaia" {% if not r.sky_finder_url %}disabled{% endif %}>
        Confirmed — real star, no Gaia DR3 source found nearby (and not a known bright star)
        {% if r.sky_finder_url %}
          (<a href="{{ r.cone_search_url }}">run live {{ '%g'|format(triage_cone_search_radius) }}&Prime; Gaia cone search to confirm</a>)
        {% endif %}
      </label>
      {% if r.cone_search_result %}
        <span class="cone-result note">Cone search result: {{ r.cone_search_result }}</span>
        <input type="hidden" name="gaia_cone_search_result" value="{{ r.cone_search_result }}">
        <input type="hidden" name="gaia_cone_search_radius_arcsec" value="{{ triage_cone_search_radius }}">
      {% endif %}

      <label>Submitter name/handle: <input type="text" name="submitter" value="{{ submitter_prefill }}" required size="24"></label>
      <label>Note (optional): <input type="text" name="note" size="40"></label>
      <button type="submit">Submit classification for all {{ r.n_records }} record{{ "s" if r.n_records != 1 else "" }}</button>
    </form>
  </div>
  {% endfor %}
  {% if not rows %}<p>No skipped records right now.</p>{% endif %}
</body>
</html>
"""


TRIAGE_GATE_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>The Spectra Pointer — Triage</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <div class="site-header">
    <h1>The Spectra Pointer</h1>
    <img class="logo-placeholder" src="/static/logo.png" alt="The Spectra Pointer logo">
  </div>""" + NAV_HTML + """
  <h2>Triage: skipped records</h2>
  <p class="note">
    Enter the name/handle you'll be submitting classifications under. It's
    remembered in a cookie on this browser (~6 months) and used to filter
    your queue below so you're not shown records you've already classified
    in a previous session.
  </p>
  <form method="get" action="/triage">
    <label>Name/handle: <input type="text" name="submitter" required size="24" autofocus></label>
    <button type="submit">Start triaging</button>
  </form>
</body>
</html>
"""


TRIAGE_SUBMITTER_COOKIE = "triage_submitter"
TRIAGE_SEED_COOKIE = "triage_seed"
TRIAGE_COOKIE_MAX_AGE_SECONDS = 180 * 24 * 3600  # ~6 months


def _triage_redirect(offset: int, **params) -> Response:
    """Builds an /triage redirect carrying the current offset (so an error,
    or a submitted/skipped record, lands back on the right spot in the
    contributor's shuffled queue instead of jumping to the start) plus
    whatever else the caller wants to set (error=, note=, ...).
    """
    params["offset"] = offset
    return redirect("/triage?" + urlencode({k: v for k, v in params.items() if v is not None}))


# Every visitor used to see the literal same fixed slice of triage_queue in
# the same order every time (a plain SQL ORDER BY over a small, infrequently
# -changing precomputed table -- see TRIAGE_QUEUE_QUERY's own LIMIT
# TRIAGE_QUEUE_TOP_N in scripts/export_to_parquet.py) -- confirmed this meant
# everyone triaging on a given day just worked through the identical sequence
# of records. Reordered here instead, once per request, keyed off a random
# per-visitor seed cookie (TRIAGE_SEED_COOKIE) so different visitors fan out
# across the pool -- named groups still always sort before nameless ones
# (matches triage_queue's own priority, no reason to ever invert that), but
# within each of those two tiers the order is a weighted random shuffle
# (Efraimidis-Spirakis weighted sampling: key = -ln(u)/weight, sorted
# ascending) so a group with many underlying records is *more likely* to
# surface early without being pinned to the exact same n_records-DESC order
# every single time. TRIAGE_QUEUE_TOP_N (now in the low thousands rather than
# 200) is cheap to pull in full and reorder in Python rather than pushing
# this into SQL.
#
# votes_by_group halves a group's weight per distinct submitter who's already
# classified it (0 votes -> full weight, 1 -> half, 2 -- the eventual target
# of "just get a couple independent eyes on each name" -- -> a quarter, and
# so on) so groups that already have enough independent eyes on them fade
# from rotation and under-covered ones surface more -- a soft nudge rather
# than a hard cutoff, since nothing here excludes a name outright once it
# hits that target (submitters can disagree, and a third opinion is still
# useful; there's also no quorum-consuming apply step wired up yet for this
# to gate, see the /triage page's own note).
def _shuffle_triage_pool(pool: list[dict], seed: str, votes_by_group: dict[tuple, int]) -> list[dict]:
    rng = random.Random(seed)

    def weight(item: dict) -> float:
        votes = votes_by_group.get((item["archive_code"], item["group_key"]), 0)
        return max(item["n_records"], 1) / (2 ** votes)

    def weighted_shuffle(items: list[dict]) -> list[dict]:
        keyed = [(-math.log(rng.random()) / weight(item), item) for item in items]
        keyed.sort(key=lambda pair: pair[0])
        return [item for _, item in keyed]

    named = [r for r in pool if r["raw_target_name"]]
    unnamed = [r for r in pool if not r["raw_target_name"]]
    return weighted_shuffle(named) + weighted_shuffle(unnamed)


@app.route("/triage")
def triage():
    # Step 1 of 2: who's triaging. A submitter name/handle used to only get
    # collected per-submission (at the bottom of each row's form, prefilled
    # from TRIAGE_SUBMITTER_COOKIE) -- nothing gated entry on it, and every
    # fresh /triage load reset to offset=0 in this visitor's shuffled queue
    # regardless, so a returning contributor landed back at the top of the
    # same sequence and re-saw records they'd already classified last
    # session (still sitting in triage_queue until the next export/import
    # cycle removes them). Gating on a name up front, then filtering the
    # pool below by that name's own submission history, fixes both: no
    # queue is shown until we know who's asking, and the queue we do show
    # excludes anything that submitter already voted on.
    gate_submitter = request.args.get("submitter", "").strip()
    if gate_submitter:
        resp = redirect("/triage")
        resp.set_cookie(TRIAGE_SUBMITTER_COOKIE, gate_submitter, max_age=TRIAGE_COOKIE_MAX_AGE_SECONDS, samesite="Lax")
        return resp

    submitter = request.cookies.get(TRIAGE_SUBMITTER_COOKIE, "").strip()
    if not submitter or request.args.get("change_submitter"):
        return Response(render_template_string(TRIAGE_GATE_TEMPLATE, active_tab="triage"))

    cur = get_cursor()
    # Reads the precomputed triage_queue table (see
    # scripts/export_to_parquet.py's TRIAGE_QUEUE_QUERY) rather than grouping
    # spectroscopy_holdings live -- a true GROUP BY (archive_code,
    # raw_target_name) over the full skipped set (12M+ rows, 900K+ distinct
    # names) OOMs the 1 GiB Cloud Run container, confirmed live against
    # production. Precomputing where memory isn't capped also means this can
    # be a cheap, small read instead of a multi-second remote scan on every
    # page load -- this project tries to keep Cloud Run request time (and
    # therefore cost) down wherever the data doesn't need to be live-fresh,
    # and a "run the export by hand every so often" cadence is already how
    # every other derived page here works. No ORDER BY/LIMIT here -- the
    # whole (already-capped-upstream) pool is fetched and reshuffled in
    # Python by _shuffle_triage_pool, per-visitor.
    cur.execute(
        """
        SELECT archive_code, display_name, group_key, raw_target_name, n_records,
               archive_obs_ids, archive_urls, raw_ra, raw_dec, position_spread_deg,
               obs_date, instrument, updated_at
        FROM triage_queue
        """
    )
    pool = _rows_as_dicts(cur)

    # Submission history, so a contributor can see this identifier already
    # has other independent votes before adding their own -- read from the
    # same public JSONL file _append_triage_submission writes to (see its
    # comment), grouped the same way triage_queue's group_key is: this
    # process has no way to know skip_classifications.applied_at (it never
    # touches Postgres at all), so this shows every submission ever made
    # under a name, not just ones not yet applied.
    submissions, submissions_error = _fetch_triage_submissions()
    submissions_by_group = defaultdict(list)
    for s in submissions:
        name = (s.get("raw_target_name") or "").strip()
        group_key = name if name else f"obs:{s.get('archive_obs_id')}"
        submissions_by_group[(s.get("archive_code"), group_key)].append(s)

    # Distinct-submitter count per group, independent of which submitter is
    # asking -- feeds _shuffle_triage_pool's weighting below so names that
    # already have several independent votes fade from everyone's rotation,
    # not just this submitter's (that's the submitter_key filter right after
    # this, which is a per-visitor exclusion rather than a shared signal).
    votes_by_group = {
        key: len({(s.get("submitter") or "").strip().casefold() for s in subs if (s.get("submitter") or "").strip()})
        for key, subs in submissions_by_group.items()
    }

    # Step 2 of 2: filter out anything this submitter already voted on.
    # Case-insensitive/trimmed compare since "handle" is free text, not an
    # account -- catches the common "Zach" vs "zach" variance without
    # requiring an exact match.
    submitter_key = submitter.casefold()
    pool = [
        r for r in pool
        if not any(
            (s.get("submitter") or "").strip().casefold() == submitter_key
            for s in submissions_by_group.get((r["archive_code"], r["group_key"]), [])
        )
    ]

    seed = request.cookies.get(TRIAGE_SEED_COOKIE) or secrets.token_hex(8)
    ordered = _shuffle_triage_pool(pool, seed, votes_by_group)
    total = len(ordered)

    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        offset = 0
    if total:
        offset %= total  # wraps back to the top of the queue past the end, e.g. after repeated skips

    rows = ordered[offset:offset + 1]
    for r in rows:
        r["records"] = [
            {
                "oid": oid,
                "url": url,
                "header_url": (
                    "/triage/header?" + urlencode({"url": url})
                    if _is_headerable_url(url) else None
                ),
            }
            for oid, url in zip(r["archive_obs_ids"] or [], r["archive_urls"] or [])
        ]
        r["records_truncated"] = r["n_records"] > len(r["records"])

    # Cone-search preview, if the contributor just clicked "run live cone
    # search" for the identifier below (see /triage/cone_search) -- carried
    # over via a redirect query string (no JS/session state in this sketch).
    preview_key = (request.args.get("preview_archive_code", ""), request.args.get("preview_group_key", ""))
    preview_result = request.args.get("preview_result", "")

    for r in rows:
        key = (r["archive_code"], r["group_key"])
        r["prior_submissions"] = submissions_by_group.get(key, [])
        if r["raw_ra"] is not None and r["raw_dec"] is not None:
            r["sky_finder_url"] = _esasky_url(r["raw_ra"], r["raw_dec"])
            r["simbad_url"] = _simbad_coord_url(r["raw_ra"], r["raw_dec"])
            r["cone_search_url"] = "/triage/cone_search?" + urlencode({
                "archive_code": r["archive_code"],
                "group_key": r["group_key"],
                "ra": r["raw_ra"],
                "dec": r["raw_dec"],
                "offset": offset,
            })
            r["cone_search_result"] = preview_result if key == preview_key else None
        else:
            r["sky_finder_url"] = None
            r["cone_search_result"] = None

    resp = Response(render_template_string(
        TRIAGE_TEMPLATE, active_tab="triage", rows=rows, submitter=submitter,
        error=request.args.get("error"), note=request.args.get("note"),
        submissions_error=submissions_error, triage_cone_search_radius=TRIAGE_CONE_SEARCH_RADIUS_ARCSEC,
        offset=offset, total=total, skip_query=urlencode({"offset": offset + 1}),
        submitter_prefill=submitter, position_spread_warn_deg=TRIAGE_POSITION_SPREAD_WARN_DEG,
    ))
    if request.cookies.get(TRIAGE_SEED_COOKIE) != seed:
        resp.set_cookie(TRIAGE_SEED_COOKIE, seed, max_age=TRIAGE_COOKIE_MAX_AGE_SECONDS, samesite="Lax")
    return resp


@app.route("/triage/cone_search")
def triage_cone_search():
    """Live Gaia DR3 cone search for one skipped identifier group -- the gate
    the design notes require before "confirmed absent from Gaia" can be
    submitted at all: a human can't reliably eyeball non-detection (Gaia goes
    to G~21, crowding/saturation effects are easy to misjudge), so this runs
    the real query and hands the actual result back rather than taking
    anyone's word.
    """
    archive_code = request.args.get("archive_code", "")
    group_key = request.args.get("group_key", "")
    try:
        offset = int(request.args.get("offset", "0"))
    except ValueError:
        offset = 0
    try:
        ra = float(request.args.get("ra", ""))
        dec = float(request.args.get("dec", ""))
    except ValueError:
        return _triage_redirect(offset, error="Missing/invalid position for cone search.")

    try:
        job = _launch_gaia_job(
            TRIAGE_GAIA_CONE_QUERY.format(ra=ra, dec=dec, radius_deg=TRIAGE_CONE_SEARCH_RADIUS_ARCSEC / 3600)
        )
        table = job.get_results()
    except Exception as exc:
        return _triage_redirect(offset, error=f"Gaia cone search failed: {exc}")

    if len(table) == 0:
        summary = f"0 Gaia DR3 sources found within {TRIAGE_CONE_SEARCH_RADIUS_ARCSEC:g}\" of ({ra:.5f}, {dec:.5f})."
    else:
        shown = [f"{int(row['source_id'])} (G={row['phot_g_mean_mag']:.1f})" for row in table[:10]]
        summary = f"{len(table)} Gaia DR3 source(s) found within {TRIAGE_CONE_SEARCH_RADIUS_ARCSEC:g}\": " + ", ".join(shown)
        if len(table) > 10:
            summary += f", … ({len(table) - 10} more, faintest first excluded)"

    return redirect("/triage?" + urlencode({
        "preview_archive_code": archive_code,
        "preview_group_key": group_key,
        "preview_result": summary,
        "offset": offset,
    }))


@app.route("/triage/submit", methods=["POST"])
def triage_submit():
    try:
        offset = int(request.form.get("offset", "0"))
    except ValueError:
        offset = 0

    archive_code = request.form.get("archive_code", "").strip()
    # Exactly one of these is set, depending on which branch of the form's
    # {% if r.raw_target_name %} the row rendered (see TRIAGE_TEMPLATE):
    # named groups vote by name (applies to every currently-skipped record
    # under that name, not just the up-to-50 sampled into triage_queue for
    # display -- see the INSERT...SELECT below); nameless groups are always
    # a single specific record, voted on directly by archive_obs_id.
    raw_target_name = request.form.get("raw_target_name", "").strip()
    archive_obs_id = request.form.get("archive_obs_id", "").strip()
    outcome = request.form.get("outcome", "").strip()
    submitter = request.form.get("submitter", "").strip()
    note = request.form.get("note", "").strip() or None

    if not archive_code or not (raw_target_name or archive_obs_id) or not submitter:
        return _triage_redirect(offset, error="archive_code, a target identifier, and submitter are all required.")

    proposed_gaia_source_id = None
    proposed_bsc_hr_number = None
    cone_radius = None
    cone_result = None

    if outcome == "attach_gaia_source":
        target = request.form.get("gaia_target", "").strip()
        if not target:
            return _triage_redirect(offset, error="Enter a Gaia source_id or star name to attach.")
        if target.isdigit():
            proposed_gaia_source_id = int(target)
        else:
            # Reuses ingest.add_star.resolve_gaia_source_id (already imported
            # above, add_star.py:117-149) -- SIMBAD-first, tight-radius Gaia
            # cone-search fallback, the same resolution path add_star_by_name()
            # uses. Deliberately NOT restricted to already-tracked stars: any
            # real Gaia DR3 source_id should be attachable here, and add_star()
            # (see the apply-step TODO below) is what fetches-and-inserts a
            # not-yet-tracked star on demand.
            try:
                proposed_gaia_source_id = resolve_gaia_source_id(target)
            except (ValueError, DALServiceError) as exc:
                return _triage_redirect(offset, error=f"Could not resolve {target!r}: {exc}")

    elif outcome == "attach_bright_star":
        target = request.form.get("bright_star_target", "").strip()
        if not target:
            return _triage_redirect(offset, error="Enter a Bright Star (HR) number or star name to attach.")
        if target.isdigit():
            proposed_bsc_hr_number = int(target)
        else:
            # Same SIMBAD-first resolution pattern as attach_gaia_source
            # above, just resolving an HR number instead of a Gaia source_id
            # -- see ingest.add_star.resolve_bsc_hr_number.
            try:
                proposed_bsc_hr_number = resolve_bsc_hr_number(target)
            except ValueError as exc:
                return _triage_redirect(offset, error=f"Could not resolve {target!r}: {exc}")

    elif outcome == "confirmed_absent_from_gaia":
        cone_result = request.form.get("gaia_cone_search_result", "").strip()
        radius_raw = request.form.get("gaia_cone_search_radius_arcsec", "").strip()
        if not cone_result or not radius_raw:
            return _triage_redirect(
                offset,
                error="Run the live Gaia cone-search preview for this row before confirming it's absent from Gaia.",
            )
        cone_radius = float(radius_raw)

    elif outcome not in ("not_a_real_target", "not_a_star"):
        return _triage_redirect(offset, error="Unrecognized outcome.")

    # archive_obs_id/raw_target_name pass through as-is (either the specific
    # record, for a nameless singleton group, or the shared name, for a named
    # group); scripts.export_to_parquet's import step is what actually
    # expands a named-group vote to every currently-matching record, since
    # this process has no live Postgres access to do that expansion itself
    # anymore -- see its import_triage_submissions().
    payload = {
        "archive_code": archive_code,
        "outcome": outcome,
        "proposed_gaia_source_id": proposed_gaia_source_id,
        "proposed_bsc_hr_number": proposed_bsc_hr_number,
        "gaia_cone_search_radius_arcsec": cone_radius,
        "gaia_cone_search_result": cone_result,
        "submitter": submitter,
        "note": note,
        "submitted_at": datetime.now(timezone.utc).isoformat(),
    }
    if raw_target_name:
        payload["raw_target_name"] = raw_target_name
    else:
        payload["archive_obs_id"] = archive_obs_id

    try:
        _append_triage_submission(payload)
    except RuntimeError as exc:
        return _triage_redirect(offset, error=str(exc))
    except Exception as exc:  # paramiko raises various exception types for network/auth failures
        return _triage_redirect(offset, error=f"Could not reach joy to record this submission: {exc}")

    # Unlike the old live-Postgres path, this can't check ON CONFLICT/dedup
    # or FK validity against spectroscopy_holdings up front -- joy_triage_
    # append.py only validates shape, not against the database. A submitter
    # voting twice on the same identifier, or an identifier that's no longer
    # actually skipped, is only caught at import time now.
    resp = _triage_redirect(
        offset + 1,  # move on to the next record in this visitor's shuffled queue
        note="Submission recorded — it'll be applied to skip_classifications the next time the export job runs.",
    )
    # Persists the submitter name/handle across submissions (prefilled via
    # TRIAGE_SUBMITTER_COOKIE in the triage() route) -- retyping it for every
    # single record was real friction now that a session means many
    # single-record submissions in a row, not one page of 20 filled out once.
    resp.set_cookie(TRIAGE_SUBMITTER_COOKIE, submitter, max_age=TRIAGE_COOKIE_MAX_AGE_SECONDS, samesite="Lax")
    return resp


if __name__ == "__main__":
    # 7860 is the port Hugging Face Spaces' Docker SDK expects apps to
    # listen on; kept as the default locally too so there's one code path.
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, threaded=True, debug=os.environ.get("FLASK_DEBUG") == "1")
