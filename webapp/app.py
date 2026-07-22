"""Minimal search webpage for the spectra database — single-star search by
Gaia source_id or name, plus a batch upload for a list of either.

Reads a read-only DuckDB view over a Parquet snapshot instead of a live
Postgres connection — this process has no DATABASE_URL and never writes.
The snapshot is written by scripts.export_to_parquet from the real Postgres
database (wherever that runs) directly into morgan's ~/public_html, which
joy's Apache (mod_userdir) already serves publicly — morgan and joy share
the same NFS home directory, so nothing needs to explicitly sync/publish
anything. This app reads it straight over HTTP via DuckDB's httpfs
extension (SPECTRA_DATA_URL, what the hosted Cloud Run service uses), or
from a local directory (SPECTRA_DATA_DIR) for local dev.

Run locally against a local export:
    python3 -m scripts.export_to_parquet --out-dir ./data
    SPECTRA_DATA_DIR=./data python3 -m webapp.app

Run against the hosted snapshot (what Cloud Run does):
    SPECTRA_DATA_URL=http://joy.chara.gsu.edu/~way/spectra_data python3 -m webapp.app
"""

from __future__ import annotations

import os
from collections import defaultdict

import astropy.units as u
import duckdb
import numpy as np
from astropy.coordinates import SkyCoord
from flask import Flask, render_template_string, request
from pyvo.dal.exceptions import DALServiceError

from ingest.add_star import resolve_gaia_source_id, resolve_stellar_gaia_ids_batch

app = Flask(__name__)

# Source_id lookups are one indexed query regardless of list size — no cap
# needed. Name lookups each cost a SIMBAD round trip (batched, but still),
# so cap the list to keep a single upload from turning into a huge SIMBAD
# query — per project to-do, laptop/small-server scale, not a bulk pipeline.
MAX_NAME_LOOKUPS = 2000

DATA_TABLES = ("stars", "archives", "spectroscopy_holdings", "archive_sync_state", "leaderboard", "cmd_stars")


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
            groups[key] = {"display_name": h["display_name"], "instrument": h["instrument"], "observations": []}
            order.append(key)
        groups[key]["observations"].append(h)
    return [groups[k] for k in order]


def _archive_status() -> list[dict]:
    cur = get_cursor()
    cur.execute(
        """
        SELECT a.display_name, s.last_run_at, s.last_run_status
        FROM archives a
        LEFT JOIN archive_sync_state s ON s.archive_code = a.archive_code
        ORDER BY a.display_name
        """
    )
    return _rows_as_dicts(cur)


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
SKY_SAMPLE_SIZE = 30000

NAV_HTML = """
  <nav class="tabs">
    <a href="/" class="{{ 'active' if active_tab == 'search' else '' }}">Search</a>
    <a href="/cmd" class="{{ 'active' if active_tab == 'cmd' else '' }}">Color-Magnitude Diagram</a>
    <a href="/timeplots" class="{{ 'active' if active_tab == 'timeplots' else '' }}">Leaderboard</a>
    <a href="/stats" class="{{ 'active' if active_tab == 'stats' else '' }}">Stats</a>
    <a href="/info" class="{{ 'active' if active_tab == 'info' else '' }}">More Info</a>
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
    nav.tabs { display: flex; gap: 0; border-bottom: 1px solid #000; margin-bottom: 1.5rem; }
    nav.tabs a { text-decoration: none; padding: 0.5rem 1rem; border: 1px solid #000; border-bottom: none;
                 margin-right: 0.3rem; color: #000; }
    nav.tabs a.active { font-weight: bold; background: #000; color: #fff; }
"""

PAGE_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spectra Database</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <h1>Spectra Database</h1>""" + NAV_HTML + """
  <form method="get" action="">
    <input type="text" name="q" class="search-input" placeholder="Gaia source_id or star name, e.g. Proxima Centauri" value="{{ query or '' }}" autofocus>
    <button type="submit">Search</button>
  </form>
  {% if resolved_source_id %}
    <p>"{{ query }}" resolved via SIMBAD to source_id {{ resolved_source_id }}.</p>
  {% endif %}

  {% if error %}
    <p class="error">Error: {{ error }}</p>
  {% endif %}

  {% if star %}
    <dl>
      <dt>Gaia source_id</dt><dd>{{ star.gaia_source_id }}</dd>
      <dt>SIMBAD</dt><dd><a href="https://simbad.cds.unistra.fr/simbad/sim-id?Ident=Gaia+DR3+{{ star.gaia_source_id }}" target="_blank" rel="noopener">open</a></dd>
      <dt>RA, Dec</dt><dd>{{ "%.6f"|format(star.ra) }}, {{ "%.6f"|format(star.dec) }}</dd>
      <dt>G mag</dt><dd>{{ star.phot_g_mean_mag if star.phot_g_mean_mag is not none else "—" }}</dd>
      <dt>Gaia RVS</dt><dd>{{ "yes" if star.has_gaia_rvs else "no" }}</dd>
      <dt>Gaia XP continuous</dt><dd>{{ "yes" if star.has_xp_continuous else "no" }}</dd>
      <dt>Known as</dt><dd>{{ (star.name_aliases | join(", ")) if star.name_aliases else (star.input_name or "—") }}</dd>
    </dl>

    {% if holdings %}
      {% for g in holdings %}
      <details{% if holdings|length == 1 %} open{% endif %}>
        <summary>{{ g.display_name }} — {{ g.instrument or "—" }} ({{ g.observations|length }} observation{{ "s" if g.observations|length != 1 else "" }})</summary>
        <table>
          <tr><th>Date</th><th>Match</th><th>Method</th><th>Link</th></tr>
          {% for h in g.observations %}
          <tr>
            <td>{{ h.obs_date or "—" }}</td>
            <td>{{ h.match_status }}</td>
            <td>{{ h.match_method }}</td>
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
        <td>{% if r.source_id %}<a href="?q={{ r.source_id }}">{{ r.source_id }}</a>{% else %}—{% endif %}</td>
        <td>{{ r.status }}</td>
        <td>{{ r.known_as or "—" }}</td>
        <td>{{ r.holdings_count if r.holdings_count is not none else "—" }}</td>
      </tr>
      {% endfor %}
    </table>
  {% endif %}

  <hr>
  <h2>Archive status</h2>
  <table>
    <tr><th>Archive</th><th>Last updated</th><th>Status</th></tr>
    {% for a in archive_status %}
    <tr>
      <td>{{ a.display_name }}</td>
      <td>{{ a.last_run_at or "never" }}</td>
      <td>{{ a.last_run_status or "—" }}</td>
    </tr>
    {% endfor %}
  </table>
</body>
</html>
"""


def _blank(query=None, error=None, resolved_source_id=None):
    return render_template_string(
        PAGE_TEMPLATE, query=query, star=None, holdings=None,
        error=error, resolved_source_id=resolved_source_id,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
        archive_status=_archive_status(),
        active_tab="search",
    )


def _blank_batch(batch_error=None, batch_note=None, batch_results=None):
    return render_template_string(
        PAGE_TEMPLATE, query=None, star=None, holdings=None,
        error=None, resolved_source_id=None,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=batch_error, batch_note=batch_note, batch_results=batch_results,
        archive_status=_archive_status(),
        active_tab="search",
    )


@app.route("/")
def search():
    query = request.args.get("q", "").strip()
    if not query:
        return _blank()

    resolved_source_id = None
    if query.isdigit():
        source_id = int(query)
    else:
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

    cur = get_cursor()
    cur.execute("SELECT * FROM stars WHERE gaia_source_id = ?", [source_id])
    rows = _rows_as_dicts(cur)
    star = rows[0] if rows else None
    if star is None:
        return _blank(
            query=query,
            error=f"No tracked star with source_id {source_id}.",
            resolved_source_id=resolved_source_id,
        )

    cur.execute(
        """
        SELECT h.*, a.display_name
        FROM spectroscopy_holdings h
        JOIN archives a ON a.archive_code = h.archive_code
        WHERE h.gaia_source_id = ?
        ORDER BY a.display_name, h.instrument, h.obs_date
        """,
        [source_id],
    )
    holdings = _group_holdings(_rows_as_dicts(cur))

    return render_template_string(
        PAGE_TEMPLATE, query=query, star=star, holdings=holdings,
        error=None, resolved_source_id=resolved_source_id,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
        archive_status=_archive_status(),
        active_tab="search",
    )


CMD_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spectra Database — Color-Magnitude Diagram</title>
  <style>""" + SHARED_STYLE + """
    #cmd-plot { width: 100%; height: 700px; margin-top: 1rem; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <h1>Spectra Database</h1>""" + NAV_HTML + """
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
      Plotly.newPlot('cmd-plot', [{
        x: bpRp,
        y: absGMag,
        text: labels,
        hovertemplate: '%{text}<extra></extra>',
        mode: 'markers',
        type: 'scattergl',
        marker: {
          size: 5, opacity: 0.75, color: bpRp,
          // Explicit, unambiguous stops rather than a named palette +
          // reversescale — low BP-RP (hot/blue stars) -> blue, high
          // BP-RP (cool/red stars) -> red, matching real star color.
          colorscale: [[0, 'blue'], [0.5, '#ccc'], [1, 'red']],
          cmin: -0.5, cmax: 5,
          line: { width: 0.3, color: 'rgba(0,0,0,0.4)' },
        },
      }], {
        xaxis: { title: 'BP - RP (mag)' },
        yaxis: { title: 'Absolute G magnitude', autorange: 'reversed' },
        hovermode: 'closest',
      }, { responsive: true });
      document.getElementById('cmd-plot').on('plotly_click', function(data) {
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
  <title>Spectra Database — Sky Map</title>
  <style>""" + SHARED_STYLE + """
    #sky-plot { width: 100%; height: 700px; margin-top: 1rem; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <h1>Spectra Database</h1>""" + NAV_HTML + """
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
    cur.execute(
        f"""
        SELECT gaia_source_id, ra, dec, phot_g_mean_mag, name_aliases, input_name
        FROM stars
        WHERE ra IS NOT NULL AND dec IS NOT NULL AND phot_g_mean_mag IS NOT NULL
        USING SAMPLE {SKY_SAMPLE_SIZE}
        """
    )
    rows = _rows_as_dicts(cur)
    x, y = _aitoff_project([r["ra"] for r in rows], [r["dec"] for r in rows])
    # Brighter (lower mag) stars drawn bigger, clipped to a sane pixel range.
    sizes = [max(1.5, min(10.0, 12.0 - r["phot_g_mean_mag"])) for r in rows]
    galactic_x, galactic_y = _galactic_plane_xy()
    return render_template_string(
        SKY_TEMPLATE,
        x=x, y=y, sizes=sizes,
        source_ids=[str(r["gaia_source_id"]) for r in rows],
        labels=[_known_as(r) for r in rows],
        galactic_x=galactic_x, galactic_y=galactic_y,
        sample_size=SKY_SAMPLE_SIZE,
        active_tab="sky",
    )


TIMEPLOTS_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spectra Database — Leaderboard</title>
  <style>""" + SHARED_STYLE + """
    #cumulative-plot, #period-plot { width: 100%; height: 500px; margin-top: 1rem; }
  </style>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
</head>
<body>
  <h1>Spectra Database</h1>""" + NAV_HTML + """
  <p class="note">Fixed 6-month periods. At each period, two top-10 lists are computed: the 10 stars with the most cumulative (all-time-so-far) observations, and the 10 with the most observations within that period alone. Every star that ever broke into either list, at any period, gets a line in both charts below — so there can be more than 10 lines total, and a line can start partway through the timeline (whenever that star first qualified) and stop appearing again once it drops out of that period's top 10, rather than dragging a stale line across the whole chart. Only counts holdings with a known observation date — some archives (DESI, SDSS-V) don't report per-observation dates at all, so a star's true total (see the Stats tab) can be higher than what's reflected here. Log scale, so a period with zero observations for a star just leaves a gap rather than a dip to zero.</p>
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
    cur.execute("SELECT gaia_source_id, label, yr, half, within_n, cumulative_n FROM leaderboard ORDER BY gaia_source_id, yr, half")
    rows = _rows_as_dicts(cur)

    period_labels: list[str] = []
    cumulative_traces: list[dict] = []
    period_traces: list[dict] = []

    if rows:
        period_keys = sorted({(r["yr"], r["half"]) for r in rows})
        period_labels = [f"{yr} H{half}" for yr, half in period_keys]

        by_star: dict[int, dict] = defaultdict(dict)
        labels_by_id: dict[int, str] = {}
        for r in rows:
            by_star[r["gaia_source_id"]][(r["yr"], r["half"])] = r
            labels_by_id[r["gaia_source_id"]] = r["label"]

        for gid in sorted(by_star):
            by_period = by_star[gid]
            # Gaia source_ids are 19-digit integers, well past JS's 53-bit
            # safe-integer range — serialized as a string so a click-through
            # can't get silently rounded by the browser (same issue fixed
            # for the CMD/Sky Map click-throughs).
            source_id = str(gid)
            cumulative_traces.append(
                {
                    "label": labels_by_id[gid],
                    "source_id": source_id,
                    "counts": [by_period[k]["cumulative_n"] if k in by_period else None for k in period_keys],
                }
            )
            period_traces.append(
                {
                    "label": labels_by_id[gid],
                    "source_id": source_id,
                    "counts": [by_period[k]["within_n"] if k in by_period else None for k in period_keys],
                }
            )

    return render_template_string(
        TIMEPLOTS_TEMPLATE,
        period_labels=period_labels,
        cumulative_traces=cumulative_traces,
        period_traces=period_traces,
        active_tab="timeplots",
    )


STATS_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spectra Database — Stats</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <h1>Spectra Database</h1>""" + NAV_HTML + """
  <dl>
    <dt>Tracked stars</dt><dd>{{ "{:,}".format(total_stars) }}</dd>
    <dt>Spectroscopy holdings</dt><dd>{{ "{:,}".format(total_holdings) }}</dd>
  </dl>

  <hr>
  <h2>Most observed stars</h2>
  <table>
    <tr><th>Star</th><th>Observations</th></tr>
    {% for r in most_observed %}
    <tr><td><a href="/?q={{ r.gaia_source_id }}">{{ r.known_as }}</a></td><td>{{ r.n }}</td></tr>
    {% endfor %}
  </table>

  <hr>
  <h2>Trending — most observed in the last {{ trending_years }} years</h2>
  {% if trending %}
    <table>
      <tr><th>Star</th><th>Observations</th></tr>
      {% for r in trending %}
      <tr><td><a href="/?q={{ r.gaia_source_id }}">{{ r.known_as }}</a></td><td>{{ r.n }}</td></tr>
      {% endfor %}
    </table>
  {% else %}
    <p class="note">Nothing in the last {{ trending_years }} years yet — most tracked holdings are decades-old archival spectra, and the bulk direct-Gaia-column archives (DESI, SDSS-V) don't carry per-observation dates at all, so "trending" will stay sparse until enough recently-dated archives (ESO, MAST, KOA, NOIRLab) are synced.</p>
  {% endif %}

  <hr>
  <h2>Holdings by archive</h2>
  <table>
    <tr><th>Archive</th><th>Holdings</th></tr>
    {% for r in by_archive %}
    <tr><td>{{ r.display_name }}</td><td>{{ "{:,}".format(r.n) }}</td></tr>
    {% endfor %}
  </table>

  <hr>
  <h2>Matches by method</h2>
  <table>
    <tr><th>Method</th><th>Count</th></tr>
    {% for r in by_method %}
    <tr><td>{{ r.match_method }}</td><td>{{ "{:,}".format(r.n) }}</td></tr>
    {% endfor %}
  </table>

  <hr>
  <h2>Nearest tracked stars</h2>
  <p class="note">By parallax (distance = 1000 / parallax_mas, no error cut applied — treat as approximate).</p>
  <table>
    <tr><th>Star</th><th>Distance (pc)</th></tr>
    {% for r in nearest %}
    <tr><td><a href="/?q={{ r.gaia_source_id }}">{{ r.known_as }}</a></td><td>{{ "%.2f"|format(r.distance_pc) }}</td></tr>
    {% endfor %}
  </table>

  <hr>
  <h2>Fastest movers</h2>
  <p class="note">By total proper motion. For reference, Barnard's Star (the fastest known) moves ~10,358 mas/yr.</p>
  <table>
    <tr><th>Star</th><th>Proper motion (mas/yr)</th></tr>
    {% for r in fastest_movers %}
    <tr><td><a href="/?q={{ r.gaia_source_id }}">{{ r.known_as }}</a></td><td>{{ "%.1f"|format(r.total_pm) }}</td></tr>
    {% endfor %}
  </table>

  <hr>
  <h2>Rough spectral-type distribution</h2>
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

# Natural OBAFGKM order — GROUP BY doesn't preserve it, so the display order
# is applied in Python after querying.
SPECTRAL_BUCKETS = ["O/B (hot)", "A", "F", "G", "K", "M (cool)"]


def _known_as(row: dict) -> str:
    if row.get("name_aliases"):
        return row["name_aliases"][0]
    return row.get("input_name") or str(row["gaia_source_id"])


@app.route("/stats")
def stats():
    cur = get_cursor()

    # stats_summary is precomputed by scripts.export_to_parquet — most-
    # observed, trending, total_holdings, by-archive and by-method all used
    # to be separate live queries here, each scanning some or all of the
    # ever-growing spectroscopy_holdings table on every request. See that
    # module for the full reasoning (same OOM-shaped risk as the
    # Leaderboard, just five smaller scans instead of one huge one).
    cur.execute("SELECT * FROM stats_summary")
    summary = _rows_as_dicts(cur)[0]
    most_observed = summary["most_observed"]
    trending = summary["trending"]
    total_stars = summary["total_stars"]
    total_holdings = summary["total_holdings"]
    by_archive = summary["by_archive"]
    by_method = summary["by_method"]
    trending_years = summary["trending_years"]

    cur.execute(
        """
        SELECT gaia_source_id, input_name, name_aliases, 1000.0 / parallax AS distance_pc
        FROM stars
        WHERE parallax > 0
        ORDER BY parallax DESC
        LIMIT 20
        """
    )
    nearest = _rows_as_dicts(cur)
    for r in nearest:
        r["known_as"] = _known_as(r)

    cur.execute(
        """
        SELECT gaia_source_id, input_name, name_aliases, sqrt(pmra * pmra + pmdec * pmdec) AS total_pm
        FROM stars
        WHERE pmra IS NOT NULL AND pmdec IS NOT NULL
        ORDER BY total_pm DESC
        LIMIT 20
        """
    )
    fastest_movers = _rows_as_dicts(cur)
    for r in fastest_movers:
        r["known_as"] = _known_as(r)

    cur.execute(
        """
        SELECT
            CASE
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.0 THEN 'O/B (hot)'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.3 THEN 'A'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.6 THEN 'F'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 0.9 THEN 'G'
                WHEN phot_bp_mean_mag - phot_rp_mean_mag < 1.5 THEN 'K'
                ELSE 'M (cool)'
            END AS bucket,
            count(*) AS n
        FROM stars
        WHERE phot_bp_mean_mag IS NOT NULL AND phot_rp_mean_mag IS NOT NULL
        GROUP BY bucket
        """
    )
    counts_by_bucket = {r["bucket"]: r["n"] for r in _rows_as_dicts(cur)}
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
        STATS_TEMPLATE,
        most_observed=most_observed, trending=trending, trending_years=trending_years,
        total_stars=total_stars, total_holdings=total_holdings,
        by_archive=by_archive, by_method=by_method,
        nearest=nearest, fastest_movers=fastest_movers, spectral_types=spectral_types,
        active_tab="stats",
    )


INFO_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spectra Database — More Info</title>
  <style>""" + SHARED_STYLE + """</style>
</head>
<body>
  <h1>Spectra Database</h1>""" + NAV_HTML + """
  <h2>How matching works</h2>
  <p>Every archive record goes through up to three match methods, tried in this order, and the first one that succeeds wins:</p>
  <ol>
    <li><b>direct_gaia_column</b> — the archive already reports a Gaia DR3 source_id for the record (e.g. DESI, LAMOST, GALAH, SDSS-V). This is just a lookup against the tracked-star list, not a positional or name match, so it's the most reliable method.</li>
    <li><b>name_resolved</b> — no Gaia column, but the archive's reported target name matches one of a tracked star's cached SIMBAD aliases. Tried <i>before</i> position deliberately: Gaia's single-star astrometric solution can be biased for close visual binaries, which can break a positional match even with otherwise-correct proper motion — an identifier match sidesteps that failure mode entirely.</li>
    <li><b>positional_easy_match</b> — no Gaia column and no name match. The record's reported RA/Dec is checked against tracked stars only (not the full Gaia catalog), each candidate's proper motion propagated to the observation's epoch, within a fixed 1.0 arcsecond radius. Exactly one candidate within radius → matched. More than one → <b>needs_review</b> (ambiguous, gaia_source_id left unassigned). Zero → recorded as <b>skipped</b> (see below) rather than dropped.</li>
  </ol>
  <p class="note">The 1.0" match radius is the same for every archive and instrument. Some instruments have a real, documented systematic offset between their reported pointing and the true catalog position (e.g. finder-camera-derived coordinates) — if that offset ever exceeds 1.0", the record ends up in the skipped queue rather than getting mismatched (the tight radius protects against false positives, at the cost of some real holdings not surfacing automatically).</p>

  <h2>What's likely missing</h2>
  <p>This is a "pointer" database, not a spectra archive — it tracks whether an archive has a spectrum for a star and links to it, not the spectrum data (flux/wavelength arrays) itself. A few concrete, known gaps beyond that:</p>
  <ul>
    <li><b>Archives not yet implemented at all</b>: WEAVE and 4MOST (both not yet public as surveys).</li>
    <li><b>Partial coverage within an implemented archive</b>: MAST only covers HST (JWST hits a server-side timeout on the same query shape, not yet worked around). NOIRLab only covers the SOAR Goodman Spectrograph (several other NOIRLab-hosted spectrographs — CHIRON, echelle, KOSMOS, ARCoIRIS, TripleSpec, COSMOS, SAMI — share the same API but aren't wired up). KOA only covers HIRES (DEIMOS/ESI/LRIS/NIRES aren't yet added). CARMENES only covers the public DR1 GTO portal, not the co-added template library or broader CAHA archive. LBT only covers PEPSI (MODS and LUCI, also spectroscopy-capable, aren't yet added).</li>
    <li><b>Name resolution gaps</b>: not every archive-reported target name resolves to a tracked star via SIMBAD, and it varies a lot by archive — some archives (e.g. NOIRLab) report a much higher fraction of unresolvable names than others, often because the reported name is a survey-internal field ID or calibration marker rather than an actual star name. These records aren't dropped: they're persisted with match_status <b>skipped</b> so they can be manually or crowd-sourced attached to a real Gaia source later. See the Skipped records section below for live, per-archive counts.</li>
    <li><b>Gaia XP continuous spectra</b>: flagged as available per-star (see the "Gaia XP continuous" field on a star's page) but not ingested as data — same lean-pointer tradeoff as everything else here.</li>
    <li><b>SDSS legacy vs. SDSS-V</b>: legacy optical spectroscopy is capped at MJD 58932 (~2020); anything after that boundary lives in the separate SDSS-V optical archive instead, on a different pipeline.</li>
  </ul>

  <p class="note">See the Stats tab for current holdings-by-archive and matches-by-method breakdowns, and the Search page's Archive status footer for when each archive was last synced.</p>

  <h2>Needs-review queue</h2>
  <p class="note">Ambiguous positional matches — 2+ tracked stars fell within the 1.0" radius of the archive's reported position, so no single star was assigned. Most recent {{ needs_review|length }} shown{% if needs_review_total > needs_review|length %} of {{ "{:,}".format(needs_review_total) }} total{% endif %}.</p>
  {% if needs_review %}
    <table>
      <tr><th>Archive</th><th>Reported name</th><th>Reported RA, Dec</th><th>Date</th><th>Best separation</th></tr>
      {% for r in needs_review %}
      <tr>
        <td>{{ r.display_name }}</td>
        <td>{{ r.raw_target_name or "—" }}</td>
        <td>{{ "%.4f, %.4f"|format(r.raw_ra, r.raw_dec) if r.raw_ra is not none and r.raw_dec is not none else "—" }}</td>
        <td>{{ r.obs_date or "—" }}</td>
        <td>{{ '%.2f"'|format(r.theta_arcsec) if r.theta_arcsec is not none else "—" }}</td>
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
      <tr><th>Archive</th><th>Reported name</th><th>Reported RA, Dec</th><th>Date</th></tr>
      {% for r in skipped %}
      <tr>
        <td>{{ r.display_name }}</td>
        <td>{{ r.raw_target_name or "—" }}</td>
        <td>{{ "%.4f, %.4f"|format(r.raw_ra, r.raw_dec) if r.raw_ra is not none and r.raw_dec is not none else "—" }}</td>
        <td>{{ r.obs_date or "—" }}</td>
      </tr>
      {% endfor %}
    </table>
  {% else %}
    <p>None yet.</p>
  {% endif %}
</body>
</html>
"""


@app.route("/info")
def info():
    cur = get_cursor()
    cur.execute("SELECT count(*) FROM spectroscopy_holdings WHERE match_status = 'needs_review'")
    needs_review_total = cur.fetchone()[0]

    cur.execute(
        """
        SELECT a.display_name, h.raw_target_name, h.raw_ra, h.raw_dec, h.obs_date, h.theta_arcsec
        FROM spectroscopy_holdings h
        JOIN archives a ON a.archive_code = h.archive_code
        WHERE h.match_status = 'needs_review'
        ORDER BY h.updated_at DESC
        LIMIT 20
        """
    )
    needs_review = _rows_as_dicts(cur)

    cur.execute(
        """
        SELECT h.archive_code, a.display_name, count(*) AS n
        FROM spectroscopy_holdings h
        JOIN archives a ON a.archive_code = h.archive_code
        WHERE h.match_status = 'skipped'
        GROUP BY h.archive_code, a.display_name
        ORDER BY n DESC
        """
    )
    skipped_by_archive = _rows_as_dicts(cur)

    archive_filter = request.args.get("archive", "").strip()
    if archive_filter:
        cur.execute(
            """
            SELECT a.display_name, h.raw_target_name, h.raw_ra, h.raw_dec, h.obs_date
            FROM spectroscopy_holdings h
            JOIN archives a ON a.archive_code = h.archive_code
            WHERE h.match_status = 'skipped' AND h.archive_code = ?
            ORDER BY h.updated_at DESC
            LIMIT 20
            """,
            [archive_filter],
        )
    else:
        cur.execute(
            """
            SELECT a.display_name, h.raw_target_name, h.raw_ra, h.raw_dec, h.obs_date
            FROM spectroscopy_holdings h
            JOIN archives a ON a.archive_code = h.archive_code
            WHERE h.match_status = 'skipped'
            ORDER BY h.updated_at DESC
            LIMIT 20
            """
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
        cur.execute(
            "SELECT gaia_source_id, name_aliases, input_name FROM stars WHERE list_contains(?, gaia_source_id)",
            [all_source_ids],
        )
        tracked = {row["gaia_source_id"]: row for row in _rows_as_dicts(cur)}

        cur.execute(
            """
            SELECT gaia_source_id, COUNT(*) AS n
            FROM spectroscopy_holdings
            WHERE list_contains(?, gaia_source_id)
            GROUP BY gaia_source_id
            """,
            [all_source_ids],
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

    note = f"{len(entries)} entries looked up."
    if truncated:
        note += f" {truncated} additional name(s) beyond the {MAX_NAME_LOOKUPS} cap were skipped entirely."

    return _blank_batch(batch_error=batch_error, batch_note=note, batch_results=results)


if __name__ == "__main__":
    # 7860 is the port Hugging Face Spaces' Docker SDK expects apps to
    # listen on; kept as the default locally too so there's one code path.
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port, threaded=True, debug=os.environ.get("FLASK_DEBUG") == "1")
