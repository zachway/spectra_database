"""Minimal search webpage for the spectra database — single-star search by
Gaia source_id or name, plus a batch upload for a list of either.

Kept to a single file, no build step, no external JS framework, so it runs
as a plain user on a Linux server with nothing more than `pip install flask`
and the DATABASE_URL env var already used everywhere else in this project.

Run locally:
    DATABASE_URL=postgresql:///spectra_local python3 -m webapp.app
"""

import os

import psycopg
from flask import Flask, render_template_string, request
from pyvo.dal.exceptions import DALServiceError

from ingest.add_star import resolve_gaia_source_id, resolve_stellar_gaia_ids_batch

app = Flask(__name__)

# Source_id lookups are one indexed query regardless of list size — no cap
# needed. Name lookups each cost a SIMBAD round trip (batched, but still),
# so cap the list to keep a single upload from turning into a huge SIMBAD
# query — per project to-do, laptop/small-server scale, not a bulk pipeline.
MAX_NAME_LOOKUPS = 2000


def get_connection() -> psycopg.Connection:
    return psycopg.connect(os.environ["DATABASE_URL"])


PAGE_TEMPLATE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Spectra Database</title>
  <style>
    body { font-family: monospace; max-width: 800px; margin: 2rem auto; padding: 0 1rem; color: #000; background: #fff; }
    dt { font-weight: bold; float: left; width: 140px; clear: left; }
    dd { margin-left: 140px; }
    table { width: 100%; border-collapse: collapse; margin-top: 1rem; }
    th, td { text-align: left; padding: 0.3rem 0.5rem; border-bottom: 1px solid #000; }
    a { color: #000; }
    .error { font-weight: bold; border: 1px solid #000; padding: 0.5rem; }
    .note { font-style: italic; }
    textarea { width: 100%; font-family: monospace; }
    hr { margin: 2rem 0; border: none; border-top: 1px solid #000; }
  </style>
</head>
<body>
  <h1>Spectra Database</h1>
  <form method="get" action="/">
    <input type="text" name="q" placeholder="Gaia source_id or star name, e.g. Proxima Centauri" value="{{ query or '' }}" autofocus>
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
      <dt>RA, Dec</dt><dd>{{ "%.6f"|format(star.ra) }}, {{ "%.6f"|format(star.dec) }}</dd>
      <dt>G mag</dt><dd>{{ star.phot_g_mean_mag if star.phot_g_mean_mag is not none else "—" }}</dd>
      <dt>Gaia RVS</dt><dd>{{ "yes" if star.has_gaia_rvs else "no" }}</dd>
      <dt>Known as</dt><dd>{{ (star.name_aliases | join(", ")) if star.name_aliases else (star.input_name or "—") }}</dd>
    </dl>

    {% if holdings %}
      <table>
        <tr><th>Archive</th><th>Instrument</th><th>Date</th><th>Match</th><th>Link</th></tr>
        {% for h in holdings %}
        <tr>
          <td>{{ h.display_name }}</td>
          <td>{{ h.instrument or "—" }}</td>
          <td>{{ h.obs_date or "—" }}</td>
          <td>{{ h.match_status }}</td>
          <td><a href="{{ h.archive_url }}" target="_blank" rel="noopener">open</a></td>
        </tr>
        {% endfor %}
      </table>
    {% else %}
      <p>No spectroscopy holdings found for this star yet.</p>
    {% endif %}
  {% endif %}

  <hr>

  <h2>Batch lookup</h2>
  <p class="note">Paste or upload a list of Gaia source_ids and/or star names, one per line. Name lookups (anything non-numeric) are capped at {{ max_name_lookups }} per batch; source_id lookups are not.</p>
  <form method="post" action="/batch" enctype="multipart/form-data">
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
        PAGE_TEMPLATE, query=query, star=None, holdings=None,
        error=error, resolved_source_id=resolved_source_id,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
    )


def _blank_batch(batch_error=None, batch_note=None, batch_results=None):
    return render_template_string(
        PAGE_TEMPLATE, query=None, star=None, holdings=None,
        error=None, resolved_source_id=None,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=batch_error, batch_note=batch_note, batch_results=batch_results,
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

    with get_connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute("SELECT * FROM stars WHERE gaia_source_id = %s", (source_id,))
        star = cur.fetchone()
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
            WHERE h.gaia_source_id = %s
            ORDER BY a.display_name, h.obs_date
            """,
            (source_id,),
        )
        holdings = cur.fetchall()

    return render_template_string(
        PAGE_TEMPLATE, query=query, star=star, holdings=holdings,
        error=None, resolved_source_id=resolved_source_id,
        max_name_lookups=MAX_NAME_LOOKUPS,
        batch_error=None, batch_note=None, batch_results=None,
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
        with get_connection() as conn, conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                "SELECT gaia_source_id, name_aliases, input_name FROM stars WHERE gaia_source_id = ANY(%s)",
                (all_source_ids,),
            )
            tracked = {row["gaia_source_id"]: row for row in cur.fetchall()}

            cur.execute(
                """
                SELECT gaia_source_id, COUNT(*) AS n
                FROM spectroscopy_holdings
                WHERE gaia_source_id = ANY(%s)
                GROUP BY gaia_source_id
                """,
                (all_source_ids,),
            )
            holdings_counts = {row["gaia_source_id"]: row["n"] for row in cur.fetchall()}

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
    # 5000 is claimed by macOS's AirPlay Receiver on modern macOS (Monterey+)
    # — hitting it gives a confusing "access denied" instead of ever reaching
    # Flask. 5001 avoids the conflict without touching system settings.
    app.run(host="127.0.0.1", port=5001, debug=True)
