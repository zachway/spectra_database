"""CARMENES via the broader CAHA archive (VIS + NIR) — HTML form POST, no TAP/API.

Closes both remaining CARMENES gaps at once: the NIR channel (never in
DR1, see below) and the broader CAHA archive (carmenes.py only covers the
static 2016-2020 GTO DR1 portal). This module queries the general Calar
Alto Archive instead (caha.sdc.cab.inta-csic.es/calto/), which covers
CARMENES's full operational history, both channels, as ordinary
observations alongside every other CAHA instrument.

Search endpoint found by reading jsp/searchform.jsp's own <form> (POST to
searchform.jsp itself, action-relative) — no TAP, no documented API, a
plain HTML results table (parsed here via BeautifulSoup, same tool
carmenes.py already uses for its own static table). Confirmed live: the
instrument checkbox for CARMENES spectroscopy is `carspe_raw`/`carspe_red`
(found by reading the form's own checkbox `name` attributes — CARMENES is
listed under "3.5m Telescope: Spectroscopy" alongside CAFE/PMAS/TWIN, not
obviously named). Sending only those two (all other instruments' checkbox
fields omitted entirely, which is "unchecked" for an HTML form) scopes the
search to CARMENES alone — confirmed live, every returned row has
Instrument=CARMENES.

DR1's public zip bundles are VIS-only — confirmed live, every single row
of DR1's own table links to a "*_VIS.zip", no NIR counterpart appears
anywhere on that page. This CAHA-wide archive is the only place NIR shows
up: each real exposure appears as *two* rows here (confirmed live, e.g.
CAHA_ID 245832/245833 -- same night, same target, one row's "Grism/
Grating" column reads "vis", the other "nir"). That column is what labels
`instrument` as "CARMENES VIS" vs "CARMENES NIR" below.

29,379 total CARMENES rows confirmed live (query spanning 2010-2026,
CARMENES itself started 2016). Data reaches to 2025-07 as of this
session (2026-07) -- ~1yr behind, consistent with the same proprietary-
period model as CAB's other archives (GTC, Asiago).

Real, confirmed data-quality issue, not a parsing bug: some rows share
identical ra/dec across different OBJECT names within the same date
(e.g. four different HIP-numbered targets on 2016-04-18, all reporting
the same ra/dec) — looks like a display artifact upstream, not real
astrometry. ra/dec are still passed through as the usual raw-position
audit trail, but this is exactly the kind of case the project's
identifier-first matching (see sync.matcher's module docstring) already
exists for: raw_target_name is the trustworthy field here, position isn't.

No native Gaia column -- raw_target_name is set and gaia_source_id left
None, same as every other name-based archive here (eso.py, koa.py, ...).
Unlike carmenes.py's own DR1 module, this does NOT do its own SIMBAD
resolution -- ingest.add_star.discover_stars (called generically from
sync.runner for every archive) already batch-resolves raw_target_name,
so duplicating that here would just be redundant work.

Pagination: the form only exposes a fixed date range + page number
(`pag`) + page size (`result`, max 250) -- no "ID greater than X" filter,
so a value-based watermark like every TAP-based archive here isn't
expressible. Instead this tracks (last_page, last_page_row_count): a page
with exactly PAGE_SIZE rows is "full" (more pages exist), advances to the
next page next run; a page with fewer rows is the current frontier --
re-fetched every run, but only re-processed if its row count changed
(new rows appended there since last time). This relies on the archive
behaving like a stable, append-only, offset-paginated result set
(confirmed live: fetching the same page twice returns the same rows, and
new data lands at increasing CAHA_IDs) -- if that assumption ever breaks,
UNIQUE(archive_code, archive_obs_id) still prevents corruption, just not
staleness. Re-processing an unchanged frontier page returns zero new
records specifically so sync.main's "stop on empty page" driver behaves
correctly, same concern gemini.py's own docstring documents for its
window-based pagination.
"""

from __future__ import annotations

from datetime import date

import requests
from astropy.time import Time
from bs4 import BeautifulSoup

from sync.base import RawObservation

SEARCH_URL = "https://caha.sdc.cab.inta-csic.es/calto/jsp/searchform.jsp"
FETCH_URL = "https://caha.sdc.cab.inta-csic.es/calto/servlet/FetchSci?id={caha_id}&tipe=raw&t=web"

PAGE_SIZE = 250  # the form's own max

# Comfortably before CARMENES's 2016 start -- avoids hunting for the exact bound.
FIRST_DATE = date(2010, 1, 1)


def _fetch_page(page: int) -> list[dict]:
    today = date.today()
    resp = requests.post(
        SEARCH_URL,
        data={
            "objID": "",
            "size": "0.05",
            "dateini_d": str(FIRST_DATE.day), "dateini_m": f"{FIRST_DATE.month:02d}", "dateini_y": str(FIRST_DATE.year),
            "dateend_d": str(today.day), "dateend_m": f"{today.month:02d}", "dateend_y": str(today.year),
            "carspe_raw": "1",
            "carspe_red": "1",
            "result": str(PAGE_SIZE),
            "pag": str(page),
            "order": "obs_group",
            "submit": "Submit Query",
        },
        timeout=60,
    )
    resp.raise_for_status()
    resp.encoding = "iso-8859-1"
    soup = BeautifulSoup(resp.text, "html.parser")

    rows = []
    for tr in soup.find_all("tr"):
        cells = tr.find_all("td", class_="resfield")
        if len(cells) < 15:
            continue
        vals = [c.get_text(strip=True) for c in cells]
        fetch_link = tr.find("a", href=lambda h: h and "FetchSci" in h)
        if fetch_link is None:
            continue
        rows.append(
            {
                "caha_id": vals[0],
                "object": vals[1],
                "ra": vals[2],
                "dec": vals[3],
                "channel": vals[8],
                "obs_date": vals[11],
            }
        )
    return rows


def _to_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    page = cursor.get("last_page", 1)
    last_row_count = cursor.get("last_page_row_count", -1)

    rows = _fetch_page(page)

    if len(rows) == last_row_count:
        # Frontier page unchanged since last run -- nothing new here.
        return [], cursor

    records = []
    for row in rows:
        channel = row["channel"].strip().lower()
        instrument = "CARMENES NIR" if channel == "nir" else "CARMENES VIS"
        records.append(
            RawObservation(
                archive_obs_id=row["caha_id"],
                archive_url=FETCH_URL.format(caha_id=row["caha_id"]),
                instrument=instrument,
                obs_date=Time(row["obs_date"]).to_datetime().date() if row["obs_date"] else None,
                ra=_to_float(row["ra"]),
                dec=_to_float(row["dec"]),
                raw_target_name=row["object"] or None,
            )
        )

    if len(rows) == PAGE_SIZE:
        new_cursor = {"last_page": page + 1, "last_page_row_count": 0}
    else:
        new_cursor = {"last_page": page, "last_page_row_count": len(rows)}

    return records, new_cursor
