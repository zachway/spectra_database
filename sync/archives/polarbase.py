"""PolarBase (ESPaDOnS/CFHT, Narval/neo-Narval/TBL, SPIRou/CFHT, HARPSpol/ESO 3.6m) -- REST JSON, not TAP.

Petit et al. 2014 PASP database of reduced, normalized Stokes-parameter
spectropolarimetric products -- a different, additive data product from
cfht_cadc.py's raw ESPaDOnS ObsCore rows, not a duplicate.

The registered SSA service (ivo://ov-gso/ssap/polarbase, confirmed live at
both www.polarbase.ovgso.fr/download/ssa_polarbase and the older
polarbase.irap.omp.eu/download/ssa_polarbase -- same backend IP, same
VOTable error response, genuinely the same service behind two domains, not
two independent archives) turned out to be a dead end for a bulk pull: its
own getMetadata response confirms POS is mandatory and SIZE is capped at
5 deg diameter, cone-search only, no full-sky/unfiltered mode. Both
domains' own `/tap` route 200s but is just the SPA's catch-all shell
(confirmed live, same red herring salt_hrs.py found at ssda.saao.ac.za/tap),
not a real TAP endpoint.

The real access path is an undocumented JSON REST API, found by pulling the
SPA's own JS bundle and grepping for "/api/" -- fully documented (once
found) at /api/docs/openapi.yaml, a real Swagger/OpenAPI spec. POST
/api/spectra with an empty body, or a `date` filter, returns real rows
(no POS needed at all) -- but confirmed live to hard-cap at 10,000 records
per response regardless of how few or many actually match, with no
ORDER BY control at all (no sort field in the documented schema). Naively
watermarking on the max date seen per page is unsafe here: a single
unfiltered call already returns dates spanning nearly the archive's entire
2005-2026 range (confirmed live, not chronologically ordered), so jumping
the cursor straight to that page's max would silently abandon every
matching row the 10,000-cap left behind at lower dates that just didn't
happen to be selected. Confirmed live per calendar year: 2007 through 2024
each independently return exactly 10,000 (the cap) when queried alone,
so even yearly windows aren't safe either.

Paginated instead via an adaptive calendar-window walk, same shape as
ing.py/gtc.py's own undocumented-row-cap workaround: walk forward from a
safe pre-archive start date one window at a time; if a window's result
hits the 10,000 cap, halve the window and retry the same start (the result
can't be trusted as complete); once a window comes back under cap, accept
it, advance past it, and grow the window back up for the next span (capped
at MAX_WINDOW_DAYS) so quiet stretches don't cost one request per day.
Confirmed live: a single month (2018-08) returned 3,979 -- well under cap
-- so month-scale windows are the right default size for this archive's
real density.

Confirmed live to cover five real instruments, not just ESPaDOnS/Narval as
expected going in: espadons (CFHT), narval + neo_narval (TBL, Pic du Midi,
NOT covered by any other archive here), spirou (CFHT, near-IR), and
harpspol (ESO 3.6m/HARPS in polarimetric mode). A full walk from
ARCHIVE_START to present (2000-01-01 through 2026-08, 64 windows)
converged to 346,273 distinct real spectra, confirmed live -- far more
than the low-tens-of-thousands a quick, non-exhaustive manual sample
suggested going in, which is exactly the undercount this module's
calendar-window walk (rather than a naive date watermark) exists to avoid.

The API's own join has a real duplicate-row artifact: some (id_observation,
stokes) pairs come back as two byte-for-byte identical records within the
very same response (confirmed live; every field identical between the
pair, not just id/stokes) -- deduped client-side by id_observation alone,
confirmed live that's unique once the exact-duplicate rows are removed.

Real ra/dec (decimal degrees, `alpha`/`delta`) and target name
(`name_simbad`) confirmed live on every sampled row -- normal
identifier-then-position matching applies, not name-only. name_simbad
carries genuine SIMBAD-style padding (e.g. "*  20 CVn", multiple internal
spaces) -- whitespace-collapsed before matching.

reduction_status is hardcoded 'reduced' -- PolarBase serves normalized,
wavelength-calibrated Stokes-parameter science products by design, never
raw detector frames (there is no raw-frame download path on this API at
all).

archive_url points at /api/plot_spectra/{id_observation}, a real,
directly-fetchable per-observation JSON endpoint (confirmed live) serving
the actual spectrum data -- there is no per-observation HTML landing page
in the SPA's own routes to link to instead.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import requests

from sync.base import RawObservation

API_URL = "https://www.polarbase.ovgso.fr/api/spectra"
PLOT_URL = "https://www.polarbase.ovgso.fr/api/plot_spectra/{id_observation}"

INSTRUMENT_DISPLAY = {
    "espadons": "ESPaDOnS",
    "narval": "Narval",
    "neo_narval": "neo-Narval",
    "spirou": "SPIRou",
    "harpspol": "HARPSpol",
}

# Confirmed live -- see module docstring. Safely before the earliest real
# row seen (2005-05-21).
ARCHIVE_START = date(2000, 1, 1)

# Confirmed live hard cap on /api/spectra's response size, regardless of
# how many rows actually match the filter.
RESPONSE_CAP = 10000

INITIAL_WINDOW_DAYS = 30
MAX_WINDOW_DAYS = 365
MIN_WINDOW_DAYS = 1

_TIMESTAMP_FMT = "%Y-%m-%d %H:%M:%S"


def _clean(value) -> str | None:
    if value is None or value == "None" or value == "":
        return None
    return str(value)


def _clean_float(value) -> float | None:
    value = _clean(value)
    return float(value) if value is not None else None


def _run_query(window_start: date, window_end: date) -> list[dict]:
    body = {
        "date": f"{window_start.strftime(_TIMESTAMP_FMT)} .. {window_end.strftime(_TIMESTAMP_FMT)}",
        "type_date": "iso",
    }
    response = requests.post(API_URL, json=body, timeout=(15, 60))
    response.raise_for_status()
    return response.json()["records"]


def _to_records(items: list[dict]) -> list[RawObservation]:
    records = []
    seen_ids: set[str] = set()
    for item in items:
        obs_id = _clean(item.get("id_observation"))
        if obs_id is None or obs_id in seen_ids:
            # Duplicate (id_observation, stokes) rows -- see module
            # docstring; the API's own join re-emits byte-identical rows.
            continue
        seen_ids.add(obs_id)

        raw_date = _clean(item.get("date"))
        obs_date = datetime.strptime(raw_date[:10], "%Y-%m-%d").date() if raw_date else None

        raw_instrument = _clean(item.get("instrument"))
        name = _clean(item.get("name_simbad")) or _clean(item.get("name_object"))

        records.append(
            RawObservation(
                archive_obs_id=obs_id,
                archive_url=PLOT_URL.format(id_observation=obs_id),
                instrument=INSTRUMENT_DISPLAY.get(raw_instrument, raw_instrument),
                obs_date=obs_date,
                program_id=_clean(item.get("run_id")),
                ra=_clean_float(item.get("alpha")),
                dec=_clean_float(item.get("delta")),
                raw_target_name=" ".join(name.split()) if name else None,
                reduction_status="reduced",
            )
        )
    return records


def fetch(cursor: dict) -> tuple[list[RawObservation], dict]:
    window_start = date.fromisoformat(cursor["window_start"]) if cursor.get("window_start") else ARCHIVE_START
    window_days = cursor.get("window_days", INITIAL_WINDOW_DAYS)

    # Wall-clock "today" -- once window_start passes it, there's nothing
    # new to fetch until real time advances, same shape as any other
    # actively-growing archive's watermark reaching the present.
    today = datetime.now(timezone.utc).date()
    if window_start > today:
        return [], cursor

    while True:
        window_end = min(window_start + timedelta(days=window_days), today + timedelta(days=1))
        items = _run_query(window_start, window_end)
        if len(items) >= RESPONSE_CAP and window_days > MIN_WINDOW_DAYS:
            # Can't trust this window as complete -- bisect and retry the
            # same start (see module docstring).
            window_days = max(MIN_WINDOW_DAYS, window_days // 2)
            continue
        break

    records = _to_records(items)

    # Grow the window back up for the next span once comfortably under
    # cap, so quiet stretches don't cost one request per (small) window.
    next_window_days = window_days
    if len(items) < RESPONSE_CAP // 2:
        next_window_days = min(MAX_WINDOW_DAYS, window_days * 2)

    new_cursor = {"window_start": window_end.isoformat(), "window_days": next_window_days}
    return records, new_cursor
